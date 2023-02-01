import io
import os
import stat
import glob
import typing
import functools
import logging
import struct

from .abc import LibraryProviderABC
from ..item import LibraryItem
from ...timer import Timer
from .library_utils.inotify import inotify_init, inotify_add_watch, IN_CREATE, IN_ISDIR, IN_ALL_EVENTS, _EVENT_FMT, _EVENT_SIZE, IN_MOVED_TO

#

L = logging.getLogger(__name__)

#


class FileSystemLibraryProvider(LibraryProviderABC):

	def __init__(self, library, path, *, set_ready=True):
		'''
		`set_ready` can be used to disable/defer `self._set_ready` call.
		'''

		super().__init__(library)
		self.BasePath = os.path.abspath(path)
		while self.BasePath.endswith("/"):
			self.BasePath = self.BasePath[:-1]

		L.info("is connected.", struct_data={'path': path})
		# Filesystem is always ready (or you have a serious problem)
		if set_ready:
			self.App.TaskService.schedule(self._set_ready())

		# Open inotify file descriptor
		self.fd = inotify_init()

		self.App.Loop.add_reader(self.fd, self._on_inotify_read)
		self.AggrTimer = Timer(self.App, self._on_aggr_timer)
		self.AggrEvents = []
		self.WDs = {}


	async def read(self, path: str) -> typing.IO:

		assert path[:1] == '/'
		if path != '/':
			node_path = self.BasePath + path
		else:
			node_path = self.BasePath

		assert '//' not in node_path
		assert node_path[0] == '/'
		assert len(node_path) == 1 or node_path[-1:] != '/'

		try:
			return io.FileIO(node_path, 'rb')

		except FileNotFoundError:
			return None

		except IsADirectoryError:
			return None


	async def list(self, path: str) -> list:
		return self._list(path)

	def _list(self, path: str):

		assert path[:1] == '/'
		if path != '/':
			node_path = self.BasePath + path
		else:
			node_path = self.BasePath

		assert '//' not in node_path
		assert node_path[0] == '/'
		assert len(node_path) == 1 or node_path[-1:] != '/'

		iglobpath = os.path.join(node_path, "*")

		exists = os.access(node_path, os.R_OK) and os.path.isdir(node_path)
		if not exists:
			raise KeyError(" '{}' not found".format(path))

		items = []
		for fname in glob.iglob(iglobpath):

			fstat = os.stat(fname)

			assert fname.startswith(node_path)
			fname = fname[len(node_path) + 1:]

			if stat.S_ISREG(fstat.st_mode):
				ftype = "item"
			elif stat.S_ISDIR(fstat.st_mode):
				ftype = "dir"
			else:
				ftype = "?"

			# Remove any component that starts with '.'
			startswithdot = functools.reduce(lambda x, y: x or y.startswith('.'), fname.split(os.path.sep), False)
			if startswithdot:
				continue

			items.append(LibraryItem(
				name=(path + fname) if path == '/' else (path + '/' + fname),
				type=ftype,
				providers=[self],
			))

		return items


	def _on_inotify_read(self):
		data = os.read(self.fd, 64 * 1024)

		pos = 0
		while pos < len(data):
			wd, mask, cookie, namesize = struct.unpack_from(_EVENT_FMT, data, pos)
			pos += _EVENT_SIZE + namesize
			name = (data[pos - namesize: pos].split(b'\x00', 1)[0]).decode()

			if mask & IN_ISDIR == IN_ISDIR and (mask & IN_CREATE == IN_CREATE or mask & IN_MOVED_TO == IN_MOVED_TO):
				subscribed_path, child_path = self.WDs[wd]
				self._subscribe_recursive(subscribed_path, "/".join([child_path, name]))

			self.AggrEvents.append((wd, mask, cookie, os.fsdecode(name)))

		self.AggrTimer.restart(0.2)


	async def _on_aggr_timer(self):
		to_advertise = set()
		# TODO: race condition?: self.AggrEvents can be modified during this for cycle by _on_inotify_read() method
		# copy self.AggrEvents, clear self.AggrEvents and iterate through a copy?
		for wd, mask, cookie, name in self.AggrEvents:
			subscribed_path, _ = self.WDs.get(wd)
			to_advertise.add(subscribed_path)
		self.AggrEvents.clear()

		for path in to_advertise:
			self.App.PubSub.publish("ASABLibrary.change!", self, path)


	def subscribe(self, path):
		self._subscribe_recursive(path, path)

	def _subscribe_recursive(self, subscribed_path, path_to_be_listed):
		binary = (self.BasePath + path_to_be_listed).encode()
		wd = inotify_add_watch(self.fd, binary, IN_ALL_EVENTS)
		if wd == -1:
			# TODO: -1 means some error - what should happen then?
			return
		self.WDs[wd] = (subscribed_path, path_to_be_listed)

		for item in self._list(path_to_be_listed):
			if item.type == "dir":
				self._subscribe_recursive(subscribed_path, item.name)


	async def finalize(self, app):
		self.App.Loop.remove_reader(self.fd)
		os.close(self.fd)
