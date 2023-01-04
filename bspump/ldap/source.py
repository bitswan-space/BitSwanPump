import logging
import ldap
import ldap.controls

from ..abc.source import TriggerSource

#

L = logging.getLogger(__name__)

#

class LDAPSource(TriggerSource):

	ConfigDefaults = {
		"base": "dc=example,dc=org",
		"filter": "(&(objectClass=inetOrgPerson)(cn=*))",
		"attributes": "sAMAccountName cn createTimestamp modifyTimestamp UserAccountControl email",
		"results_per_page": 1000,
		"attribute_encoding": "utf-8",
	}

	def __init__(self, app, pipeline, connection, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)
		self.Connection = pipeline.locate_connection(app, connection)
		self.ProactorService = app.get_service("asab.ProactorService")
		self.Scope = ldap.SCOPE_SUBTREE
		self.Base = self.Config.get("base")
		self.Filter = self.Config.get("filter")
		self.Attributes = self.Config.get("attributes").split(" ")
		self.AttributeEncoding = self.Config.get("attribute_encoding")
		self.ResultsPerPage = self.Config.getint("results_per_page")

	async def cycle(self):
		# TODO: Throttling
		await self.Pipeline.ready()
		cookie = b""
		while True:
			page, cookie = await self.ProactorService.execute(
				self._search_worker, cookie)
			for entry in page:
				await self.process(entry, context={})
			if cookie is None or len(cookie) == 0:
				break

	def _search_worker(self, cookie=b""):
		page = []
		with self.Connection.ldap_client() as client:
			paged_results_control = ldap.controls.SimplePagedResultsControl(
				True,
				size=self.ResultsPerPage,
				cookie=cookie
			)
			msgid = client.search_ext(
				base=self.Base,
				scope=self.Scope,
				filterstr=self.Filter,
				attrlist=self.Attributes,
				serverctrls=[paged_results_control],
			)
			res_type, res_data, res_msgid, serverctrls = client.result3(msgid)
			for dn, attrs in res_data:
				if dn is None:
					# Skip system entries
					continue

				event = {"dn": dn}
				# LDAP returns all attributes as lists of bytestrings, e.g.:
				#   {"sAMAccountName": [b"vhavel"], ...}
				# Unpack and decode them
				for k, v in attrs.items():
					if isinstance(v, list):
						if len(v) < 1:
							continue
						elif len(v) == 1:
							v = v[0].decode(self.AttributeEncoding)
						else:
							v = [item.decode(self.AttributeEncoding) for item in v]
					event[k] = v
				page.append(event)

			for sc in serverctrls:
				if sc.controlType == ldap.controls.SimplePagedResultsControl.controlType:
					cookie = sc.cookie
					break
			else:
				L.error("Server ignores RFC 2696 control: No serverctrls in result")
				cookie = b""

			return page, cookie
