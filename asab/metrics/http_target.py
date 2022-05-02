import logging
import aiohttp

import asab

#

L = logging.getLogger(__name__)

#


class HTTPTarget(asab.ConfigObject):
	def __init__(self, svc, config_section_name, config=None):
		super().__init__(config_section_name, config)
		self.URL = self.Config.get('url')


	async def process(self, mlist):
		async with aiohttp.ClientSession() as session:
			async with session.post(self.URL, json=mlist) as resp:
				response = await resp.text()
				if resp.status != 200:
					L.warning("Error when sending metrics by HTTPTarget: {}\n{}".format(resp.status, response))
