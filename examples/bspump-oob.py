#!/usr/bin/env python3
import logging

import aiohttp
import requests
import time

import asab
import asab.proactor
import bspump
import bspump.common
import bspump.http
import bspump.trigger

###

L = logging.getLogger(__name__)


###


class SampleOOBGenerator(bspump.Generator):
	"""
	Generator processes originally synchronous events "out-of-band" e.g. out of the synchronous
	processing within the pipeline.

	Specific implementation of Generator should implement the generate method to process events
	while performing long running (asynchronous) tasks such as HTTP requests.
	The long running tasks may enrich events with relevant information, such as output of external calculations.

	"""

	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)

		app.add_module(asab.proactor.Module)
		self.ProactorService = app.get_service("asab.ProactorService")

	async def generate(self, context, event, depth):
		# Run asynchronous heavy task
		L.debug("Running long operation asynchronously and waiting for the result...")
		async with aiohttp.ClientSession() as session:
			async with session.get("https://reqres.in/api/{}/2".format(event.get("description", "unknown"))) as resp:
				if resp.status != 200:
					return event
				color = await resp.json()
				event["color"] = color

		# Run synchronous heavy task on thread
		L.debug("Running long operation on thread and waiting for the result...")
		event = await self.ProactorService.execute(
			self.process_on_thread,
			context,
			event
		)

		await self.Pipeline.inject(context, event, depth)

	def process_on_thread(self, context, event):
		r = requests.get("https://reqres.in/api/{}/4".format(event.get("description", "unknown")))
		event["second_color"] = r.json()
		event["time"] = time.time()
		return event


class SamplePipeline(bspump.Pipeline):

	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		self.build(
			bspump.http.HTTPClientSource(app, self, config={
				'url': 'https://api.coindesk.com/v1/bpi/currentprice.json'
			}).on(bspump.trigger.PeriodicTrigger(app, 1)),
			bspump.common.BytesToStringParser(app, self),
			bspump.common.JsonToDictParser(app, self),
			SampleOOBGenerator(app, self),
			bspump.common.PPrintSink(app, self),
		)


if __name__ == '__main__':
	app = bspump.BSPumpApplication()

	svc = app.get_service("bspump.PumpService")

	sample_pipeline = SamplePipeline(app, 'SamplePipeline')
	svc.add_pipeline(sample_pipeline)

	app.run()
