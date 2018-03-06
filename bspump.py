#!/usr/bin/env python3
import asyncio
import asab
import bspump
import bspump.socket
import bspump.common


class SamplePipeline(bspump.Pipeline):

	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		self.construct(
			bspump.socket.TCPStreamSource(app, self),
			bspump.common.JSON2DictProcessor(app, self),
			bspump.common.PPrintSink(app, self)
		)


if __name__ == '__main__':
	app = bspump.BSPumpApplication()
	svc = app.get_service("bspump.PumpService")

	# Construct and register Pipeline
	pl = SamplePipeline(app, 'mypipeline')
	svc.add_pipeline(pl)

	app.run()
