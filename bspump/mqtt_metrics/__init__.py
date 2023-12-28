import json
import logging
import re
import time

import asab
import paho.mqtt.client as mqtt
from asab.web.rest.json import JSONDumper

L = logging.getLogger(__name__)


def get_pipeline_topology(pump, pipeline):
    dumper = JSONDumper(pretty=False)
    pipelines = json.loads(dumper(pump.Pipelines))

    pipeline_obj = pump.Pipelines[pipeline]

    pipeline_data = pipelines[pipeline]
    components = []
    metrics_components = []
    components.append(pipeline_data["Sources"][0])
    # get metrics
    for metric in pipeline_data["Metrics"]:
        if (
            metric["type"] == "Counter"
            and metric["name"] == "bspump.pipeline.eps_processor"
        ):
            metrics_components.append(metric)
    for processor in pipeline_data["Processors"][0]:
        components.append(processor)

    output = {
        "topology": {},
        "display-style": "graph",
        "display-priority": "shown",
    }
    for i in range(len(components)):
        component_data = {}

        component_obj = pipeline_obj.locate_source(components[i]["Id"])
        if component_obj is None:
            component_obj = pipeline_obj.locate_processor(components[i]["Id"])

        # If it's not the last element, add the "wires" key
        if i < len(components) - 1:
            component_data["wires"] = [components[i + 1]["Id"]]
        else:
            component_data["wires"] = []

        # Implement getting properties
        component_data["properties"] = {
            key: value for key, value in component_obj.Config.items()
        }
        component_data["capabilities"] = ["subscribable-events"]

        for metric in metrics_components:
            if metric["static_tags"]["processor"] == components[i]["Id"]:
                component_data["metrics"] = {
                    "eps.in": metric["fieldset"][0]["values"]["eps.in"],
                    "eps.out": metric["fieldset"][0]["values"]["eps.out"],
                }
                break
        output["topology"][components[i]["Id"]] = component_data

    # add metrics to the source as it doesn't have any metrics from the metric service
    if "metrics" not in output["topology"][components[0]["Id"]]:
        if "eps.out" in output["topology"][components[1]["Id"]]["metrics"]:
            output["topology"][components[0]["Id"]]["metrics"] = {}
            output["topology"][components[0]["Id"]]["metrics"]["eps.out"] = int(
                output["topology"][components[1]["Id"]]["metrics"]["eps.out"]
            )

    return output


def get_pipelines(pipelines: dict):
    output = {"topology": {}, "display-style": "graph", "display-priority": "hidden"}
    for pipeline in pipelines.values():
        pipeline_dict = {
            "wires": [],
            "properties": [],
            "metrics": [],
            "capabilities": ["has-children"],
        }
        output["topology"][pipeline["Id"]] = pipeline_dict

    return output


def get_message_structure():
    return {
        "timestamp": time.time_ns(),
        "data": {},
        "count": 0,
        "remaining_subscription_count": 0,
    }


class MQTTService(asab.Service):
    def __init__(self, app, service_name="bspump.MQTTService"):
        super().__init__(app, service_name)
        self.App = app
        broker = asab.Config["MQTTMetrics"].get("broker")
        self.host, self.port = broker.split(":")
        self.client = mqtt.Client()
        self.dumper = JSONDumper(pretty=False)

        self.sub_queue = []

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, int(self.port), 60)
        self.client.loop_start()
        self.connected = False
        try:
            self.max_count = int(asab.Config["MQTTMetrics"].get("max_count"))
        except Exception:
            L.warning(
                "MQTTService: The max_count parameter is not set in the configuration file or is not an integer. Default value is 100."
            )
            self.max_count = 100

        self.dumper = JSONDumper(pretty=False)

    def on_message(self, client, userdata, message):
        payload = message.payload.decode("utf-8")
        svc = self.App.get_service("bspump.PumpService")
        topic = message.topic

        # Regex patterns
        pipelines_list_pattern = (
            r"^/c/(?P<deployment_identifier>[^/]+)/topology/subscribe$"
        )
        pipeline_components_pattern = r"^/c/(?P<deployment_identifier>[^/]+)/c/(?P<pipeline_identifier>[^/]+)/topology/subscribe$"
        events_pattern = r"^/c/(?P<deployment_identifier>[^/]+)/c/(?P<pipeline_identifier>[^/]+)/c/(?P<component_identifier>[^/]+)/events/subscribe$"
        metrics_pattern = r"^/c/(?P<deployment_identifier>[^/]+)/c/(?P<pipeline_identifier>[^/]+)/c/(?P<component_identifier>[^/]+)/metrics/subscribe$"

        # Matching
        pipelines_list = re.match(pipelines_list_pattern, topic)
        pipeline_components = re.match(pipeline_components_pattern, topic)
        events = re.match(events_pattern, topic)
        metrics = re.match(metrics_pattern, topic)

        # Get list of pipelines from application
        try:
            payload = json.loads(payload)
        except json.decoder.JSONDecodeError:
            payload = message.payload.decode("utf-8")
            L.warning(
                f"Payload sent to {topic} is not a valid JSON. Payload: {payload}"
            )
            return

        count = payload.get("count")
        if count is None or count < 0:
            return

        count = min(count, self.max_count)
        if pipelines_list:
            new_message = get_message_structure()
            new_message["data"] = get_pipelines(json.loads(self.dumper(svc.Pipelines)))
            new_message["count"] = 1
            new_message["remaining_subscription_count"] = count - 1
            client.publish(
                f"/c/{self.App.DeploymentId}/topology", json.dumps(new_message)
            )

        # Get components of one pipeline
        if pipeline_components:
            pipeline = pipeline_components.group("pipeline_identifier")
            new_message = get_message_structure()

            new_message["data"] = get_pipeline_topology(svc, pipeline)
            new_message["count"] = 1
            new_message["remaining_subscription_count"] = count - 1
            client.publish(
                f"/c/{self.App.DeploymentId}/c/{pipeline}/topology",
                json.dumps(new_message),
            )

        if events:
            pipeline = events.group("pipeline_identifier")
            processor = events.group("component_identifier")
            pipeline = svc.locate(f"{pipeline}")

            source = pipeline.locate_source(f"{processor}")
            if source is not None:
                source.EventsToPublish = count
            else:
                L.info(f"adding {processor} to {pipeline.Id}")
                pipeline.PublishingProcessors[processor] = max(
                    count, pipeline.PublishingProcessors[processor]
                )

        if metrics:
            pipeline = metrics.group("pipeline_identifier")
            processor = metrics.group("component_identifier")
            pipeline = svc.locate(f"{pipeline}")

            source = pipeline.locate_source(f"{processor}")
            if source is not None:
                source.MetricsToPublish = count
            else:
                pipeline.MetricsProcessors[processor]["count"] = max(
                    count, pipeline.MetricsProcessors[processor]["count"]
                )

    # Callback when connected to the MQTT broker
    def on_connect(self, client, userdata, flags, rc):
        self.apply_subscriptions()
        self.connected = True

    def apply_subscriptions(self):
        self.client.subscribe(f"/c/{self.App.DeploymentId}/topology/subscribe")

        for sub in self.sub_queue:
            self.client.subscribe(f"/c/{self.App.DeploymentId}/{sub}")

    def add_pipeline(self, pipeline):
        self.sub_queue.append(f"c/{pipeline}/topology/subscribe")
        if self.connected:
            self.apply_subscriptions()

    def subscribe(self, pipeline, component):
        self.sub_queue.append(f"c/{pipeline}/c/{component}/events/subscribe")
        self.sub_queue.append(f"c/{pipeline}/c/{component}/metrics/subscribe")
        if self.connected:
            self.apply_subscriptions()

    def publish_event(self, pipeline, component, event, count_remaining):
        data = get_message_structure()
        if isinstance(event, bytes):
            event = event.decode("utf-8")
        data["data"] = event
        data["count"] = component.EventCount
        data["remaining_subscription_count"] = count_remaining
        self.client.publish(
            f"/c/{self.App.DeploymentId}/c/{pipeline}/c/{component.Id}/events",
            json.dumps(data),
        )

    def publish_metrics(self, pipeline, processor_id, metrics_data, count_remaining):
        # test metrics
        data = {}
        data["data"] = metrics_data
        data["remaining_subscription_count"] = count_remaining
        self.client.publish(
            f"/c/{self.App.DeploymentId}/c/{pipeline}/c/{processor_id}/metrics",
            json.dumps(data),
        )
