import re
import abc
import socket
import logging
import asyncio

import aiohttp
from bspump.asab import Service, Config, Configurable
import dataclasses

#

L = logging.getLogger(__name__)

#


@dataclasses.dataclass
class Alert:
        source: str
        alert_cls: str
        alert_id: str
        title: str
        data: dict
        detail: str = ""
        exception: Exception = None


class AlertProviderABC(Configurable, abc.ABC):
    ConfigDefaults = {}

    def __init__(self, config_section_name):
        super().__init__(config_section_name=config_section_name)

    async def initialize(self, app):
        pass

    async def finalize(self, app):
        pass

    @abc.abstractmethod
    def trigger(self, alert: Alert):
        pass


class AlertAsyncProviderABC(AlertProviderABC):

    def __init__(self, config_section_name):
        super().__init__(config_section_name=config_section_name)
        self.Queue = asyncio.Queue()
        self.MainTask = None


    async def initialize(self, app):
        self._start_main_task()

    async def finalize(self, app):
        if self.MainTask is not None:
            mt = self.MainTask
            self.MainTask = None
            mt.cancel()

    def trigger(self, alert: Alert):
        self.Queue.put_nowait(alert)

    def _start_main_task(self):
        assert self.MainTask is None
        self.MainTask = asyncio.ensure_future(self._main())
        self.MainTask.add_done_callback(self._main_done)

    def _main_done(self, x):
        if self.MainTask is None:
            return

        self.MainTask.result()

        self.MainTask = None
        self._start_main_task()

    @abc.abstractmethod
    async def _main(self):
        pass


class AlertHTTPProviderABC(AlertAsyncProviderABC):
    ConfigDefaults = {
        "url": "",
    }

    def __init__(self, config_section_name):
        super().__init__(config_section_name=config_section_name)
        self.URL = self.Config["url"]


class OpsGenieAlertProvider(AlertHTTPProviderABC):
    ConfigDefaults = {
        # US: https://api.opsgenie.com
        # EU: https://api.eu.opsgenie.com
        "url": "https://api.eu.opsgenie.com",
        # See https://docs.opsgenie.com/docs/authentication
        # E.g. `eb243592-faa2-4ba2-a551q-1afdf565c889`
        "api_key": "",
        # Coma separated tags to be added to the request
        "tags": "",
    }

    def __init__(self, config_section_name, *args):
        super().__init__(config_section_name=config_section_name)
        self.APIKey = self.Config["api_key"]
        self.Tags = re.split(r"[,\s]+", self.Config["tags"], re.MULTILINE)
        self.Hostname = socket.gethostname()

    async def _main(self):
        while True:
            a = await self.Queue.get()

            headers = {"Authorization": "GenieKey {}".format(self.APIKey)}

            create_alert = {
                "message": a.title,
                "note": a.detail,
                "alias": "{}:{}:{}".format(a.source, a.alert_cls, a.alert_id),
                "tags": self.Tags,
                "details": {
                    "source": a.source,
                    "class": a.alert_cls,
                    "id": a.alert_id,
                },
                "entity": a.source,
                "source": self.Hostname,
            }

            if a.data:
                create_alert["details"].update(a.data)

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    self.URL + "/v2/alerts", json=create_alert
                ) as resp:
                    if resp.status != 202:
                        text = await resp.text()
                        L.warning(
                            "Failed to create the alert ({}):\n'{}'".format(
                                resp.status, text
                            )
                        )
                    else:
                        await resp.text()


class PagerDutyAlertProvider(AlertHTTPProviderABC):
    ConfigDefaults = {
        "url": "https://events.pagerduty.com",
        # Your api key generated by PagerDuty
        "api_key": "",
        # Integration key (or routing_key) from a Service directory
        # Choose "Use our API directly" and "Events API v2"
        "integration_key": "",
    }

    def __init__(self, config_section_name, *args, **kwargs):
        super().__init__(config_section_name=config_section_name)
        self.APIKey = self.Config["api_key"]
        self.IntegrationKey = self.Config["integration_key"]

    async def _main(self):
        while True:
            a = await self.Queue.get()

            headers = {"Authorization": "Token token={}".format(self.APIKey)}

            create_alert = {
                "event_action": "trigger",
                "routing_key": self.IntegrationKey,
                "dedup_key": "{}:{}:{}".format(a.source, a.alert_cls, a.alert_id),
                "client": "Asab Alert Service",
                "payload": {
                    "summary": a.title,
                    "severity": "warning",
                    "source": a.source,
                    "group": a.alert_cls,
                    "custom_details": {
                        "source": a.source,
                        "class": a.alert_cls,
                        "id": a.alert_id,
                    },
                },
            }

            if a.data:
                create_alert["payload"]["custom_details"].update(a.data)

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    self.URL + "/v2/enqueue", json=create_alert
                ) as resp:
                    if resp.status != 202:
                        text = await resp.text()
                        L.warning(
                            "Failed to create the alert ({}):\n'{}'".format(
                                resp.status, text
                            )
                        )
                    else:
                        await resp.text()


class SentryAlertProvider(AlertAsyncProviderABC):
    def __init__(self, config_section_name="", application=None):
        super().__init__(config_section_name=config_section_name)
        import bspump.asab.sentry
        self.SentryService = bspump.asab.sentry.SentryService(application)

    async def _main(self):
        while True:
            a = await self.Queue.get()
            self.SentryService.set_tags({"source": a.source, "class": a.alert_cls, "id": a.alert_id})
            self.SentryService.capture_exception(a.exception)

    async def finalize(self, app):
        await super().finalize(app)


class AlertService(Service):
    def __init__(self, app, service_name="asab.AlertService"):
        super().__init__(app, service_name)
        self.Providers = []

        for section in Config.sections():
            if not section.startswith("asab:alert:"):
                continue

            provider_cls = {
                "asab:alert:opsgenie": OpsGenieAlertProvider,
                "asab:alert:pagerduty": PagerDutyAlertProvider,
                "asab:alert:sentry": SentryAlertProvider,
            }.get(section)
            if provider_cls is None:
                L.warning("Unknwn alert provider: {}".format(section))
                continue

            self.Providers.append(provider_cls(config_section_name=section,application=app))

    async def initialize(self, app):
        await asyncio.gather(*[p.initialize(app) for p in self.Providers])

    async def finalize(self, app):
        await asyncio.gather(*[p.finalize(app) for p in self.Providers])

    def trigger(
        self,
        alert: Alert,
    ):
        for p in self.Providers:
            p.trigger(alert)