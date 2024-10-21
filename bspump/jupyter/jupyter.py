import asyncio
from dataclasses import dataclass
from functools import partial
import bspump
import os
from typing import Any, Callable, List

# Define globals if not define already
if "bitswan_auto_pipeline" not in globals():
    __bitswan_processors = []
    __bitswan_pipelines = {}
    __bitswan_current_pipeline = None
    __bitswan_dev_runtime = None
    __bitswan_connections = []
    __bitswan_lookups = []
    _bitswan_app_post_inits = []
    bitswan_auto_pipeline = {}
    __bs_step_locals = {}
    bitswan_test_mode = []
    __bitswan_autopipeline_count = 1
    bitswan_test_probes = {}
    bitswan_tested_pipelines = set()


class DevPipeline(bspump.Pipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DevApp(bspump.BSPumpApplication):
    def __init__(self, args=[]):
        super().__init__(args=args)


class DevRuntime:
    def __init__(self, args=[]):
        self.events: list[tuple[str, list[Any]]] = []
        self.dev_app = DevApp(args)

    def clear(self, name: str, event: list[Any]) -> None:
        self.events = [(name, event)]

    def get_prev_events(self, name) -> tuple[list[tuple[str, list[Any]]], list[Any]]:
        new_eventss = []
        prev_events = []
        for event in self.events:
            if event[0] == name:
                break
            prev_events = event[1]
            new_eventss.append(event)
        return new_eventss, prev_events

    def step(self, name: str, func) -> None:
        new_eventss, prev_events = self.get_prev_events(name)
        new_events = [func(prev_event) for prev_event in prev_events]
        [print(event) for event in new_events]
        new_eventss.append((name, new_events))
        self.events = new_eventss

    async def async_step(self, name: str, func) -> None:
        new_eventss, prev_events = self.get_prev_events(name)
        new_events = []

        async def inject(event):
            new_events.append(event)

        [await func(inject, prev_event) for prev_event in prev_events]
        [print(event) for event in new_events]
        new_eventss.append((name, new_events))
        self.events = new_eventss


def auto_pipeline(source=None, sink=None):
    if source is None:
        raise Exception(
            "When calling auto_pipeline must specify a function that returns a source."
        )
    if sink is None:
        raise Exception(
            "When calling auto_pipeline must specify a function that returns a sink."
        )

    global __bitswan_autopipeline_count
    new_pipeline(f"auto_pipeline_{__bitswan_autopipeline_count}")
    __bitswan_autopipeline_count += 1

    import inspect

    frame = inspect.currentframe()
    register_source(source, test_events=frame.f_back.f_locals.get("test_events"))
    bitswan_auto_pipeline["sink"] = sink


async def test_devruntime():
    test_runtime = DevRuntime()

    def test_func(prev_event):
        return prev_event + 1

    tr = test_runtime
    tr.clear("sample", [1, 2, 3])
    tr.step("step1", test_func)
    tr.step("step2", test_func)
    tr.step("step3", test_func)
    assert tr.events == [
        ("sample", [1, 2, 3]),
        ("step1", [2, 3, 4]),
        ("step2", [3, 4, 5]),
        ("step3", [4, 5, 6]),
    ]

    def test_func1(prev_event):
        return prev_event + 7

    tr.step("step2", test_func1)
    assert tr.events == [
        ("sample", [1, 2, 3]),
        ("step1", [2, 3, 4]),
        ("step2", [9, 10, 11]),
    ]

    async def test_func2(injector, prev_event):
        if prev_event % 2 == 0:
            injector(prev_event + 1)

    await tr.async_step("step3", test_func2)
    assert tr.events == [
        ("sample", [1, 2, 3]),
        ("step1", [2, 3, 4]),
        ("step2", [9, 10, 11]),
        ("step3", [11]),
    ]


def is_running_in_jupyter():
    try:
        from IPython import get_ipython

        # Check if the IPython kernel is running which is a strong indication of a Jupyter environment
        if "IPKernelApp" in get_ipython().config:
            return True
        if (
            "VSCODE_PID" in os.environ
        ):  # Check for Visual Studio Code's Jupyter extension
            return True
        return False
    except Exception:
        return False


__bitswan_dev = is_running_in_jupyter()


def ensure_bitswan_runtime(func):
    def wrapper(*args, **kwargs):
        global __bitswan_dev_runtime
        if not __bitswan_dev_runtime:
            __bitswan_dev_runtime = DevRuntime()

        return func(*args, **kwargs)

    return wrapper


def init_bitswan_jupyter(config_path: str = None):
    """Helper function to initialize Bitswan in Jupyter environment,
    needs to be called before any other Bitswan function.
    Does not need to be called if config file is not needed

    Args:
        config_path (str, optional): Path to the config file. Defaults to None.
    """
    global __bitswan_dev_runtime

    args = ["-c", config_path] if config_path else []
    __bitswan_dev_runtime = DevRuntime(args)


def add_test_probe(name):
    global bitswan_test_mode
    global bitswan_test_probes
    if bitswan_test_mode:
        import inspect

        frame = inspect.currentframe()
        try:
            probe, expected = bitswan_test_probes.get(name, (None, None))
            if probe is not None:
                print(f"    │ Probing {name}.")
                probed = probe(frame.f_back.f_locals, frame.f_back.f_globals)
                if not probed == expected:
                    print(
                        "    └ \033[91m"
                        + f"Probe {name} failed. Got {probed} expected {expected}."
                        + "\033[0m"
                    )
                    exit(1)
        finally:
            del frame


@ensure_bitswan_runtime
def sample_events(events: List[Any]):
    """Inject sample events into the current pipeline for testing

    Args:
        events (List[Any]): List of events to be injected
    """
    global __bitswan_dev_runtime
    __bitswan_dev_runtime.clear("__sample", events)


def register_app_post_init(func):
    """
    Ex:
    @register_app_post_init
    def post_init(app):
        app.PubSub.subscribe("Application.tick!", app.tick)
    """
    global _bitswan_app_post_inits
    _bitswan_app_post_inits.append(func)


@ensure_bitswan_runtime
def register_connection(func):
    """
    Ex:
    @register_connection
    def connection(app):
        return bspump.kafka.KafkaConnection(app, "KafkaConnection")
    """
    global __bitswan_connections
    global __bitswan_dev_runtime
    __bitswan_connections.append(func)
    if __bitswan_dev:
        global __bitswan_dev_runtime
        connection = func(__bitswan_dev_runtime.dev_app)
        __bitswan_dev_runtime.dev_app.PumpService.add_connection(connection)


async def retrieve_sample_events(limit: int = 10) -> None:
    """Get sample events from the source registered to the current pipeline and register them for testing,
    has to be awaited in Jupyter environment

    Ex:
        await retrieve_sample_events(100)

    Args:
        limit (int, optional): Number of events to retrieve. Defaults to 10.

    Returns:
        None


    """
    global __bitswan_dev_runtime
    # Capture the current state of __bitswan_processors
    current_processors = list(__bitswan_processors)

    class TmpPipeline(bspump.Pipeline):
        def __init__(self, app, pipeline_id):
            super().__init__(app, pipeline_id)
            processors = []
            for processor in current_processors:
                instance = processor(app, self)
                processors.append(instance)
            self.build(*processors)
            self.events = []

        def inject(self, context, event, depth):
            if len(self.events) >= limit:
                return
            print(event)
            self.events.append(event)

        def get_events(self):
            print(f"Collected {len(self.events)} events")
            return self.events

    pipeline = TmpPipeline(__bitswan_dev_runtime.dev_app, __bitswan_current_pipeline)
    pipeline.start()

    try:
        while 1:
            if len(pipeline.events) >= limit:
                await pipeline.stop()
                break
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        await pipeline.stop()

    __bitswan_dev_runtime.clear("__sample", pipeline.get_events())
    return


@ensure_bitswan_runtime
def register_lookup(func):
    """
    Ex:
    @register_lookup
    def lookup(app):
      return ENodeBLookup(app, id='ENodeBLookup')
    """
    global __bitswan_lookups
    __bitswan_lookups.append(func)
    if __bitswan_dev:
        global __bitswan_dev_runtime
        lookup = func(__bitswan_dev_runtime.dev_app)
        __bitswan_dev_runtime.dev_app.PumpService.add_lookup(lookup)


def new_pipeline(name: str):
    """Creates and registers a new pipeline

    Args:
        name (str): Name of the pipeline
    """
    global __bitswan_processors
    global __bitswan_current_pipeline
    __bitswan_processors = []
    __bitswan_current_pipeline = name


def end_pipeline():
    """Ends the current pipeline and appends it to the list of pipelines"""
    global __bitswan_current_pipeline
    global __bitswan_processors
    global __bitswan_pipelines
    global __bitswan_dev

    if __bitswan_dev:
        return

    # Capture the current state of __bitswan_processors
    current_processors = list(__bitswan_processors)

    class Pipeline(bspump.Pipeline):
        def __init__(self, app, pipeline_id):
            super().__init__(app, pipeline_id)
            processors = []
            for processor in current_processors:
                instance = processor(app, self)
                processors.append(instance)
            self.build(*processors)

    # Append the new Pipeline class
    __bitswan_pipelines[__bitswan_current_pipeline] = Pipeline


def register_source(func, test_events=None):
    """
    Ex:
    @register_source
    def source(app, pipeline):
        return bspump.socket.TCPStreamSource(app, pipeline)
    """
    if test_events is None:
        import inspect

        frame = inspect.currentframe()
        test_events = frame.f_back.f_locals.get("test_events", {})
    global __bitswan_processors
    global bitswan_test_mode
    if bitswan_test_mode:
        import bspump.test

        def test_source(app, pipeline):
            return bspump.test.TestSource(app, pipeline, test_events=test_events)

        __bitswan_processors.append(test_source)
    else:
        __bitswan_processors.append(func)


@ensure_bitswan_runtime
def register_processor(func):
    """
    Ex:
    @register_processor
    def processor(app, pipeline):
        return bspump.socket.TCPStreamProcessor(app, pipeline)
    """
    global __bitswan_processors
    global __bitswan_dev
    global __bitswan_dev_runtime
    global __bitswan_current_pipeline
    if not __bitswan_dev:
        __bitswan_processors.append(func)
    else:
        pipeline = DevPipeline(
            app=__bitswan_dev_runtime.dev_app, id=__bitswan_current_pipeline
        )
        processor = func(__bitswan_dev_runtime.dev_app, pipeline)
        callable_process = partial(processor.process, processor)
        __bitswan_dev_runtime.step(func.__name__, callable_process)


@ensure_bitswan_runtime
def register_generator(func):
    """
    Ex:
    @register_generator
    def generator(app, pipeline):
        return MyGeneratorClass(app, pipeline)
    """

    async def _fn():
        global __bitswan_processors
        global __bitswan_dev
        global __bitswan_dev_runtime
        global __bitswan_current_pipeline
        if not __bitswan_dev:
            # TODO: check this
            __bitswan_processors.append(func)
        else:
            pipeline = DevPipeline(
                __bitswan_dev_runtime.dev_app, __bitswan_current_pipeline
            )
            generator = func(__bitswan_dev_runtime.dev_app, pipeline)

            async def asfunc(inject, event):
                async def super_inject(context, event, depth):
                    return await inject(event)

                pipeline.inject = super_inject
                return await generator.generate(None, event, 0)

            await __bitswan_dev_runtime.async_step(func.__name__, asfunc)

        return func

    def wrapper(*args, **kwargs):
        import asyncio
        import nest_asyncio

        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(_fn())
        loop.run_until_complete(task)

    wrapper()
    return _fn


def register_sink(func):
    """
    Ex:
    @register_sink
    def sink(app, pipeline):
        return bspump.socket.TCPStreamSink(app, pipeline)
    """
    global __bitswan_processors
    global bitswan_test_mode
    if bitswan_test_mode:
        import bspump.test

        def test_sink(app, pipeline):
            return bspump.test.TestSink(app, pipeline)

        __bitswan_processors.append(test_sink)
    else:
        __bitswan_processors.append(func)


def snake_to_camel_case(name):
    """Convert snake_case name to CamelCase."""
    return "".join(word.capitalize() for word in name.split("_"))


@ensure_bitswan_runtime
def step(func: Callable[[Any], Any]):
    """Decorator that registers a new processor with the given function

    Ex:
        @step
        def my_processor(event):
            return {"processed": event}

    Args:
        func (Callable[[Any], Any]): Function to be registered as a processor
    """
    global __bitswan_processors
    global __bitswan_dev
    global __bitswan_dev_runtime
    if not __bitswan_dev:
        # Convert function name from snake case to CamelCase and create a unique class name
        class_name = snake_to_camel_case(func.__name__) + "Processor"

        # Dynamically create a new Processor class with the custom class name
        CustomProcessor = type(
            class_name,
            (bspump.Processor,),
            {"process": lambda self, context, event: func(event)},
        )

        # Append the new Processor to the __bitswan_processors list
        __bitswan_processors.append(CustomProcessor)
    else:
        __bitswan_dev_runtime.step(func.__name__, func)

    # Return the original function unmodified
    return func


def async_step(func):
    # Convert function name from snake case to CamelCase and create a unique class name
    global __bitswan_dev
    class_name = snake_to_camel_case(func.__name__) + "Generator"

    # Dynamically create a new Generator class with the custom class name
    async def _generate(self, context, event, depth):
        async def injector(event):
            # TODO: do better
            if __bitswan_dev:
                return await self.Pipeline.inject(context, event, depth)
            else:
                return self.Pipeline.inject(context, event, depth)

        return await func(injector, event)

    CustomGenerator = type(
        class_name,
        (bspump.Generator,),
        # Async generate function calls func with injector and event. The injector is taken from the pipeline.
        {"generate": _generate},
    )

    def generator(app, pipeline):
        return CustomGenerator(app, pipeline)

    generator.__name__ = func.__name__
    register_generator(generator)


def _init_pipelines(app, service):
    global __bitswan_pipelines
    for name, pipeline in __bitswan_pipelines.items():
        service.add_pipeline(pipeline(app, name))


def _init_connections(app, service):
    global __bitswan_connections
    for connection in __bitswan_connections:
        service.add_connection(connection(app))


def _init_lookups(app, service):
    global __bitswan_lookups
    for lookup in __bitswan_lookups:
        service.add_lookup(lookup(app))


class App(bspump.BSPumpApplication):
    def init_componets(self):
        global _bitswan_app_post_inits
        svc = self.get_service("bspump.PumpService")
        _init_connections(self, svc)
        _init_lookups(self, svc)
        _init_pipelines(self, svc)
        for func in _bitswan_app_post_inits:
            func(self)


default_webserver = os.environ.get("BITSWAN_DEFAULT_WEBSERVER", "f") in [
    "t",
    "true",
    "True",
    "1",
]
deploy_secret = os.environ.get("BITSWAN_DEPLOY_SECRET", None)

if default_webserver or deploy_secret:
    from bspump.http.web.server import WebServerConnection

    @register_connection
    def webserver(app):
        return WebServerConnection(
            app, "DefaultWebServerConnection"
        )


if deploy_secret:
    import zipfile
    from bspump.http.web.server import *

    new_pipeline("Deploy pipeline")

    @register_source
    def deploy_source(app, pipeline):
        return WebRouteSource(
            app, pipeline, method="POST", route="/__jupyter-deploy-pipeline/"
        

    @async_step
    async def deploy_processor(inject, ctx):
        if not ctx["request"].query.get("secret") == deploy_secret:
            ctx["status"] = 403
            ctx["response"] = {"error": "Wrong secret"}
            await inject(ctx)
            return
        else:
            ctx["status"] = 200
            ctx["response"] = {"status": "Uploaded new assets"}
            # first store the content to /tmp/pipeline.zip
            print("Deploying pipeline")
            with open("/tmp/pipeline.zip", "wb") as f:
                print("Writing to /tmp/pipeline.zip")
                content = ctx["request"].content
                while True:
                    chunk = await content.read()
                    if not chunk:
                        break
                    f.write(chunk)

            # then extract the content to /opt/piplines
            try:
                with zipfile.ZipFile("/tmp/pipeline.zip", "r") as zip_ref:
                    zip_ref.extractall("/opt/pipelines")
            except Exception as e:
                ctx["status"] = 500
                ctx["response"] = {"error": str(e)}
                await inject(ctx)
                return
            if ctx["request"].query.get("restart") == "true":
                ctx["response"] = {"status": "Restarting"}
                ctx["response_future"].set_result(
                    aiohttp.web.json_response(ctx["response"], status=ctx["status"])
                )
                await asyncio.sleep(0.5)
                os.execv("/bin/sh", ["/bin/sh", "/opt/entrypoint.sh"])
            await inject(ctx)
            return

    @register_sink
    def deploy_sink(app, pipeline):
        return JSONWebSink(app, pipeline)

    end_pipeline()


def deploy():
    import os
    import json

    is_vscode = "VSCODE_PID" in os.environ

    @dataclass
    class DeployDetails:
        notebook_json: dict[str, Any]
        deploy_secret: str
        deploy_url: str

    def get_deploy_details() -> DeployDetails | None:
        """Get the notebook JSON, deploy secret and deploy URL

        Raises:
            e: RuntimeError: No VSCode or Colab environment detected. Please run this in a different environment.

        Returns:
            DeployDetails | None: DeployDetails object containing the notebook JSON, deploy secret and deploy URL
        """

        if is_vscode:
            from IPython import get_ipython

            ip = get_ipython()
            if not ip:
                return None

            notebook_path = os.path.abspath(ip.user_ns.get("__vsc_ipynb_file__", ""))

            if not notebook_path or not notebook_path.endswith(".ipynb"):
                return None

            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook_json = json.load(f)

            details = DeployDetails(
                json.dumps(notebook_json, sort_keys=True, indent=2),
                os.environ["BITSWAN_DEPLOY_SECRET"],
                os.environ[
                    "BITSWAN_DEPLOY_URL"
                ],  # raises KeyError if not set, to match google colab behavior
            )
            return details
        else:
            try:
                from google.colab import _message, userdata
            except ImportError:
                raise RuntimeError(
                    "No VSCode or Colab environment detected. Please run this in a different environment."
                )

            notebook_json_string = _message.blocking_request(
                "get_ipynb", request="", timeout_sec=None
            )
            details = DeployDetails(
                json.dumps(notebook_json_string["ipynb"], sort_keys=True, indent=2),
                userdata.get("BITSWAN_DEPLOY_SECRET"),
                userdata.get("BITSWAN_DEPLOY_URL"),
            )
            return details

    deploy_details = get_deploy_details()
    if not deploy_details:
        print("Error retrieving notebook contents")
        return
    deploy_url = os.path.join(
        deploy_details.deploy_url,
        "__jupyter-deploy-pipeline/",
        "?secret=" + deploy_secret + "&restart=true",
    )
    print("Packing for deployment...")
    import zipfile

    with zipfile.ZipFile("main.zip", "w") as myzip:
        myzip.writestr("main.ipynb", deploy_details.notebook_json)
    import requests

    print("Uploading to server..")
    with open("main.zip", "rb") as f:
        r = requests.post(deploy_url, data=f)
        print(json.loads(r.text)["status"])
