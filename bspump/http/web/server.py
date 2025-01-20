import logging
import asyncio
import json
import jwt
from jwt.exceptions import ExpiredSignatureError, DecodeError
import base64
from io import BytesIO

from ...abc.source import Source
from ...abc.sink import Sink
from ...abc.connection import Connection

import aiohttp.web
from aiohttp.web import Request


L = logging.getLogger(__name__)


def recursive_merge(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            dict1[key] = recursive_merge(dict1[key], value)
        else:
            dict1[key] = value
    return dict1


class WebServerConnection(Connection):
    """
    Source with events from a specific route.
    """

    ConfigDefaults = {
        "port": 8080,
        "max_body_size_bytes": 1024 * 1024 * 1000,
    }

    def __init__(self, app, id=None, config=None):
        super().__init__(app, id=id, config=config)

        self.aiohttp_app = aiohttp.web.Application(
            client_max_size=int(self.Config["max_body_size_bytes"])
        )
        self.aiohttp_app.router.add_static("/static/", "./static", show_index=True)
        self.start_server()

    def start_server(self):
        print("Starting webserver")
        try:
            self.App.Loop.create_task(
                aiohttp.web._run_app(
                    self.aiohttp_app,
                    port=int(self.Config["port"]),
                )
            )
        except Exception as e:
            print("Exception: {}".format(e))
            import traceback

            traceback.print_exc()


class WebRouteSource(Source):
    """
    WebSource is a source that listens on a specified port and serves HTTP requests.
    """

    def __init__(
        self,
        app,
        pipeline,
        connection="DefaultWebServerConnection",
        method="GET",
        route="/",
        id=None,
        config=None,
    ):
        super().__init__(app, pipeline, id=id, config=config)
        pipeline.StopOnErrors = False

        try:
            self.Connection = pipeline.locate_connection(app, connection)
        except KeyError:
            if connection == "DefaultWebServerConnection":
                self.Connection = WebServerConnection(app, "DefaultWebServerConnection")
                app.PumpService.add_connection(self.Connection)
        self.aiohttp_app = self.Connection.aiohttp_app
        self.aiohttp_app.router.add_route(method, route, self.handle_request)

    async def main(self):
        pass

    async def handle_request(self, request):
        try:
            response_future = asyncio.Future()
            await self.process(
                {
                    "request": request,
                    "response_future": response_future,
                    "status": 200,
                }
            )
            return await response_future
        except Exception:
            L.exception("Exception in WebSource")
            return aiohttp.web.Response(status=500)


async def gate_response(request, test_secret, response_fn):
    secret = request.query.get("secret")
    if secret is None:
        auth = request.headers.get("Authorization")
        if auth is not None:
            if auth.startswith("Bearer "):
                secret = auth[7:]
        if secret is None:
            return aiohttp.web.Response(
                text="Secret is missing. Pass via query parameter 'secret' or in the Authorization as 'Bearer <secret>'",
                status=401,
            )
    if not test_secret(secret):
        return aiohttp.web.Response(text="Invalid secret", status=403)
    return await response_fn()


class ProtectedWebRouteSource(WebRouteSource):
    """
    Web route source that requires a secret in a qparam or in the BearerToken.
    """

    async def handle_request(self, request):
        try:

            async def response_fn():
                response_future = asyncio.Future()
                await self.process(
                    {
                        "request": request,
                        "response_future": response_future,
                        "status": 200,
                    }
                )
                return await response_future

            return await gate_response(request, self.Config["secret"], response_fn)
        except Exception:
            L.exception("Exception in WebSource")
            return aiohttp.web.Response(status=500)


class FieldSet:
    def __init__(self, name, fields=None, fieldset_intro="", display="", required=True):
        self.fields = fields
        if fields is None:
            self.fields = []
        self.name = name
        self.default = {}
        self.display = display if display else name
        self.fieldset_intro = fieldset_intro
        self.required: bool = required
        self.prefix = f"fieldset___{self.name}___"

    def set_subfield_names(self):
        for field in self.fields:
            field.field_name = f"{self.prefix}{field.name}"

    def html(self, defaults, *args, **kwargs):
        fields = ""
        self.set_subfield_names()
        for field in self.fields:
            fields += field.html(defaults.get(field.name, field.default))
        return f"""
        <div style="margin-left: 20px; border-left: 1px solid black; padding-left: 10px;margin-top: 30px;">
            <legend><b>{self.display}</b></legend>
            {self.fieldset_intro}
            {fields}
        </div>
        """

    def restructure_data(self, dfrom, dto):
        self.set_subfield_names()
        dto[self.name] = {}
        for field in self.fields:
            field.restructure_data(dfrom, dto[self.name])

    def clean(self, data, request=None):
        for field in self.fields:
            field.clean(data[self.name])


class Field:
    def __init__(self, name, **kwargs):
        self.name = name
        if "___" in name:
            raise ValueError("Field name cannot contain '___'")
        self.hidden: bool = kwargs.get("hidden", False)
        self.readonly: bool = kwargs.get("readonly", False)
        self.required: bool = kwargs.get("required", True)
        self.display: str = kwargs.get("display", self.name)
        self.default = kwargs.get("default", "")
        self.field_name: str = f"f___{self.name}"
        self.default_classes = kwargs.get(
            "default_css_classes",
            "bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500",
        )
        if self.readonly:
            self.default_classes = kwargs.get(
                "default_css_classes",
                "bg-gray-500 border border-gray-300 text-white text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500",
            )

    @property
    def default_input_props(self):
        if self.readonly:
            readonly = "readonly"
        else:
            readonly = ""
        return f'name="{self.field_name}" id="{self.field_name}" {readonly}'

    def restructure_data(self, dfrom, dto):
        dto[self.name] = dfrom.get(self.field_name, self.default)

    def clean(self, data, request=None):
        pass

    def html(self, default=""):
        if not default:
            default = self.default
        return f"""

      <div class="mt-10 grid grid-cols-1 gap-x-6 gap-y-8 sm:grid-cols-6" style='{'display: none' if self.hidden else ''}'>
        <div class="sm:col-span-4">
                <label for="{self.field_name}" class="block text-sm font-bold text-gray-700">{self.display}</label>
                {self.inner_html(default, self.readonly)}
        </div>
        </div>
        """


class TextField(Field):
    def inner_html(self, default="", readonly=False):
        return f"""
        <div class="mt-1">
            <input type="text" class="{self.default_classes}" value="{default}" {self.default_input_props}>
        </div>
        """


class ChoiceField(Field):
    def __init__(self, name, choices, **kwargs):
        super().__init__(name, **kwargs)
        self.choices = choices

    def inner_html(self, default="", readonly=False):
        return f"""
        <div class="mt-1">
            <select class="{self.default_classes}" value="{default}" {self.default_input_props}>
                {"".join(
                    f'<option value="{choice}" {"selected" if choice == default else ""}>{choice}</option>'
                    for choice in self.choices
                )}
            </select>
        </div>
        """


class CheckboxField(Field):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = self.default or False

    def inner_html(self, default="", readonly=False):
        readonly_attr = "disabled" if readonly else ""
        return f"""
                    <input type="checkbox" {"checked" if default == True or (default and default.lower() in ("true", "t")) else ""} class="{self.default_classes}" {self.default_input_props} {readonly_attr}>
                """

    def clean(self, data, request=None):
        if type(data.get(self.name)) == str:
            data[self.name] = data.get(self.name, False) == "on"


class IntField(Field):
    def inner_html(self, default=0, readonly=False):
        if not default:
            default = 0
        return f"""
        <div class="mt-1">
            <input type="number" value="{default}" class="{self.default_classes}" {self.default_input_props}>
        </div>
        """

    def clean(self, data, request=None):
        if type(data.get(self.name)) == str:
            data[self.name] = int(data.get(self.name, 0))


class FloatField(Field):
    def inner_html(self, default=0, readonly=False):
        if not default:
            default = 0.0
        return f"""
        <div class="mt-1">
            <input type="number" value="{default}" class="{self.default_classes}" {self.default_input_props}>
        </div>
        """

    def clean(self, data, request=None):
        if type(data.get(self.name)) == str:
            data[self.name] = float(data.get(self.name, 0))


class FileField(Field):
    """
    The value ends up being the bytes of the uploaded file.
    """

    def inner_html(self, default="", readonly=False):
        return f"""
        <div class="mt-1">
            <input type="file" class="{self.default_classes}" {self.default_input_props}>
        </div>
        """

    def clean(self, data, request=None):
        if request.content_type == "application/json":
            decoded_data = base64.b64decode(data.get(self.name, ""))
            data[self.name] = BytesIO(decoded_data)
        else:
            data[self.name] = data[self.name].file


class RawJSONField(Field):
    def inner_html(self, default="", readonly=False):
        return f"""
      <div class="mt-1">
      <textarea class="{self.default_classes}" {self.default_input_props}>{default}</textarea>
      </div>
      """

    def clean(self, data, request=None):
        if type(data.get(self.name)) == str:
            data[self.name] = json.loads(data.get(self.name, "{}"))


class WebFormSource(WebRouteSource):
    def __init__(
        self,
        app,
        pipeline,
        connection="DefaultWebServerConnection",
        route="/",
        fields: Field = None,
        id=None,
        config=None,
        form_intro="",
    ):
        super().__init__(
            app,
            pipeline,
            connection=connection,
            route=route,
            method="GET",
            id=id,
            config=config,
        )
        self.fields = fields
        self.form_intro = form_intro
        self.aiohttp_app.router.add_route("POST", route, self.handle_post)

    async def handle_request(self, request):
        return aiohttp.web.Response(
            text=self.render_form(request),
            content_type="text/html",
        )

    def validate_field_presence(self, data, fields=None):
        if not fields:
            fields = self.fields
        for field in fields:
            if field.required and field.name not in data:
                raise ValueError(f"Field {field.name} is required")
            if field.required and hasattr(field, "fields"):
                self.validate_field_presence(data[field.name], field.fields)

    async def extract_data(self, request):
        if request.content_type == "application/json":
            data = await request.json()
        else:
            dfrom = dict(await request.post())
            data = {}
            for field in self.fields:
                field.restructure_data(dfrom, data)
        return data

    async def clean_and_process(self, request, data):
        for field in self.fields:
            try:
                field.clean(data, request=request)
            except ValueError as e:
                return aiohttp.web.Response(
                    text=self.render_form(request, errors={field.name: e}),
                    content_type="text/html",
                    status=400,
                )
        response_future = asyncio.Future()
        await self.process(
            {
                "request": request,
                "response_future": response_future,
                "status": 200,
                "form": data,
            }
        )
        return await response_future

    async def handle_post(self, request: Request):
        try:
            data = await self.extract_data(request)
        except json.decoder.JSONDecodeError:
            return aiohttp.web.Response(status=400, text="Invalid JSON")
        try:
            self.validate_field_presence(data)
        except ValueError as e:
            return aiohttp.web.Response(
                status=400,
                text=f"Schema validation error: {e}",
                content_type="text/plain",
            )
        return await self.clean_and_process(request, data)

    def extract_defaults(self, request):
        defaults = {}
        for query_param, value in request.query.items():
            if "___" in query_param:
                parts = query_param.split("___")
                current_dict = defaults
                for fieldset in parts[:-1]:
                    if not current_dict.get(fieldset):
                        current_dict[fieldset] = {}
                    current_dict = current_dict[fieldset]
                current_dict[parts[-1]] = value
        return defaults

    def render_form(self, request, errors={}):
        defaults = self.extract_defaults(request)

        for field in self.fields:
            if field.name in request.query:
                defaults[field.name] = request.query[field.name]
            elif field.name not in defaults:
                defaults[field.name] = field.default

        top = f"""
        <html>
        <head>
        <link rel="stylesheet" href="/static/css/tailwind.css">
        <script>

            function submitForm() {{
                document.getElementById("loading").style.display = "block";
                document.getElementById("main-form").submit();
            }}
        </script>
        </head>
        <BODY>
        <form id="main-form" method="post" enctype="multipart/form-data">
        <div id="loading" style="display:none">
            <div class="fixed top-0 left-0 h-screen w-screen bg-black bg-opacity-50 z-50 flex justify-center items-center">
                <div class="bg-white p-4 rounded-lg">
                    <div class="text-center">Processing...</div>
                </div>
            </div>
        </div>
        <div class="space-y-12">
        <div class="max-w-7xl mx-auto py-12 px-4 sm:px-6 lg:py-16 lg:px-8 bg-gray shadow sm:rounded-lg">
        {self.form_intro}
        """
        fields = ""
        for field in self.fields:
            fields += field.html(defaults[field.name])
            if field.name in errors:
                fields += f'<div class="text-red-500">{errors[field.name]}</div>'
        bottom = """
        <div class="mt-10 grid grid-cols-1 gap-x-6 gap-y-8 sm:grid-cols-6">
        <div class="sm:col-span-4">
        <button type="button" onclick=submitForm() class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded">Submit</button>
        </div>
        </div>
        </div>
        </div>
        </form>
        </body>
        </html>
        """
        return top + fields + bottom


class ProtectedWebFormSource(WebFormSource):
    async def handle_request(self, request):
        su = super()

        async def response_fn():
            return await su.handle_request(request)

        return await gate_response(
            request, lambda secret: self.test_secret(secret), response_fn
        )

    def test_secret(self, secret):
        return secret == self.Config["secret"]

    async def handle_post(self, request):
        su = super()

        async def response_fn():
            return await su.handle_post(request)

        return await gate_response(
            request, lambda secret: self.test_secret(secret), response_fn
        )


class JWTWebFormSource(ProtectedWebFormSource):
    """
    ProtectedWebFormSource that is gated by a JWT token issued at some time and
    due to expire. All hidden fields are encoded in the JWT token and values
    for any other fields encoded in the JWT token take precedence over user
    input
    """

    def __init__(
        self,
        app,
        pipeline,
        connection="DefaultWebServerConnection",
        route="/",
        fields: Field = None,
        id=None,
        config=None,
        form_intro="",
    ):
        self.fields = fields + [IntField("exp", hidden=True, display="Expires at: ")]
        super().__init__(
            app,
            pipeline,
            connection,
            route,
            self.fields,
            id=id,
            config=config,
            form_intro="",
        )

    def test_secret(self, secret):
        print(self.Config["jwt-secret"])
        try:
            jwt.decode(secret, self.Config["jwt-secret"], algorithms=["HS256"])
        except DecodeError as e:
            print(e)
            return False
        except ExpiredSignatureError as e:
            print(e)
            return False
        return True

    def extract_defaults(self, request):
        su = super()
        defaults = su.extract_defaults(request)
        defaults = recursive_merge(
            defaults,
            jwt.decode(
                request.query["secret"], self.Config["jwt-secret"], algorithms=["HS256"]
            ),
        )
        return defaults

    async def handle_post(self, request):
        try:
            data = await self.extract_data(request)
        except json.decoder.JSONDecodeError:
            return aiohttp.web.Response(status=400, text="Invalid JSON")

        try:
            data = recursive_merge(
                data,
                jwt.decode(
                    request.query["secret"],
                    self.Config["jwt-secret"],
                    algorithms=["HS256"],
                ),
            )
        except DecodeError as e:
            return aiohttp.web.Response(text=f"Invalid secret: {e}", status=400)
        except ExpiredSignatureError as e:
            return aiohttp.web.Response(text=f"JWT Token expired: {e}", status=403)

        try:
            self.validate_field_presence(data)
        except ValueError as e:
            return aiohttp.web.Response(
                status=400,
                text=f"Schema validation error: {e}",
                content_type="text/plain",
            )

        return await self.clean_and_process(request, data)


class WebSink(Sink):
    """
    WebSink is a sink that sends HTTP requests.
    """

    def process(self, context, event):
        content_type = event.get("content_type", "text/html")
        # if bytes we use a binary response otherwise we use text
        if isinstance(event["response"], bytes):
            event["response_future"].set_result(
                aiohttp.web.Response(
                    status=event["status"],
                    body=event["response"],
                    content_type=content_type,
                )
            )
        else:
            event["response_future"].set_result(
                aiohttp.web.Response(
                    status=event["status"],
                    text=event["response"],
                    content_type=content_type,
                )
            )


class JSONWebSink(Sink):
    """
    JSONWebSink is a sink that sends HTTP requests with JSON content.
    """

    def process(self, context, event):
        """
        Process the incoming event and respond with either JSON or HTML.
        """
        if event["request"].content_type == "application/json":
            event["response_future"].set_result(
                aiohttp.web.json_response(event["response"], status=event["status"])
            )
        else:
            html_content = self.render_html_output(event["response"])
            event["response_future"].set_result(
                aiohttp.web.Response(
                    text=html_content,
                    content_type="text/html",
                    status=200,
                )
            )

    def render_html_output(self, json_data):
        top = """
              <html>
              <head>
              <link rel="stylesheet" href="/static/css/tailwind.css">
              <script>

                  function submitForm() {
                      document.getElementById("loading").style.display = "block";
                      document.getElementById("main-form").submit();
                  }
              </script>
              </head>
              <BODY>
              <form id="main-form" method="post">
              <div id="loading" style="display:none">
                  <div class="fixed top-0 left-0 h-screen w-screen bg-black bg-opacity-50 z-50 flex justify-center items-center">
                      <div class="bg-white p-4 rounded-lg">
                          <div class="text-center">Processing...</div>
                      </div>
                  </div>
              </div>
              <div class="space-y-12">
              <div class="max-w-7xl mx-auto py-12 px-4 sm:px-6 lg:py-16 lg:px-8 bg-gray shadow sm:rounded-lg">
              <h1 class="text-3xl font-bold text-gray-800 mb-6 border-b pb-4">Results</h1>
              """

        bottom = """
              </div>
              </div>
              </form>
              </body>
              </html>
              """

        fields = []
        res_string = self.format_json_to_html(json_data, fields)
        return top + res_string + bottom

    def format_json_to_html(self, json_data, fields):
        for key, value in json_data.items():
            if isinstance(value, dict):  # Handle nested dictionaries
                fields.append(
                    f""" 
                <div class="p-2 border border-gray-100 shadow-md rounded mt-4 mb-2">
                <h2 class="font-semibold text-lg">{key}</h2>
                <div class="ml-4">"""
                )
                self.format_json_to_html(value, fields)
                fields.append(
                    """ 
                           </div>
                           </div>
                            """
                )
            elif isinstance(value, list):  # Handle lists
                fields.append(
                    f""" 
                <div class="p-2 border border-gray-100 shadow-md rounded mt-4 mb-2">
                <h2 class="font-semibold text-lg">{key}</h2>
                <div class="ml-4">"""
                )
                self.format_list(key, value, fields)
                fields.append(
                    """ 
                               </div>
                               </div>
                                """
                )
            else:
                self.format_key_value(key, value, fields)

        return "".join(fields)

    def format_list(self, key, json_data_lst, fields):
        for item in json_data_lst:
            if isinstance(item, list):
                self.format_list(key, item, fields)
            if isinstance(item, dict):
                self.format_json_to_html(item, fields)
            else:
                self.format_key_value(key, item, fields)

    def format_key_value(self, key, value, fields):
        if isinstance(value, bool):
            fd = CheckboxField(key, readonly=True, default=value)
        elif isinstance(value, int):
            fd = IntField(key, readonly=True, default=value)
        else:
            fd = TextField(key, readonly=True, default=value)
        fields.append(fd.html())
