import importlib
import json
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


class SergoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

from sergo import utils
from sergo.request import Response, StandardizedRequest
from settings import logger

connection = utils.LazyImport("sergo.connection")


class URLConflictError(Exception):
    pass


class BaseHandler(ABC):
    @abstractmethod
    def handle(self, request: Any) -> Any:
        pass

    @abstractmethod
    def configure(self):
        pass

    @staticmethod
    def finalize_and_return(response: Response) -> Any:
        connection.connection.close()
        return response

    @staticmethod
    def find_urlpatterns():
        urlpatterns = {}
        directory_path = Path('urls')
        for file_path in directory_path.rglob('*.py'):
            if file_path.name == '__init__.py':
                continue
            module_path = '.'.join(file_path.with_suffix('').parts)
            try:
                module = importlib.import_module(module_path)
                if hasattr(module, 'urlpatterns'):
                    for url, view in module.urlpatterns.items():
                        if url in urlpatterns and urlpatterns[url] != view:
                            raise URLConflictError(f"Conflicting URL pattern '{url}' in {module_path}")
                        urlpatterns[url] = view
            except ImportError as e:
                print(f"Error importing {module_path}: {e}")
        return urlpatterns

    def authenticate(self, request: StandardizedRequest) -> StandardizedRequest:
        """Run the auth backend if configured. Sets request.user."""
        if hasattr(self, '_auth_backend') and self._auth_backend:
            request.user = self._auth_backend(request)
        return request

    def _resolve_handler(self, request: StandardizedRequest):
        """Authenticate, route, and return (handler, response) or (None, error_response)."""
        response = Response()
        logger.info(f"{request.method} {request.path}")
        logger.debug(f"params={request.query_params}")
        try:
            request = self.authenticate(request)
        except Exception as e:
            logger.warning(f"401 {request.path}: {e}")
            response.body, response.status_code = {"message": str(e)}, 401
            return None, request, response
        try:
            view_set = self.find_urlpatterns()[request.path.rstrip('/')]()
        except KeyError:
            logger.warning(f"404 {request.path}")
            response.body, response.status_code = {"message": f"Path {request.path} not found"}, 404
            return None, request, response
        try:
            handler = view_set.viable_method_handlers[request.method]
        except KeyError:
            logger.warning(f"405 {request.method} {request.path}")
            response.body, response.status_code = {"message": f"Method {request.method} not allowed"}, 405
            return None, request, response
        return handler, request, response

    def process_request(self, request: StandardizedRequest) -> Response:
        handler, request, response = self._resolve_handler(request)
        if not handler:
            return self.finalize_and_return(response)
        try:
            response.body = handler(request)
            response.status_code = 200
            logger.info(f"200 {request.method} {request.path}")
            logger.debug(f"Response body: {response.body}")
        except (Exception, KeyboardInterrupt) as e:
            logger.error(e, exc_info=True)
            response.body, response.status_code = {"message": str(e)}, 400
        return self.finalize_and_return(response)

    async def async_process_request(self, request: StandardizedRequest) -> Response:
        import inspect
        handler, request, response = self._resolve_handler(request)
        if not handler:
            return self.finalize_and_return(response)
        try:
            result = handler(request)
            if inspect.isawaitable(result):
                result = await result
            response.body = result
            response.status_code = 200
            logger.info(f"200 {request.method} {request.path}")
            logger.debug(f"Response body: {response.body}")
        except (Exception, KeyboardInterrupt) as e:
            logger.error(e, exc_info=True)
            response.body, response.status_code = {"message": str(e)}, 400
        return self.finalize_and_return(response)


class AzureFunctionHandler(BaseHandler):
    def handle(self, req):
        standardized_request = StandardizedRequest.translate_azure(req)
        response = self.process_request(standardized_request)
        from azure.functions import HttpResponse
        return HttpResponse(
            body=json.dumps(response.body, cls=SergoJSONEncoder),
            mimetype="application/json",
            status_code=response.status_code
        )

    def configure(self):
        pass


class FastAPIHandler(BaseHandler):
    async def handle(self, request, body=None, files=None):
        from starlette.responses import Response as StarletteResponse
        standardized_request = StandardizedRequest.translate_fastapi(request, body, files=files)
        response = await self.async_process_request(standardized_request)
        return StarletteResponse(
            content=json.dumps(response.body, cls=SergoJSONEncoder),
            media_type="application/json",
            status_code=response.status_code
        )

    def configure(self, task_loop=None, auth_backend=None, host="0.0.0.0", port=8000, on_setup=None):
        self.build_app(task_loop=task_loop, auth_backend=auth_backend, on_setup=on_setup)
        import uvicorn
        uvicorn.run(self.app, host=host, port=port)
        return self.app

    def build_app(self, task_loop=None, auth_backend=None, on_setup=None):
        """Build the FastAPI app without starting the server. Use with external uvicorn."""
        self._auth_backend = auth_backend
        from contextlib import asynccontextmanager
        from fastapi import FastAPI, Request

        @asynccontextmanager
        async def lifespan(app):
            if task_loop:
                await task_loop.start()
            yield
            if task_loop:
                await task_loop.stop()

        self.app = FastAPI(lifespan=lifespan)

        if on_setup:
            on_setup(self.app)

        @self.app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
        async def fastapi_handler(request: Request):
            content_type = request.headers.get('content-type', '')
            if 'multipart/form-data' in content_type:
                from sergo.request import UploadedFile
                form = await request.form()
                body, files = {}, {}
                for key, value in form.multi_items():
                    if hasattr(value, 'read'):
                        content = await value.read()
                        files[key] = UploadedFile(filename=value.filename, content=content, content_type=value.content_type)
                    else:
                        body[key] = value
                return await self.handle(request, body, files)
            raw = await request.body()
            body = await request.json() if raw else None
            return await self.handle(request, body)

        return self.app


def get_handler(handler=None):
    if not handler:
        import settings
        handler = settings.HANDLER
    return utils.import_string(handler)()
