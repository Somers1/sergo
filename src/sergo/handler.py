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

    def process_request(self, request: StandardizedRequest) -> Response:
        response = Response()
        try:
            view_set = self.find_urlpatterns()[request.path.rstrip('/')]()
        except KeyError:
            response.body = {"message": f"Path {request.path} not found"}
            response.status_code = 404
            return self.finalize_and_return(response)
        try:
            handler = view_set.viable_method_handlers[request.method]
        except KeyError:
            response.body = {"message": f"Method {request.method} not allowed"}
            response.status_code = 405
            return self.finalize_and_return(response)
        try:
            response.body = handler(request)
            response.status_code = 200
        except (Exception, KeyboardInterrupt) as e:
            logger.error(e, exc_info=True)
            response.body = {"message": str(e)}
            response.status_code = 400
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
    def handle(self, request, body=None):
        from starlette.responses import Response as StarletteResponse
        standardized_request = StandardizedRequest.translate_fastapi(request, body)
        response = self.process_request(standardized_request)
        return StarletteResponse(
            content=json.dumps(response.body, cls=SergoJSONEncoder),
            media_type="application/json",
            status_code=response.status_code
        )

    def configure(self):
        from fastapi import FastAPI, Request
        app = FastAPI()

        @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
        async def fastapi_handler(request: Request):
            raw = await request.body()
            body = await request.json() if raw else None
            return self.handle(request, body)

        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
        return app


def get_handler(handler=None):
    if not handler:
        import settings
        handler = settings.HANDLER
    return utils.import_string(handler)()
