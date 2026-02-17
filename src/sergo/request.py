from typing import Dict, Any
from urllib.parse import urlparse


class StandardizedRequest:
    def __init__(
            self, method: str, url: str,
            headers: Dict[str, str] = None,
            query_params: Dict[str, str] = None,
            body: Any = None
    ):
        self.method = method.upper()
        self.url = url
        self.headers = headers
        self.query_params = query_params or {}
        self.body = body
        parsed_url = urlparse(url)
        self.path = parsed_url.path

    @classmethod
    def translate_azure(cls, request: Any):
        return cls(
            method=request.method,
            url=request.url,
            headers=dict(request.headers),
            query_params=dict(request.params),
            body=request.get_body().decode() if request.get_body() else None
        )

    @classmethod
    def translate_fastapi(cls, request: Any, body=None):
        return cls(
            method=request.method,
            url=str(request.url),
            headers=dict(request.headers),
            query_params=dict(request.query_params),
            body=body
        )


class Response:
    def __init__(self, status_code: int = None, body: Any = None, headers: Dict[str, str] = None):
        self.status_code = status_code
        self.body = body
        self.headers = headers

    def __str__(self):
        return f"Response(status_code={self.status_code}, body={self.body})"
