from typing import Dict, Any
from urllib.parse import urlparse


class UploadedFile:
    """A file uploaded via multipart form data."""
    def __init__(self, filename: str, content: bytes, content_type: str = 'application/octet-stream'):
        self.filename = filename
        self.content = content
        self.content_type = content_type

    def __repr__(self):
        return f'UploadedFile({self.filename!r}, {len(self.content)} bytes, {self.content_type})'


class StandardizedRequest:
    def __init__(
            self, method: str, url: str,
            headers: Dict[str, str] = None,
            query_params: Dict[str, str] = None,
            body: Any = None,
            files: Dict[str, 'UploadedFile'] = None,
    ):
        self.method = method.upper()
        self.url = url
        self.headers = headers or {}
        self.query_params = query_params or {}
        self.body = body
        self.files = files or {}
        self.user = None  # Set by auth backend if configured
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
    def translate_fastapi(cls, request: Any, body=None, files=None):
        return cls(
            method=request.method,
            url=str(request.url),
            headers=dict(request.headers),
            query_params=dict(request.query_params),
            body=body,
            files=files,
        )


class Response:
    def __init__(self, status_code: int = None, body: Any = None, headers: Dict[str, str] = None):
        self.status_code = status_code
        self.body = body
        self.headers = headers

    def __str__(self):
        return f"Response(status_code={self.status_code}, body={self.body})"
