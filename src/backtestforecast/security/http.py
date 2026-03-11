from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from backtestforecast.observability import REQUEST_ID_HEADER

EXEMPT_BODY_LIMIT_PATHS = frozenset({"/v1/billing/webhook"})


class RequestBodyLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_body_bytes: int) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self.max_body_bytes = max(1, int(max_body_bytes))

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        if request.url.path in EXEMPT_BODY_LIMIT_PATHS:
            return await call_next(request)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_body_bytes:
                    return self._payload_too_large(request)
            except ValueError:
                return self._payload_too_large(request)
        if request.method in {"POST", "PUT", "PATCH"}:
            body = await request.body()
            if len(body) > self.max_body_bytes:
                return self._payload_too_large(request)
        return await call_next(request)

    @staticmethod
    def _payload_too_large(request: Request) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        response = JSONResponse(
            status_code=413,
            content={
                "error": {
                    "code": "payload_too_large",
                    "message": "The request body exceeded the maximum allowed size.",
                    "request_id": request_id,
                }
            },
        )
        if request_id:
            response.headers[REQUEST_ID_HEADER] = request_id
        return response


class ApiSecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        response.headers.setdefault("Cache-Control", "no-store")
        return response
