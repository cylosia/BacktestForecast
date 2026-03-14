from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from backtestforecast.observability import REQUEST_ID_HEADER

BODY_LIMIT_OVERRIDES: dict[str, int] = {
    "/v1/billing/webhook": 256_000,
}


class RequestBodyLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_body_bytes: int) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self.max_body_bytes = max(1, int(max_body_bytes))

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        effective_limit = BODY_LIMIT_OVERRIDES.get(request.url.path, self.max_body_bytes)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > effective_limit:
                    return self._payload_too_large(request)
            except ValueError:
                return self._payload_too_large(request)
        if request.method in {"POST", "PUT", "PATCH"} and content_length is None:
            body = await request.body()
            if len(body) > effective_limit:
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


API_VERSION = "0.1.0"


class ApiSecurityHeadersMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, app_env: str | None = None) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        if app_env is None:
            from backtestforecast.config import get_settings

            app_env = get_settings().app_env
        self._is_production = app_env in ("production", "staging")

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers["X-API-Version"] = API_VERSION
        if self._is_production:
            response.headers.setdefault("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
        return response
