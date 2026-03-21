from __future__ import annotations

import json
from collections.abc import Callable

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from backtestforecast.observability import REQUEST_ID_HEADER
from backtestforecast.version import get_public_version

BODY_LIMIT_OVERRIDES: dict[str, int] = {
    "/v1/billing/webhook": 512_000,
    "/v1/events/backtests": 0,
    "/v1/events/scans": 0,
    "/v1/events/sweeps": 0,
    "/v1/events/exports": 0,
    "/v1/events/analyses": 0,
}


class _BodyTooLarge(Exception):
    pass


class RequestBodyLimitMiddleware:
    """Pure ASGI middleware that rejects oversized request bodies without
    buffering the response, preserving SSE (EventSourceResponse) streaming."""

    def __init__(self, app: ASGIApp, max_body_bytes: int | Callable[[], int]) -> None:
        self.app = app
        self._max_body_bytes = max_body_bytes

    def _resolve_max_body_bytes(self) -> int:
        raw = self._max_body_bytes() if callable(self._max_body_bytes) else self._max_body_bytes
        return max(1, int(raw))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        path = scope.get("path", "/").rstrip("/") or "/"
        effective_limit = BODY_LIMIT_OVERRIDES.get(path, self._resolve_max_body_bytes())
        content_length_str = headers.get("content-length")

        if content_length_str is not None:
            try:
                if int(content_length_str) > effective_limit:
                    await self._send_413(scope, send)
                    return
            except ValueError:
                await self._send_413(scope, send)
                return

        method = scope.get("method", "GET")
        if method in {"POST", "PUT", "PATCH", "DELETE"} and content_length_str is None:
            total_bytes = 0

            async def limited_receive() -> Message:
                nonlocal total_bytes
                message = await receive()
                if message["type"] == "http.request":
                    body = message.get("body", b"")
                    total_bytes += len(body)
                    if total_bytes > effective_limit:
                        raise _BodyTooLarge()
                return message

            response_started = False
            original_send = send

            async def tracked_send(message: Message) -> None:
                nonlocal response_started
                if message["type"] == "http.response.start":
                    response_started = True
                await original_send(message)

            try:
                await self.app(scope, limited_receive, tracked_send)
            except _BodyTooLarge:
                if not response_started:
                    await self._send_413(scope, send)
            return

        await self.app(scope, receive, send)

    @staticmethod
    async def _send_413(scope: Scope, send: Send) -> None:
        request_id = None
        state = scope.get("state")
        if state is not None:
            request_id = getattr(state, "request_id", None)

        body = json.dumps(
            {
                "error": {
                    "code": "payload_too_large",
                    "message": "The request body exceeded the maximum allowed size.",
                    "request_id": request_id,
                }
            }
        ).encode("utf-8")

        resp_headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ]
        if request_id:
            resp_headers.append((REQUEST_ID_HEADER.encode(), request_id.encode()))

        await send({"type": "http.response.start", "status": 413, "headers": resp_headers})
        await send({"type": "http.response.body", "body": body})


class DynamicCORSMiddleware:
    """Apply CORS headers using the current settings on every request."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        allow_origins: Callable[[], list[str]],
        allow_credentials: bool,
        allow_methods: list[str],
        allow_headers: list[str],
        expose_headers: list[str],
        max_age: int,
    ) -> None:
        self.app = app
        self._allow_origins = allow_origins
        self._allow_credentials = allow_credentials
        self._allow_methods = ", ".join(allow_methods)
        self._allow_headers = ", ".join(allow_headers)
        self._expose_headers = ", ".join(expose_headers)
        self._max_age = str(max_age)

    def _resolve_origin(self, origin: str | None) -> str | None:
        if not origin:
            return None
        normalized_origin = normalize_origin(origin)
        allowed_origins = {normalize_origin(value) for value in self._allow_origins()}
        if "*" in allowed_origins:
            return origin
        if normalized_origin in allowed_origins:
            return origin
        return None

    @staticmethod
    def _is_preflight(headers: Headers, method: str) -> bool:
        return (
            method == "OPTIONS"
            and headers.get("origin") is not None
            and headers.get("access-control-request-method") is not None
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        method = scope.get("method", "GET")
        origin = headers.get("origin")
        allowed_origin = self._resolve_origin(origin)

        if self._is_preflight(headers, method):
            if allowed_origin is None:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 400,
                        "headers": [(b"content-length", b"0")],
                    }
                )
                await send({"type": "http.response.body", "body": b""})
                return

            preflight_headers = [
                (b"access-control-allow-origin", allowed_origin.encode()),
                (b"access-control-allow-methods", self._allow_methods.encode()),
                (b"access-control-allow-headers", self._allow_headers.encode()),
                (b"access-control-max-age", self._max_age.encode()),
                (b"vary", b"Origin"),
                (b"content-length", b"0"),
            ]
            if self._allow_credentials:
                preflight_headers.append((b"access-control-allow-credentials", b"true"))
            await send({"type": "http.response.start", "status": 200, "headers": preflight_headers})
            await send({"type": "http.response.body", "body": b""})
            return

        async def send_with_cors(message: Message) -> None:
            if message["type"] == "http.response.start" and allowed_origin is not None:
                response_headers = MutableHeaders(scope=message)
                response_headers["Access-Control-Allow-Origin"] = allowed_origin
                response_headers.append("Vary", "Origin")
                if self._allow_credentials:
                    response_headers["Access-Control-Allow-Credentials"] = "true"
                if self._expose_headers:
                    response_headers["Access-Control-Expose-Headers"] = self._expose_headers
            await send(message)

        await self.app(scope, receive, send_with_cors)


class DynamicTrustedHostMiddleware:
    """Validate Host against the current settings on every request."""

    def __init__(self, app: ASGIApp, allowed_hosts: Callable[[], list[str]]) -> None:
        self.app = app
        self._allowed_hosts = allowed_hosts

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        host_header = headers.get("host", "")
        host = host_header.split(":", 1)[0].lower()
        allowed_hosts = [entry.lower() for entry in self._allowed_hosts()]

        if "*" in allowed_hosts or host in allowed_hosts:
            await self.app(scope, receive, send)
            return

        body = json.dumps({"error": {"code": "invalid_host", "message": "Invalid host header."}}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def normalize_origin(value: str) -> str:
    v = value.strip().lower().rstrip("/")
    if v.startswith("https://") and v.endswith(":443"):
        v = v[:-4]
    elif v.startswith("http://") and v.endswith(":80"):
        v = v[:-3]
    return v


class ApiSecurityHeadersMiddleware:
    def __init__(self, app: ASGIApp, app_env: str | None = None) -> None:
        self.app = app
        if app_env is None:
            from backtestforecast.config import get_settings

            app_env = get_settings().app_env
        self._is_production = app_env in ("production", "staging")
        self._show_version = app_env in ("development", "test")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def send_with_headers(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start" and not response_started:
                response_started = True
                headers = MutableHeaders(scope=message)
                headers.setdefault("X-Content-Type-Options", "nosniff")
                headers.setdefault("X-Frame-Options", "DENY")
                headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
                headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
                headers.setdefault("Cache-Control", "private, no-store")
                headers.setdefault("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
                headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
                headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
                headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
                headers.setdefault("X-XSS-Protection", "0")
                headers.setdefault("X-Robots-Tag", "noindex, nofollow")
                if self._is_production:
                    headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
                if self._show_version:
                    headers["X-API-Version"] = get_public_version()
            await send(message)

        await self.app(scope, receive, send_with_headers)
