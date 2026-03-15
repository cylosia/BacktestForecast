from __future__ import annotations

import json

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from backtestforecast.observability import REQUEST_ID_HEADER

BODY_LIMIT_OVERRIDES: dict[str, int] = {
    "/v1/billing/webhook": 256_000,
}


class _BodyTooLarge(Exception):
    pass


class RequestBodyLimitMiddleware:
    """Pure ASGI middleware that rejects oversized request bodies without
    buffering the response, preserving SSE (EventSourceResponse) streaming."""

    def __init__(self, app: ASGIApp, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max(1, int(max_body_bytes))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        path = scope.get("path", "/").rstrip("/") or "/"
        effective_limit = BODY_LIMIT_OVERRIDES.get(path, self.max_body_bytes)
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
        if method in {"POST", "PUT", "PATCH"} and content_length_str is None:
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

        body = json.dumps({
            "error": {
                "code": "payload_too_large",
                "message": "The request body exceeded the maximum allowed size.",
                "request_id": request_id,
            }
        }).encode("utf-8")

        resp_headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ]
        if request_id:
            resp_headers.append(
                (REQUEST_ID_HEADER.encode(), request_id.encode())
            )

        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": resp_headers,
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })


API_VERSION = "0.1.0"


class ApiSecurityHeadersMiddleware:
    """Pure ASGI middleware that adds security headers to every response
    without buffering the response body, preserving SSE streaming."""

    def __init__(self, app: ASGIApp, app_env: str | None = None) -> None:
        self.app = app
        if app_env is None:
            from backtestforecast.config import get_settings

            app_env = get_settings().app_env
        self._is_production = app_env in ("production", "staging")

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
                headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
                headers.setdefault("Cache-Control", "no-store")
                headers.setdefault("Content-Security-Policy", "default-src 'self'")
                headers["X-API-Version"] = API_VERSION
                if self._is_production:
                    headers.setdefault(
                        "Strict-Transport-Security",
                        "max-age=63072000; includeSubDomains; preload",
                    )
            await send(message)

        await self.app(scope, receive, send_with_headers)
