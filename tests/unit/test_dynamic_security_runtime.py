from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.responses import PlainTextResponse

from apps.api.app.main import _RequestTimeoutMiddleware
from backtestforecast.security.http import (
    ApiSecurityHeadersMiddleware,
    DynamicCORSMiddleware,
    RequestBodyLimitMiddleware,
)


def _run_asgi(app, scope: dict, body: bytes = b"") -> tuple[int, dict[str, str]]:
    messages: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict) -> None:
        messages.append(message)

    asyncio.run(app(scope, receive, send))
    start = next(message for message in messages if message["type"] == "http.response.start")
    headers = {k.decode().lower(): v.decode() for k, v in start.get("headers", [])}
    return start["status"], headers


def test_dynamic_cors_middleware_reloads_origins_per_request() -> None:
    state = {"origins": ["http://localhost:3000"]}

    async def ok_app(scope, receive, send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = DynamicCORSMiddleware(
        ok_app,
        allow_origins=lambda: state["origins"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Request-ID", "X-Requested-With", "Accept"],
        expose_headers=["X-Request-ID"],
        max_age=600,
    )

    status, headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/me", "headers": [(b"origin", b"http://localhost:3000")]},
    )
    assert status == 200
    assert headers.get("access-control-allow-origin") == "http://localhost:3000"

    state["origins"] = ["https://app.example.com"]
    _, stale_headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/me", "headers": [(b"origin", b"http://localhost:3000")]},
    )
    assert "access-control-allow-origin" not in stale_headers

    _, refreshed_headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/me", "headers": [(b"origin", b"https://app.example.com")]},
    )
    assert refreshed_headers.get("access-control-allow-origin") == "https://app.example.com"


def test_sse_proxy_requires_origin_or_referer() -> None:
    source = Path("apps/web/app/api/events/[...path]/route.ts").read_text()

    assert "isAllowedSseProxyOrigin" in source
    assert 'from "@/lib/api/sse-origin"' in source


def test_api_security_headers_reload_app_env_per_request() -> None:
    state = {"app_env": "development"}

    async def ok_app(scope, receive, send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = ApiSecurityHeadersMiddleware(ok_app, app_env_resolver=lambda: state["app_env"])

    _, dev_headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/health", "headers": []},
    )
    assert dev_headers.get("x-api-version")
    assert "strict-transport-security" not in dev_headers

    state["app_env"] = "production"
    _, prod_headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/health", "headers": []},
    )
    assert "x-api-version" not in prod_headers
    assert prod_headers.get("strict-transport-security") == "max-age=31536000; includeSubDomains"


def test_request_timeout_middleware_reloads_timeout_per_request() -> None:
    state = {"timeout": 1}

    async def slow_app(scope, receive, send) -> None:
        await asyncio.sleep(0.01)
        response = PlainTextResponse("ok")
        await response(scope, receive, send)

    app = _RequestTimeoutMiddleware(
        slow_app,
        timeout_seconds=1,
        timeout_seconds_resolver=lambda: state["timeout"],
    )

    status, headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/me", "headers": []},
    )
    assert status == 200
    assert "x-debug-timeout" not in headers

    state["timeout"] = 0
    status, headers = _run_asgi(
        app,
        {"type": "http", "method": "GET", "path": "/v1/me", "headers": []},
    )
    assert status == 504
    assert headers.get("x-debug-timeout") == "0"


def test_request_body_limit_middleware_reloads_per_request() -> None:
    state = {"limit": 4}

    async def ok_app(scope, receive, send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = RequestBodyLimitMiddleware(ok_app, max_body_bytes=lambda: state["limit"])

    status, _ = _run_asgi(
        app,
        {"type": "http", "method": "POST", "path": "/v1/me", "headers": [(b"content-length", b"5")]},
    )
    assert status == 413

    state["limit"] = 5
    status, _ = _run_asgi(
        app,
        {"type": "http", "method": "POST", "path": "/v1/me", "headers": [(b"content-length", b"5")]},
    )
    assert status == 200
