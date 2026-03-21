from __future__ import annotations

import asyncio
from pathlib import Path

from backtestforecast.security.http import DynamicCORSMiddleware


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

    assert "if (!candidate) return false;" in source
    assert "future auth or cookie change" in source
