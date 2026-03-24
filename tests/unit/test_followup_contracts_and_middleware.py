from __future__ import annotations

from datetime import date

import pytest

from backtestforecast.models import NightlyPipelineRun
from backtestforecast.security.http import (
    DynamicCORSMiddleware,
    DynamicTrustedHostMiddleware,
    RequestBodyLimitMiddleware,
)
from backtestforecast.services.daily_picks import DailyPicksService


async def _run_asgi(app, scope, receive_messages: list[dict] | None = None) -> list[dict]:
    messages: list[dict] = []
    pending = list(receive_messages or [{"type": "http.request", "body": b"", "more_body": False}])

    async def receive() -> dict:
        if pending:
            return pending.pop(0)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        messages.append(message)

    await app(scope, receive, send)
    return messages


@pytest.mark.anyio
async def test_dynamic_trusted_host_middleware_reacts_to_runtime_changes() -> None:
    state = {"hosts": ["allowed.test"]}

    async def ok_app(scope, receive, send) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = DynamicTrustedHostMiddleware(ok_app, allowed_hosts=lambda: state["hosts"])
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/v1/backtests",
        "headers": [(b"host", b"allowed.test")],
    }

    allowed = await _run_asgi(middleware, scope)
    assert allowed[0]["status"] == 204

    state["hosts"] = ["other.test"]
    blocked = await _run_asgi(middleware, scope)
    assert blocked[0]["status"] == 400


@pytest.mark.anyio
async def test_dynamic_cors_middleware_reacts_to_runtime_changes() -> None:
    state = {"origins": ["https://allowed.test"]}

    async def ok_app(scope, receive, send) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = DynamicCORSMiddleware(
        ok_app,
        allow_origins=lambda: state["origins"],
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["Authorization"],
        expose_headers=["X-Request-ID"],
        max_age=60,
    )
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/v1/backtests",
        "headers": [
            (b"host", b"api.test"),
            (b"origin", b"https://allowed.test"),
        ],
    }

    allowed = await _run_asgi(middleware, scope)
    headers = dict(allowed[0]["headers"])
    assert headers[b"access-control-allow-origin"] == b"https://allowed.test"

    state["origins"] = ["https://other.test"]
    blocked = await _run_asgi(middleware, scope)
    headers = dict(blocked[0]["headers"])
    assert b"access-control-allow-origin" not in headers


@pytest.mark.anyio
async def test_request_body_limit_middleware_reacts_to_runtime_changes() -> None:
    state = {"limit": 8}

    async def ok_app(scope, receive, send) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(ok_app, max_body_bytes=lambda: state["limit"])
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/backtests",
        "headers": [(b"content-length", b"9")],
        "state": type("State", (), {"request_id": "req-1"})(),
    }

    rejected = await _run_asgi(middleware, scope)
    assert rejected[0]["status"] == 413

    state["limit"] = 16
    allowed = await _run_asgi(middleware, scope)
    assert allowed[0]["status"] == 204


def test_daily_picks_history_serializer_includes_error_code() -> None:
    run = NightlyPipelineRun(
        status="failed",
        trade_date=date(2025, 3, 5),
        stage="forecast_rank",
        symbols_screened=10,
        recommendations_produced=0,
        error_code="pipeline_failed",
        error_message="Upstream timed out",
    )

    payload = DailyPicksService._run_to_dict(run)

    assert payload["error_code"] == "pipeline_failed"
    assert payload["error_message"] == "Upstream timed out"
