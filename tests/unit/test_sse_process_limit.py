"""Verify SSE process-level connection limit constant."""
from __future__ import annotations


def test_process_limit_is_reasonable():
    from apps.api.app.routers.events import SSE_MAX_CONNECTIONS_PROCESS
    from backtestforecast.config import get_settings

    settings = get_settings()
    assert SSE_MAX_CONNECTIONS_PROCESS > 0
    assert settings.sse_redis_max_connections > SSE_MAX_CONNECTIONS_PROCESS
