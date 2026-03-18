"""Verify SSE process-level connection limit constant."""
from __future__ import annotations


def test_process_limit_is_reasonable():
    from apps.api.app.routers.events import SSE_MAX_CONNECTIONS_PROCESS
    assert 50 <= SSE_MAX_CONNECTIONS_PROCESS <= 500
