"""Basic SSE slot structure test."""
from apps.api.app.routers.events import SSE_MAX_CONNECTIONS_PER_USER, SSE_MAX_CONNECTIONS_PROCESS


def test_sse_limits_are_positive():
    assert SSE_MAX_CONNECTIONS_PER_USER > 0
    assert SSE_MAX_CONNECTIONS_PROCESS > SSE_MAX_CONNECTIONS_PER_USER
