"""Test that SSE endpoints work with all middleware enabled."""
import pytest


def test_sse_headers_include_no_buffering():
    """Verify SSE responses include X-Accel-Buffering: no."""
    import ast
    import inspect
    from apps.api.app.routers import events
    source = inspect.getsource(events)
    assert "X-Accel-Buffering" in source, "SSE endpoints must include X-Accel-Buffering header"
    assert "no" in source, "X-Accel-Buffering must be set to 'no'"
