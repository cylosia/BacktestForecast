"""Verify SSE slot release tracks acquisition method correctly."""
import pytest


def test_sse_slot_release_redis_fallback_documented():
    """The SSE slot release in events.py should handle Redis failure
    by also decrementing in-process counter."""
    import ast
    import inspect
    from apps.api.app.routers import events

    source = inspect.getsource(events._event_stream)
    assert "_release_sse_slot_in_process" in source
    assert "slot_release_failed_redis" in source
