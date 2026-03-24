"""Verify SSE slot release tracks acquisition method correctly."""
import inspect


def test_sse_slot_release_redis_fallback_documented():
    """The SSE slot release in events.py should handle Redis failure
    by also decrementing in-process counter."""
    from apps.api.app.routers import events

    event_stream_source = inspect.getsource(events._event_stream)
    release_source = inspect.getsource(events._release_sse_slot)
    assert "_release_sse_slot_in_process" in event_stream_source
    assert "sse.release_slot_redis_error" in release_source
    assert "_release_sse_slot_in_process" in release_source
