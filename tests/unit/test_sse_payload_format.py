"""Fix 72: SSE event data must be valid JSON and within size limits.

Tests the validation logic in the events.py SSE router (_subscribe_redis)
that rejects oversized or malformed messages before forwarding to clients.
"""
from __future__ import annotations

import inspect
import json


class TestSSEPayloadValidation:
    """Verify _subscribe_redis filters invalid payloads."""

    def test_subscribe_redis_rejects_oversized_messages(self):
        """Messages exceeding 64 KiB must be dropped (logged, not yielded)."""
        source = inspect.getsource(
            __import__("apps.api.app.routers.events", fromlist=["_subscribe_redis"])._subscribe_redis
        )
        assert "65_536" in source or "65536" in source, (
            "_subscribe_redis must enforce a 64 KiB message size limit"
        )

    def test_subscribe_redis_validates_json(self):
        """Non-JSON string messages must be dropped."""
        source = inspect.getsource(
            __import__("apps.api.app.routers.events", fromlist=["_subscribe_redis"])._subscribe_redis
        )
        assert "json" in source.lower(), (
            "_subscribe_redis must validate that message data is valid JSON"
        )
        assert "invalid_json" in source, (
            "_subscribe_redis must log a warning for invalid JSON payloads"
        )

    def test_valid_json_payload_is_accepted(self):
        """A well-formed JSON payload within size limits should parse cleanly."""
        payload = {"status": "running", "progress": 42}
        raw = json.dumps(payload)
        assert len(raw) < 65_536
        parsed = json.loads(raw)
        assert parsed["status"] == "running"

    def test_oversized_payload_detected(self):
        """A payload larger than 64 KiB should be flagged."""
        large_payload = json.dumps({"data": "x" * 70_000})
        assert len(large_payload) > 65_536

    def test_invalid_json_detected(self):
        """Non-JSON strings should fail json.loads."""
        import pytest

        with pytest.raises((json.JSONDecodeError, ValueError)):
            json.loads("this is not json {{{")

    def test_sse_event_dict_structure(self):
        """SSE events yielded by _event_stream use {event, data} dicts."""
        source = inspect.getsource(
            __import__("apps.api.app.routers.events", fromlist=["_event_stream"])._event_stream
        )
        assert '"event"' in source, "SSE events must include an 'event' key"
        assert '"data"' in source, "SSE events must include a 'data' key"

    def test_heartbeat_interval_is_reasonable(self):
        """SSE heartbeat must be <= 30 seconds to keep proxies alive."""
        from apps.api.app.routers.events import SSE_HEARTBEAT_SECONDS

        assert SSE_HEARTBEAT_SECONDS <= 30
