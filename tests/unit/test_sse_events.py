"""Unit tests for SSE event publishing and format.

Verifies event structure matches what the frontend expects.
"""
from __future__ import annotations

import inspect
import json
import uuid
from unittest.mock import MagicMock, patch


class TestPublishJobStatus:
    """Verify publish_job_status produces correct event payloads."""

    def test_publish_function_exists(self):
        from backtestforecast.events import publish_job_status

        assert callable(publish_job_status)

    def test_publish_accepts_required_params(self):
        from backtestforecast.events import publish_job_status

        sig = inspect.signature(publish_job_status)
        params = list(sig.parameters.keys())
        assert "job_type" in params
        assert "job_id" in params
        assert "status" in params

    def test_publish_channel_format(self):
        """Channel must follow job:{type}:{id}:status convention."""
        job_id = uuid.uuid4()
        mock_client = MagicMock()

        with patch("backtestforecast.events._get_redis", return_value=mock_client):
            from backtestforecast.events import publish_job_status

            publish_job_status("scan", job_id, "running")

        channel = mock_client.publish.call_args[0][0]
        assert channel == f"job:scan:{job_id}:status"

    def test_publish_payload_contains_status_and_job_id(self):
        job_id = uuid.uuid4()
        mock_client = MagicMock()

        with patch("backtestforecast.events._get_redis", return_value=mock_client):
            from backtestforecast.events import publish_job_status

            publish_job_status("backtest", job_id, "succeeded")

        payload = json.loads(mock_client.publish.call_args[0][1])
        assert payload["status"] == "succeeded"
        assert payload["job_id"] == str(job_id)

    def test_publish_metadata_merged_into_payload(self):
        job_id = uuid.uuid4()
        mock_client = MagicMock()

        with patch("backtestforecast.events._get_redis", return_value=mock_client):
            from backtestforecast.events import publish_job_status

            publish_job_status("export", job_id, "succeeded", metadata={"file_url": "/dl/1"})

        payload = json.loads(mock_client.publish.call_args[0][1])
        assert payload["file_url"] == "/dl/1"


class TestSSEEndpoint:
    """Verify SSE router is properly configured."""

    def test_events_router_exists(self):
        from apps.api.app.routers.events import router

        assert router is not None

    def test_events_router_prefix(self):
        from apps.api.app.routers.events import router

        assert router.prefix == "/events"

    def test_backtest_events_route_registered(self):
        from apps.api.app.routers.events import router

        paths = [route.path for route in router.routes]
        assert any("backtests/{run_id}" in p for p in paths)

    def test_scan_events_route_registered(self):
        from apps.api.app.routers.events import router

        paths = [route.path for route in router.routes]
        assert any("scans/{job_id}" in p for p in paths)

    def test_export_events_route_registered(self):
        from apps.api.app.routers.events import router

        paths = [route.path for route in router.routes]
        assert any("exports/{export_job_id}" in p for p in paths)
