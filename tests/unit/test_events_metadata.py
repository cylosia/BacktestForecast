"""Item 83: Verify publish_job_status metadata cannot override reserved keys.

The ``status`` and ``job_id`` keys in metadata must be stripped so that a
caller cannot accidentally (or maliciously) override the canonical values.
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock, patch


def test_metadata_cannot_override_status():
    """Passing metadata={"status": "succeeded"} when actual status is "failed"
    must result in payload with "status": "failed"."""
    job_id = uuid.uuid4()
    mock_client = MagicMock()

    with patch("backtestforecast.events._get_redis", return_value=mock_client):
        from backtestforecast.events import publish_job_status

        publish_job_status(
            "backtest",
            job_id,
            "failed",
            metadata={"status": "succeeded", "extra_key": "keep_me"},
        )

    mock_client.publish.assert_called_once()
    payload = json.loads(mock_client.publish.call_args[0][1])

    assert payload["status"] == "failed", (
        "metadata must not override the canonical status parameter"
    )
    assert payload["job_id"] == str(job_id), (
        "metadata must not override the canonical job_id parameter"
    )
    assert payload["extra_key"] == "keep_me", (
        "Non-reserved metadata keys must be preserved"
    )


def test_metadata_cannot_override_job_id():
    """Passing metadata={"job_id": "spoofed"} must not change the real job_id."""
    job_id = uuid.uuid4()
    mock_client = MagicMock()

    with patch("backtestforecast.events._get_redis", return_value=mock_client):
        from backtestforecast.events import publish_job_status

        publish_job_status(
            "scan",
            job_id,
            "running",
            metadata={"job_id": "spoofed-id"},
        )

    payload = json.loads(mock_client.publish.call_args[0][1])
    assert payload["job_id"] == str(job_id)


# ---------------------------------------------------------------------------
# Item 58: _fallback_persist_status rejects non-terminal status
# ---------------------------------------------------------------------------


def test_fallback_persist_status_noop_for_non_terminal_request():
    """Calling _fallback_persist_status with status 'queued' should be a no-op:
    the function only persists status transitions that are meaningful. A 'queued'
    status written via fallback would be a regression, since the job is already
    queued."""
    from unittest.mock import MagicMock, patch

    job_id = uuid.uuid4()

    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.__enter__ = MagicMock(return_value=mock_session)
    mock_session_factory.__exit__ = MagicMock(return_value=False)
    mock_session_factory.return_value = mock_session_factory

    execute_result = MagicMock()
    execute_result.rowcount = 0
    mock_session.execute.return_value = execute_result

    with patch("backtestforecast.db.session.create_worker_session", mock_session_factory):
        from backtestforecast.events import _fallback_persist_status
        _fallback_persist_status("backtest", job_id, "queued")

    assert not mock_session.execute.called, (
        "'queued' is not a valid target status; function should return early "
        "without executing any SQL"
    )


# ---------------------------------------------------------------------------
# Item 79: _fallback_persist_status ignores non-terminal
# ---------------------------------------------------------------------------


def test_fallback_persist_status_ignores_non_terminal() -> None:
    """Non-terminal statuses like 'queued' or 'running' should be no-ops."""
    from backtestforecast.events import _VALID_TARGET_STATUSES

    assert "queued" not in _VALID_TARGET_STATUSES
    assert "running" not in _VALID_TARGET_STATUSES
    assert "succeeded" in _VALID_TARGET_STATUSES
    assert "failed" in _VALID_TARGET_STATUSES
