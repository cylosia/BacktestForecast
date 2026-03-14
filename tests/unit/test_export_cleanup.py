"""Tests for export cleanup edge cases."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest


def _make_export_job(*, storage_key: str | None = "s3://key", status: str = "succeeded"):
    job = MagicMock()
    job.id = uuid.uuid4()
    job.storage_key = storage_key
    job.status = status
    job.content_bytes = b"data"
    job.expires_at = datetime.now(UTC) - timedelta(days=1)
    return job


def test_cleanup_skips_job_when_s3_delete_fails() -> None:
    """When storage delete raises, the job should NOT be marked expired."""
    from backtestforecast.services.exports import ExportService

    job = _make_export_job(storage_key="s3://bucket/key")

    mock_session = MagicMock()
    service = ExportService.__new__(ExportService)
    service.session = mock_session
    service.exports = MagicMock()
    service.exports.list_expired_for_cleanup = MagicMock(side_effect=[[job], []])

    storage = MagicMock()
    storage.delete = MagicMock(side_effect=Exception("S3 failure"))
    service._storage = storage

    cleaned = service.cleanup_expired_exports(batch_size=10)

    assert cleaned == 0
    assert job.status == "succeeded"
    assert job.storage_key == "s3://bucket/key"


def test_cleanup_marks_expired_when_delete_succeeds() -> None:
    """When storage delete succeeds, the job should be marked expired."""
    from backtestforecast.services.exports import ExportService

    job = _make_export_job(storage_key="s3://bucket/key")

    mock_session = MagicMock()
    service = ExportService.__new__(ExportService)
    service.session = mock_session
    service.exports = MagicMock()
    service.exports.list_expired_for_cleanup = MagicMock(side_effect=[[job], []])

    storage = MagicMock()
    service._storage = storage

    cleaned = service.cleanup_expired_exports(batch_size=10)

    assert cleaned == 1
    assert job.status == "expired"
    assert job.storage_key is None
    assert job.content_bytes is None
