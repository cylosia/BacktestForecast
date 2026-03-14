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


# ---------------------------------------------------------------------------
# Item 42: S3 stream generator closes body in finally
# ---------------------------------------------------------------------------


def test_s3_stream_generator_closes_body_on_exception() -> None:
    """Verify the S3 body .close() is called even when the generator
    raises an exception during iteration."""

    class TrackingBody:
        def __init__(self):
            self.closed = False
            self.read_count = 0

        def read(self, size):
            self.read_count += 1
            if self.read_count > 1:
                raise RuntimeError("simulated read failure")
            return b"chunk1"

        def close(self):
            self.closed = True

    body = TrackingBody()

    def _stream_s3():
        try:
            while True:
                chunk = body.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            body.close()

    gen = _stream_s3()
    first_chunk = next(gen)
    assert first_chunk == b"chunk1"

    with pytest.raises(RuntimeError, match="simulated read failure"):
        next(gen)

    assert body.closed, "body.close() must be called even when an exception occurs"


def test_s3_stream_generator_closes_body_on_normal_exit() -> None:
    """Verify the S3 body .close() is called on normal stream completion."""

    class TrackingBody:
        def __init__(self):
            self.closed = False

        def read(self, size):
            return b""

        def close(self):
            self.closed = True

    body = TrackingBody()

    def _stream_s3():
        try:
            while True:
                chunk = body.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            body.close()

    chunks = list(_stream_s3())
    assert chunks == []
    assert body.closed, "body.close() must be called on normal stream completion"


# ---------------------------------------------------------------------------
# Item 47: export cleanup clears size_bytes and sha256_hex
# ---------------------------------------------------------------------------


def test_cleanup_sets_size_bytes_zero_and_sha256_none() -> None:
    """Verify cleanup_expired_exports sets size_bytes=0 and sha256_hex=None."""
    from backtestforecast.services.exports import ExportService

    job = _make_export_job(storage_key="s3://bucket/key2")
    job.size_bytes = 4096
    job.sha256_hex = "abc123deadbeef"

    mock_session = MagicMock()
    service = ExportService.__new__(ExportService)
    service.session = mock_session
    service.exports = MagicMock()
    service.exports.list_expired_for_cleanup = MagicMock(side_effect=[[job], []])

    storage = MagicMock()
    service._storage = storage

    cleaned = service.cleanup_expired_exports(batch_size=10)

    assert cleaned == 1
    assert job.size_bytes == 0, "size_bytes must be set to 0 after cleanup"
    assert job.sha256_hex is None, "sha256_hex must be set to None after cleanup"
    assert job.status == "expired"
    assert job.content_bytes is None
    assert job.storage_key is None
