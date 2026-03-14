"""Unit tests for Pydantic schema serialization edge cases."""
from __future__ import annotations

from datetime import UTC, datetime


def test_pipeline_history_response_includes_next_cursor():
    from backtestforecast.schemas.analysis import PipelineHistoryResponse

    data = {"items": [], "next_cursor": "2026-01-01T00:00:00"}
    resp = PipelineHistoryResponse(**data)
    assert resp.next_cursor == "2026-01-01T00:00:00"


def test_pipeline_history_response_next_cursor_defaults_to_none():
    from backtestforecast.schemas.analysis import PipelineHistoryResponse

    resp = PipelineHistoryResponse(items=[])
    assert resp.next_cursor is None


def test_export_job_response_includes_expires_at():
    from backtestforecast.schemas.exports import ExportJobResponse
    from uuid import uuid4

    now = datetime.now(UTC)
    resp = ExportJobResponse(
        id=uuid4(),
        run_id=uuid4(),
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        created_at=now,
        expires_at=now,
    )
    assert resp.expires_at == now


def test_export_job_response_expires_at_defaults_to_none():
    from backtestforecast.schemas.exports import ExportJobResponse
    from uuid import uuid4

    resp = ExportJobResponse(
        id=uuid4(),
        run_id=uuid4(),
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        created_at=datetime.now(UTC),
    )
    assert resp.expires_at is None
