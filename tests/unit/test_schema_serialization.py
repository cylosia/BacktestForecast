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


# ---------------------------------------------------------------------------
# Item 75: validate_json_shape handles wheel force-close (no legs)
# ---------------------------------------------------------------------------


def test_validate_json_shape_wheel_force_close_no_legs():
    """A dict with 'phase' key but no 'legs' should NOT log missing-key warnings.
    The validator has a special-case: if 'phase' in data and 'legs' not in data → True."""
    from backtestforecast.schemas.json_shapes import _TRADE_DETAIL_REQUIRED_KEYS, validate_json_shape

    wheel_force_close = {
        "phase": "stock_inventory",
        "entry_mid": 100.0,
        "exit_mid": 105.0,
    }
    result = validate_json_shape(
        wheel_force_close,
        "BacktestTrade.detail_json",
        required_keys=_TRADE_DETAIL_REQUIRED_KEYS,
    )
    assert result is True, "Wheel force-close trade with phase but no legs should be valid"


def test_validate_json_shape_wheel_force_close_missing_entry_mid():
    """Even without legs, a 'phase' dict missing required keys should still
    pass because the phase-without-legs short-circuit returns True."""
    from backtestforecast.schemas.json_shapes import _TRADE_DETAIL_REQUIRED_KEYS, validate_json_shape

    data = {"phase": "covered_call"}
    result = validate_json_shape(
        data,
        "BacktestTrade.detail_json",
        required_keys=_TRADE_DETAIL_REQUIRED_KEYS,
    )
    assert result is True, "phase-only dict should short-circuit to True"
