"""Unit tests for Pydantic schema serialization edge cases."""
from __future__ import annotations

from datetime import UTC, datetime


def test_pipeline_history_response_includes_next_cursor():
    from backtestforecast.schemas.analysis import PipelineHistoryResponse

    data = {"items": [], "total": 4, "offset": 0, "limit": 2, "next_cursor": "2026-01-01T00:00:00"}
    resp = PipelineHistoryResponse(**data)
    assert resp.next_cursor == "2026-01-01T00:00:00"
    assert resp.total == 4


def test_pipeline_history_response_next_cursor_defaults_to_none():
    from backtestforecast.schemas.analysis import PipelineHistoryResponse

    resp = PipelineHistoryResponse(items=[])
    assert resp.next_cursor is None
    assert resp.total == 0


def test_export_job_response_includes_expires_at():
    from uuid import uuid4

    from backtestforecast.schemas.exports import ExportJobResponse

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
    from uuid import uuid4

    from backtestforecast.schemas.exports import ExportJobResponse

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
    The validator has a special-case: if 'phase' in data and 'legs' not in data -> True."""
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
    """A dict with 'phase' and 'entry_date' but no 'legs' should short-circuit
    to True.  Without 'entry_date' the short-circuit does not apply and
    missing required keys are flagged."""
    from backtestforecast.schemas.json_shapes import _TRADE_DETAIL_REQUIRED_KEYS, validate_json_shape

    data_with_entry_date = {"phase": "covered_call", "entry_date": "2025-03-14"}
    result = validate_json_shape(
        data_with_entry_date,
        "BacktestTrade.detail_json",
        required_keys=_TRADE_DETAIL_REQUIRED_KEYS,
    )
    assert result is True, "phase + entry_date dict should short-circuit to True"

    data_without_entry_date = {"phase": "covered_call"}
    result2 = validate_json_shape(
        data_without_entry_date,
        "BacktestTrade.detail_json",
        required_keys=_TRADE_DETAIL_REQUIRED_KEYS,
    )
    assert result2 is False, "phase-only dict without entry_date should fail validation"


def test_serialize_trade_roundtrips_through_trade_json_response():
    """Verify serialize_trade output is compatible with TradeJsonResponse (no id field needed)."""
    from datetime import date
    from types import SimpleNamespace

    from backtestforecast.schemas.backtests import TradeJsonResponse
    from backtestforecast.services.serialization import serialize_trade

    trade = SimpleNamespace(
        option_ticker="O:AAPL250321C00170000",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2025, 1, 2),
        exit_date=date(2025, 1, 15),
        expiration_date=date(2025, 3, 21),
        quantity=1,
        dte_at_open=78,
        holding_period_days=13,
        entry_underlying_close=170.0,
        exit_underlying_close=175.0,
        entry_mid=5.50,
        exit_mid=8.20,
        gross_pnl=270.0,
        net_pnl=268.70,
        total_commissions=1.30,
        entry_reason="signal",
        exit_reason="profit_target",
        detail_json={"legs": [{"type": "call", "strike": 170}]},
    )

    serialized = serialize_trade(trade)
    response = TradeJsonResponse.model_validate(serialized)
    assert response.option_ticker == "O:AAPL250321C00170000"
    assert response.net_pnl > 0
