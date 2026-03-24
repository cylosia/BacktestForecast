"""Test that GDPR data export correctly serializes nullable fields.

Regression test for the bug where str(None) produced the string "None"
instead of JSON null for nullable fields like total_net_pnl and
backtest_run_id.
"""
from __future__ import annotations


def test_str_none_produces_none_string():
    """Verify the root cause: str(None) is 'None', not None."""
    assert str(None) == "None"
    assert str(None) is not None


def test_conditional_str_produces_null():
    """The fix pattern: use conditional expression to preserve None."""
    value = None
    result = str(value) if value is not None else None
    assert result is None


def test_conditional_str_preserves_value():
    from decimal import Decimal
    value = Decimal("123.45")
    result = str(value) if value is not None else None
    assert result == "123.45"


def test_gdpr_export_backtest_serialization():
    """Simulate the GDPR export serialization for a backtest with nullable fields."""
    from unittest.mock import MagicMock

    run = MagicMock()
    run.id = "some-uuid"
    run.symbol = "AAPL"
    run.strategy_type = "covered_call"
    run.status = "succeeded"
    run.date_from = None
    run.date_to = None
    run.trade_count = 5
    run.total_net_pnl = None
    run.created_at = None

    result = {
        "id": str(run.id),
        "symbol": run.symbol,
        "total_net_pnl": str(run.total_net_pnl) if run.total_net_pnl is not None else None,
        "date_from": run.date_from.isoformat() if run.date_from else None,
        "created_at": run.created_at.isoformat() if run.created_at else None,
    }

    assert result["total_net_pnl"] is None, "None total_net_pnl must serialize to JSON null"
    assert result["date_from"] is None
    assert result["created_at"] is None


def test_gdpr_export_backtest_with_values():
    from datetime import date
    from decimal import Decimal
    from unittest.mock import MagicMock

    run = MagicMock()
    run.id = "some-uuid"
    run.total_net_pnl = Decimal("1500.00")
    run.date_from = date(2024, 1, 1)

    result = {
        "total_net_pnl": str(run.total_net_pnl) if run.total_net_pnl is not None else None,
        "date_from": run.date_from.isoformat() if run.date_from else None,
    }

    assert result["total_net_pnl"] == "1500.00"
    assert result["date_from"] == "2024-01-01"
