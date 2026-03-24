"""Verify sweep service can create internal backtest requests with empty entry_rules.

This guards against the regression where min_length=1 on CreateBacktestRunRequest
broke all sweeps. The schema must allow empty entry_rules for internal use.
"""
from __future__ import annotations

from backtestforecast.schemas.backtests import CreateBacktestRunRequest


def test_sweep_internal_request_with_empty_rules():
    """Sweep/scan services must be able to create requests with no entry rules."""
    request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_call",
        start_date="2024-01-01",
        end_date="2024-06-01",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=2,
        commission_per_contract=1,
        entry_rules=[],
    )
    assert request.entry_rules == []
    assert request.symbol == "SPY"
