"""Verify that entry_rules enforcement works correctly.

The schema allows empty entry_rules (needed by sweep/scan services internally).
The API router enforces at least one rule for user-facing backtest creation.
"""
from __future__ import annotations

import pytest

from backtestforecast.schemas.backtests import CreateBacktestRunRequest


def test_empty_entry_rules_allowed_at_schema_level():
    """Sweep/scan services need to create internal requests with no entry rules."""
    req = CreateBacktestRunRequest(
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
    assert len(req.entry_rules) == 0


def test_one_entry_rule_accepted():
    req = CreateBacktestRunRequest(
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
        entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 30, "period": 14}],
    )
    assert len(req.entry_rules) == 1


def test_router_rejects_empty_entry_rules(client, auth_headers):
    """The create backtest endpoint enforces at least one entry rule."""
    response = client.post(
        "/v1/backtests",
        json={
            "symbol": "SPY",
            "strategy_type": "long_call",
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 10,
            "account_size": 10000,
            "risk_per_trade_pct": 2,
            "commission_per_contract": 1,
            "entry_rules": [],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422
