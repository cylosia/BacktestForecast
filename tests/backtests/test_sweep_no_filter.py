"""Verify that sweeps with empty entry_rules ("no_filter") work correctly.

Sweeps intentionally use empty entry_rules to test parameter combinations
without signal-based filtering. This test guards against regressions where
empty entry_rules are rejected at the schema level.
"""
from __future__ import annotations

from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.schemas.scans import RuleSetDefinition
from backtestforecast.schemas.sweeps import CreateSweepRequest


def test_create_backtest_request_allows_empty_entry_rules():
    """Internal backtest requests (used by sweep/scan services) must accept empty entry_rules."""
    request = CreateBacktestRunRequest(
        symbol="TSLA",
        strategy_type="bull_put_credit_spread",
        start_date="2024-01-01",
        end_date="2024-06-01",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=5,
        commission_per_contract=0.65,
        entry_rules=[],
    )
    assert request.entry_rules == []


def test_rule_set_definition_allows_empty_entry_rules():
    """RuleSetDefinition must accept empty entry_rules for sweep 'no_filter' pattern."""
    rule_set = RuleSetDefinition(name="no_filter", entry_rules=[])
    assert rule_set.entry_rules == []
    assert rule_set.name == "no_filter"


def test_create_sweep_request_accepts_no_filter_rule_set():
    """The sweep schema must accept rule sets with empty entry_rules."""
    payload = CreateSweepRequest(
        symbol="SPY",
        strategy_types=["bull_put_credit_spread"],
        start_date="2024-01-01",
        end_date="2024-06-01",
        target_dte=30,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=5,
        commission_per_contract=0.65,
        entry_rule_sets=[{"name": "no_filter", "entry_rules": []}],
    )
    assert len(payload.entry_rule_sets) == 1
    assert payload.entry_rule_sets[0].entry_rules == []
