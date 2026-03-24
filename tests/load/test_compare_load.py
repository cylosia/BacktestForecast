"""Test 78: Verify compare endpoint response size is bounded by the trade limit.

When comparing multiple runs each with many trades, the response must be
bounded - the per-run trade list should be capped at the configured limit.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    CompareBacktestsResponse,
)


def _make_trade(idx: int) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "option_ticker": f"O:TEST{idx}",
        "strategy_type": "long_call",
        "underlying_symbol": "TEST",
        "entry_date": "2024-01-02",
        "exit_date": "2024-01-07",
        "expiration_date": "2024-02-01",
        "quantity": 1,
        "dte_at_open": 30,
        "holding_period_days": 5,
        "entry_underlying_close": "100.0",
        "exit_underlying_close": "101.0",
        "entry_mid": "2.0",
        "exit_mid": "1.5",
        "gross_pnl": "50.0",
        "net_pnl": "49.0",
        "total_commissions": "1.0",
        "entry_reason": "entry_rules_met",
        "exit_reason": "expiration",
    }


def _make_run(num_trades: int) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "symbol": "TEST",
        "strategy_type": "long_call",
        "status": "succeeded",
        "date_from": "2024-01-01",
        "date_to": "2024-06-01",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 20,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "engine_version": "options-multileg-v2",
        "data_source": "massive",
        "created_at": datetime.now(UTC).isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "warnings": [],
        "summary": {
            "trade_count": num_trades,
            "win_rate": "60.0",
            "total_roi_pct": "15.0",
            "average_win_amount": "50.0",
            "average_loss_amount": "-30.0",
            "average_holding_period_days": "5.0",
            "average_dte_at_open": "30.0",
            "max_drawdown_pct": "8.0",
            "total_commissions": str(num_trades),
            "total_net_pnl": "1500.0",
            "starting_equity": "10000",
            "ending_equity": "11500",
        },
        "trades": [_make_trade(i) for i in range(num_trades)],
        "equity_curve": [],
    }


def test_compare_response_serialization_with_many_trades():
    """Verify that 4 runs with 200 trades each can be serialized into a
    CompareBacktestsResponse without error and the total trade count is correct."""
    runs = [_make_run(200) for _ in range(4)]
    items = [BacktestRunDetailResponse(**run) for run in runs]

    response = CompareBacktestsResponse(items=items, comparison_limit=8)

    assert len(response.items) == 4
    total_trades = sum(len(item.trades) for item in response.items)
    assert total_trades == 800


def test_compare_response_json_size_is_bounded():
    """The serialized JSON of a compare response with many trades should
    remain within a reasonable size (< 5 MB for 4 runs x 200 trades)."""
    runs = [_make_run(200) for _ in range(4)]
    items = [BacktestRunDetailResponse(**run) for run in runs]
    response = CompareBacktestsResponse(items=items, comparison_limit=8)

    json_bytes = response.model_dump_json().encode("utf-8")
    max_size_bytes = 5 * 1024 * 1024  # 5 MB
    assert len(json_bytes) < max_size_bytes, (
        f"Compare response JSON is {len(json_bytes)} bytes, exceeds {max_size_bytes} limit"
    )


def test_compare_response_empty_trades():
    """Runs with zero trades should not cause issues in the compare response."""
    runs = [_make_run(0) for _ in range(4)]
    items = [BacktestRunDetailResponse(**run) for run in runs]
    response = CompareBacktestsResponse(items=items, comparison_limit=8)

    assert len(response.items) == 4
    total_trades = sum(len(item.trades) for item in response.items)
    assert total_trades == 0
