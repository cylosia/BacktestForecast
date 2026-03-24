from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch


def test_genetic_fitness_uses_standard_sweep_scoring() -> None:
    from backtestforecast.services import sweep_genetic_runtime as runtime

    runtime._RUNTIME.clear()
    runtime._RUNTIME.update({
        "payload": SimpleNamespace(
            symbol="AAPL",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 6, 1),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=14,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0.65,
            entry_rule_sets=[],
            exit_rule_sets=[],
            slippage_pct=0,
        ),
        "strategy_type": "long_call",
        "bundle": object(),
        "execution_service": SimpleNamespace(
            execute_request=lambda request, bundle=None: SimpleNamespace(
                summary=SimpleNamespace(
                    trade_count=12,
                    decided_trades=2,
                    win_rate=61.0,
                    total_roi_pct=9.5,
                    sharpe_ratio=1.25,
                    max_drawdown_pct=4.0,
                )
            )
        ),
        "started_at": 0.0,
        "timeout_seconds": 999999.0,
    })

    with patch("backtestforecast.services.sweep_genetic_runtime._time.monotonic", return_value=1.0), \
         patch("backtestforecast.services.sweeps.SweepService._score_candidate_from_summary", return_value=123.4) as scorer:
        score = runtime.evaluate_sweep_individual([])

    assert score == 123.4
    scorer.assert_called_once_with({
        "trade_count": 12,
        "decided_trades": 2,
        "win_rate": 61.0,
        "total_roi_pct": 9.5,
        "sharpe_ratio": 1.25,
        "max_drawdown_pct": 4.0,
    })


def test_standard_sweep_scoring_uses_decided_trades_for_min_trade_gate() -> None:
    from backtestforecast.services.sweeps import SweepService

    cfg = {
        "win_rate_weight": 0.25,
        "roi_weight": 0.35,
        "sharpe_weight": 0.20,
        "drawdown_weight": 0.20,
        "sharpe_multiplier": 2.0,
        "min_trades": 3,
    }

    blocked = SweepService._score_candidate_from_summary(
        {
            "trade_count": 12,
            "decided_trades": 2,
            "win_rate": 61.0,
            "total_roi_pct": 9.5,
            "sharpe_ratio": 1.25,
            "max_drawdown_pct": 4.0,
        },
        cfg,
    )
    allowed = SweepService._score_candidate_from_summary(
        {
            "trade_count": 12,
            "decided_trades": 3,
            "win_rate": 61.0,
            "total_roi_pct": 9.5,
            "sharpe_ratio": 1.25,
            "max_drawdown_pct": 4.0,
        },
        cfg,
    )

    assert blocked == 0.0
    assert allowed > 0.0
