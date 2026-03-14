"""Item 96: Verify that pipeline score adjustment doesn't compound.

The stage-5 forecast overlay adjusts ``candidate.score`` by multiplying by
fixed factors (e.g. 1.2 for directional alignment, 1.0 + (rate - 60)/200
for positive outcome rate). If the adjustment code were accidentally
called twice on the same candidate, the score would compound, producing a
different result.

This test verifies that the adjustment logic produces the same final score
regardless of whether it's "applied once" or if we reset and reapply — i.e.
the function is not mutating shared state in a way that compounds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


def test_score_adjustment_does_not_compound() -> None:
    """Calling _stage5_forecast_and_rank twice on the same candidates
    should not produce different scores if the candidates are fresh copies."""
    from backtestforecast.pipeline.service import FullBacktestResult
    from backtestforecast.pipeline.regime import RegimeSnapshot, RegimeLabel

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([RegimeLabel.NEUTRAL]),
        indicators={},
    )
    summary = {
        "total_roi_pct": 15.0,
        "win_rate": 70.0,
        "max_drawdown_pct": 5.0,
        "trade_count": 20,
    }

    def _make_candidate(score: float) -> FullBacktestResult:
        return FullBacktestResult(
            symbol="TEST",
            strategy_type="bull_put_credit_spread",
            regime=regime,
            close_price=100.0,
            target_dte=30,
            config_snapshot={},
            summary=summary,
            trades_json=[],
            equity_curve_json=[],
            score=score,
        )

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": 5.0,
                "positive_outcome_rate_pct": 65.0,
            }

    from unittest.mock import MagicMock
    from backtestforecast.pipeline.service import NightlyPipelineService

    session = MagicMock()
    service = NightlyPipelineService(
        session,
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
        forecaster=MockForecaster(),
    )

    initial_score = 10.0
    candidates_pass1 = [_make_candidate(initial_score)]
    result1 = service._stage5_forecast_and_rank(candidates_pass1, date(2025, 6, 1))
    score_after_pass1 = result1[0].score

    candidates_pass2 = [_make_candidate(initial_score)]
    result2 = service._stage5_forecast_and_rank(candidates_pass2, date(2025, 6, 1))
    score_after_pass2 = result2[0].score

    assert score_after_pass1 == score_after_pass2, (
        f"Score after first pass ({score_after_pass1}) != second pass ({score_after_pass2}). "
        "The forecast overlay may be compounding score adjustments."
    )
    assert score_after_pass1 != initial_score, (
        "Score should have been adjusted by the forecast overlay"
    )
