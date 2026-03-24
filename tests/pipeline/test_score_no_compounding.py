"""Item 96: Verify that pipeline score adjustment doesn't compound.

The stage-5 forecast overlay adjusts ``candidate.score`` by multiplying by
fixed factors (e.g. 1.2 for directional alignment, 1.0 + (rate - 60)/200
for positive outcome rate). If the adjustment code were accidentally
called twice on the same candidate, the score would compound, producing a
different result.

This test verifies that the adjustment logic produces the same final score
regardless of whether it's "applied once" or if we reset and reapply - i.e.
the function is not mutating shared state in a way that compounds.
"""

from __future__ import annotations

from datetime import date


def test_score_adjustment_does_not_compound() -> None:
    """Calling _stage5_forecast_and_rank twice on the same candidates
    should not produce different scores if the candidates are fresh copies."""
    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import FullBacktestResult

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.NEUTRAL]),
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
            max_holding_days=30,
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

    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.service import NightlyPipelineService

    session = MagicMock()
    service = NightlyPipelineService(
        session,
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
        forecaster=MockForecaster(),
    )

    executor = ThreadPoolExecutor(max_workers=2)
    try:
        initial_score = 10.0
        candidates_pass1 = [_make_candidate(initial_score)]
        result1 = service._stage5_forecast_and_rank(
            candidates_pass1, date(2025, 6, 1), executor=executor
        )
        score_after_pass1 = result1[0].score

        candidates_pass2 = [_make_candidate(initial_score)]
        result2 = service._stage5_forecast_and_rank(
            candidates_pass2, date(2025, 6, 1), executor=executor
        )
        score_after_pass2 = result2[0].score

        assert score_after_pass1 == score_after_pass2, (
            f"Score after first pass ({score_after_pass1}) != second pass ({score_after_pass2}). "
            "The forecast overlay may be compounding score adjustments."
        )
        assert score_after_pass1 != initial_score, (
            "Score should have been adjusted by the forecast overlay"
        )
    finally:
        executor.shutdown(wait=True)


def test_stage5_forecast_uses_max_holding_days_cap() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import FullBacktestResult, NightlyPipelineService

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.NEUTRAL]),
    )
    captured: dict[str, int] = {}

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            captured["horizon_days"] = horizon_days
            return {
                "expected_return_median_pct": 1.0,
                "positive_outcome_rate_pct": 55.0,
            }

    service = NightlyPipelineService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
        forecaster=MockForecaster(),
    )
    candidate = FullBacktestResult(
        symbol="TEST",
        strategy_type="long_call",
        regime=regime,
        close_price=100.0,
        target_dte=45,
        max_holding_days=14,
        config_snapshot={"target_dte": 45, "max_holding_days": 14},
        summary={"total_roi_pct": 3.0, "trade_count": 10},
        trades_json=[],
        equity_curve_json=[],
        score=1.0,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        service._stage5_forecast_and_rank([candidate], date(2025, 6, 1), executor=executor)

    assert captured["horizon_days"] == 14


def test_stage5_neutral_strategy_ignores_directional_positive_rate_bonus() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import FullBacktestResult, NightlyPipelineService

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.NEUTRAL]),
    )

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": 4.0,
                "positive_outcome_rate_pct": 90.0,
            }

    service = NightlyPipelineService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
        forecaster=MockForecaster(),
    )
    candidate = FullBacktestResult(
        symbol="TEST",
        strategy_type="iron_condor",
        regime=regime,
        close_price=100.0,
        target_dte=30,
        max_holding_days=14,
        config_snapshot={"target_dte": 30, "max_holding_days": 14},
        summary={"total_roi_pct": 3.0, "trade_count": 10},
        trades_json=[],
        equity_curve_json=[],
        score=1.0,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        ranked = service._stage5_forecast_and_rank([candidate], date(2025, 6, 1), executor=executor)

    assert ranked[0].score == 1.0


def test_stage5_bearish_strategy_uses_strategy_aware_positive_outcome_rate_directly() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import FullBacktestResult, NightlyPipelineService

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.BEARISH]),
    )

    class MockForecaster:
        def __init__(self, positive_outcome_rate_pct: float) -> None:
            self._rate = positive_outcome_rate_pct

        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": -4.0,
                "positive_outcome_rate_pct": self._rate,
            }

    def _run(rate: float) -> float:
        service = NightlyPipelineService(
            MagicMock(),
            market_data_fetcher=MagicMock(),
            backtest_executor=MagicMock(),
            forecaster=MockForecaster(rate),
        )
        candidate = FullBacktestResult(
            symbol="TEST",
            strategy_type="bear_put_debit_spread",
            regime=regime,
            close_price=100.0,
            target_dte=30,
            max_holding_days=14,
            config_snapshot={"target_dte": 30, "max_holding_days": 14},
            summary={"total_roi_pct": 3.0, "trade_count": 10},
            trades_json=[],
            equity_curve_json=[],
            score=1.0,
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            ranked = service._stage5_forecast_and_rank([candidate], date(2025, 6, 1), executor=executor)
        return ranked[0].score

    assert _run(80.0) > _run(40.0)


def test_stage5_supportive_forecast_does_not_make_negative_score_more_negative() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import FullBacktestResult, NightlyPipelineService

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.BULLISH]),
    )

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": 4.0,
                "positive_outcome_rate_pct": 80.0,
            }

    service = NightlyPipelineService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
        forecaster=MockForecaster(),
    )
    candidate = FullBacktestResult(
        symbol="TEST",
        strategy_type="long_call",
        regime=regime,
        close_price=100.0,
        target_dte=30,
        max_holding_days=14,
        config_snapshot={"target_dte": 30, "max_holding_days": 14},
        summary={"total_roi_pct": 3.0, "trade_count": 10},
        trades_json=[],
        equity_curve_json=[],
        score=-10.0,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        ranked = service._stage5_forecast_and_rank([candidate], date(2025, 6, 1), executor=executor)

    assert ranked[0].score > -10.0


def test_pipeline_stage3_selection_retains_non_positive_candidates_for_forecast_stage() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.scoring import compute_backtest_score
    from backtestforecast.pipeline.service import NightlyPipelineService, SymbolStrategyPair

    regime = RegimeSnapshot(
        symbol="TEST",
        close_price=100.0,
        regimes=frozenset([Regime.BULLISH]),
    )
    executor = MagicMock()
    executor.run_quick_backtest.return_value = {
        "trade_count": 8,
        "decided_trades": 1,
        "win_rate": 100.0,
        "total_roi_pct": 2.0,
        "total_net_pnl": 200.0,
        "max_drawdown_pct": 40.0,
        "sharpe_ratio": -1.0,
    }
    service = NightlyPipelineService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=executor,
        forecaster=None,
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        results = service._stage3_quick_backtest(
            [SymbolStrategyPair(symbol="TEST", strategy_type="long_call", regime=regime, close_price=100.0)],
            date(2025, 6, 1),
            executor=pool,
        )

    assert results
    long_call_result = next(result for result in results if result.strategy_type == "long_call")
    assert long_call_result.score == compute_backtest_score(executor.run_quick_backtest.return_value)
