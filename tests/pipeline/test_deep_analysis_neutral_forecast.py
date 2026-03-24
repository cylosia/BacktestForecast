from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock


def test_deep_analysis_neutral_strategy_ignores_directional_positive_rate_bonus() -> None:
    from backtestforecast.pipeline.deep_analysis import LandscapeCell, SymbolDeepAnalysisService
    from backtestforecast.pipeline.scoring import compute_backtest_score

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": 4.0,
                "positive_outcome_rate_pct": 90.0,
            }

    executor = MagicMock()
    executor.run_full_backtest.return_value = {
        "trade_count": 12,
        "win_rate": 60.0,
        "total_roi_pct": 8.0,
        "max_drawdown_pct": 6.0,
        "sharpe_ratio": 1.3,
        "trades": [],
        "equity_curve": [],
    }
    service = SymbolDeepAnalysisService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=executor,
        forecaster=MockForecaster(),
    )

    results = service._deep_dive(
        "AAPL",
        date(2025, 6, 1),
        [
            LandscapeCell(
                strategy_type="iron_condor",
                strategy_label="Iron Condor",
                target_dte=45,
                max_holding_days=14,
                config_snapshot={"target_dte": 45, "max_holding_days": 14},
                trade_count=12,
                win_rate=60.0,
                total_roi_pct=8.0,
                max_drawdown_pct=6.0,
                score=1.0,
            )
        ],
    )

    assert results
    assert results[0].score == compute_backtest_score(executor.run_full_backtest.return_value)


def test_deep_analysis_bearish_strategy_uses_strategy_aware_positive_outcome_rate_directly() -> None:
    from backtestforecast.pipeline.deep_analysis import LandscapeCell, SymbolDeepAnalysisService

    class MockForecaster:
        def __init__(self, positive_outcome_rate_pct: float) -> None:
            self._rate = positive_outcome_rate_pct

        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": -4.0,
                "positive_outcome_rate_pct": self._rate,
            }

    def _run(rate: float) -> float:
        executor = MagicMock()
        executor.run_full_backtest.return_value = {
            "trade_count": 12,
            "win_rate": 60.0,
            "total_roi_pct": 8.0,
            "max_drawdown_pct": 6.0,
            "sharpe_ratio": 1.3,
            "trades": [],
            "equity_curve": [],
        }
        service = SymbolDeepAnalysisService(
            MagicMock(),
            market_data_fetcher=MagicMock(),
            backtest_executor=executor,
            forecaster=MockForecaster(rate),
        )
        results = service._deep_dive(
            "AAPL",
            date(2025, 6, 1),
            [
                LandscapeCell(
                    strategy_type="bear_put_debit_spread",
                    strategy_label="Bear Put Debit Spread",
                    target_dte=45,
                    max_holding_days=14,
                    config_snapshot={"target_dte": 45, "max_holding_days": 14},
                    trade_count=12,
                    win_rate=60.0,
                    total_roi_pct=8.0,
                    max_drawdown_pct=6.0,
                    score=1.0,
                )
            ],
        )
        assert results
        return results[0].score

    assert _run(80.0) > _run(40.0)


def test_deep_analysis_supportive_forecast_does_not_make_negative_score_more_negative() -> None:
    from backtestforecast.pipeline.deep_analysis import LandscapeCell, SymbolDeepAnalysisService

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            return {
                "expected_return_median_pct": 4.0,
                "positive_outcome_rate_pct": 80.0,
            }

    executor = MagicMock()
    executor.run_full_backtest.return_value = {
        "trade_count": 12,
        "win_rate": 60.0,
        "total_roi_pct": 8.0,
        "max_drawdown_pct": 6.0,
        "sharpe_ratio": 1.3,
        "trades": [],
        "equity_curve": [],
    }
    service = SymbolDeepAnalysisService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=executor,
        forecaster=MockForecaster(),
    )

    results = service._deep_dive(
        "AAPL",
        date(2025, 6, 1),
        [
            LandscapeCell(
                strategy_type="long_call",
                strategy_label="Long Call",
                target_dte=45,
                max_holding_days=14,
                config_snapshot={"target_dte": 45, "max_holding_days": 14},
                trade_count=12,
                win_rate=60.0,
                total_roi_pct=8.0,
                max_drawdown_pct=6.0,
                score=-10.0,
            )
        ],
    )

    assert results
    assert results[0].score > -10.0
