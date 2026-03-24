from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock


def test_deep_analysis_forecast_uses_max_holding_days_cap() -> None:
    from backtestforecast.pipeline.deep_analysis import LandscapeCell, SymbolDeepAnalysisService

    captured: dict[str, int] = {}

    class MockForecaster:
        def get_forecast(self, *, symbol, strategy_type, horizon_days, as_of_date=None):
            captured["horizon_days"] = horizon_days
            return {
                "expected_return_median_pct": 2.0,
                "positive_outcome_rate_pct": 58.0,
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
                score=1.0,
            )
        ],
    )

    assert results
    assert captured["horizon_days"] == 14


def test_deep_analysis_landscape_uses_finite_drawdown_for_displayed_stats() -> None:
    from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService

    executor = MagicMock()
    executor.run_quick_backtest.return_value = {
        "trade_count": 5,
        "win_rate": 60.0,
        "total_roi_pct": 9.0,
        "max_drawdown_pct": float("nan"),
        "sharpe_ratio": 1.1,
    }
    service = SymbolDeepAnalysisService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=executor,
        forecaster=None,
    )

    cells = service._build_landscape("AAPL", date(2025, 6, 1))

    assert cells
    assert cells[0].max_drawdown_pct == 50.0


def test_deep_analysis_landscape_skips_zero_trade_cells() -> None:
    from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService

    executor = MagicMock()
    executor.run_quick_backtest.return_value = {
        "trade_count": 0,
        "win_rate": 100.0,
        "total_roi_pct": 9.0,
        "max_drawdown_pct": 1.0,
        "sharpe_ratio": 1.1,
    }
    service = SymbolDeepAnalysisService(
        MagicMock(),
        market_data_fetcher=MagicMock(),
        backtest_executor=executor,
        forecaster=None,
    )

    cells = service._build_landscape("AAPL", date(2025, 6, 1))

    assert cells == []


def test_deep_analysis_top_candidates_are_not_filtered_by_positive_pre_forecast_score() -> None:
    from backtestforecast.pipeline.deep_analysis import LandscapeCell

    landscape = [
        LandscapeCell(
            strategy_type="long_call",
            strategy_label="Long Call",
            target_dte=30,
            max_holding_days=14,
            config_snapshot={"target_dte": 30, "max_holding_days": 14},
            trade_count=8,
            win_rate=100.0,
            total_roi_pct=2.0,
            max_drawdown_pct=40.0,
            score=-5.0,
        ),
        LandscapeCell(
            strategy_type="bear_put_debit_spread",
            strategy_label="Bear Put Debit Spread",
            target_dte=30,
            max_holding_days=14,
            config_snapshot={"target_dte": 30, "max_holding_days": 14},
            trade_count=8,
            win_rate=55.0,
            total_roi_pct=6.0,
            max_drawdown_pct=8.0,
            score=1.0,
        ),
    ]

    seen_strategies: set[str] = set()
    top_candidates: list[LandscapeCell] = []
    for cell in landscape:
        if cell.strategy_type not in seen_strategies:
            seen_strategies.add(cell.strategy_type)
            top_candidates.append(cell)
        if len(top_candidates) >= 10:
            break

    assert [cell.strategy_type for cell in top_candidates] == [
        "long_call",
        "bear_put_debit_spread",
    ]
