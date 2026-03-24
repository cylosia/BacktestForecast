from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import MagicMock


def test_pipeline_quick_and_full_backtests_share_the_same_ranking_formula() -> None:
    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.scoring import compute_backtest_score
    from backtestforecast.pipeline.service import NightlyPipelineService, QuickBacktestResult, SymbolStrategyPair

    summary = {
        "trade_count": 12,
        "win_rate": 62.5,
        "total_roi_pct": 18.0,
        "total_net_pnl": 900.0,
        "max_drawdown_pct": 7.0,
        "sharpe_ratio": 1.4,
    }
    regime = RegimeSnapshot(symbol="AAPL", close_price=100.0, regimes=frozenset([Regime.BULLISH]))

    executor = MagicMock()
    executor.run_quick_backtest.return_value = summary
    executor.run_full_backtest.return_value = {**summary, "trades": [], "equity_curve": []}
    service = NightlyPipelineService(MagicMock(), market_data_fetcher=MagicMock(), backtest_executor=executor, forecaster=None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        quick_results = service._stage3_quick_backtest(
            [SymbolStrategyPair(symbol="AAPL", strategy_type="long_call", regime=regime, close_price=100.0)],
            date(2025, 1, 31),
            executor=pool,
        )
        full_results = service._stage4_full_backtest(
            [
                QuickBacktestResult(
                    symbol="AAPL",
                    strategy_type="long_call",
                    regime=regime,
                    close_price=100.0,
                    target_dte=30,
                    config_snapshot={"target_dte": 30, "strategy_overrides": None},
                    trade_count=summary["trade_count"],
                    win_rate=summary["win_rate"],
                    total_roi_pct=summary["total_roi_pct"],
                    net_pnl=summary["total_net_pnl"],
                    max_drawdown_pct=summary["max_drawdown_pct"],
                    score=0.0,
                )
            ],
            date(2025, 1, 31),
            executor=pool,
        )

    expected_score = compute_backtest_score(summary)
    assert quick_results
    assert len(full_results) == 1
    assert {result.score for result in quick_results} == {expected_score}
    assert full_results[0].score == expected_score
