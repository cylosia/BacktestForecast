from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from types import SimpleNamespace
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
                    max_holding_days=30,
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


def test_pipeline_adapter_preserves_sharpe_ratio_in_quick_and_full_summaries() -> None:
    from backtestforecast.pipeline.adapters import PipelineBacktestExecutor

    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.return_value = object()
    execution_service.execute_request.return_value = SimpleNamespace(
        summary=SimpleNamespace(
            trade_count=12,
            decided_trades=12,
            win_rate=62.5,
            total_roi_pct=18.0,
            total_net_pnl=900.0,
            max_drawdown_pct=7.0,
            sharpe_ratio=1.4,
            average_holding_period_days=9.0,
            starting_equity=10_000.0,
            ending_equity=11_800.0,
        ),
        trades=[],
        equity_curve=[],
        warnings=[],
    )

    executor = PipelineBacktestExecutor(execution_service=execution_service)
    quick = executor.run_quick_backtest(
        "AAPL",
        "long_call",
        date(2025, 1, 1),
        date(2025, 3, 1),
        30,
    )
    full = executor.run_full_backtest(
        "AAPL",
        "long_call",
        date(2025, 1, 1),
        date(2025, 3, 1),
        30,
    )

    assert quick is not None
    assert full is not None
    assert quick["sharpe_ratio"] == 1.4
    assert full["sharpe_ratio"] == 1.4
    assert quick["decided_trades"] == 12
    assert full["decided_trades"] == 12


def test_pipeline_adapter_serializes_trade_net_pnl_safely() -> None:
    from backtestforecast.pipeline.adapters import PipelineBacktestExecutor

    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.return_value = object()
    execution_service.execute_request.return_value = SimpleNamespace(
        summary=SimpleNamespace(
            trade_count=1,
            decided_trades=1,
            win_rate=100.0,
            total_roi_pct=5.0,
            total_net_pnl=500.0,
            max_drawdown_pct=4.0,
            sharpe_ratio=1.0,
            average_holding_period_days=6.0,
            starting_equity=10_000.0,
            ending_equity=10_500.0,
        ),
        trades=[
            SimpleNamespace(
                option_ticker="AAPL250321C00100000",
                strategy_type="long_call",
                underlying_symbol="AAPL",
                entry_date=date(2025, 1, 2),
                exit_date=date(2025, 1, 9),
                expiration_date=date(2025, 3, 21),
                quantity=1,
                dte_at_open=30,
                holding_period_days=7,
                entry_underlying_close=100.0,
                exit_underlying_close=105.0,
                entry_mid=2.5,
                exit_mid=3.0,
                gross_pnl=50.0,
                net_pnl=float("inf"),
                total_commissions=1.0,
                entry_reason="signal",
                exit_reason="target",
                detail_json={},
            )
        ],
        equity_curve=[],
        warnings=[],
    )

    executor = PipelineBacktestExecutor(execution_service=execution_service)
    full = executor.run_full_backtest(
        "AAPL",
        "long_call",
        date(2025, 1, 1),
        date(2025, 3, 1),
        30,
    )

    assert full is not None
    assert full["trades"][0]["net_pnl"] == 0.0


def test_pipeline_score_sanitizes_non_finite_metrics() -> None:
    from backtestforecast.pipeline.scoring import compute_backtest_score

    summary = {
        "trade_count": 12,
        "win_rate": float("nan"),
        "total_roi_pct": float("inf"),
        "max_drawdown_pct": float("nan"),
        "sharpe_ratio": float("-inf"),
    }

    assert compute_backtest_score(summary) == -10.0


def test_pipeline_score_uses_decided_trades_for_sample_factor() -> None:
    from backtestforecast.pipeline.scoring import compute_backtest_score

    lightly_decided = {
        "trade_count": 20,
        "decided_trades": 2,
        "win_rate": 100.0,
        "total_roi_pct": 12.0,
        "max_drawdown_pct": 4.0,
        "sharpe_ratio": 1.0,
    }
    fully_decided = {**lightly_decided, "decided_trades": 20}

    assert compute_backtest_score(lightly_decided) < compute_backtest_score(fully_decided)


def test_pipeline_quick_backtest_uses_finite_drawdown_for_displayed_candidate_stats() -> None:
    from backtestforecast.pipeline.regime import Regime, RegimeSnapshot
    from backtestforecast.pipeline.service import NightlyPipelineService, SymbolStrategyPair

    regime = RegimeSnapshot(symbol="AAPL", close_price=100.0, regimes=frozenset([Regime.BULLISH]))
    executor = MagicMock()
    executor.run_quick_backtest.return_value = {
        "trade_count": 4,
        "win_rate": 55.0,
        "total_roi_pct": 7.0,
        "total_net_pnl": 350.0,
        "max_drawdown_pct": float("nan"),
        "sharpe_ratio": 0.8,
    }
    service = NightlyPipelineService(MagicMock(), market_data_fetcher=MagicMock(), backtest_executor=executor, forecaster=None)

    with ThreadPoolExecutor(max_workers=1) as pool:
        results = service._stage3_quick_backtest(
            [SymbolStrategyPair(symbol="AAPL", strategy_type="long_call", regime=regime, close_price=100.0)],
            date(2025, 1, 31),
            executor=pool,
        )

    assert results
    assert results[0].max_drawdown_pct == 50.0


def test_pipeline_adapter_sanitizes_non_finite_summary_metrics_for_payloads() -> None:
    from backtestforecast.pipeline.adapters import PipelineBacktestExecutor

    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.return_value = object()
    execution_service.execute_request.return_value = SimpleNamespace(
        summary=SimpleNamespace(
            trade_count=3,
            decided_trades=3,
            win_rate=float("nan"),
            total_roi_pct=float("inf"),
            total_net_pnl=float("-inf"),
            max_drawdown_pct=float("nan"),
            sharpe_ratio=float("inf"),
            average_holding_period_days=float("nan"),
            starting_equity=10_000.0,
            ending_equity=float("inf"),
        ),
        trades=[],
        equity_curve=[],
        warnings=[],
    )

    executor = PipelineBacktestExecutor(execution_service=execution_service)
    quick = executor.run_quick_backtest("AAPL", "long_call", date(2025, 1, 1), date(2025, 3, 1), 30)
    full = executor.run_full_backtest("AAPL", "long_call", date(2025, 1, 1), date(2025, 3, 1), 30)

    assert quick is not None
    assert full is not None
    assert quick["win_rate"] == 0.0
    assert quick["total_roi_pct"] == 0.0
    assert quick["total_net_pnl"] == 0.0
    assert quick["max_drawdown_pct"] == 50.0
    assert quick["sharpe_ratio"] == 0.0
    assert full["ending_equity"] == 0.0
