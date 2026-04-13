from __future__ import annotations

from datetime import date, timedelta

import pytest

from backtestforecast.market_data.types import DailyBar
from backtestforecast.underlying_rotation import (
    UnderlyingRotationConfig,
    UnderlyingRotationBacktestResult,
    UnderlyingRotationDataset,
    UnderlyingRotationOptimizationRow,
    UnderlyingRotationSearchSpace,
    UnderlyingUniverseMember,
    UnderlyingRotationWalkForwardSplit,
    _PriceHistory,
    _RankingPlan,
    _build_ranking_plan,
    _optimization_summary_sort_key,
    aggregate_rolling_walk_forward_results,
    build_rolling_split_result,
    build_trailing_annual_walk_forward_splits,
    optimize_underlying_rotation,
    recommend_rolling_challenger,
    run_underlying_rotation_backtest,
)


def _build_dataset(price_map: dict[str, list[tuple[float, float, float, float]]]) -> UnderlyingRotationDataset:
    start = date(2020, 1, 1)
    histories: dict[str, _PriceHistory] = {}
    trade_dates: set[date] = set()
    universe_members: dict[str, UnderlyingUniverseMember] = {}
    for symbol, bars in price_map.items():
        daily_bars = []
        for offset, (open_price, high_price, low_price, close_price) in enumerate(bars):
            trade_date = start + timedelta(days=offset)
            trade_dates.add(trade_date)
            daily_bars.append(
                DailyBar(
                    trade_date=trade_date,
                    open_price=open_price,
                    high_price=high_price,
                    low_price=low_price,
                    close_price=close_price,
                    volume=1_000_000.0,
                )
            )
        ordered_bars = tuple(daily_bars)
        histories[symbol] = _PriceHistory(
            symbol=symbol,
            bars=ordered_bars,
            dates=tuple(item.trade_date for item in ordered_bars),
            bars_by_date={item.trade_date: item for item in ordered_bars},
            closes=tuple(item.close_price for item in ordered_bars),
        )
        universe_members[symbol] = UnderlyingUniverseMember(
            symbol=symbol,
            training_bar_count=len(ordered_bars),
            avg_dollar_volume=100_000_000.0,
            min_close_price=min(item.close_price for item in ordered_bars),
        )
    return UnderlyingRotationDataset(
        start_date=min(trade_dates),
        end_date=max(trade_dates),
        trade_dates=tuple(sorted(trade_dates)),
        histories=histories,
        universe_members=universe_members,
    )


def test_build_ranking_plan_orders_symbols_by_weighted_return() -> None:
    dataset = _build_dataset(
        {
            "A": [(100, 100, 100, 100), (101, 101, 101, 101), (102, 102, 102, 102), (103, 103, 103, 103), (104, 104, 104, 104)],
            "B": [(100, 100, 100, 100), (100, 100, 100, 100), (100, 100, 100, 100), (100, 100, 100, 100), (100, 100, 100, 100)],
            "C": [(100, 100, 100, 100), (99, 99, 99, 99), (98, 98, 98, 98), (97, 97, 97, 97), (96, 96, 96, 96)],
        }
    )

    plan = _build_ranking_plan(
        dataset,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 5),
        lookback_days=(1, 2, 3),
        lookback_weights=(0.5, 0.3, 0.2),
        rebalance_frequency_days=1,
        max_ranked_symbols=3,
    )

    assert plan.ranked_symbols_by_execution_date[date(2020, 1, 5)] == ("A", "B", "C")


def test_backtest_trailing_stop_exits_early_and_holds_cash() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (100, 110, 99, 110),
                (108, 109, 104, 105),
                (104, 104, 103, 103),
            ]
        }
    )
    config = UnderlyingRotationConfig(
        portfolio_size=1,
        lookback_days=(1, 2, 3),
        lookback_weights=(1.0, 0.0, 0.0),
        trailing_stop_pct=0.05,
        rebalance_frequency_days=10,
    )
    result = run_underlying_rotation_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
        ranking_plan=_RankingPlan(
            ranked_symbols_by_execution_date={
                date(2020, 1, 2): ("A",),
            }
        ),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "trailing_stop"
    assert float(trade.exit_underlying_close) == pytest.approx(104.5)
    assert result.summary.total_roi_pct == pytest.approx(4.5, abs=1e-6)


def test_backtest_rebalance_keeps_incumbents_that_still_rank() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (101, 101, 101, 101),
                (102, 102, 102, 102),
                (103, 103, 103, 103),
            ]
        }
    )
    config = UnderlyingRotationConfig(
        portfolio_size=1,
        lookback_days=(1, 2, 3),
        lookback_weights=(1.0, 0.0, 0.0),
        trailing_stop_pct=0.0,
        rebalance_frequency_days=1,
    )
    result = run_underlying_rotation_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
        ranking_plan=_RankingPlan(
            ranked_symbols_by_execution_date={
                date(2020, 1, 2): ("A",),
                date(2020, 1, 3): ("A",),
            }
        ),
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_date == date(2020, 1, 2)
    assert result.trades[0].exit_reason == "backtest_end"


def test_optimizer_prefers_the_higher_return_training_configuration() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (150, 150, 150, 150),
                (150, 150, 150, 150),
                (150, 150, 150, 150),
                (140, 140, 140, 140),
                (130, 130, 130, 130),
                (120, 120, 120, 120),
                (110, 110, 110, 110),
            ],
            "B": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (130, 130, 130, 130),
                (140, 140, 140, 140),
                (150, 150, 150, 150),
                (160, 160, 160, 160),
                (170, 170, 170, 170),
            ],
        }
    )
    result = optimize_underlying_rotation(
        dataset,
        search_space=UnderlyingRotationSearchSpace(
            portfolio_sizes=(1,),
            lookback_triplets=((1, 2, 3),),
            weight_triplets=((1.0, 0.0, 0.0), (0.0, 0.0, 1.0)),
            trailing_stop_pcts=(0.0,),
            rebalance_frequencies=(1,),
        ),
        train_start=date(2020, 1, 1),
        train_end=date(2020, 1, 6),
        validation_start=date(2020, 1, 7),
        validation_end=date(2020, 1, 8),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
        top_validation_count=2,
    )

    assert result.candidate_count == 2
    assert result.best_config.lookback_weights == (1.0, 0.0, 0.0)
    assert result.best_train_result.summary.total_roi_pct > 0


def test_roi_objective_with_drawdown_cap_prefers_capped_candidate() -> None:
    capped = _optimization_summary_sort_key(
        type(
            "Summary",
            (),
            {"total_roi_pct": 10.0, "sharpe_ratio": 0.5, "max_drawdown_pct": 12.0},
        )(),
        objective="roi",
        max_drawdown_pct_cap=15.0,
    )
    uncapped = _optimization_summary_sort_key(
        type(
            "Summary",
            (),
            {"total_roi_pct": 25.0, "sharpe_ratio": 1.0, "max_drawdown_pct": 25.0},
        )(),
        objective="roi",
        max_drawdown_pct_cap=15.0,
    )

    assert capped > uncapped


def test_build_trailing_annual_walk_forward_splits_uses_trailing_train_window() -> None:
    splits = build_trailing_annual_walk_forward_splits(
        validation_start_year=2016,
        validation_end_year=2018,
        train_years=5,
        validation_years=1,
        step_years=1,
    )

    assert splits == (
        UnderlyingRotationWalkForwardSplit(
            train_start=date(2011, 1, 1),
            train_end=date(2015, 12, 31),
            validation_start=date(2016, 1, 1),
            validation_end=date(2016, 12, 31),
        ),
        UnderlyingRotationWalkForwardSplit(
            train_start=date(2012, 1, 1),
            train_end=date(2016, 12, 31),
            validation_start=date(2017, 1, 1),
            validation_end=date(2017, 12, 31),
        ),
        UnderlyingRotationWalkForwardSplit(
            train_start=date(2013, 1, 1),
            train_end=date(2017, 12, 31),
            validation_start=date(2018, 1, 1),
            validation_end=date(2018, 12, 31),
        ),
    )


def test_rolling_aggregation_and_decision_keep_incumbent_when_improvement_small() -> None:
    config_a = UnderlyingRotationConfig(
        portfolio_size=8,
        lookback_days=(30, 90, 180),
        lookback_weights=(0.45, 0.25, 0.30),
        trailing_stop_pct=0.08,
        rebalance_frequency_days=21,
    )
    config_b = UnderlyingRotationConfig(
        portfolio_size=10,
        lookback_days=(30, 120, 180),
        lookback_weights=(0.45, 0.30, 0.25),
        trailing_stop_pct=0.08,
        rebalance_frequency_days=21,
    )

    def _result(config: UnderlyingRotationConfig, roi: float, drawdown: float, sharpe: float | None) -> UnderlyingRotationBacktestResult:
        summary = type(
            "Summary",
            (),
            {
                "trade_count": 0,
                "decided_trades": 0,
                "win_rate": 0.0,
                "total_roi_pct": roi,
                "average_win_amount": 0.0,
                "average_loss_amount": 0.0,
                "average_holding_period_days": 0.0,
                "average_dte_at_open": 0.0,
                "max_drawdown_pct": drawdown,
                "total_commissions": 0.0,
                "total_net_pnl": 0.0,
                "starting_equity": 100000.0,
                "ending_equity": 100000.0,
                "profit_factor": None,
                "payoff_ratio": None,
                "expectancy": 0.0,
                "sharpe_ratio": sharpe,
                "sortino_ratio": None,
                "cagr_pct": None,
                "calmar_ratio": None,
                "max_consecutive_wins": 0,
                "max_consecutive_losses": 0,
                "recovery_factor": None,
            },
        )()
        return UnderlyingRotationBacktestResult(config=config, summary=summary, trades=(), equity_curve=(), warnings=())

    split_1 = build_rolling_split_result(
        UnderlyingRotationWalkForwardSplit(
            train_start=date(2011, 1, 1),
            train_end=date(2015, 12, 31),
            validation_start=date(2016, 1, 1),
            validation_end=date(2016, 12, 31),
        ),
        type(
            "OptResult",
            (),
            {
                "universe_size": 100,
                "top_rows": (
                    UnderlyingRotationOptimizationRow(config=config_a, train_result=_result(config_a, 5.0, 10.0, 0.5), validation_result=_result(config_a, 4.0, 10.0, 0.4)),
                    UnderlyingRotationOptimizationRow(config=config_b, train_result=_result(config_b, 5.5, 11.0, 0.6), validation_result=_result(config_b, 4.7, 10.5, 0.45)),
                ),
            },
        )(),
    )
    split_2 = build_rolling_split_result(
        UnderlyingRotationWalkForwardSplit(
            train_start=date(2012, 1, 1),
            train_end=date(2016, 12, 31),
            validation_start=date(2017, 1, 1),
            validation_end=date(2017, 12, 31),
        ),
        type(
            "OptResult",
            (),
            {
                "universe_size": 100,
                "top_rows": (
                    UnderlyingRotationOptimizationRow(config=config_a, train_result=_result(config_a, 6.0, 10.0, 0.55), validation_result=_result(config_a, 3.8, 11.0, 0.35)),
                    UnderlyingRotationOptimizationRow(config=config_b, train_result=_result(config_b, 6.2, 11.5, 0.65), validation_result=_result(config_b, 4.4, 12.0, 0.40)),
                ),
            },
        )(),
    )

    aggregated = aggregate_rolling_walk_forward_results((split_1, split_2), max_drawdown_pct_cap=15.0)
    decision = recommend_rolling_challenger(
        aggregated,
        incumbent_config=config_a,
        min_median_validation_roi_improvement_pct=1.0,
    )

    assert aggregated[0].config == config_b
    assert decision.action == "keep_incumbent"
    assert decision.incumbent is not None
    assert decision.challenger.config == config_b
