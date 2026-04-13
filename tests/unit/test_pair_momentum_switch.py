from __future__ import annotations

from datetime import date, timedelta

import pytest

from backtestforecast.market_data.types import DailyBar
from backtestforecast.pair_momentum_switch import (
    PairMomentumSwitchConfig,
    optimize_pair_momentum_switch,
    run_pair_momentum_switch_backtest,
)
from backtestforecast.underlying_rotation import UnderlyingRotationDataset, UnderlyingUniverseMember, _PriceHistory


def _build_dataset(
    price_map: dict[str, list[tuple[float, float, float, float]]],
    *,
    raw_price_map: dict[str, list[tuple[float, float, float, float]]] | None = None,
) -> UnderlyingRotationDataset:
    start = date(2020, 1, 1)
    histories: dict[str, _PriceHistory] = {}
    raw_histories: dict[str, _PriceHistory] = {}
    trade_dates: set[date] = set()
    universe_members: dict[str, UnderlyingUniverseMember] = {}

    def _build_histories(
        bars_by_symbol: dict[str, list[tuple[float, float, float, float]]],
    ) -> dict[str, _PriceHistory]:
        built: dict[str, _PriceHistory] = {}
        for symbol, bars in bars_by_symbol.items():
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
            built[symbol] = _PriceHistory(
                symbol=symbol,
                bars=ordered_bars,
                dates=tuple(item.trade_date for item in ordered_bars),
                bars_by_date={item.trade_date: item for item in ordered_bars},
                closes=tuple(item.close_price for item in ordered_bars),
            )
        return built

    histories = _build_histories(price_map)
    if raw_price_map is not None:
        raw_histories = _build_histories(raw_price_map)

    for symbol, history in histories.items():
        ordered_bars = history.bars
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
        raw_histories=raw_histories,
    )


def test_pair_momentum_switch_enters_leader_and_switches_when_leadership_changes() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (119, 119, 119, 119),
                (118, 118, 118, 118),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
            ],
        }
    )
    config = PairMomentumSwitchConfig(symbols=("A", "B"), lookback_days=1, rebalance_frequency_days=1)

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 5),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert [trade.underlying_symbol for trade in result.trades] == ["A", "B"]
    assert result.trades[0].entry_date == date(2020, 1, 3)
    assert result.trades[0].exit_date == date(2020, 1, 5)
    assert result.trades[0].exit_reason == "switch"
    assert result.trades[1].entry_date == date(2020, 1, 5)
    assert result.trades[1].exit_reason == "backtest_end"


def test_pair_momentum_switch_optimizer_prefers_better_lookback() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (101, 101, 101, 101),
                (102, 102, 102, 102),
                (103, 103, 103, 103),
                (104, 104, 104, 104),
                (90, 90, 90, 90),
                (80, 80, 80, 80),
                (70, 70, 70, 70),
                (60, 60, 60, 60),
                (50, 50, 50, 50),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
                (96, 96, 96, 96),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (130, 130, 130, 130),
                (140, 140, 140, 140),
                (150, 150, 150, 150),
            ],
        }
    )

    result = optimize_pair_momentum_switch(
        dataset,
        symbols=("A", "B"),
        lookback_days=(1, 3),
        rebalance_frequency_days=1,
        train_start=date(2020, 1, 1),
        train_end=date(2020, 1, 6),
        validation_start=date(2020, 1, 7),
        validation_end=date(2020, 1, 10),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
        top_validation_count=2,
        objective="roi",
    )

    assert result.candidate_count == 2
    assert result.best_config.lookback_days == 1
    assert result.best_validation_result is not None
    assert result.best_validation_result.summary.total_roi_pct > 0


def test_pair_momentum_switch_cash_filter_exits_to_cash_when_both_legs_nonpositive() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (109, 109, 109, 109),
                (100, 100, 100, 100),
                (99, 99, 99, 99),
            ],
            "B": [
                (100, 100, 100, 100),
                (90, 90, 90, 90),
                (89, 89, 89, 89),
                (80, 80, 80, 80),
                (79, 79, 79, 79),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        require_positive_momentum=True,
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 5),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) == 1
    assert result.trades[0].underlying_symbol == "A"
    assert result.trades[0].exit_reason == "cash_filter"


def test_pair_momentum_switch_trailing_stop_exits_same_day_after_entry() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (108, 108, 100, 105),
                (104, 104, 104, 104),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        trailing_stop_pct=0.05,
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) >= 1
    assert result.trades[0].exit_reason == "trailing_stop"
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(102.6)


def test_pair_momentum_switch_short_profits_when_selected_symbol_falls() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (100, 100, 100, 100),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        position_direction="short",
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) == 1
    assert result.trades[0].underlying_symbol == "A"
    assert float(result.trades[0].entry_underlying_close) == pytest.approx(120.0)
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(100.0)
    assert float(result.trades[0].net_pnl) == pytest.approx(16_666.666666666664)
    assert result.summary.total_roi_pct == pytest.approx(16.666666666666664)


def test_pair_momentum_switch_short_with_inverted_ranking_selects_weaker_symbol() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (130, 130, 130, 130),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (90, 90, 90, 90),
                (80, 80, 80, 80),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        position_direction="short",
        invert_ranking=True,
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) == 1
    assert result.trades[0].underlying_symbol == "B"
    assert float(result.trades[0].entry_underlying_close) == pytest.approx(90.0)
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(80.0)
    assert float(result.trades[0].net_pnl) == pytest.approx(11111.111111111111)


def test_pair_momentum_switch_short_trailing_stop_exits_when_price_rises_from_low() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (105, 110, 100, 101),
                (104, 107, 103, 104),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 80, 80),
                (79, 79, 79, 79),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        trailing_stop_pct=0.05,
        position_direction="short",
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) >= 1
    assert result.trades[0].exit_reason == "trailing_stop"
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(106.05)


def test_pair_momentum_switch_inverted_ranking_cash_filter_requires_negative_momentum() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (111, 111, 111, 111),
                (112, 112, 112, 112),
            ],
            "B": [
                (100, 100, 100, 100),
                (101, 101, 101, 101),
                (102, 102, 102, 102),
                (103, 103, 103, 103),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        position_direction="short",
        invert_ranking=True,
        require_positive_momentum=True,
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert result.trades == ()
    assert result.summary.total_roi_pct == pytest.approx(0.0)


def test_pair_momentum_switch_normalizes_inferred_splits_before_marking_pnl() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (30, 30, 30, 30),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
        }
    )
    config = PairMomentumSwitchConfig(symbols=("A", "B"), lookback_days=1, rebalance_frequency_days=1)

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) == 1
    assert result.trades[0].underlying_symbol == "A"
    assert float(result.trades[0].entry_underlying_close) == pytest.approx(120.0)
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(120.0)
    assert float(result.trades[0].net_pnl) == pytest.approx(0.0)


def test_pair_momentum_switch_uses_raw_execution_prices_with_split_adjusted_signals() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (120, 120, 120, 120),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
        },
        raw_price_map={
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (30, 30, 30, 30),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
        },
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        use_raw_execution_prices=True,
    )

    result = run_pair_momentum_switch_backtest(
        dataset,
        config=config,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        starting_equity=100_000.0,
        risk_free_rate=0.0,
    )

    assert len(result.trades) == 1
    assert result.trades[0].underlying_symbol == "A"
    assert float(result.trades[0].entry_underlying_close) == pytest.approx(120.0)
    assert float(result.trades[0].exit_underlying_close) == pytest.approx(30.0)
    assert float(result.trades[0].net_pnl) == pytest.approx(0.0)


def test_pair_momentum_switch_requires_raw_histories_when_requested() -> None:
    dataset = _build_dataset(
        {
            "A": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
            ],
            "B": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
            ],
        }
    )
    config = PairMomentumSwitchConfig(
        symbols=("A", "B"),
        lookback_days=1,
        rebalance_frequency_days=1,
        use_raw_execution_prices=True,
    )

    with pytest.raises(ValueError, match="Raw execution history unavailable"):
        run_pair_momentum_switch_backtest(
            dataset,
            config=config,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 3),
            starting_equity=100_000.0,
            risk_free_rate=0.0,
        )


def test_pair_momentum_switch_requires_exactly_two_unique_symbols() -> None:
    with pytest.raises(ValueError, match="exactly 2"):
        PairMomentumSwitchConfig(symbols=("A",), lookback_days=5)

    with pytest.raises(ValueError, match="unique"):
        PairMomentumSwitchConfig(symbols=("A", "A"), lookback_days=5)

    with pytest.raises(ValueError, match="position_direction"):
        PairMomentumSwitchConfig(symbols=("A", "B"), lookback_days=5, position_direction="sideways")
