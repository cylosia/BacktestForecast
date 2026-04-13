from __future__ import annotations

from datetime import date, timedelta

from backtestforecast.market_data.types import DailyBar
from backtestforecast.pair_signal_plans import build_pair_return_spread_signal_plan, build_xlf_regime_signal_plan
from backtestforecast.underlying_rotation import UnderlyingRotationDataset, UnderlyingUniverseMember, _PriceHistory


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


def test_xlf_regime_signal_plan_maps_positive_negative_and_neutral_states() -> None:
    dataset = _build_dataset(
        {
            "FAS": [
                (100, 100, 100, 100),
                (101, 101, 101, 101),
                (102, 102, 102, 102),
                (103, 103, 103, 103),
            ],
            "FAZ": [
                (100, 100, 100, 100),
                (99, 99, 99, 99),
                (98, 98, 98, 98),
                (97, 97, 97, 97),
            ],
            "XLF": [
                (100, 100, 100, 100),
                (102, 102, 102, 102),
                (101, 101, 101, 101),
                (101, 101, 101, 101),
            ],
        }
    )

    plan = build_xlf_regime_signal_plan(
        dataset,
        pair_symbols=("FAS", "FAZ"),
        signal_symbol="XLF",
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
        lookback_days=1,
        rebalance_frequency_days=1,
        neutral_threshold_pct=0.01,
        positive_signal_short_symbol="FAZ",
        negative_signal_short_symbol="FAS",
    )

    assert plan.ranked_symbols_by_execution_date[date(2020, 1, 3)] == ("FAZ",)
    assert plan.ranked_symbols_by_execution_date[date(2020, 1, 4)] == ()


def test_pair_return_spread_signal_plan_chooses_expected_leg() -> None:
    dataset = _build_dataset(
        {
            "FAS": [
                (100, 100, 100, 100),
                (110, 110, 110, 110),
                (120, 120, 120, 120),
                (90, 90, 90, 90),
                (80, 80, 80, 80),
            ],
            "FAZ": [
                (100, 100, 100, 100),
                (95, 95, 95, 95),
                (90, 90, 90, 90),
                (100, 100, 100, 100),
                (110, 110, 110, 110),
            ],
        }
    )

    plan = build_pair_return_spread_signal_plan(
        dataset,
        pair_symbols=("FAS", "FAZ"),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 5),
        lookback_days=1,
        rebalance_frequency_days=1,
        neutral_threshold_pct=0.0,
        positive_spread_short_symbol="FAZ",
        negative_spread_short_symbol="FAS",
    )

    assert plan.ranked_symbols_by_execution_date[date(2020, 1, 3)] == ("FAZ",)
    assert plan.ranked_symbols_by_execution_date[date(2020, 1, 5)] == ("FAS",)
