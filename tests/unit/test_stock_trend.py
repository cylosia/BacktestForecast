from __future__ import annotations

from datetime import date, timedelta

from backtestforecast.market_data.types import DailyBar
from backtestforecast.stock_trend import run_stock_condition_backtest


def _make_bars(*, count: int, start: date = date(2025, 1, 1)) -> list[DailyBar]:
    bars: list[DailyBar] = []
    for index in range(count):
        trade_date = start + timedelta(days=index)
        open_price = 100.0 + index
        close_price = open_price + 1.0
        bars.append(
            DailyBar(
                trade_date=trade_date,
                open_price=open_price,
                high_price=close_price + 1.0,
                low_price=open_price - 1.0,
                close_price=close_price,
                volume=1_000_000.0,
            )
        )
    return bars


def test_stock_trend_enters_when_condition_turns_on_and_exits_when_it_turns_off() -> None:
    bars = _make_bars(count=6)
    condition_series = [False, True, True, False, False, False]
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=condition_series,
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.entry_date == bars[2].trade_date
    assert trade.exit_date == bars[4].trade_date
    assert trade.entry_reason == "condition_on"
    assert trade.exit_reason == "condition_off"


def test_stock_trend_enters_on_first_bar_when_condition_is_already_on() -> None:
    bars = _make_bars(count=5)
    condition_series = [True, True, True, False, False]
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=condition_series,
        start_date=bars[1].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    assert result.trades[0].entry_date == bars[1].trade_date
    assert result.trades[0].entry_reason == "carry_in_condition"


def test_stock_trend_returns_no_trade_warning_when_condition_never_turns_on() -> None:
    bars = _make_bars(count=4)
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=[False, False, False, False],
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 0
    assert any(item["code"] == "no_trades" for item in result.warnings)


def test_stock_trend_trailing_stop_exits_intraday() -> None:
    bars = [
        DailyBar(date(2025, 1, 1), 100.0, 101.0, 99.0, 100.0, 1_000_000.0),
        DailyBar(date(2025, 1, 2), 100.0, 110.0, 100.0, 110.0, 1_000_000.0),
        DailyBar(date(2025, 1, 3), 110.0, 111.0, 103.0, 108.0, 1_000_000.0),
        DailyBar(date(2025, 1, 4), 108.0, 109.0, 107.0, 108.0, 1_000_000.0),
    ]
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=[False, True, True, True],
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
        trailing_stop_pct=0.05,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.exit_reason == "trailing_stop"
    assert float(trade.exit_underlying_close) == 104.5


def test_stock_trend_trailing_stop_uses_open_on_gap_below_stop() -> None:
    bars = [
        DailyBar(date(2025, 1, 1), 100.0, 101.0, 99.0, 100.0, 1_000_000.0),
        DailyBar(date(2025, 1, 2), 100.0, 110.0, 100.0, 110.0, 1_000_000.0),
        DailyBar(date(2025, 1, 3), 100.0, 101.0, 99.0, 100.0, 1_000_000.0),
        DailyBar(date(2025, 1, 4), 100.0, 101.0, 99.0, 100.0, 1_000_000.0),
    ]
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=[True, True, True, True],
        start_date=bars[1].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
        trailing_stop_pct=0.05,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.exit_reason == "trailing_stop"
    assert float(trade.exit_underlying_close) == 100.0


def test_stock_trend_entry_gate_can_delay_entry_until_confirmation() -> None:
    bars = _make_bars(count=6)
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=[True, True, True, False, False, False],
        entry_gate_series=[False, True, True, False, False, False],
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.entry_date == bars[2].trade_date
    assert trade.entry_reason == "condition_on"


def test_stock_trend_entry_gate_blocks_carry_in_without_confirmation() -> None:
    bars = _make_bars(count=5)
    result = run_stock_condition_backtest(
        bars,
        symbol="FAS",
        strategy_name="test_trend",
        condition_series=[True, True, True, False, False],
        entry_gate_series=[False, False, True, False, False],
        start_date=bars[1].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.entry_date == bars[3].trade_date
