from __future__ import annotations

from datetime import date, timedelta

import backtestforecast.stock_rsi as stock_rsi_module
from backtestforecast.market_data.types import DailyBar
from backtestforecast.stock_rsi import StockRsiConfig, run_stock_rsi_backtest


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


def test_stock_rsi_enters_on_cross_above_and_exits_on_cross_below(monkeypatch) -> None:
    bars = _make_bars(count=7)
    monkeypatch.setattr(
        stock_rsi_module,
        "rsi",
        lambda values, period: [None, 25.0, 31.0, 55.0, 75.0, 68.0, 60.0],
    )
    result = run_stock_rsi_backtest(
        bars,
        config=StockRsiConfig(symbol="FAS", rsi_period=14, entry_level=30.0, exit_level=70.0),
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.entry_date == bars[3].trade_date
    assert trade.exit_date == bars[6].trade_date
    assert trade.exit_reason == "crosses_below"
    assert trade.detail_json["entry_signal_date"] == bars[2].trade_date.isoformat()
    assert trade.detail_json["exit_signal_date"] == bars[5].trade_date.isoformat()
    assert result.summary.ending_equity > result.summary.starting_equity


def test_stock_rsi_force_closes_at_backtest_end_when_no_exit_signal(monkeypatch) -> None:
    bars = _make_bars(count=5)
    monkeypatch.setattr(
        stock_rsi_module,
        "rsi",
        lambda values, period: [None, 25.0, 31.0, 60.0, 65.0],
    )
    result = run_stock_rsi_backtest(
        bars,
        config=StockRsiConfig(symbol="FAS"),
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 1
    assert result.trades[0].exit_reason == "backtest_end"
    assert result.trades[0].exit_date == bars[-1].trade_date


def test_stock_rsi_returns_no_trades_warning_when_no_crossovers(monkeypatch) -> None:
    bars = _make_bars(count=6)
    monkeypatch.setattr(
        stock_rsi_module,
        "rsi",
        lambda values, period: [None, 40.0, 45.0, 50.0, 55.0, 58.0],
    )
    result = run_stock_rsi_backtest(
        bars,
        config=StockRsiConfig(symbol="FAS"),
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        starting_equity=10_000.0,
        risk_free_rate=0.0,
    )

    assert result.summary.trade_count == 0
    assert any(item["code"] == "no_trades" for item in result.warnings)
