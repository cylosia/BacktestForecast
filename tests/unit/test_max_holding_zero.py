"""Fix 78: max_holding_days=0 is clamped to 1 in _resolve_exit.

After Fix 25, passing max_holding_days=0 should be clamped to 1, so a
position held for 0 trading days should NOT trigger an exit.
"""
from __future__ import annotations

from datetime import date

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import OpenMultiLegPosition, OpenOptionLeg
from backtestforecast.market_data.types import DailyBar


def _make_bar(trade_date: date, close: float = 150.0) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close,
        high_price=close + 1,
        low_price=close - 1,
        close_price=close,
        volume=1_000_000,
    )


def _make_position(entry_date: date, entry_index: int = 0) -> OpenMultiLegPosition:
    leg = OpenOptionLeg(
        ticker="SPY230120P00400000",
        contract_type="put",
        side=-1,
        strike_price=400.0,
        expiration_date=date(2023, 3, 17),
        quantity_per_unit=1,
        entry_mid=5.0,
        last_mid=4.5,
    )
    return OpenMultiLegPosition(
        display_ticker="SPY230120P00400000",
        strategy_type="short_put",
        underlying_symbol="SPY",
        entry_date=entry_date,
        entry_index=entry_index,
        quantity=1,
        dte_at_open=56,
        option_legs=[leg],
        capital_required_per_unit=5000.0,
    )


class TestMaxHoldingZero:
    """Verify max_holding_days=0 is clamped to 1."""

    def test_zero_max_holding_days_clamped_to_one(self):
        """A position held for 0 trading days must NOT trigger exit."""
        entry_date = date(2023, 1, 3)
        bar = _make_bar(entry_date)
        position = _make_position(entry_date, entry_index=5)

        should_exit, _reason = OptionsBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=0,
            backtest_end_date=date(2023, 6, 30),
            last_bar_date=date(2023, 6, 30),
            current_bar_index=5,
        )
        assert not should_exit, (
            "max_holding_days=0 should be clamped to 1, so 0 trading days "
            "held should NOT trigger exit"
        )

    def test_one_trading_day_held_triggers_exit_with_zero(self):
        """After 1 trading day, the clamped limit (1) should trigger exit."""
        entry_date = date(2023, 1, 3)
        bar = _make_bar(date(2023, 1, 4))
        position = _make_position(entry_date, entry_index=5)

        should_exit, reason = OptionsBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=0,
            backtest_end_date=date(2023, 6, 30),
            last_bar_date=date(2023, 6, 30),
            current_bar_index=6,
        )
        assert should_exit, (
            "After 1 trading day held, clamped max_holding_days=1 should exit"
        )
        assert reason == "max_holding_days"

    def test_negative_max_holding_days_also_clamped(self):
        """Negative max_holding_days should also be clamped to 1."""
        entry_date = date(2023, 1, 3)
        bar = _make_bar(entry_date)
        position = _make_position(entry_date, entry_index=5)

        should_exit, _ = OptionsBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=-5,
            backtest_end_date=date(2023, 6, 30),
            last_bar_date=date(2023, 6, 30),
            current_bar_index=5,
        )
        assert not should_exit, (
            "Negative max_holding_days clamped to 1; 0 days held => no exit"
        )

    def test_normal_max_holding_days_unaffected(self):
        """Normal positive max_holding_days should work as before."""
        entry_date = date(2023, 1, 3)
        bar = _make_bar(date(2023, 1, 4))
        position = _make_position(entry_date, entry_index=0)

        should_exit, _reason = OptionsBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=5,
            backtest_end_date=date(2023, 6, 30),
            last_bar_date=date(2023, 6, 30),
            current_bar_index=1,
        )
        assert not should_exit, (
            "1 trading day held with max_holding_days=5 should NOT exit"
        )
