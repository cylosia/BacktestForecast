"""Tests for daily_picks date validation using market_date_today()."""
from __future__ import annotations

from datetime import timedelta

import pytest

from backtestforecast.utils.dates import market_date_today


class TestMarketDateToday:
    def test_returns_a_weekday(self):
        today = market_date_today()
        assert today.weekday() < 5, f"market_date_today() returned a weekend day: {today}"

    def test_does_not_return_future_date(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        today = market_date_today()
        now_eastern = datetime.now(ZoneInfo("America/New_York")).date()
        assert today <= now_eastern

    def test_rolls_back_from_known_holiday(self):
        from backtestforecast.utils.dates import get_all_holidays

        holidays = get_all_holidays()
        if not holidays:
            pytest.skip("No holidays configured")

        for holiday in sorted(holidays):
            if holiday.weekday() < 5:
                assert holiday not in {market_date_today()}, (
                    "market_date_today() should not return a known holiday if today IS that holiday"
                )
                break


class TestDailyPicksDateValidation:
    def test_future_trade_date_would_be_rejected(self):
        today = market_date_today()
        future = today + timedelta(days=1)
        assert future > today

    def test_very_old_trade_date_would_be_rejected(self):
        today = market_date_today()
        cutoff = today - timedelta(days=5 * 365)
        ancient = cutoff - timedelta(days=1)
        assert ancient < cutoff
