"""Tests for BillingService._normalize_utc static method."""
from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from backtestforecast.services.billing import BillingService


class TestNormalizeUtc:
    def test_naive_datetime_adds_utc(self):
        """Naive datetime (no timezone) should get UTC tzinfo."""
        naive = datetime(2024, 3, 15, 12, 0, 0)
        result = BillingService._normalize_utc(naive)
        assert result.tzinfo is UTC
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15
        assert result.hour == 12

    def test_utc_datetime_passes_through(self):
        """UTC-aware datetime should pass through unchanged."""
        utc_dt = datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC)
        result = BillingService._normalize_utc(utc_dt)
        assert result is utc_dt
        assert result.tzinfo is UTC

    def test_non_utc_timezone_preserved(self):
        """Non-UTC timezone should be preserved (no conversion)."""
        eastern = datetime(2024, 3, 15, 12, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        result = BillingService._normalize_utc(eastern)
        assert result is eastern
        assert result.tzinfo is not None
        assert result.tzinfo != UTC
