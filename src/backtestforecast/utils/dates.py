"""Canonical date utilities for US-market-aligned operations."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_US_EASTERN = ZoneInfo("America/New_York")


def market_date_today() -> date:
    """Return the most recent trading day in US Eastern time.

    On weekends the date is rolled back to the preceding Friday so that
    pipeline runs, scans, and analysis always reference a real trading day.
    Market holidays are not handled — only weekday adjustment is performed.
    """
    today = datetime.now(_US_EASTERN).date()
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    return today
