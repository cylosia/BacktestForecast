"""Canonical date utilities for US-market-aligned operations."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

_US_EASTERN = ZoneInfo("America/New_York")


def market_date_today() -> date:
    """Return today's date in US Eastern time (the canonical market timezone).

    All internal date comparisons for trading data (pipeline, scans, analysis)
    should use this function so that "today" is consistent across the codebase
    regardless of the server's timezone or UTC offset.
    """
    return datetime.now(_US_EASTERN).date()
