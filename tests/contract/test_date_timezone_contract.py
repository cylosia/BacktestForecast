"""Contract test: frontend date validation matches backend ET behavior.

The backend rejects end_date > market_date_today() (US Eastern time).
The frontend must be at least as restrictive, using an ET-aware boundary
so it never accepts a date the backend would reject.

This test verifies the structural alignment, not runtime behavior (which
would need a browser environment for the frontend JavaScript).
"""
from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_TS = PROJECT_ROOT / "apps" / "web" / "lib" / "backtests" / "validation.ts"
UTILS_TS = PROJECT_ROOT / "apps" / "web" / "lib" / "utils.ts"


def _read_ts(path: Path) -> str:
    assert path.exists(), f"File not found: {path}"
    return path.read_text(encoding="utf-8")


def test_frontend_validation_uses_et_offset() -> None:
    """Frontend date validation must use an ET offset, not raw UTC."""
    source = _read_ts(VALIDATION_TS)
    assert "etOffsetMs" in source or "ET" in source, (
        "validation.ts must use an ET-aware offset for date checking, "
        "not raw UTC. Look for etOffsetMs or similar."
    )
    assert "todayEt" in source or "nowEt" in source, (
        "validation.ts must compute today's date in ET, not UTC."
    )


def test_frontend_validation_mentions_eastern_time_in_error() -> None:
    """Error message should mention Eastern time to avoid user confusion."""
    source = _read_ts(VALIDATION_TS)
    assert "Eastern" in source or "ET" in source, (
        "Date validation error message should mention 'Eastern time' or 'ET' "
        "so users understand why their date was rejected."
    )


def test_frontend_uses_et_aware_default_dates() -> None:
    """Default form dates (daysAgoET) must use ET, not UTC."""
    source = _read_ts(VALIDATION_TS)
    assert "daysAgoET" in source, (
        "validation.ts must use daysAgoET() for default dates, not daysAgo() "
        "which uses UTC and can be off by a day."
    )


def test_utils_has_daysAgoET_function() -> None:
    """The utils module must export a daysAgoET function."""
    source = _read_ts(UTILS_TS)
    assert "function daysAgoET" in source or "daysAgoET" in source, (
        "utils.ts must have a daysAgoET function for ET-aware date defaults."
    )


def test_backend_uses_market_date_today() -> None:
    """Backend date validation must use market_date_today(), not datetime.now()."""
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest
    import inspect
    source = inspect.getsource(CreateBacktestRunRequest.validate_request)
    assert "market_date_today" in source, (
        "CreateBacktestRunRequest.validate_request must use market_date_today() "
        "for US Eastern time date boundary."
    )


def test_frontend_max_window_matches_backend() -> None:
    """Frontend max backtest window must match backend config."""
    source = _read_ts(VALIDATION_TS)
    assert "1825" in source, (
        "Frontend validation must enforce 1825-day (5-year) max window "
        "matching backend max_backtest_window_days."
    )
    from backtestforecast.config import get_settings
    assert get_settings().max_backtest_window_days == 1825
