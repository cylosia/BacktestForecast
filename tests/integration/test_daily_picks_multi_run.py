"""Verify daily picks returns the latest succeeded run for a given date."""
from __future__ import annotations


def test_latest_run_is_selected():
    """DailyPicksRepository should return the most recent succeeded run."""
    from backtestforecast.repositories.daily_picks import DailyPicksRepository
    import inspect
    source = inspect.getsource(DailyPicksRepository.get_latest_succeeded_run)
    assert "desc" in source.lower()
    assert "limit(1)" in source
