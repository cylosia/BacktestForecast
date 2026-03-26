"""Verify daily picks returns gracefully when no pipeline runs exist."""
from __future__ import annotations

from backtestforecast.services.daily_picks import DailyPicksService


def test_no_pipeline_runs(db_session):
    service = DailyPicksService(db_session)
    result = service.get_latest_picks()
    assert result.status == "no_data"
    assert result.items == []
