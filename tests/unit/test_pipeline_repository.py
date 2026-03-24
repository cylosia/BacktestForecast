"""Tests for the DailyPicksRepository (formerly NightlyPipelineRunRepository)."""
from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from backtestforecast.repositories.daily_picks import DailyPicksRepository


def _make_run(
    *,
    status: str = "succeeded",
    trade_date: date | None = None,
    created_at: datetime | None = None,
):
    run = MagicMock()
    run.id = uuid.uuid4()
    run.status = status
    run.trade_date = trade_date or date(2026, 3, 17)
    run.created_at = created_at or datetime.now(UTC)
    run.completed_at = datetime.now(UTC)
    run.duration_seconds = Decimal("42.5")
    run.symbols_screened = 100
    run.symbols_after_screen = 50
    run.pairs_generated = 200
    run.quick_backtests_run = 150
    run.full_backtests_run = 30
    run.recommendations_produced = 10
    run.error_message = None
    return run


class TestDailyPicksRepository:
    def test_get_latest_succeeded_run_returns_none_for_empty_db(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = DailyPicksRepository(session)
        result = repo.get_latest_succeeded_run()
        assert result is None
        session.scalar.assert_called_once()

    def test_get_latest_succeeded_run_with_trade_date(self):
        session = MagicMock()
        run = _make_run()
        session.scalar.return_value = run
        repo = DailyPicksRepository(session)
        result = repo.get_latest_succeeded_run(trade_date=date(2026, 3, 17))
        assert result == run

    def test_get_recommendations_for_run_returns_list(self):
        session = MagicMock()
        rec = MagicMock()
        session.scalars.return_value = [rec]
        repo = DailyPicksRepository(session)
        result = repo.get_recommendations_for_run(uuid.uuid4(), limit=10)
        assert result == [rec]

    def test_list_pipeline_history_without_cursor(self):
        session = MagicMock()
        runs = [_make_run(), _make_run()]
        session.scalars.return_value = runs
        repo = DailyPicksRepository(session)
        result = repo.list_pipeline_history(limit=10)
        assert result == runs

    def test_list_pipeline_history_with_cursor(self):
        session = MagicMock()
        session.scalars.return_value = []
        repo = DailyPicksRepository(session)
        cursor_dt = datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC)
        cursor_id = uuid.uuid4()
        result = repo.list_pipeline_history(limit=5, cursor_before=(cursor_dt, cursor_id))
        assert result == []
