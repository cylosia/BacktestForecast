"""Tests for StripeEventRepository correctness."""
from __future__ import annotations

from unittest.mock import MagicMock

from backtestforecast.models import StripeEvent
from backtestforecast.repositories.stripe_events import StripeEventRepository


class TestStripeEventRepository:
    def test_init_stores_session(self):
        session = MagicMock()
        repo = StripeEventRepository(session)
        assert repo.session is session

    def test_mark_processed_executes_update(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 1
        repo = StripeEventRepository(session)
        repo.mark_processed("evt_123")
        session.execute.assert_called_once()

    def test_mark_error_truncates_detail(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 1
        repo = StripeEventRepository(session)
        long_detail = "x" * 3000
        repo.mark_error("evt_123", long_detail)
        session.execute.assert_called_once()
        stmt = session.execute.call_args.args[0]
        assert stmt._values[StripeEvent.__table__.c.error_detail].value == long_detail[:2000]

    def test_list_recent_returns_list(self):
        session = MagicMock()
        events = [MagicMock(), MagicMock()]
        session.scalars.return_value = events
        repo = StripeEventRepository(session)
        result = repo.list_recent(limit=10)
        assert result == events

    def test_get_by_stripe_id(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = StripeEventRepository(session)
        result = repo.get_by_stripe_id("evt_nonexistent")
        assert result is None
