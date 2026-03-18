"""Tests for BacktestTemplateRepository correctness."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from backtestforecast.repositories.templates import BacktestTemplateRepository


class TestBacktestTemplateRepository:
    def test_init_stores_session(self):
        session = MagicMock()
        repo = BacktestTemplateRepository(session)
        assert repo.session is session

    def test_add_template(self):
        session = MagicMock()
        repo = BacktestTemplateRepository(session)
        template = MagicMock()
        result = repo.add(template)
        session.add.assert_called_once_with(template)
        session.flush.assert_called_once()
        assert result is template

    def test_delete_template(self):
        session = MagicMock()
        repo = BacktestTemplateRepository(session)
        template = MagicMock()
        repo.delete(template)
        session.delete.assert_called_once_with(template)
        session.flush.assert_called_once()

    def test_get_for_user_returns_none_when_not_found(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = BacktestTemplateRepository(session)
        result = repo.get_for_user(uuid.uuid4(), uuid.uuid4())
        assert result is None

    def test_count_for_user(self):
        session = MagicMock()
        session.scalar.return_value = 3
        repo = BacktestTemplateRepository(session)
        assert repo.count_for_user(uuid.uuid4()) == 3
