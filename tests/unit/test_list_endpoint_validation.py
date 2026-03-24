"""Test boundary validation for list endpoint parameters.

Covers the risk that negative limit or offset values produce incorrect SQL
queries or unexpected behavior.
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from backtestforecast.errors import AppValidationError
from backtestforecast.models import User
from backtestforecast.services.backtests import BacktestService


def _make_user(**overrides) -> User:
    defaults = dict(
        id=uuid4(),
        clerk_user_id="clerk_test",
        plan_tier="free",
        subscription_status=None,
        subscription_current_period_end=None,
        cancel_at_period_end=False,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    user = MagicMock(spec=User)
    for k, v in defaults.items():
        setattr(user, k, v)
    return user


class TestListRunsValidation:
    def test_negative_limit_raises(self):
        session = MagicMock()
        service = BacktestService(session)
        user = _make_user()
        with pytest.raises(AppValidationError, match="limit must be >= 1"):
            service.list_runs(user, limit=-1)

    def test_zero_limit_raises(self):
        session = MagicMock()
        service = BacktestService(session)
        user = _make_user()
        with pytest.raises(AppValidationError, match="limit must be >= 1"):
            service.list_runs(user, limit=0)

    def test_negative_offset_raises(self):
        session = MagicMock()
        service = BacktestService(session)
        user = _make_user()
        with pytest.raises(AppValidationError, match="offset must be >= 0"):
            service.list_runs(user, offset=-1)

    def test_valid_limit_and_offset_accepted(self):
        session = MagicMock()
        service = BacktestService(session)
        user = _make_user()

        repo_mock = MagicMock()
        repo_mock.list_for_user_with_capped_count.return_value = ([], 0)
        service.run_repository = repo_mock

        result = service.list_runs(user, limit=10, offset=0)
        assert result.items == []
        assert result.total == 0
        repo_mock.list_for_user_with_capped_count.assert_called_once()

    def test_history_cap_is_enforced_in_repository_query(self):
        session = MagicMock()
        service = BacktestService(session)
        user = _make_user(plan_tier="free")

        repo_mock = MagicMock()
        repo_mock.list_for_user_with_capped_count.return_value = ([], 25)
        service.run_repository = repo_mock

        service.list_runs(user, limit=50, offset=25)

        _, kwargs = repo_mock.list_for_user_with_capped_count.call_args
        assert kwargs["max_items"] == 25
        assert kwargs["offset"] == 25
