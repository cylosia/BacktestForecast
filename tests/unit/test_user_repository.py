"""Tests for UserRepository correctness."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from backtestforecast.repositories.users import UserRepository


class TestUserRepository:
    def test_get_or_create_returns_existing_user(self):
        session = MagicMock()
        user = MagicMock()
        user.email = "test@example.com"

        repo = UserRepository(session)
        with patch.object(repo, "get_by_clerk_user_id", return_value=user):
            result = repo.get_or_create("clerk_123", "test@example.com")
            assert result == user

    def test_get_or_create_updates_email_if_changed(self):
        session = MagicMock()
        user = MagicMock()
        user.email = "old@example.com"

        repo = UserRepository(session)
        with patch.object(repo, "get_by_clerk_user_id", return_value=user):
            result = repo.get_or_create("clerk_123", "new@example.com")
            assert result.email == "new@example.com"
            session.add.assert_called_once_with(user)
            session.flush.assert_called_once()

    def test_get_or_create_does_not_update_email_if_none(self):
        session = MagicMock()
        user = MagicMock()
        user.email = "test@example.com"

        repo = UserRepository(session)
        with patch.object(repo, "get_by_clerk_user_id", return_value=user):
            result = repo.get_or_create("clerk_123", None)
            assert result == user
            session.add.assert_not_called()

    def test_get_by_stripe_subscription_id_uses_correct_column(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = UserRepository(session)
        result = repo.get_by_stripe_subscription_id("sub_123")
        assert result is None
        session.scalar.assert_called_once()
