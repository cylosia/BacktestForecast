"""Tests for UserRepository correctness.

Includes mock-based tests for fast isolation and SQLite-backed tests for
integration with real SQLAlchemy queries.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from backtestforecast.models import User
from backtestforecast.repositories.users import UserRepository

# ---- Mock-based tests (original) ----------------------------------------


class TestUserRepositoryMocked:
    def test_get_or_create_returns_existing_user(self):
        session = MagicMock()
        user = MagicMock()
        user.email = "test@example.com"

        repo = UserRepository(session)
        with patch.object(repo, "get_by_clerk_user_id", return_value=user):
            result = repo.get_or_create("clerk_123", "test@example.com")
            assert result == user

    def test_get_or_create_does_not_update_email_if_none(self):
        session = MagicMock()
        user = MagicMock()
        user.email = "test@example.com"

        repo = UserRepository(session)
        with patch.object(repo, "get_by_clerk_user_id", return_value=user):
            result = repo.get_or_create("clerk_123", None)
            assert result == user

    def test_get_by_stripe_subscription_id_uses_correct_column(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = UserRepository(session)
        result = repo.get_by_stripe_subscription_id("sub_123")
        assert result is None
        session.scalar.assert_called_once()


# ---- SQLite-backed integration tests ------------------------------------


@pytest.fixture
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


class TestGetByStripeCustomerId:
    def test_found(self, db_session: Session):
        user = User(clerk_user_id="clerk_1", stripe_customer_id="cus_abc123")
        db_session.add(user)
        db_session.commit()

        repo = UserRepository(db_session)
        result = repo.get_by_stripe_customer_id("cus_abc123")
        assert result is not None
        assert result.id == user.id
        assert result.stripe_customer_id == "cus_abc123"

    def test_not_found(self, db_session: Session):
        repo = UserRepository(db_session)
        result = repo.get_by_stripe_customer_id("cus_nonexistent")
        assert result is None

    def test_does_not_match_other_fields(self, db_session: Session):
        user = User(
            clerk_user_id="clerk_2",
            stripe_customer_id="cus_real",
            stripe_subscription_id="sub_other",
        )
        db_session.add(user)
        db_session.commit()

        repo = UserRepository(db_session)
        assert repo.get_by_stripe_customer_id("sub_other") is None


class TestGetByStripeSubscriptionId:
    def test_found(self, db_session: Session):
        user = User(clerk_user_id="clerk_3", stripe_subscription_id="sub_xyz789")
        db_session.add(user)
        db_session.commit()

        repo = UserRepository(db_session)
        result = repo.get_by_stripe_subscription_id("sub_xyz789")
        assert result is not None
        assert result.id == user.id

    def test_not_found(self, db_session: Session):
        repo = UserRepository(db_session)
        result = repo.get_by_stripe_subscription_id("sub_nonexistent")
        assert result is None


class TestGetOrCreate:
    def test_creates_new_user(self, db_session: Session):
        repo = UserRepository(db_session)
        user = repo.get_or_create("clerk_new_user", "new@example.com")
        db_session.commit()

        assert user is not None
        assert user.clerk_user_id == "clerk_new_user"
        assert user.email == "new@example.com"

        fetched = repo.get_by_clerk_user_id("clerk_new_user")
        assert fetched is not None
        assert fetched.id == user.id

    def test_returns_existing_without_duplicate(self, db_session: Session):
        repo = UserRepository(db_session)
        first = repo.get_or_create("clerk_existing", "first@example.com")
        db_session.commit()

        second = repo.get_or_create("clerk_existing", "first@example.com")
        assert second.id == first.id

        from sqlalchemy import func, select
        count = db_session.scalar(
            select(func.count()).select_from(User).where(User.clerk_user_id == "clerk_existing")
        )
        assert count == 1, "get_or_create must not create a duplicate"

    def test_updates_email_on_existing_user(self, db_session: Session):
        repo = UserRepository(db_session)
        user = repo.get_or_create("clerk_email_update", "old@example.com")
        db_session.commit()

        returned = repo.get_or_create("clerk_email_update", "new@example.com")
        db_session.commit()

        assert returned.id == user.id
        db_session.refresh(returned)
        assert returned.email == "new@example.com"

    def test_none_email_does_not_overwrite(self, db_session: Session):
        repo = UserRepository(db_session)
        user = repo.get_or_create("clerk_keep_email", "keep@example.com")
        db_session.commit()

        returned = repo.get_or_create("clerk_keep_email", None)
        assert returned.id == user.id
        db_session.refresh(returned)
        assert returned.email == "keep@example.com"

    def test_creates_user_with_none_email(self, db_session: Session):
        repo = UserRepository(db_session)
        user = repo.get_or_create("clerk_no_email", None)
        db_session.commit()

        assert user.email is None
        assert user.clerk_user_id == "clerk_no_email"
