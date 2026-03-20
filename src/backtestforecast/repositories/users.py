from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import User


class UserRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, user_id: UUID) -> User | None:
        return self.session.get(User, user_id)

    def get_by_clerk_user_id(self, clerk_user_id: str) -> User | None:
        stmt = select(User).where(User.clerk_user_id == clerk_user_id)
        return self.session.scalar(stmt)

    def sync_email_if_needed(self, user: User, email: str | None) -> bool:
        """Update the stored email only when a non-empty new value differs.

        Returns True when a mutation was applied to the session.
        """
        if not email or user.email == email:
            return False
        nested = self.session.begin_nested()
        try:
            self.session.execute(
                update(User)
                .where(User.id == user.id, User.email != email)
                .values(email=email)
            )
            nested.commit()
            self.session.refresh(user)
            return True
        except Exception:
            nested.rollback()
            raise

    def get_by_stripe_customer_id(self, stripe_customer_id: str) -> User | None:
        stmt = select(User).where(User.stripe_customer_id == stripe_customer_id)
        return self.session.scalar(stmt)

    def get_by_stripe_subscription_id(self, stripe_subscription_id: str) -> User | None:
        stmt = select(User).where(User.stripe_subscription_id == stripe_subscription_id)
        return self.session.scalar(stmt)

    def get_or_create(self, clerk_user_id: str, email: str | None) -> User:
        existing = self.get_by_clerk_user_id(clerk_user_id)
        if existing is not None:
            self.sync_email_if_needed(existing, email)
            return existing

        user = User(clerk_user_id=clerk_user_id, email=email)
        nested = self.session.begin_nested()
        self.session.add(user)
        try:
            nested.commit()
        except IntegrityError:
            nested.rollback()
            self.session.expire_all()
            existing = self.get_by_clerk_user_id(clerk_user_id)
            if existing is not None:
                return existing
            import time as _time
            _time.sleep(0.01)
            self.session.expire_all()
            existing = self.get_by_clerk_user_id(clerk_user_id)
            if existing is not None:
                return existing
            raise
        return user
