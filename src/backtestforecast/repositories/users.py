from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
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

    def get_by_stripe_customer_id(self, stripe_customer_id: str) -> User | None:
        stmt = select(User).where(User.stripe_customer_id == stripe_customer_id)
        return self.session.scalar(stmt)

    def get_by_stripe_subscription_id(self, stripe_subscription_id: str) -> User | None:
        stmt = select(User).where(User.stripe_subscription_id == stripe_subscription_id)
        return self.session.scalar(stmt)

    def get_or_create(self, clerk_user_id: str, email: str | None) -> User:
        existing = self.get_by_clerk_user_id(clerk_user_id)
        if existing is not None:
            if email and existing.email != email:
                existing.email = email
                self.session.add(existing)
                self.session.flush()
            return existing

        user = User(clerk_user_id=clerk_user_id, email=email)
        nested = self.session.begin_nested()
        self.session.add(user)
        try:
            nested.commit()
        except IntegrityError:
            nested.rollback()
            import time as _time
            for _attempt in range(3):
                self.session.expire(user)
                existing = self.get_by_clerk_user_id(clerk_user_id)
                if existing is not None:
                    return existing
                _time.sleep(0.05 * (2 ** _attempt))
            raise
        return user
