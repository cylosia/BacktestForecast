"""Item 66: Test out-of-order webhook plan updates.

Verifies _apply_subscription_to_user: when a webhook with an older
current_period_end arrives after one with a newer period_end, the older
one should be ignored (user plan should not be downgraded).
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from backtestforecast.models import User
from backtestforecast.services.billing import BillingService


def _make_settings() -> MagicMock:
    settings = MagicMock()
    settings.stripe_secret_key = "sk_test_xxx"
    settings.stripe_webhook_secret = "whsec_test"
    settings.app_public_url = "http://localhost:3000"
    settings.stripe_price_lookup = {
        ("pro", "monthly"): "price_pro_monthly",
        ("premium", "monthly"): "price_premium_monthly",
    }
    return settings


def _strip_tz(dt: datetime) -> datetime:
    """Strip timezone info for SQLite-safe comparison."""
    return dt.replace(tzinfo=None)


def test_out_of_order_webhook_does_not_downgrade(db_session: Session) -> None:
    user = User(
        clerk_user_id="clerk_ooo_test",
        email="ooo@test.com",
        plan_tier="pro",
        subscription_status="active",
        stripe_subscription_id="sub_123",
        stripe_customer_id="cus_123",
        stripe_price_id="price_pro_monthly",
        subscription_billing_interval="monthly",
        subscription_current_period_end=datetime(2025, 3, 1, tzinfo=UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    settings = _make_settings()
    service = BillingService(db_session, settings=settings)

    newer_sub = {
        "id": "sub_123",
        "customer": "cus_123",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2025, 4, 1, tzinfo=UTC).timestamp()),
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_pro_monthly",
                        "recurring": {"interval": "month"},
                    }
                }
            ]
        },
        "metadata": {"user_id": str(user.id), "requested_tier": "pro"},
    }
    service._apply_subscription_to_user(user, newer_sub)
    db_session.commit()
    db_session.refresh(user)

    assert _strip_tz(user.subscription_current_period_end) == _strip_tz(datetime(2025, 4, 1, tzinfo=UTC))
    assert user.plan_tier == "pro"

    older_sub = {
        "id": "sub_123",
        "customer": "cus_123",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2025, 3, 1, tzinfo=UTC).timestamp()),
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_pro_monthly",
                        "recurring": {"interval": "month"},
                    }
                }
            ]
        },
        "metadata": {"user_id": str(user.id), "requested_tier": "pro"},
    }
    service._apply_subscription_to_user(user, older_sub)
    db_session.commit()
    db_session.refresh(user)

    assert _strip_tz(user.subscription_current_period_end) == _strip_tz(datetime(2025, 4, 1, tzinfo=UTC)), (
        "Older webhook should be ignored — period_end must remain at the newer value"
    )
    assert user.plan_tier == "pro"


def test_different_subscription_id_not_dropped(db_session: Session) -> None:
    """Item 82: When subscription_id differs from the stored one and the
    incoming period_end is later, the webhook must be processed (potential
    upgrade/downgrade that creates a new Stripe subscription)."""
    user = User(
        clerk_user_id="clerk_diff_sub_test",
        email="diff_sub@test.com",
        plan_tier="pro",
        subscription_status="active",
        stripe_subscription_id="sub_old",
        stripe_customer_id="cus_789",
        stripe_price_id="price_pro_monthly",
        subscription_billing_interval="monthly",
        subscription_current_period_end=datetime(2025, 3, 1, tzinfo=UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    settings = _make_settings()
    service = BillingService(db_session, settings=settings)

    new_sub_with_later_period = {
        "id": "sub_new",
        "customer": "cus_789",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2025, 4, 15, tzinfo=UTC).timestamp()),
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_premium_monthly",
                        "recurring": {"interval": "month"},
                    }
                }
            ]
        },
        "metadata": {"user_id": str(user.id), "requested_tier": "premium"},
    }
    service._apply_subscription_to_user(user, new_sub_with_later_period)
    db_session.commit()
    db_session.refresh(user)

    assert user.stripe_subscription_id == "sub_new", (
        "Webhook with different subscription_id and later period must be processed, not skipped"
    )
    assert _strip_tz(user.subscription_current_period_end) == _strip_tz(datetime(2025, 4, 15, tzinfo=UTC))


def test_newer_webhook_does_update(db_session: Session) -> None:
    user = User(
        clerk_user_id="clerk_ooo_test2",
        email="ooo2@test.com",
        plan_tier="pro",
        subscription_status="active",
        stripe_subscription_id="sub_456",
        stripe_customer_id="cus_456",
        stripe_price_id="price_pro_monthly",
        subscription_billing_interval="monthly",
        subscription_current_period_end=datetime(2025, 3, 1, tzinfo=UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    settings = _make_settings()
    service = BillingService(db_session, settings=settings)

    newer_sub = {
        "id": "sub_456",
        "customer": "cus_456",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2025, 5, 1, tzinfo=UTC).timestamp()),
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_pro_monthly",
                        "recurring": {"interval": "month"},
                    }
                }
            ]
        },
        "metadata": {"user_id": str(user.id), "requested_tier": "pro"},
    }
    service._apply_subscription_to_user(user, newer_sub)
    db_session.commit()
    db_session.refresh(user)

    assert _strip_tz(user.subscription_current_period_end) == _strip_tz(datetime(2025, 5, 1, tzinfo=UTC)), (
        "Newer webhook should be applied"
    )
