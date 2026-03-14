"""Test 62: Stripe webhook ordering with different subscription IDs.

Verifies that when a webhook arrives for a different subscription_id than
the user's current active subscription, it is skipped and the user is not
modified (the stale-subscription guard in _apply_subscription_to_user).
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


def test_different_sub_id_skipped_when_active(db_session: Session) -> None:
    """Webhook with subscription_id != user's current active sub is skipped."""
    user = User(
        clerk_user_id="clerk_overwrite_test",
        email="overwrite@test.com",
        plan_tier="pro",
        subscription_status="active",
        stripe_subscription_id="sub_new",
        stripe_customer_id="cus_overwrite",
        stripe_price_id="price_pro_monthly",
        subscription_billing_interval="monthly",
        subscription_current_period_end=datetime(2025, 4, 1, tzinfo=UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    settings = _make_settings()
    service = BillingService(db_session, settings=settings)

    stale_sub = {
        "id": "sub_old",
        "customer": "cus_overwrite",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2025, 5, 1, tzinfo=UTC).timestamp()),
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

    service._apply_subscription_to_user(user, stale_sub)
    db_session.commit()
    db_session.refresh(user)

    assert user.stripe_subscription_id == "sub_new", (
        "Stale subscription webhook should be skipped; sub_id should remain sub_new"
    )
    assert user.plan_tier == "pro", (
        "Plan tier should remain unchanged when stale webhook is skipped"
    )
    assert user.subscription_current_period_end == datetime(2025, 4, 1, tzinfo=UTC), (
        "Period end should remain unchanged when stale webhook is skipped"
    )


def test_different_sub_id_skipped_preserves_trialing(db_session: Session) -> None:
    """Guard also protects trialing status from stale subscription events."""
    user = User(
        clerk_user_id="clerk_trial_guard",
        email="trial_guard@test.com",
        plan_tier="pro",
        subscription_status="trialing",
        stripe_subscription_id="sub_trial_current",
        stripe_customer_id="cus_trial",
        stripe_price_id="price_pro_monthly",
        subscription_billing_interval="monthly",
        subscription_current_period_end=datetime(2025, 4, 1, tzinfo=UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    settings = _make_settings()
    service = BillingService(db_session, settings=settings)

    stale_sub = {
        "id": "sub_old_trial",
        "customer": "cus_trial",
        "status": "canceled",
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

    service._apply_subscription_to_user(user, stale_sub)
    db_session.commit()
    db_session.refresh(user)

    assert user.stripe_subscription_id == "sub_trial_current"
    assert user.subscription_status == "trialing"


def test_same_sub_id_is_processed(db_session: Session) -> None:
    """Control: same subscription_id webhook IS processed normally."""
    user = User(
        clerk_user_id="clerk_same_sub",
        email="same_sub@test.com",
        plan_tier="pro",
        subscription_status="active",
        stripe_subscription_id="sub_same",
        stripe_customer_id="cus_same",
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
        "id": "sub_same",
        "customer": "cus_same",
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

    assert user.subscription_current_period_end == datetime(2025, 5, 1, tzinfo=UTC), (
        "Same-sub webhook with newer period should be applied"
    )
