"""Verify past_due subscription status preserves plan tier for grace period."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backtestforecast.billing.entitlements import normalize_plan_tier, PlanTier, PAID_STATUSES


def test_past_due_not_in_paid_statuses():
    """past_due must NOT be in PAID_STATUSES to trigger the grace path."""
    assert "past_due" not in PAID_STATUSES


def test_normalize_plan_tier_past_due_within_grace():
    """A past_due user within the grace window should retain their earned tier."""
    from datetime import UTC, datetime, timedelta

    period_end = datetime.now(UTC) + timedelta(days=1)
    tier = normalize_plan_tier("pro", "past_due", period_end)
    assert tier == PlanTier.PRO, f"Expected PRO within grace, got {tier}"


def test_normalize_plan_tier_past_due_expired_grace():
    """A past_due user beyond the grace window should be downgraded to FREE."""
    from datetime import UTC, datetime, timedelta

    period_end = datetime.now(UTC) - timedelta(days=30)
    tier = normalize_plan_tier("pro", "past_due", period_end)
    assert tier == PlanTier.FREE, f"Expected FREE after grace expired, got {tier}"


def test_billing_service_preserves_tier_on_past_due():
    """_apply_subscription_to_user must NOT overwrite plan_tier to free for past_due."""
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    session.scalar.return_value = None  # will be overridden below
    settings = MagicMock()
    settings.app_env = "development"
    service = BillingService(session, settings=settings)

    user = MagicMock()
    user.id = "user-123"
    user.plan_tier = "pro"
    user.subscription_status = "active"
    user.stripe_subscription_id = "sub_123"
    user.subscription_current_period_end = None
    user.cancel_at_period_end = False

    session.scalar.return_value = user

    subscription = {
        "id": "sub_123",
        "customer": "cus_123",
        "status": "past_due",
        "cancel_at_period_end": False,
        "current_period_end": None,
        "items": {"data": [{"price": {"id": "price_pro_monthly"}}]},
        "metadata": {},
    }

    with patch.object(service, "_configured_tier_for_price", return_value="pro"):
        service._apply_subscription_to_user(user, subscription)

    assert user.plan_tier == "pro", (
        f"past_due should preserve pro tier, got {user.plan_tier}"
    )
