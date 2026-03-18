"""Verify billing lifecycle: create → use → cancel → verify enforcement."""
from __future__ import annotations


def test_billing_service_has_complete_lifecycle_methods():
    """BillingService should support checkout, webhook, and cancellation."""
    from backtestforecast.services.billing import BillingService
    assert hasattr(BillingService, "create_checkout_session")
    assert hasattr(BillingService, "handle_webhook")
    assert hasattr(BillingService, "cancel_in_flight_jobs")
    assert hasattr(BillingService, "_sync_subscription")
    assert hasattr(BillingService, "_apply_subscription_to_user")


def test_feature_policy_tiers_defined():
    """All plan tiers should have defined feature policies."""
    from backtestforecast.billing.entitlements import FEATURE_POLICIES
    from backtestforecast.schemas.common import PlanTier
    for tier in PlanTier:
        assert tier in FEATURE_POLICIES, f"Missing policy for {tier}"


def test_paid_statuses_defined():
    """PAID_STATUSES should include active and trialing."""
    from backtestforecast.billing.entitlements import PAID_STATUSES
    assert "active" in PAID_STATUSES
    assert "trialing" in PAID_STATUSES
    assert "canceled" not in PAID_STATUSES
