"""Test that unknown Stripe subscription statuses have a 72-hour grace period.

Previously, unknown statuses preserved the paid tier indefinitely. Now
the user keeps their tier for 72 hours after current_period_end, then
is downgraded to FREE.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backtestforecast.billing.entitlements import PlanTier, normalize_plan_tier


def test_unknown_status_within_grace_preserves_tier() -> None:
    """Within 72h of period_end, unknown status preserves the tier."""
    period_end = datetime.now(UTC) - timedelta(hours=1)
    result = normalize_plan_tier(
        "pro",
        subscription_status="some_new_status",
        subscription_current_period_end=period_end,
    )
    assert result == PlanTier.PRO


def test_unknown_status_after_grace_downgrades_to_free() -> None:
    """After 72h past period_end, unknown status downgrades to FREE."""
    period_end = datetime.now(UTC) - timedelta(hours=73)
    result = normalize_plan_tier(
        "premium",
        subscription_status="some_new_status",
        subscription_current_period_end=period_end,
    )
    assert result == PlanTier.FREE


def test_unknown_status_no_period_end_downgrades_to_free() -> None:
    """Without period_end, unknown status downgrades to FREE (can't compute grace deadline)."""
    result = normalize_plan_tier(
        "pro",
        subscription_status="some_new_status",
        subscription_current_period_end=None,
    )
    assert result == PlanTier.FREE


def test_known_paid_status_not_affected() -> None:
    """Active/trialing statuses are not affected by this logic."""
    result = normalize_plan_tier(
        "premium",
        subscription_status="active",
        subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
    )
    assert result == PlanTier.PREMIUM


def test_known_inactive_status_downgrades() -> None:
    """Canceled/unpaid statuses immediately downgrade."""
    result = normalize_plan_tier(
        "premium",
        subscription_status="canceled",
    )
    assert result == PlanTier.FREE
