"""Verify unknown subscription statuses default to PlanTier.FREE."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backtestforecast.billing.entitlements import PlanTier, normalize_plan_tier


class TestUnknownSubscriptionStatus:
    def test_unknown_status_with_future_period_end_grants_grace(self):
        result = normalize_plan_tier(
            plan_tier="pro",
            subscription_status="suspended",
            subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
        )
        assert result == PlanTier.PRO

    def test_completely_novel_status_with_future_period_end_grants_grace(self):
        result = normalize_plan_tier(
            plan_tier="premium",
            subscription_status="some_future_stripe_status",
            subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
        )
        assert result == PlanTier.PREMIUM

    def test_unknown_status_returns_free_after_grace_expires(self):
        result = normalize_plan_tier(
            plan_tier="pro",
            subscription_status="suspended",
            subscription_current_period_end=datetime.now(UTC) - timedelta(days=10),
        )
        assert result == PlanTier.FREE

    def test_known_active_status_returns_correct_tier(self):
        result = normalize_plan_tier(
            plan_tier="pro",
            subscription_status="active",
            subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
        )
        assert result == PlanTier.PRO

    def test_known_canceled_status_returns_free(self):
        result = normalize_plan_tier(
            plan_tier="premium",
            subscription_status="canceled",
            subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
        )
        assert result == PlanTier.FREE

    def test_none_status_returns_free(self):
        result = normalize_plan_tier(
            plan_tier="pro",
            subscription_status=None,
            subscription_current_period_end=None,
        )
        assert result == PlanTier.FREE
