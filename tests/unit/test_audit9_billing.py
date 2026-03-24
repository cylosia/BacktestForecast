"""Billing-specific tests for audit round 9 fixes."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest


class TestPlanTierSnapshot:
    """Fix #11: plan_tier_snapshot should use normalized tier."""

    def test_normalize_plan_tier_for_past_due_beyond_grace(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        period_end = datetime(2026, 1, 1, tzinfo=UTC)  # far in the past
        result = normalize_plan_tier("pro", "past_due", period_end)
        assert result == PlanTier.FREE

    def test_normalize_plan_tier_for_active(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        period_end = datetime(2026, 12, 31, tzinfo=UTC)  # far in the future
        result = normalize_plan_tier("premium", "active", period_end)
        assert result == PlanTier.PREMIUM

    def test_normalize_plan_tier_for_canceled(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier("pro", "canceled", None)
        assert result == PlanTier.FREE


class TestBillingWebhookIdempotency:
    """Fix #53: Webhook events are idempotent."""

    def test_handle_webhook_delegates_to_webhook_handler(self):
        from backtestforecast.services.billing import BillingService

        service = BillingService.__new__(BillingService)
        service.webhook_handler = MagicMock()
        service.webhook_handler.handle_webhook.return_value = {"status": "duplicate"}

        result = service.handle_webhook(
            b"payload",
            "sig_header",
            request_id="req_1",
            ip_address="1.2.3.4",
        )

        service.webhook_handler.handle_webhook.assert_called_once_with(
            b"payload",
            "sig_header",
            request_id="req_1",
            ip_address="1.2.3.4",
        )
        assert result["status"] == "duplicate"


class TestSweepAccessEntitlement:
    """Verify sweep entitlement gating."""

    def test_free_tier_cannot_sweep(self):
        from backtestforecast.billing.entitlements import ensure_sweep_access
        from backtestforecast.errors import FeatureLockedError

        with pytest.raises(FeatureLockedError):
            ensure_sweep_access(
                "free", "active", datetime(2026, 12, 31, tzinfo=UTC)
            )

    def test_pro_tier_can_sweep(self):
        from backtestforecast.billing.entitlements import ensure_sweep_access

        ensure_sweep_access("pro", "active", datetime(2026, 12, 31, tzinfo=UTC))
