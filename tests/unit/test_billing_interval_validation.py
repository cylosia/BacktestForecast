"""Test that billing interval fields use enum/pattern validation.

Regression test for the contract mismatch where subscription_billing_interval
accepted any string without validation, despite the DB CHECK constraint.
"""
from __future__ import annotations

from pydantic import ValidationError
import pytest


class TestCheckoutSessionResponse:
    def test_valid_billing_interval_monthly(self):
        from backtestforecast.schemas.billing import CheckoutSessionResponse
        r = CheckoutSessionResponse(
            session_id="cs_123",
            checkout_url="https://checkout.stripe.com/test",
            tier="pro",
            billing_interval="monthly",
        )
        assert str(r.billing_interval) == "monthly"

    def test_valid_billing_interval_yearly(self):
        from backtestforecast.schemas.billing import CheckoutSessionResponse
        r = CheckoutSessionResponse(
            session_id="cs_456",
            checkout_url="https://checkout.stripe.com/test",
            tier="premium",
            billing_interval="yearly",
        )
        assert str(r.billing_interval) == "yearly"

    def test_invalid_billing_interval_rejected(self):
        from backtestforecast.schemas.billing import CheckoutSessionResponse
        with pytest.raises(ValidationError):
            CheckoutSessionResponse(
                session_id="cs_789",
                checkout_url="https://checkout.stripe.com/test",
                tier="pro",
                billing_interval="weekly",
            )

    def test_tier_uses_plan_tier_enum(self):
        from backtestforecast.schemas.billing import CheckoutSessionResponse
        r = CheckoutSessionResponse(
            session_id="cs_abc",
            checkout_url="https://checkout.stripe.com/test",
            tier="pro",
            billing_interval="monthly",
        )
        from backtestforecast.schemas.common import PlanTier
        assert r.tier == PlanTier.PRO


class TestBillingStateResponse:
    def test_valid_billing_interval(self):
        from backtestforecast.schemas.billing import BillingStateResponse
        r = BillingStateResponse(plan_tier="pro", subscription_billing_interval="monthly")
        assert str(r.subscription_billing_interval) == "monthly"

    def test_none_billing_interval(self):
        from backtestforecast.schemas.billing import BillingStateResponse
        r = BillingStateResponse(plan_tier="free", subscription_billing_interval=None)
        assert r.subscription_billing_interval is None

    def test_invalid_billing_interval_rejected(self):
        from backtestforecast.schemas.billing import BillingStateResponse
        with pytest.raises(ValidationError):
            BillingStateResponse(plan_tier="pro", subscription_billing_interval="weekly")


class TestCurrentUserBillingInterval:
    def test_valid_interval_accepted(self):
        from backtestforecast.schemas.backtests import CurrentUserResponse
        field = CurrentUserResponse.model_fields["subscription_billing_interval"]
        metadata = field.metadata
        has_pattern = any(
            hasattr(m, "pattern") for m in metadata
        )
        assert has_pattern, (
            "subscription_billing_interval must have pattern validation "
            "constraining to monthly|yearly"
        )
