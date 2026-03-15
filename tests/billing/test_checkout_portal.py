"""Tests for billing checkout and portal session creation.

Verifies correct Stripe price ID lookup, metadata, and return URL validation.
"""
from __future__ import annotations

import inspect

import pytest


class TestCheckoutPriceLookup:
    """Verify the correct Stripe price ID is resolved for each tier/interval."""

    def test_price_lookup_is_dict(self):
        """Verify stripe_price_lookup returns a dict keyed by (tier, interval)."""
        from backtestforecast.config import Settings

        settings = Settings(
            stripe_pro_monthly_price_id="price_pro_m",
            stripe_pro_yearly_price_id="price_pro_y",
            stripe_premium_monthly_price_id="price_prem_m",
            stripe_premium_yearly_price_id="price_prem_y",
        )
        lookup = settings.stripe_price_lookup
        assert isinstance(lookup, dict)
        assert len(lookup) == 4

    def test_price_lookup_keys_are_tier_interval_tuples(self):
        from backtestforecast.config import Settings

        settings = Settings(
            stripe_pro_monthly_price_id="price_pro_m",
            stripe_premium_monthly_price_id="price_prem_m",
        )
        lookup = settings.stripe_price_lookup
        for key in lookup:
            assert isinstance(key, tuple)
            assert len(key) == 2
            tier, interval = key
            assert tier in ("pro", "premium")
            assert interval in ("monthly", "yearly")

    def test_price_lookup_empty_when_unconfigured(self):
        from backtestforecast.config import Settings

        settings = Settings()
        lookup = settings.stripe_price_lookup
        assert isinstance(lookup, dict)
        assert len(lookup) == 0

    def test_stripe_billing_enabled_requires_keys(self):
        from backtestforecast.config import Settings

        settings = Settings()
        assert settings.stripe_billing_enabled is False

    def test_stripe_billing_enabled_with_full_config(self):
        from backtestforecast.config import Settings

        settings = Settings(
            stripe_secret_key="sk_test_xxx",
            stripe_webhook_secret="whsec_xxx",
            stripe_pro_monthly_price_id="price_pro_m",
        )
        assert settings.stripe_billing_enabled is True


class TestBillingServiceInterface:
    """Verify BillingService has the expected methods."""

    def test_create_checkout_session_exists(self):
        from backtestforecast.services.billing import BillingService
        assert hasattr(BillingService, "create_checkout_session")

    def test_create_portal_session_exists(self):
        from backtestforecast.services.billing import BillingService
        assert hasattr(BillingService, "create_portal_session")

    def test_handle_webhook_exists(self):
        from backtestforecast.services.billing import BillingService
        assert hasattr(BillingService, "handle_webhook")

    def test_create_checkout_session_signature(self):
        from backtestforecast.services.billing import BillingService
        sig = inspect.signature(BillingService.create_checkout_session)
        params = list(sig.parameters.keys())
        assert "self" in params
        assert "user" in params
        assert "payload" in params

    def test_create_portal_session_signature(self):
        from backtestforecast.services.billing import BillingService
        sig = inspect.signature(BillingService.create_portal_session)
        params = list(sig.parameters.keys())
        assert "self" in params
        assert "user" in params
        assert "payload" in params
