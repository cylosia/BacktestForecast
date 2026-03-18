"""Test that the webhook body limit override is at least 256 KB."""
from __future__ import annotations

from backtestforecast.security.http import BODY_LIMIT_OVERRIDES


def test_billing_webhook_override_exists():
    assert "/v1/billing/webhook" in BODY_LIMIT_OVERRIDES


def test_billing_webhook_limit_at_least_256kb():
    limit = BODY_LIMIT_OVERRIDES["/v1/billing/webhook"]
    assert limit >= 256_000, f"Webhook body limit is {limit}, expected >= 256000"


def test_body_limit_overrides_values_are_positive():
    for path, limit in BODY_LIMIT_OVERRIDES.items():
        assert isinstance(limit, int), f"Limit for {path} should be int"
        assert limit > 0, f"Limit for {path} should be positive"
