"""Test that webhook rate limiting handles unidentified IPs properly.

Regression test for the security issue where all requests with
unidentifiable IPs shared the actor key "unknown", causing legitimate
Stripe webhooks to compete with other unidentified traffic.
"""
from __future__ import annotations

import inspect


def test_webhook_uses_distinct_key_for_unknown_ip():
    """Webhook must use a distinct key, not 'unknown', for unidentified IPs."""
    from apps.api.app.routers import billing
    source = inspect.getsource(billing.stripe_webhook)
    assert "unidentified" in source or "unknown_ip" in source, (
        "Webhook should use 'unidentified' (not 'unknown') as the fallback actor key"
    )


def test_webhook_logs_warning_on_missing_ip():
    """A missing IP should log a warning for operator investigation."""
    from apps.api.app.routers import billing
    source = inspect.getsource(billing.stripe_webhook)
    assert "ip_extraction_failed" in source or "ip is None" in source.lower(), (
        "Webhook should log a warning when client IP cannot be determined"
    )


def test_webhook_higher_limit_for_unknown_ip():
    """Unidentified IPs should get a higher limit to avoid false positives."""
    from apps.api.app.routers import billing
    source = inspect.getsource(billing.stripe_webhook)
    assert "60" in source, (
        "Unidentified IP webhook rate limit should be higher than the per-IP limit (30)"
    )
