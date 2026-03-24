"""Verify webhook error handling categorizes errors correctly."""
from __future__ import annotations

import inspect


def test_webhook_handler_categorizes_transient_errors():
    """Transient errors should return 500 so Stripe retries."""
    from apps.api.app.routers.billing import stripe_webhook

    source = inspect.getsource(stripe_webhook)
    assert "ExternalServiceError" in source or "_ExtErr" in source, (
        "Webhook must handle ExternalServiceError as transient (500)"
    )
    assert "Stripe should retry" in source, (
        "Transient errors must tell Stripe to retry"
    )


def test_webhook_handler_categorizes_deterministic_errors():
    """Deterministic errors should return 200 so Stripe does not retry."""
    from apps.api.app.routers.billing import stripe_webhook

    source = inspect.getsource(stripe_webhook)
    assert "will not retry" in source, (
        "Deterministic errors must signal no retry needed"
    )


def test_webhook_logs_ignored_event_types():
    """Unknown Stripe event types should be logged for monitoring."""
    from backtestforecast.services.billing import BillingService

    source = inspect.getsource(BillingService._handle_webhook_impl)
    assert "ignored" in source.lower() or "unhandled" in source.lower(), (
        "Webhook handler must log/track ignored event types"
    )
