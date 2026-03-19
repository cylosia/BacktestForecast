"""Tests for BillingService._mark_stripe_event_error after rollback."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_mark_stripe_event_error_handles_rollback():
    """_mark_stripe_event_error should work after a session rollback."""
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    stripe_events = MagicMock()
    stripe_events.mark_error.return_value = MagicMock(rowcount=1)

    service = BillingService.__new__(BillingService)
    service.session = session
    service.stripe_events = stripe_events

    service._mark_stripe_event_error("evt_123", "test error", event_type="test", livemode=False)

    stripe_events.mark_error.assert_called_once_with("evt_123", "test error")
    session.commit.assert_called()


def test_mark_stripe_event_error_creates_record_on_zero_rows():
    """When mark_error affects 0 rows, a new StripeEvent should be created."""
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    stripe_events = MagicMock()
    stripe_events.mark_error.return_value = MagicMock(rowcount=0)

    service = BillingService.__new__(BillingService)
    service.session = session
    service.stripe_events = stripe_events

    service._mark_stripe_event_error("evt_456", "not found", event_type="checkout.session.completed", livemode=True)

    session.add.assert_called_once()
