"""Tests for BillingService._mark_stripe_event_error after rollback.

NOTE: Uses ``BillingService.__new__()`` to bypass ``__init__`` and test the
method in isolation with mock dependencies.  If ``__init__`` gains new
required attributes, these tests will still pass but won't catch init
regressions — integration tests cover that path.
"""
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
    """When mark_error returns False (0 rows affected), a new StripeEvent should be created."""
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    stripe_events = MagicMock()
    stripe_events.mark_error.return_value = False

    service = BillingService.__new__(BillingService)
    service.session = session
    service.stripe_events = stripe_events

    service._mark_stripe_event_error("evt_456", "not found", event_type="checkout.session.completed", livemode=True)

    session.add.assert_called_once()
