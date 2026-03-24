"""Verify portal session creation requires stripe_customer_id."""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from backtestforecast.errors import NotFoundError
from backtestforecast.services.billing import BillingService


def test_portal_session_requires_customer_id():
    """A user without stripe_customer_id should get NotFoundError."""
    mock_session = MagicMock()
    service = BillingService(mock_session)

    user = MagicMock()
    user.stripe_customer_id = None
    user.id = uuid4()

    payload = MagicMock()
    payload.return_path = "/app"

    with pytest.raises(NotFoundError, match="No Stripe customer"):
        service.create_portal_session(user, payload)
