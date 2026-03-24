"""Tests for account deletion Stripe cleanup."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock


class TestCleanupStripe:
    def test_cancels_subscription_and_deletes_customer(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        billing.get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_123", "cus_456", uuid.uuid4())

        client.subscriptions.cancel.assert_called_once_with("sub_123")
        client.customers.delete.assert_called_once_with("cus_456")
        assert result == "ok"

    def test_handles_no_subscription(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        billing.get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, None, "cus_456", uuid.uuid4())

        client.subscriptions.cancel.assert_not_called()
        client.customers.delete.assert_called_once_with("cus_456")
        assert result == "ok"

    def test_handles_no_customer(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        billing.get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_123", None, uuid.uuid4())

        client.subscriptions.cancel.assert_called_once_with("sub_123")
        client.customers.delete.assert_not_called()
        assert result == "ok"

    def test_subscription_cancel_failure_does_not_block_customer_delete(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        client.subscriptions.cancel.side_effect = Exception("Stripe error")
        billing.get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_123", "cus_456", uuid.uuid4())

        client.customers.delete.assert_called_once_with("cus_456")
        assert result == "partial"

    def test_stripe_client_unavailable(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        billing.get_stripe_client.side_effect = Exception("No Stripe config")

        result = _cleanup_stripe(billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "client_unavailable"
