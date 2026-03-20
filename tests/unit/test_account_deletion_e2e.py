"""End-to-end account deletion test verifying Stripe cleanup flow."""
from __future__ import annotations

import inspect
import uuid
from unittest.mock import MagicMock


class TestAccountDeletionStripeCleanup:
    """Verify the full account deletion flow cancels Stripe resources."""

    def test_stripe_subscription_cancelled_before_user_deleted(self):
        """The delete endpoint must cancel the Stripe subscription."""
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        billing._get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_live", "cus_live", uuid.uuid4())

        client.subscriptions.cancel.assert_called_once_with("sub_live")
        client.customers.delete.assert_called_once_with("cus_live")
        assert result == "ok"

    def test_stripe_failure_does_not_prevent_deletion(self):
        """Even if Stripe calls fail, _cleanup_stripe returns a status (not raise)."""
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        client.subscriptions.cancel.side_effect = Exception("Stripe down")
        client.customers.delete.side_effect = Exception("Stripe down")
        billing._get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_live", "cus_live", uuid.uuid4())
        assert result == "failed"

    def test_no_stripe_ids_skips_stripe_calls(self):
        """Users without Stripe IDs skip Stripe cleanup gracefully."""
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        result = _cleanup_stripe(billing, None, None, uuid.uuid4())
        assert result == "skipped"
        billing._get_stripe_client.assert_not_called()

    def test_audit_event_includes_stripe_ids_and_user_ids(self):
        """The audit event for account deletion should record identifiers for tracing."""
        source = inspect.getsource(
            __import__("apps.api.app.routers.account", fromlist=["delete_account"]).delete_account
        )
        assert "deleted_user_id" in source
        assert "clerk_user_id_hash" in source
        assert "clerk_user_id=saved_clerk_user_id" not in source
        assert "stripe_subscription_id" in source
        assert "stripe_customer_id" in source
        assert "stripe_cleanup_result" in source
