"""Test that failed Stripe cleanup during account deletion triggers an async retry.

When the synchronous _cleanup_stripe() call returns "partial", "failed", or
"client_unavailable", the account deletion endpoint must dispatch the
maintenance.cleanup_stripe_orphan Celery task to retry asynchronously.
"""
from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_billing(cleanup_result: str = "ok"):
    """Create a mock BillingService that returns the given cleanup result."""
    mock = MagicMock()
    mock.cancel_in_flight_jobs.return_value = []
    mock.get_stripe_client.return_value = MagicMock()
    mock.close = MagicMock()
    return mock


class TestStripeCleanupRetryDispatch:
    def test_dispatch_called_on_partial_cleanup(self, monkeypatch):
        """When _cleanup_stripe returns 'partial', the retry task is dispatched."""
        import sys
        import types as _types
        import uuid

        mock_celery = MagicMock()
        mock_celery.send_task.return_value = _types.SimpleNamespace(id="fake")
        mock_module = _types.ModuleType("apps.worker.app.celery_app")
        mock_module.celery_app = mock_celery
        monkeypatch.setitem(sys.modules, "apps.worker.app.celery_app", mock_module)

        from apps.api.app.routers.account import _dispatch_stripe_cleanup_retry

        _dispatch_stripe_cleanup_retry(
            subscription_id="sub_123",
            customer_id="cus_456",
            user_id=uuid.uuid4(),
            sync_result="partial",
        )

        mock_celery.send_task.assert_called_once()

    def test_dispatch_not_called_on_ok(self):
        """When _cleanup_stripe returns 'ok', no retry should be dispatched."""
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        client = mock_billing.get_stripe_client.return_value
        import uuid

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "ok"

    def test_dispatch_not_called_when_nothing_to_clean(self):
        """When there's no subscription or customer, cleanup is skipped."""
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        import uuid

        result = _cleanup_stripe(mock_billing, None, None, uuid.uuid4())
        assert result == "skipped"

    def test_cleanup_stripe_returns_failed_on_both_errors(self):
        """When both subscription cancel and customer delete fail, result is 'failed'."""
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        client = mock_billing.get_stripe_client.return_value
        client.subscriptions.cancel.side_effect = Exception("stripe down")
        client.customers.delete.side_effect = Exception("stripe down")
        import uuid

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "failed"

    def test_cleanup_stripe_returns_partial_on_sub_failure(self):
        """When subscription cancel fails but customer delete succeeds, result is 'partial'."""
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        client = mock_billing.get_stripe_client.return_value
        client.subscriptions.cancel.side_effect = Exception("timeout")
        import uuid

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "partial"

    def test_cleanup_stripe_returns_client_unavailable(self):
        """When the Stripe client can't be created, result is 'client_unavailable'."""
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        mock_billing.get_stripe_client.side_effect = Exception("config error")
        import uuid

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "client_unavailable"

    def test_dispatch_helper_does_not_raise_on_celery_failure(self, monkeypatch):
        """_dispatch_stripe_cleanup_retry must swallow Celery send failures."""
        import sys
        import types as _types
        import uuid

        mock_celery = MagicMock()
        mock_celery.send_task.side_effect = ConnectionError("broker down")
        mock_module = _types.ModuleType("apps.worker.app.celery_app")
        mock_module.celery_app = mock_celery
        monkeypatch.setitem(sys.modules, "apps.worker.app.celery_app", mock_module)

        from apps.api.app.routers.account import _dispatch_stripe_cleanup_retry
        _dispatch_stripe_cleanup_retry("sub_1", "cus_1", uuid.uuid4(), "failed")
