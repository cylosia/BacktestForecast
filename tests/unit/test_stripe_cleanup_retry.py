"""Tests for durable Stripe cleanup retry dispatch."""
from __future__ import annotations

import uuid
from contextlib import nullcontext
from unittest.mock import MagicMock, patch


def _make_mock_billing(cleanup_result: str = "ok"):
    """Create a mock BillingService that returns the given cleanup result."""
    mock = MagicMock()
    mock.cancel_in_flight_jobs.return_value = []
    mock.get_stripe_client.return_value = MagicMock()
    mock.close = MagicMock()
    return mock


class TestStripeCleanupRetryDispatch:
    def test_dispatch_called_via_durable_outbox_helper(self, monkeypatch):
        from apps.api.app.routers import account
        from apps.api.app import dispatch as dispatch_module
        from backtestforecast.db import session as session_module

        fake_session = MagicMock()
        dispatch_calls: list[dict[str, object]] = []

        def _fake_dispatch(**kwargs):
            dispatch_calls.append(kwargs)

        monkeypatch.setattr(session_module, "create_session", lambda: nullcontext(fake_session))
        monkeypatch.setattr(dispatch_module, "dispatch_outbox_task", _fake_dispatch)

        user_id = uuid.uuid4()
        account._dispatch_stripe_cleanup_retry(
            subscription_id="sub_123",
            customer_id="cus_456",
            user_id=user_id,
            sync_result="partial",
        )

        assert len(dispatch_calls) == 1
        call = dispatch_calls[0]
        assert call["db"] is fake_session
        assert call["task_name"] == "maintenance.cleanup_stripe_orphan"
        assert call["queue"] == "recovery"
        assert call["task_kwargs"] == {
            "subscription_id": "sub_123",
            "customer_id": "cus_456",
            "user_id_str": str(user_id),
        }

    def test_dispatch_logs_retry_backoff_and_timeout_visibility(self, monkeypatch):
        from apps.api.app.routers import account
        from apps.api.app import dispatch as dispatch_module
        from backtestforecast.db import session as session_module

        monkeypatch.setattr(session_module, "create_session", lambda: nullcontext(MagicMock()))
        monkeypatch.setattr(dispatch_module, "dispatch_outbox_task", lambda **_kwargs: None)

        with patch.object(account, "logger") as mock_logger:
            account._dispatch_stripe_cleanup_retry(
                subscription_id="sub_123",
                customer_id="cus_456",
                user_id=uuid.uuid4(),
                sync_result="partial",
            )

        _, kwargs = mock_logger.info.call_args
        assert kwargs["initial_countdown_seconds"] == 30
        assert kwargs["max_retries"] == 5
        assert kwargs["retry_backoff_schedule_seconds"] == [30, 60, 120, 240, 480]
        assert kwargs["retry_soft_time_limit_seconds"] == 60
        assert kwargs["retry_time_limit_seconds"] == 90

    def test_dispatch_not_called_on_ok(self):
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "ok"

    def test_dispatch_not_called_when_nothing_to_clean(self):
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()

        result = _cleanup_stripe(mock_billing, None, None, uuid.uuid4())
        assert result == "skipped"

    def test_cleanup_stripe_returns_failed_on_both_errors(self):
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        client = mock_billing.get_stripe_client.return_value
        client.subscriptions.cancel.side_effect = Exception("stripe down")
        client.customers.delete.side_effect = Exception("stripe down")

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "failed"

    def test_cleanup_stripe_returns_partial_on_sub_failure(self):
        from apps.api.app.routers import account

        mock_billing = _make_mock_billing()
        client = mock_billing.get_stripe_client.return_value
        client.subscriptions.cancel.side_effect = Exception("timeout")

        with patch.object(account, "logger") as mock_logger:
            result = account._cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())

        assert result == "partial"
        warning_kwargs = mock_logger.warning.call_args_list[0].kwargs
        assert warning_kwargs["retry_backoff_schedule_seconds"] == [30, 60, 120, 240, 480]
        assert warning_kwargs["retry_soft_time_limit_seconds"] == 60
        assert warning_kwargs["retry_time_limit_seconds"] == 90

    def test_cleanup_stripe_returns_client_unavailable(self):
        from apps.api.app.routers.account import _cleanup_stripe

        mock_billing = _make_mock_billing()
        mock_billing.get_stripe_client.side_effect = Exception("config error")

        result = _cleanup_stripe(mock_billing, "sub_123", "cus_456", uuid.uuid4())
        assert result == "client_unavailable"

    def test_dispatch_helper_does_not_raise_on_outbox_failure(self, monkeypatch):
        from apps.api.app.routers import account
        from apps.api.app import dispatch as dispatch_module
        from backtestforecast.db import session as session_module

        monkeypatch.setattr(session_module, "create_session", lambda: nullcontext(MagicMock()))

        def _raise(**_kwargs):
            raise ConnectionError("broker down")

        monkeypatch.setattr(dispatch_module, "dispatch_outbox_task", _raise)

        account._dispatch_stripe_cleanup_retry("sub_1", "cus_1", uuid.uuid4(), "failed")
