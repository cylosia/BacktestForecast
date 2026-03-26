"""Tests for BillingService._get_or_create_customer race condition handling.

Covers the advisory-lock + CAS-UPDATE pattern that prevents duplicate Stripe
customers when concurrent requests hit the billing service.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

pytestmark = pytest.mark.filterwarnings("ignore:MASSIVE_API_KEY:UserWarning")


def _make_user(*, stripe_customer_id: str | None = None):
    user = MagicMock()
    user.id = uuid4()
    user.clerk_user_id = f"user_{user.id.hex[:12]}"
    user.email = "test@example.com"
    user.stripe_customer_id = stripe_customer_id
    return user


def _make_billing_service(session: MagicMock, stripe_client: MagicMock):
    from backtestforecast.config import Settings
    from backtestforecast.services.billing import BillingService

    settings = Settings(
        stripe_secret_key="sk_test_fake",
        stripe_webhook_secret="whsec_fake",
        _env_file=None,
    )
    svc = BillingService(session, settings=settings)
    svc._stripe_client = stripe_client
    return svc


class TestGetOrCreateCustomerRace:
    def test_returns_existing_customer_id(self):
        """If the user already has a stripe_customer_id, return it immediately
        without calling Stripe or acquiring the advisory lock."""
        session = MagicMock()
        stripe_client = MagicMock()
        svc = _make_billing_service(session, stripe_client)

        user = _make_user(stripe_customer_id="cus_existing123")

        result = svc._get_or_create_customer(user)

        assert result == "cus_existing123"
        stripe_client.customers.create.assert_not_called()
        session.execute.assert_not_called()

    def test_creates_new_customer_on_first_call(self):
        """When stripe_customer_id is None, the method must create a Stripe
        customer and persist the ID via CAS UPDATE."""
        session = MagicMock()
        stripe_client = MagicMock()

        new_customer = SimpleNamespace(id="cus_new_456")
        stripe_client.customers.create.return_value = new_customer

        cas_result = MagicMock()
        cas_result.rowcount = 1
        session.execute.side_effect = [
            MagicMock(),   # pg_advisory_xact_lock
            cas_result,    # CAS UPDATE
        ]
        session.refresh = MagicMock()

        user = _make_user(stripe_customer_id=None)
        # After advisory lock, refresh still shows no customer
        def _refresh(u):
            pass
        session.refresh.side_effect = _refresh

        svc = _make_billing_service(session, stripe_client)
        result = svc._get_or_create_customer(user)

        assert result == "cus_new_456"
        stripe_client.customers.create.assert_called_once()
        create_params = stripe_client.customers.create.call_args
        assert create_params.kwargs["params"]["email"] == "test@example.com"
        assert str(user.id) in create_params.kwargs["params"]["metadata"]["user_id"]
        session.flush.assert_called_once()

    def test_race_lost_deletes_orphan(self):
        """When CAS UPDATE returns rowcount=0 (another session won the race),
        the orphan Stripe customer must be deleted and the winner's ID returned."""
        session = MagicMock()
        stripe_client = MagicMock()

        orphan_customer = SimpleNamespace(id="cus_orphan_789")
        stripe_client.customers.create.return_value = orphan_customer
        stripe_client.customers.delete.return_value = None

        cas_result = MagicMock()
        cas_result.rowcount = 0
        session.execute.side_effect = [
            MagicMock(),   # pg_advisory_xact_lock
            cas_result,    # CAS UPDATE - lost race
        ]

        user = _make_user(stripe_customer_id=None)
        winner_id = "cus_winner_abc"

        refresh_call_count = 0
        def _refresh(u):
            nonlocal refresh_call_count
            refresh_call_count += 1
            if refresh_call_count == 1:
                pass  # after advisory lock - still None
            else:
                u.stripe_customer_id = winner_id

        session.refresh.side_effect = _refresh

        svc = _make_billing_service(session, stripe_client)
        result = svc._get_or_create_customer(user)

        assert result == winner_id
        stripe_client.customers.delete.assert_called_once_with("cus_orphan_789")

    @patch("backtestforecast.services.billing.logger")
    def test_race_lost_orphan_cleanup_failure_dispatches_task(self, mock_logger):
        """If orphan customer deletion fails, a Celery recovery task must
        be dispatched to clean it up asynchronously."""
        session = MagicMock()
        stripe_client = MagicMock()

        orphan_customer = SimpleNamespace(id="cus_orphan_fail")
        stripe_client.customers.create.return_value = orphan_customer
        stripe_client.customers.delete.side_effect = Exception("Stripe API error")

        cas_result = MagicMock()
        cas_result.rowcount = 0
        session.execute.side_effect = [
            MagicMock(),   # pg_advisory_xact_lock
            cas_result,    # CAS UPDATE - lost race
        ]

        user = _make_user(stripe_customer_id=None)
        winner_id = "cus_winner_xyz"

        refresh_call_count = 0
        def _refresh(u):
            nonlocal refresh_call_count
            refresh_call_count += 1
            if refresh_call_count == 1:
                pass
            else:
                u.stripe_customer_id = winner_id

        session.refresh.side_effect = _refresh

        with patch("apps.worker.app.celery_app.celery_app") as mock_celery:
            svc = _make_billing_service(session, stripe_client)
            result = svc._get_or_create_customer(user)

        assert result == winner_id
        mock_celery.send_task.assert_called_once()
        task_call = mock_celery.send_task.call_args
        assert task_call.args[0] == "maintenance.cleanup_stripe_orphan"
        assert task_call.kwargs["kwargs"]["customer_id"] == "cus_orphan_fail"
        assert task_call.kwargs["queue"] == "recovery"
