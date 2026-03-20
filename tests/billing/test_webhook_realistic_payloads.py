"""Tests for Stripe webhook handling with realistic event structures."""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backtestforecast.models import User
from backtestforecast.services.billing import BillingService


def _make_fake_stripe(event_dict):
    """Return a fake Stripe client that returns *event_dict* from construct_event."""
    def construct_event(payload, sig_header, secret):
        return event_dict

    return SimpleNamespace(
        construct_event=construct_event,
        subscriptions=SimpleNamespace(retrieve=lambda sid: event_dict["data"]["object"]),
    )


@pytest.fixture()
def test_user(db_session) -> User:
    user = User(
        clerk_user_id="clerk_webhook_realistic",
        email="realistic@example.com",
        plan_tier="free",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def test_checkout_session_completed_extracts_subscription(db_session, test_user, monkeypatch):
    """checkout.session.completed with a subscription_id in the data must
    trigger subscription sync and upgrade the user's plan."""
    import backtestforecast.services.billing as billing_mod

    event = {
        "id": "evt_checkout_real_001",
        "type": "checkout.session.completed",
        "livemode": False,
        "created": int(datetime(2026, 3, 20, tzinfo=UTC).timestamp()),
        "data": {
            "object": {
                "id": "cs_test_abc123",
                "customer": "cus_real_001",
                "subscription": "sub_real_001",
                "metadata": {
                    "user_id": str(test_user.id),
                    "requested_tier": "pro",
                },
            },
        },
    }

    fake_subscription = {
        "id": "sub_real_001",
        "customer": "cus_real_001",
        "status": "active",
        "cancel_at_period_end": False,
        "current_period_end": int(datetime(2026, 4, 20, tzinfo=UTC).timestamp()),
        "metadata": {"user_id": str(test_user.id), "requested_tier": "pro"},
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_pro_monthly",
                        "recurring": {"interval": "month"},
                    },
                },
            ],
        },
    }

    def fake_get_client(self, **kwargs):
        client = SimpleNamespace(
            construct_event=lambda p, s, sec: event,
            subscriptions=SimpleNamespace(retrieve=lambda sid: fake_subscription),
        )
        return client

    monkeypatch.setattr(billing_mod.BillingService, "_get_stripe_client", fake_get_client)

    service = BillingService(db_session)
    result = service.handle_webhook(
        b"{}",
        "sig_checkout_test",
        request_id="req-checkout-001",
        ip_address="10.0.0.1",
    )

    assert result["status"] == "ok"
    assert result["event_type"] == "checkout.session.completed"

    db_session.expire_all()
    db_session.refresh(test_user)
    assert test_user.plan_tier == "pro"
    assert test_user.subscription_status == "active"


def test_subscription_updated_handles_status_change(db_session, test_user, monkeypatch):
    """customer.subscription.updated with a status change must update the
    user's subscription_status accordingly."""
    import backtestforecast.services.billing as billing_mod

    test_user.plan_tier = "pro"
    test_user.subscription_status = "active"
    test_user.stripe_subscription_id = "sub_update_001"
    db_session.commit()

    event = {
        "id": "evt_sub_update_001",
        "type": "customer.subscription.updated",
        "livemode": False,
        "created": int(datetime(2026, 3, 20, 12, 0, tzinfo=UTC).timestamp()),
        "data": {
            "object": {
                "id": "sub_update_001",
                "customer": "cus_update_001",
                "status": "past_due",
                "cancel_at_period_end": False,
                "current_period_end": int(datetime(2026, 4, 20, tzinfo=UTC).timestamp()),
                "metadata": {
                    "user_id": str(test_user.id),
                    "requested_tier": "pro",
                },
                "items": {
                    "data": [
                        {
                            "price": {
                                "id": "price_pro_monthly",
                                "recurring": {"interval": "month"},
                            },
                        },
                    ],
                },
            },
        },
    }

    def fake_get_client(self, **kwargs):
        return SimpleNamespace(construct_event=lambda p, s, sec: event)

    monkeypatch.setattr(billing_mod.BillingService, "_get_stripe_client", fake_get_client)

    service = BillingService(db_session)
    result = service.handle_webhook(
        b"{}",
        "sig_sub_update",
        request_id="req-sub-update-001",
        ip_address="10.0.0.2",
    )

    assert result["status"] == "ok"
    assert result["event_type"] == "customer.subscription.updated"

    db_session.expire_all()
    db_session.refresh(test_user)
    assert test_user.subscription_status == "past_due"


def test_unknown_event_type_returns_200(db_session, test_user, monkeypatch):
    """An unrecognized event type must be acknowledged (status ok) without error."""
    import backtestforecast.services.billing as billing_mod

    event = {
        "id": "evt_unknown_001",
        "type": "invoice.voided",
        "livemode": False,
        "created": int(datetime(2026, 3, 20, tzinfo=UTC).timestamp()),
        "data": {
            "object": {
                "id": "inv_voided_001",
            },
        },
    }

    def fake_get_client(self, **kwargs):
        return SimpleNamespace(construct_event=lambda p, s, sec: event)

    monkeypatch.setattr(billing_mod.BillingService, "_get_stripe_client", fake_get_client)

    service = BillingService(db_session)
    result = service.handle_webhook(
        b"{}",
        "sig_unknown_event",
        request_id="req-unknown-001",
        ip_address="10.0.0.3",
    )

    assert result["status"] == "ok"
    assert result["event_type"] == "invoice.voided"
