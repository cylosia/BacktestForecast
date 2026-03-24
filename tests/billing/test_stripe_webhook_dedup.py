from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backtestforecast.models import User
from backtestforecast.repositories.users import UserRepository
from backtestforecast.services.billing import BillingService

EVENT_ID = "evt_dedup_test_001"


def _make_fake_stripe(user_id: str):
    def construct_event(payload, sig_header, secret):
        return {
            "id": EVENT_ID,
            "type": "customer.subscription.updated",
            "livemode": False,
            "data": {
                "object": {
                    "id": "sub_dedup_123",
                    "customer": "cus_dedup_123",
                    "status": "active",
                    "cancel_at_period_end": False,
                    "current_period_end": int(datetime(2026, 4, 1, tzinfo=UTC).timestamp()),
                    "metadata": {"user_id": user_id, "requested_tier": "pro"},
                    "items": {
                        "data": [
                            {
                                "price": {
                                    "id": "price_pro_monthly",
                                    "recurring": {"interval": "month"},
                                }
                            }
                        ]
                    },
                }
            },
        }

    return SimpleNamespace(construct_event=construct_event)


@pytest.fixture()
def test_user(db_session) -> User:
    user = User(
        clerk_user_id="clerk_dedup_test",
        email="dedup@example.com",
        plan_tier="free",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture()
def billing_service(db_session, test_user, monkeypatch):
    import backtestforecast.services.billing as billing_mod

    def fake_get_client(self, **kwargs):
        return _make_fake_stripe(str(test_user.id))

    monkeypatch.setattr(billing_mod.BillingService, "_get_stripe_client", fake_get_client)
    return BillingService(db_session)


def test_handle_webhook_first_call_returns_ok_second_returns_duplicate(
    billing_service, db_session, test_user
):
    result1 = billing_service.handle_webhook(
        b"{}",
        "sig",
        request_id="req-1",
        ip_address="127.0.0.1",
    )
    assert result1["status"] == "ok"
    assert result1["event_type"] == "customer.subscription.updated"

    result2 = billing_service.handle_webhook(
        b"{}",
        "sig",
        request_id="req-2",
        ip_address="127.0.0.1",
    )
    assert result2["status"] == "duplicate"
    assert result2["event_type"] == "customer.subscription.updated"


def test_handle_webhook_side_effects_only_once(billing_service, db_session, test_user):
    billing_service.handle_webhook(b"{}", "sig")
    billing_service.handle_webhook(b"{}", "sig")

    db_session.expire_all()
    user_repo = UserRepository(db_session)
    user = user_repo.get_by_id(test_user.id)
    assert user is not None
    assert user.plan_tier == "pro"
    assert user.subscription_status == "active"

    from backtestforecast.models import StripeEvent

    stripe_events = list(
        db_session.query(StripeEvent).filter(
            StripeEvent.stripe_event_id == EVENT_ID,
        )
    )
    assert len(stripe_events) == 1
    assert stripe_events[0].event_type == "customer.subscription.updated"
    assert stripe_events[0].idempotency_status == "processed"


# ---------------------------------------------------------------------------
# FIX 56: _recover_stale_claim only resets "processing" events
# ---------------------------------------------------------------------------


def test_recover_stale_claim_ignores_processed_events(db_session):
    """A StripeEvent with status 'processed' older than 5 min should NOT be
    reset by _recover_stale_claim - only 'processing' events are recovered."""
    from backtestforecast.models import StripeEvent
    from backtestforecast.repositories.stripe_events import StripeEventRepository

    old_time = datetime(2025, 1, 1, 0, 0, 0)

    processed_event = StripeEvent(
        stripe_event_id="evt_processed_stable",
        event_type="invoice.paid",
        livemode=False,
        idempotency_status="processed",
        payload_summary={},
    )
    db_session.add(processed_event)
    db_session.flush()

    db_session.execute(
        StripeEvent.__table__.update()
        .where(StripeEvent.id == processed_event.id)
        .values(created_at=old_time)
    )
    db_session.flush()

    repo = StripeEventRepository(db_session)
    recovered = repo._recover_stale_claim("evt_processed_stable")
    assert recovered is False, "Processed events must NOT be recovered"

    db_session.expire_all()
    event = repo.get_by_stripe_id("evt_processed_stable")
    assert event is not None
    assert event.idempotency_status == "processed"


def test_recover_stale_claim_resets_processing_events(db_session):
    """A StripeEvent with status 'processing' older than the TTL should be
    deleted by _recover_stale_claim so a fresh claim can be inserted."""
    from backtestforecast.models import StripeEvent
    from backtestforecast.repositories.stripe_events import StripeEventRepository

    old_time = datetime(2025, 1, 1, 0, 0, 0)

    processing_event = StripeEvent(
        stripe_event_id="evt_processing_stale",
        event_type="invoice.paid",
        livemode=False,
        idempotency_status="processing",
        payload_summary={},
    )
    db_session.add(processing_event)
    db_session.flush()

    db_session.execute(
        StripeEvent.__table__.update()
        .where(StripeEvent.id == processing_event.id)
        .values(created_at=old_time)
    )
    db_session.flush()
    db_session.expunge(processing_event)

    repo = StripeEventRepository(db_session)
    recovered = repo._recover_stale_claim("evt_processing_stale")
    assert recovered is True, "Stale processing events must be recovered"

    event = repo.get_by_stripe_id("evt_processing_stale")
    assert event is None, "Stale event should be deleted, not updated"
