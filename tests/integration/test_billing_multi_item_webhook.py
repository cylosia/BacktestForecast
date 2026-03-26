from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from backtestforecast.config import get_settings
from backtestforecast.models import User


def test_webhook_multi_item_subscription_uses_configured_plan_price(
    client,
    auth_headers,
    db_session,
    monkeypatch,
):
    import backtestforecast.services.billing as billing_services

    user_id = client.get("/v1/me", headers=auth_headers).json()["id"]
    settings = get_settings()
    settings.stripe_premium_yearly_price_id = "price_premium_yearly"

    def fake_stripe(self):
        return SimpleNamespace(
            construct_event=lambda p, s, sec: {
                "id": "evt_multi_item_route_001",
                "type": "customer.subscription.updated",
                "livemode": False,
                "created": int(datetime(2026, 3, 20, 18, 0, tzinfo=UTC).timestamp()),
                "data": {
                    "object": {
                        "id": "sub_multi_item_route_001",
                        "customer": "cus_multi_item_route_001",
                        "status": "active",
                        "cancel_at_period_end": False,
                        "current_period_end": int(datetime(2027, 3, 20, tzinfo=UTC).timestamp()),
                        "metadata": {"user_id": user_id},
                        "items": {
                            "data": [
                                {
                                    "price": {
                                        "id": "price_addon_metered",
                                        "recurring": {"interval": "month"},
                                    }
                                },
                                {
                                    "price": {
                                        "id": "price_premium_yearly",
                                        "recurring": {"interval": "year"},
                                    }
                                },
                            ]
                        },
                    }
                },
            }
        )

    monkeypatch.setattr(billing_services.BillingService, "_get_stripe_client", fake_stripe)

    response = client.post(
        "/v1/billing/webhook",
        content=b"{}",
        headers={"Stripe-Signature": "sig", "Host": "localhost"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["event_type"] == "customer.subscription.updated"

    db_session.expire_all()
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    assert user.plan_tier == "premium"
    assert user.subscription_status == "active"
    assert user.subscription_billing_interval == "yearly"
    assert user.stripe_price_id == "price_premium_yearly"
