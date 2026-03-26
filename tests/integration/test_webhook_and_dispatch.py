"""Tests for billing webhook error handling and dispatch failure path."""
from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


def test_webhook_returns_500_for_transient_errors(client: TestClient) -> None:
    """Unexpected errors during webhook processing should return 500 so Stripe retries."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=RuntimeError("transient DB failure"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=b'{"type": "test"}',
            headers={"Stripe-Signature": "t=1,v1=abc"},
        )

    assert response.status_code == 500
    body = response.json()
    assert body["received"] is False
    assert "retry" in body["reason"].lower()


def test_webhook_reraises_auth_error(client: TestClient) -> None:
    """AuthenticationError (bad signature) should NOT be caught as 500."""
    from backtestforecast.errors import AuthenticationError

    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=AuthenticationError("Invalid Stripe webhook signature."),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=b'{"type": "test"}',
            headers={"Stripe-Signature": "t=1,v1=abc"},
        )

    assert response.status_code in (401, 403)


def test_dispatch_failure_marks_job_failed(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Celery broker is down, the job should stay queued for outbox recovery."""
    from kombu.exceptions import OperationalError
    from backtestforecast.security import get_rate_limiter

    class _BrokenCelery:
        def send_task(self, *args, **kwargs):
            raise OperationalError("broker down")

    class _UnlimitedRateLimiter:
        def check(self, *args, **kwargs):
            return None

    get_rate_limiter().reset()
    monkeypatch.setattr("apps.worker.app.celery_app.celery_app", _BrokenCelery())
    monkeypatch.setattr("apps.api.app.routers.backtests.get_rate_limiter", lambda: _UnlimitedRateLimiter())

    response = client.post(
        "/v1/backtests",
        json={
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 7,
            "max_holding_days": 21,
            "account_size": 10000,
            "risk_per_trade_pct": 2.0,
            "commission_per_contract": 0.65,
            "entry_rules": [
                {"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}
            ],
        },
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body.get("error_code") in (None, "dispatch_sla_exceeded")
