"""Test: webhook error handling distinguishes deterministic vs transient errors.

Deterministic AppErrors should return 200 (acknowledged, don't retry).
Transient exceptions should return 500 (Stripe should retry).
Unhandled exceptions (TypeError, KeyError, etc.) must also return 500.
"""
from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backtestforecast.errors import AppValidationError, ExternalServiceError

_WEBHOOK_BODY = b'{"type": "test"}'
_WEBHOOK_HEADERS = {"Stripe-Signature": "t=1,v1=abc"}


def test_webhook_deterministic_error_returns_200(client: TestClient):
    """An AppError during webhook processing should return 200 so Stripe doesn't retry."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=AppValidationError("Bad data shape"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ignored"
        assert body["code"] == "validation_error"


def test_webhook_transient_runtime_error_returns_500(client: TestClient):
    """An unexpected RuntimeError should return 500 so Stripe retries."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=RuntimeError("DB connection lost"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500
        body = response.json()
        assert body["received"] is False
        assert "retry" in body["reason"].lower()


def test_webhook_type_error_returns_500(client: TestClient):
    """A TypeError (code bug) must return 500, not 200.

    This is the critical regression test: before the fix, unhandled
    exceptions returned 200 and Stripe would never retry, permanently
    losing the event.
    """
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=TypeError("'NoneType' has no attribute 'id'"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500, (
            "TypeError must produce 500 so Stripe retries - "
            "returning 200 would permanently lose the webhook event"
        )
        body = response.json()
        assert body["received"] is False


def test_webhook_key_error_returns_500(client: TestClient):
    """A KeyError (missing dict key) must return 500."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=KeyError("missing_field"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500
        body = response.json()
        assert body["received"] is False


def test_webhook_attribute_error_returns_500(client: TestClient):
    """An AttributeError must return 500."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=AttributeError("'NoneType' object"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500
        body = response.json()
        assert body["received"] is False


def test_webhook_external_service_error_returns_500(client: TestClient):
    """An ExternalServiceError (Stripe API failure) should return 500 for retry."""
    with patch(
        "backtestforecast.services.billing.BillingService.handle_webhook",
        side_effect=ExternalServiceError("Stripe API down"),
    ):
        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500
        body = response.json()
        assert body["error"]["code"] == "external_service_error"
