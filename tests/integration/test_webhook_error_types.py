"""Test: webhook error handling distinguishes deterministic vs transient errors.

Deterministic AppErrors should return 200 (acknowledged, don't retry).
Transient exceptions should return 500 (Stripe should retry).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backtestforecast.errors import AppError, ValidationError


_WEBHOOK_BODY = b'{"type": "test"}'
_WEBHOOK_HEADERS = {"Stripe-Signature": "t=1,v1=abc"}


def test_webhook_deterministic_error_returns_200(client: TestClient):
    """An AppError during webhook processing should return 200 so Stripe doesn't retry."""
    with patch("apps.api.app.routers.billing.BillingService") as mock_cls:
        instance = MagicMock()
        instance.handle_webhook.side_effect = ValidationError("Bad data shape")
        mock_cls.return_value = instance

        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 200
        body = response.json()
        assert body.get("code") == "validation_error"


def test_webhook_transient_error_returns_500(client: TestClient):
    """An unexpected exception should return 500 so Stripe retries."""
    with patch("apps.api.app.routers.billing.BillingService") as mock_cls:
        instance = MagicMock()
        instance.handle_webhook.side_effect = RuntimeError("DB connection lost")
        mock_cls.return_value = instance

        response = client.post(
            "/v1/billing/webhook",
            content=_WEBHOOK_BODY,
            headers=_WEBHOOK_HEADERS,
        )
        assert response.status_code == 500
        body = response.json()
        assert body["error"]["code"] == "webhook_processing_failed"
