"""Tests for rate_limit_fail_closed default and production enforcement (audit item C-5)."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from backtestforecast.config import Settings


def test_rate_limit_fail_closed_defaults_true():
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings(
            database_url="sqlite://",
            redis_url="redis://localhost:6379/0",
        )
    assert settings.rate_limit_fail_closed is True


def test_production_rejects_fail_open():
    with pytest.raises(ValueError, match="RATE_LIMIT_FAIL_CLOSED"):
        Settings(
            app_env="production",
            database_url="postgresql+psycopg://u:strongpass@db:5432/app?sslmode=verify-full",
            redis_url="redis://localhost:6379/0",
            redis_password="secret",
            clerk_issuer="https://clerk.example.com",
            clerk_audience="my-app",
            clerk_secret_key="sk_live_test",
            clerk_jwt_key="-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n-----END PUBLIC KEY-----",
            clerk_authorized_parties_raw="https://app.example.com",
            log_json=True,
            ip_hash_salt="a-very-secure-salt-value-here",
            metrics_token="metrics-secret-token",
            rate_limit_fail_closed=False,
            stripe_secret_key="sk_live_x",
            stripe_webhook_secret="whsec_x",
            stripe_pro_monthly_price_id="price_1",
            stripe_pro_yearly_price_id="price_2",
            stripe_premium_monthly_price_id="price_3",
            stripe_premium_yearly_price_id="price_4",
        )
