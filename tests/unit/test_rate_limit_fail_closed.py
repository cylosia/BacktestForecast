"""Tests for rate-limit outage policy defaults and production enforcement."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from backtestforecast.config import Settings


def _production_settings(**overrides) -> Settings:
    defaults = {
        "app_env": "production",
        "app_public_url": "https://app.example.com",
        "api_public_url": "https://api.example.com",
        "database_url": "postgresql+psycopg://u:strongpass@db:5432/app?sslmode=require",
        "redis_url": "redis://localhost:6379/0",
        "redis_cache_url": "redis://localhost:6379/1",
        "celery_result_backend_url": "redis://localhost:6379/2",
        "redis_password": "secret",
        "clerk_issuer": "https://clerk.example.com",
        "clerk_audience": "my-app",
        "clerk_secret_key": "sk_live_test",
        "clerk_jwt_key": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n-----END PUBLIC KEY-----",
        "clerk_authorized_parties_raw": "https://app.example.com",
        "web_cors_origins": ["https://app.example.com"],
        "log_json": True,
        "ip_hash_salt": "a-very-secure-salt-value-here",
        "metrics_token": "metrics-secret-token",
        "admin_token": "admin-secret-token",
        "stripe_secret_key": "sk_live_x",
        "stripe_webhook_secret": "whsec_x",
        "stripe_pro_monthly_price_id": "price_1",
        "stripe_pro_yearly_price_id": "price_2",
        "stripe_premium_monthly_price_id": "price_3",
        "stripe_premium_yearly_price_id": "price_4",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def test_rate_limit_defaults_use_degraded_memory_fallback():
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings(
            database_url="sqlite://",
            redis_url="redis://localhost:6379/0",
        )
    assert settings.rate_limit_fail_closed is False
    assert settings.rate_limit_degraded_memory_fallback is True


def test_production_rejects_when_both_fail_closed_and_fallback_are_disabled():
    with pytest.raises(ValueError, match="RATE_LIMIT_FAIL_CLOSED=true"):
        _production_settings(
            rate_limit_fail_closed=False,
            rate_limit_degraded_memory_fallback=False,
        )


def test_production_allows_degraded_memory_fallback_mode():
    settings = _production_settings(
        rate_limit_fail_closed=False,
        rate_limit_degraded_memory_fallback=True,
    )

    assert settings.rate_limit_fail_closed is False
    assert settings.rate_limit_degraded_memory_fallback is True
