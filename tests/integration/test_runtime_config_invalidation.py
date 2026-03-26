from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.app.main import app
from backtestforecast.config import get_settings, invalidate_settings


def test_runtime_config_invalidation_updates_health_version_visibility(monkeypatch) -> None:
    settings = get_settings()
    original_env = settings.app_env
    original_ip_hash_salt = settings.ip_hash_salt
    original_clerk_issuer = settings.clerk_issuer
    original_clerk_audience = settings.clerk_audience
    original_clerk_jwt_key = settings.clerk_jwt_key
    original_clerk_secret_key = settings.clerk_secret_key
    original_log_json = settings.log_json
    original_metrics_token = settings.metrics_token
    original_admin_token = settings.admin_token
    original_redis_password = settings.redis_password
    original_redis_url = settings.redis_url
    original_redis_cache_url = settings.redis_cache_url
    original_celery_result_backend_url = settings.celery_result_backend_url
    original_api_allowed_hosts_raw = settings.api_allowed_hosts_raw
    original_web_cors_origins_raw = settings.web_cors_origins_raw
    original_clerk_authorized_parties_raw = settings.clerk_authorized_parties_raw
    original_app_public_url = settings.app_public_url
    original_database_url = settings.database_url
    original_feature_backtests_enabled = settings.feature_backtests_enabled
    original_feature_scanner_enabled = settings.feature_scanner_enabled
    original_feature_sweeps_enabled = settings.feature_sweeps_enabled
    original_feature_analysis_enabled = settings.feature_analysis_enabled
    original_feature_billing_enabled = settings.feature_billing_enabled
    original_rate_limit_fail_closed = settings.rate_limit_fail_closed
    try:
        monkeypatch.setenv("APP_ENV", "development")
        invalidate_settings()
        with TestClient(app, base_url="http://localhost") as client:
            dev_resp = client.get("/health/live")
            assert "version" in dev_resp.json()

        monkeypatch.setenv("APP_ENV", "production")
        monkeypatch.setenv("IP_HASH_SALT", "integration-test-production-salt-123")
        monkeypatch.setenv("CLERK_ISSUER", "https://clerk.example.test")
        monkeypatch.setenv("CLERK_AUDIENCE", "backtestforecast-test")
        monkeypatch.setenv("CLERK_JWT_KEY", "integration-test-jwt-key")
        monkeypatch.setenv("CLERK_SECRET_KEY", "sk_test_integration_secret")
        monkeypatch.setenv("LOG_JSON", "true")
        monkeypatch.setenv("METRICS_TOKEN", "integration-metrics-token")
        monkeypatch.setenv("ADMIN_TOKEN", "integration-admin-token")
        monkeypatch.setenv("REDIS_PASSWORD", "integration-redis-password")
        monkeypatch.setenv("REDIS_URL", "redis://broker:integration-redis-password@localhost:6379/0")
        monkeypatch.setenv("REDIS_CACHE_URL", "redis://cache:integration-redis-password@localhost:6379/1")
        monkeypatch.setenv("CELERY_RESULT_BACKEND_URL", "redis://results:integration-redis-password@localhost:6379/2")
        monkeypatch.setenv("API_ALLOWED_HOSTS_RAW", "api.example.test")
        monkeypatch.setenv("WEB_CORS_ORIGINS_RAW", "https://app.example.test")
        monkeypatch.setenv("CLERK_AUTHORIZED_PARTIES_RAW", "https://app.example.test")
        monkeypatch.setenv("APP_PUBLIC_URL", "https://app.example.test")
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+psycopg://integration:StrongPassword123@localhost:5432/backtestforecast?sslmode=require",
        )
        monkeypatch.setenv("FEATURE_BACKTESTS_ENABLED", "false")
        monkeypatch.setenv("FEATURE_SCANNER_ENABLED", "false")
        monkeypatch.setenv("FEATURE_SWEEPS_ENABLED", "false")
        monkeypatch.setenv("FEATURE_ANALYSIS_ENABLED", "false")
        monkeypatch.setenv("FEATURE_BILLING_ENABLED", "false")
        monkeypatch.setenv("RATE_LIMIT_FAIL_CLOSED", "true")
        invalidate_settings()
        with TestClient(app, base_url="http://localhost") as client:
            prod_resp = client.get("/health/live")
            assert "version" not in prod_resp.json()
    finally:
        monkeypatch.setenv("APP_ENV", original_env)
        monkeypatch.setenv("IP_HASH_SALT", original_ip_hash_salt)
        if original_clerk_issuer:
            monkeypatch.setenv("CLERK_ISSUER", original_clerk_issuer)
        else:
            monkeypatch.delenv("CLERK_ISSUER", raising=False)
        if original_clerk_audience:
            monkeypatch.setenv("CLERK_AUDIENCE", original_clerk_audience)
        else:
            monkeypatch.delenv("CLERK_AUDIENCE", raising=False)
        if original_clerk_jwt_key:
            monkeypatch.setenv("CLERK_JWT_KEY", original_clerk_jwt_key)
        else:
            monkeypatch.delenv("CLERK_JWT_KEY", raising=False)
        if original_clerk_secret_key:
            monkeypatch.setenv("CLERK_SECRET_KEY", original_clerk_secret_key)
        else:
            monkeypatch.delenv("CLERK_SECRET_KEY", raising=False)
        monkeypatch.setenv("LOG_JSON", "true" if original_log_json else "false")
        if original_metrics_token:
            monkeypatch.setenv("METRICS_TOKEN", original_metrics_token)
        else:
            monkeypatch.delenv("METRICS_TOKEN", raising=False)
        if original_admin_token:
            monkeypatch.setenv("ADMIN_TOKEN", original_admin_token)
        else:
            monkeypatch.delenv("ADMIN_TOKEN", raising=False)
        if original_redis_password:
            monkeypatch.setenv("REDIS_PASSWORD", original_redis_password)
        else:
            monkeypatch.delenv("REDIS_PASSWORD", raising=False)
        monkeypatch.setenv("REDIS_URL", original_redis_url)
        if original_redis_cache_url:
            monkeypatch.setenv("REDIS_CACHE_URL", original_redis_cache_url)
        else:
            monkeypatch.delenv("REDIS_CACHE_URL", raising=False)
        if original_celery_result_backend_url:
            monkeypatch.setenv("CELERY_RESULT_BACKEND_URL", original_celery_result_backend_url)
        else:
            monkeypatch.delenv("CELERY_RESULT_BACKEND_URL", raising=False)
        monkeypatch.setenv("API_ALLOWED_HOSTS_RAW", original_api_allowed_hosts_raw)
        monkeypatch.setenv("WEB_CORS_ORIGINS_RAW", original_web_cors_origins_raw)
        monkeypatch.setenv("CLERK_AUTHORIZED_PARTIES_RAW", original_clerk_authorized_parties_raw)
        monkeypatch.setenv("APP_PUBLIC_URL", original_app_public_url)
        monkeypatch.setenv("DATABASE_URL", original_database_url)
        monkeypatch.setenv("FEATURE_BACKTESTS_ENABLED", "true" if original_feature_backtests_enabled else "false")
        monkeypatch.setenv("FEATURE_SCANNER_ENABLED", "true" if original_feature_scanner_enabled else "false")
        monkeypatch.setenv("FEATURE_SWEEPS_ENABLED", "true" if original_feature_sweeps_enabled else "false")
        monkeypatch.setenv("FEATURE_ANALYSIS_ENABLED", "true" if original_feature_analysis_enabled else "false")
        monkeypatch.setenv("FEATURE_BILLING_ENABLED", "true" if original_feature_billing_enabled else "false")
        monkeypatch.setenv("RATE_LIMIT_FAIL_CLOSED", "true" if original_rate_limit_fail_closed else "false")
        invalidate_settings()
