from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.app.main import app
from backtestforecast.config import get_settings, invalidate_settings


def test_runtime_config_invalidation_updates_health_version_visibility(monkeypatch) -> None:
    settings = get_settings()
    original_env = settings.app_env
    try:
        monkeypatch.setenv("APP_ENV", "development")
        invalidate_settings()
        with TestClient(app, base_url="http://localhost") as client:
            dev_resp = client.get("/health/live")
            assert "version" in dev_resp.json()

        monkeypatch.setenv("APP_ENV", "production")
        invalidate_settings()
        with TestClient(app, base_url="http://localhost") as client:
            prod_resp = client.get("/health/live")
            assert "version" not in prod_resp.json()
    finally:
        monkeypatch.setenv("APP_ENV", original_env)
        invalidate_settings()
