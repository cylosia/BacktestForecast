"""Tests for configuration validation edge cases."""
from __future__ import annotations

import pytest

import backtestforecast.config as config_module
from backtestforecast.config import Settings


class TestSettingsValidation:
    def test_default_settings_load(self):
        settings = Settings()
        assert settings.app_env == "development"
        assert settings.app_name == "BacktestForecast API"

    def test_invalid_app_env_raises(self):
        with pytest.raises(ValueError, match="app_env must be one of"):
            Settings(app_env="invalid_env")

    def test_invalid_log_level_raises(self):
        with pytest.raises(ValueError, match="log_level must be one of"):
            Settings(log_level="INVALID")

    def test_ip_hash_salt_too_short(self):
        with pytest.raises(ValueError, match="at least 16 characters"):
            Settings(ip_hash_salt="short")

    def test_invalid_cidr_raises(self):
        with pytest.raises(ValueError, match="Invalid CIDR"):
            Settings(trusted_proxy_cidrs="not-a-cidr")

    def test_port_range_validation(self):
        with pytest.raises(ValueError, match="Port must be between"):
            Settings(api_port=99999)

    def test_risk_free_rate_range(self):
        with pytest.raises(ValueError, match="risk_free_rate must be between"):
            Settings(risk_free_rate=0.5)

    def test_sentry_sample_rate_range(self):
        with pytest.raises(ValueError, match="sentry_traces_sample_rate must be between"):
            Settings(sentry_traces_sample_rate=2.0)

    def test_symbols_csv_override(self):
        settings = Settings(pipeline_default_symbols_csv="AAPL,MSFT,INVALID!!!")
        assert "AAPL" in settings.pipeline_default_symbols
        assert "MSFT" in settings.pipeline_default_symbols

    def test_symbols_capped_at_max(self):
        symbols = ",".join(f"SYM{i}" for i in range(300))
        settings = Settings(pipeline_default_symbols_csv=symbols)
        assert len(settings.pipeline_default_symbols) <= Settings.MAX_SYMBOLS

    def test_web_cors_origins_parsing(self):
        settings = Settings(web_cors_origins_raw="http://localhost:3000,https://app.example.com")
        assert "http://localhost:3000" in settings.web_cors_origins
        assert "https://app.example.com" in settings.web_cors_origins

    def test_web_cors_origins_rejects_invalid(self):
        with pytest.raises(ValueError, match="all entries were invalid"):
            settings = Settings(web_cors_origins_raw="not-a-url")
            _ = settings.web_cors_origins

    def test_stripe_price_lookup(self):
        settings = Settings(
            stripe_pro_monthly_price_id="price_pro_m",
            stripe_pro_yearly_price_id="price_pro_y",
        )
        lookup = settings.stripe_price_lookup
        assert lookup[("pro", "monthly")] == "price_pro_m"
        assert lookup[("pro", "yearly")] == "price_pro_y"

    def test_redis_cache_url_defaults_to_redis_url(self):
        settings = Settings(redis_url="redis://localhost:6379/0", redis_password=None)
        assert settings.redis_cache_url == settings.redis_url == "redis://localhost:6379/0"

    def test_apply_runtime_local_overrides_rewrites_localhost_redis_urls(self):
        settings = Settings(
            app_env="development",
            redis_url="redis://localhost:6379/0",
            redis_cache_url="redis://localhost:6380/1",
            celery_result_backend_url="redis://localhost:6379/2",
        )

        rewritten = config_module._apply_runtime_local_overrides(settings)

        assert rewritten.redis_url == "redis://127.0.0.1:6379/0"
        assert rewritten.redis_cache_url == "redis://127.0.0.1:6380/1"
        assert rewritten.celery_result_backend_url == "redis://127.0.0.1:6379/2"

    def test_get_settings_loads_repo_env_files(self, monkeypatch):
        captured: dict[str, object] = {}

        class _FakeSettings:
            def __init__(self, **kwargs):
                captured["env_file"] = kwargs.get("_env_file")
                self.app_env = "development"
                self.massive_api_key = "test-env-key"
                self.redis_url = "redis://localhost:6379/0"
                self.redis_cache_url = None
                self.celery_result_backend_url = None

        monkeypatch.setattr(config_module, "Settings", _FakeSettings)
        monkeypatch.setattr(config_module, "_repo_env_files", lambda: ("repo.env", "apps/api/.env"))
        monkeypatch.setattr(config_module, "_settings_cache", None)

        settings = config_module.get_settings()

        assert captured["env_file"] == ("repo.env", "apps/api/.env")
        assert settings.massive_api_key == "test-env-key"
        assert settings.redis_url == "redis://127.0.0.1:6379/0"
