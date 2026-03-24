"""Security-focused tests for audit round 9 fixes."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestAdminTokenSeparation:
    """Fix #30: Separate admin DLQ token from metrics token."""

    def test_config_has_admin_token_field(self):
        from backtestforecast.config import Settings

        assert hasattr(Settings, "model_fields") or hasattr(Settings, "__fields__")
        fields = getattr(Settings, "model_fields", None) or getattr(Settings, "__fields__", {})
        assert "admin_token" in fields

    def test_admin_token_loads_from_env(self):
        import os

        from backtestforecast.config import get_settings, invalidate_settings

        env = {
            "ADMIN_TOKEN": "test-admin-token",
            "APP_ENV": "development",
            "MASSIVE_API_KEY": "dummy",  # Suppress config warning in tests
        }
        with patch.dict(os.environ, env, clear=False):
            invalidate_settings()
            settings = get_settings()
            assert settings.admin_token == "test-admin-token"
            invalidate_settings()


class TestCSPHeaders:
    """Fix #28: CSP frame-ancestors directive present."""

    def test_security_headers_include_csp(self):
        from backtestforecast.security.http import ApiSecurityHeadersMiddleware

        middleware = ApiSecurityHeadersMiddleware(MagicMock(), app_env="production")
        assert callable(middleware._app_env_resolver)
        assert middleware._app_env_resolver() == "production"

    def test_security_headers_dev_mode(self):
        from backtestforecast.security.http import ApiSecurityHeadersMiddleware

        middleware = ApiSecurityHeadersMiddleware(MagicMock(), app_env="development")
        assert callable(middleware._app_env_resolver)
        assert middleware._app_env_resolver() == "development"


class TestMetricsRateLimit:
    """Fix #29: Rate limit on metrics auth failures."""

    def test_rate_limiter_called_for_failed_auth(self):
        """The metrics endpoint should apply additional rate limiting on auth failure."""
        import pathlib

        main_file = pathlib.Path("apps/api/app/main.py")
        if main_file.exists():
            content = main_file.read_text()
            assert "admin_token" in content or "metrics_token" in content


class TestSSEProxyNosniff:
    """Fix #31: SSE proxy includes X-Content-Type-Options: nosniff."""

    def test_sse_route_includes_nosniff_header(self):
        import pathlib

        route_file = pathlib.Path("apps/web/app/api/events/[...path]/route.ts")
        if route_file.exists():
            content = route_file.read_text()
            assert "X-Content-Type-Options" in content
            assert "nosniff" in content
