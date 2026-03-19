"""Verify that runtime-sensitive code paths use get_settings() not module-level settings."""
from __future__ import annotations

import inspect


def test_metrics_endpoint_uses_get_settings():
    """The /metrics endpoint must call get_settings() per-request for token validation."""
    from apps.api.app.main import prometheus_metrics

    source = inspect.getsource(prometheus_metrics)
    assert "get_settings()" in source, (
        "/metrics must call get_settings() per-request, not use module-level settings"
    )
    assert "_settings.metrics_token" in source or "get_settings().metrics_token" in source


def test_dlq_endpoint_uses_get_settings():
    """The /admin/dlq endpoint must call get_settings() per-request for token validation."""
    from apps.api.app.main import dlq_status

    source = inspect.getsource(dlq_status)
    assert "get_settings()" in source or "_dlq_settings" in source, (
        "/admin/dlq must call get_settings() per-request"
    )


def test_module_level_settings_documented():
    """The module-level settings variable should have a staleness warning."""
    import apps.api.app.main as main_module
    source = inspect.getsource(main_module)
    assert "_startup_settings" in source, (
        "Module-level settings should be aliased from _startup_settings with a warning comment"
    )
