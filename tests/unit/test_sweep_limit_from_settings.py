"""Test that the sweep concurrent limit uses settings, not a hardcoded value.

Before the fix, SweepService._enforce_sweep_quota used a hardcoded
_MAX_CONCURRENT_SWEEPS = 2 while the router used settings.max_concurrent_sweeps
(default 10). This caused inconsistent enforcement.

After the fix, the service uses settings.max_concurrent_sweeps via a property.
"""
from __future__ import annotations

import inspect


class TestSweepLimitFromSettings:
    def test_service_uses_settings_not_hardcoded(self):
        """SweepService must reference get_settings() for the concurrent limit."""
        from backtestforecast.services.sweeps import SweepService

        source = inspect.getsource(SweepService)
        assert "_MAX_CONCURRENT_SWEEPS" not in source, (
            "SweepService should not use a hardcoded _MAX_CONCURRENT_SWEEPS constant"
        )
        assert "max_concurrent_sweeps" in source, (
            "SweepService should reference max_concurrent_sweeps from settings"
        )

    def test_router_does_not_duplicate_limit_check(self):
        """The sweeps router should not have its own concurrent limit check.

        After the fix, only the service enforces the limit (atomically via
        FOR UPDATE), so the router should not contain a separate non-atomic check.
        """
        from apps.api.app.routers import sweeps as sweeps_router

        source = inspect.getsource(sweeps_router)
        assert "max_concurrent_sweeps" not in source, (
            "Router should not check max_concurrent_sweeps - "
            "the service enforces this atomically"
        )

    def test_settings_max_concurrent_sweeps_has_default(self):
        """The settings field must have a reasonable default."""
        from backtestforecast.config import Settings

        field_info = Settings.model_fields.get("max_concurrent_sweeps")
        assert field_info is not None, "max_concurrent_sweeps field must exist on Settings"
        assert field_info.default is not None, "max_concurrent_sweeps must have a default"
        assert field_info.default >= 1, "Default must be at least 1"

    def test_service_property_returns_settings_value(self):
        """The _max_concurrent_sweeps property should return the settings value."""
        from unittest.mock import MagicMock, patch

        from backtestforecast.services.sweeps import SweepService

        mock_session = MagicMock()
        service = SweepService(mock_session)

        mock_settings = MagicMock()
        mock_settings.max_concurrent_sweeps = 42
        with patch("backtestforecast.services.sweeps.get_settings", return_value=mock_settings):
            assert service._max_concurrent_sweeps == 42
