"""Fix 80: ExportService.create_export refuses non-test environments.

Tests that the synchronous create_export method refuses to run when
app_env is 'production' or 'staging'.
"""
from __future__ import annotations

import inspect

import pytest


class TestExportProductionGuard:
    """Verify create_export blocks production and staging environments."""

    def test_create_export_asserts_non_production(self):
        """create_export must assert app_env is not production."""
        from backtestforecast.services.exports import ExportService

        source = inspect.getsource(ExportService.create_export)
        assert "production" in source, (
            "create_export must check for 'production' app_env"
        )
        assert "RuntimeError" in source, (
            "create_export must raise RuntimeError to block production usage"
        )

    def test_create_export_asserts_non_staging(self):
        """create_export must also block staging environment."""
        from backtestforecast.services.exports import ExportService

        source = inspect.getsource(ExportService.create_export)
        assert '("test", "development")' in source, (
            "create_export must only allow test/development environments"
        )

    def test_create_export_raises_runtime_error_in_production(self, monkeypatch):
        """Calling create_export with app_env='production' must raise."""
        from unittest.mock import MagicMock

        from backtestforecast.config import Settings

        mock_settings = MagicMock(spec=Settings)
        mock_settings.app_env = "production"

        monkeypatch.setattr(
            "backtestforecast.services.exports.get_settings",
            lambda: mock_settings,
        )

        from backtestforecast.services.exports import ExportService

        mock_session = MagicMock()
        mock_storage = MagicMock()

        service = ExportService.__new__(ExportService)
        service.session = mock_session
        service._storage = mock_storage
        service.exports = MagicMock()
        service.backtests = MagicMock()
        service.audit = MagicMock()
        service.backtest_service = MagicMock()

        mock_user = MagicMock()
        mock_payload = MagicMock()

        with pytest.raises(RuntimeError, match="production"):
            service.create_export(mock_user, mock_payload)

    def test_create_export_raises_runtime_error_in_staging(self, monkeypatch):
        """Calling create_export with app_env='staging' must also raise."""
        from unittest.mock import MagicMock

        from backtestforecast.config import Settings

        mock_settings = MagicMock(spec=Settings)
        mock_settings.app_env = "staging"

        monkeypatch.setattr(
            "backtestforecast.services.exports.get_settings",
            lambda: mock_settings,
        )

        from backtestforecast.services.exports import ExportService

        mock_session = MagicMock()
        service = ExportService.__new__(ExportService)
        service.session = mock_session
        service._storage = MagicMock()
        service.exports = MagicMock()
        service.backtests = MagicMock()
        service.audit = MagicMock()
        service.backtest_service = MagicMock()

        mock_user = MagicMock()
        mock_payload = MagicMock()

        with pytest.raises(RuntimeError, match="production"):
            service.create_export(mock_user, mock_payload)
