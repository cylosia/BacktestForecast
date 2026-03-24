"""Tests verifying critical audit fixes 21-30 remain in place."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


class TestPipelineForecasterCacheKeyIncludesHorizon:
    """Fix 1: PipelineForecaster cache key must include horizon_days so that
    forecasts with different horizons are not served from the same cache entry."""

    def test_cache_key_differs_by_horizon_days(self):
        import inspect

        from backtestforecast.pipeline.adapters import PipelineForecaster
        source = inspect.getsource(PipelineForecaster.get_forecast)
        assert "(symbol, end_date, horizon_days)" in source, (
            "PipelineForecaster cache key must include horizon_days"
        )

    def test_get_forecast_uses_horizon_in_cache_key(self):
        from backtestforecast.pipeline.adapters import PipelineForecaster

        mock_forecaster = MagicMock()
        mock_market_data = MagicMock()
        mock_market_data.get_daily_bars.return_value = [MagicMock() for _ in range(100)]

        mock_result = MagicMock()
        mock_result.expected_return_low_pct = 1.0
        mock_result.expected_return_median_pct = 2.0
        mock_result.expected_return_high_pct = 3.0
        mock_result.positive_outcome_rate_pct = 60.0
        mock_result.analog_count = 20
        mock_result.horizon_days = 30
        mock_forecaster.forecast.return_value = mock_result

        pf = PipelineForecaster(mock_forecaster, mock_market_data)
        as_of = date(2025, 6, 1)

        pf.get_forecast("AAPL", "long_call", 30, as_of_date=as_of)
        pf.get_forecast("AAPL", "long_call", 60, as_of_date=as_of)

        assert len(pf._bar_cache) == 2, (
            "Two different horizon_days should produce two cache entries"
        )


class TestEngineRejectsEntryWhenCashGoesNegative:
    """Fix 3: The engine must skip entries whose total cost (including
    slippage) would drive available cash below zero."""

    def test_negative_cash_entry_is_rejected(self):
        import inspect

        from backtestforecast.backtests.engine import OptionsBacktestEngine

        source = inspect.getsource(OptionsBacktestEngine.run)
        assert "negative_cash_rejected" in source, (
            "Engine.run must contain a 'negative_cash_rejected' warning path"
        )
        assert "cash - total_entry_cost < 0" in source, (
            "Engine.run must check 'cash - total_entry_cost < 0'"
        )


class TestDailyPicksHistoryChecksFeatureFlag:
    """Fix 5: The /daily-picks/history endpoint must gate access behind
    the feature_daily_picks_enabled flag."""

    def test_history_endpoint_checks_feature_flag(self):
        import inspect

        from apps.api.app.routers.daily_picks import get_pipeline_history

        source = inspect.getsource(get_pipeline_history)
        assert "feature_daily_picks_enabled" in source, (
            "get_pipeline_history must check feature_daily_picks_enabled"
        )
        assert "FeatureLockedError" in source, (
            "get_pipeline_history must raise FeatureLockedError when disabled"
        )


class TestPoolShutdownUsesWait:
    """Fix 7: ThreadPoolExecutor.shutdown() must use wait=True to prevent
    worker threads from being abandoned."""

    def test_deep_analysis_pool_shutdown_uses_wait_true(self):
        import inspect

        from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService

        source = inspect.getsource(SymbolDeepAnalysisService)
        assert "with ThreadPoolExecutor" in source, (
            "SymbolDeepAnalysisService must manage ThreadPoolExecutor with a context manager "
            "or explicit wait=True shutdown"
        )


class TestAsyncMassiveClientEnforcesHTTPS:
    """Fix 12: AsyncMassiveClient must reject non-HTTPS base URLs in
    production and staging environments."""

    def test_async_client_rejects_http_in_production(self):
        from backtestforecast.errors import ConfigurationError

        mock_settings = MagicMock()
        mock_settings.massive_api_key = "test-key"
        mock_settings.massive_base_url = "http://insecure.example.com"
        mock_settings.app_env = "production"
        mock_settings.massive_timeout_seconds = 10
        mock_settings.massive_max_retries = 3
        mock_settings.massive_retry_backoff_seconds = 1.0

        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import AsyncMassiveClient

            with pytest.raises(ConfigurationError, match="HTTPS"):
                AsyncMassiveClient(api_key="test-key", base_url="http://insecure.example.com")

    def test_async_client_allows_https_in_production(self):
        mock_settings = MagicMock()
        mock_settings.massive_api_key = "test-key"
        mock_settings.massive_base_url = "https://secure.example.com"
        mock_settings.app_env = "production"
        mock_settings.massive_timeout_seconds = 10
        mock_settings.massive_max_retries = 3
        mock_settings.massive_retry_backoff_seconds = 1.0

        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import AsyncMassiveClient

            client = AsyncMassiveClient(api_key="test-key", base_url="https://secure.example.com")
            assert client.base_url == "https://secure.example.com"

    def test_async_client_allows_http_in_development(self):
        mock_settings = MagicMock()
        mock_settings.massive_api_key = "test-key"
        mock_settings.massive_base_url = "http://localhost:8080"
        mock_settings.app_env = "development"
        mock_settings.massive_timeout_seconds = 10
        mock_settings.massive_max_retries = 3
        mock_settings.massive_retry_backoff_seconds = 1.0

        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import AsyncMassiveClient

            client = AsyncMassiveClient(api_key="test-key", base_url="http://localhost:8080")
            assert client.base_url == "http://localhost:8080"
