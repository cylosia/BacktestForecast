"""Tests for NightlyPipelineService interface and configuration.

Verifies the pipeline service has the expected methods and can be
instantiated without requiring external services.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest


class TestPipelineServiceInterface:
    """Verify NightlyPipelineService has expected methods."""

    def test_service_class_exists(self):
        from backtestforecast.pipeline.service import NightlyPipelineService

        assert NightlyPipelineService is not None

    def test_run_pipeline_method_exists(self):
        from backtestforecast.pipeline.service import NightlyPipelineService

        assert hasattr(NightlyPipelineService, "run_pipeline")

    def test_service_accepts_session(self):
        """Verify the service constructor accepts a session parameter."""
        from backtestforecast.pipeline.service import NightlyPipelineService

        sig = inspect.signature(NightlyPipelineService.__init__)
        params = list(sig.parameters.keys())
        assert "session" in params

    def test_service_accepts_injected_dependencies(self):
        """Verify the service constructor accepts market_data_fetcher, backtest_executor, and forecaster."""
        from backtestforecast.pipeline.service import NightlyPipelineService

        sig = inspect.signature(NightlyPipelineService.__init__)
        params = list(sig.parameters.keys())
        assert "market_data_fetcher" in params
        assert "backtest_executor" in params
        assert "forecaster" in params

    def test_service_can_be_constructed_with_mocks(self):
        """Verify the service can be instantiated without external services."""
        from backtestforecast.pipeline.service import NightlyPipelineService

        service = NightlyPipelineService(
            session=MagicMock(),
            market_data_fetcher=MagicMock(),
            backtest_executor=MagicMock(),
        )
        assert service is not None
        assert service.session is not None
        assert service.market_data is not None
        assert service.executor is not None
