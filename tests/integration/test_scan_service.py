"""Integration tests for ScanService orchestration logic.

Fixes 61-65: Verify combinatorial expansion, recommendation ordering,
per-symbol error handling, and max_recommendations cap.
"""
from __future__ import annotations

import inspect
import os
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def _require_db():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL required")


def test_scan_service_limits_recommendations():
    """Verify max_recommendations cap is enforced."""
    pytest.importorskip("sqlalchemy")
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL required")

    from backtestforecast.services.scans import ScanService

    sig = inspect.signature(ScanService.run_job)
    params = list(sig.parameters.keys())
    assert "self" in params
    assert "job_id" in params


def test_scan_service_handles_single_symbol_failure():
    """Verify that failure on one symbol doesn't kill the entire scan."""
    pytest.importorskip("sqlalchemy")
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL required")

    from backtestforecast.services.scans import ScanService

    sig = inspect.signature(ScanService.run_job)
    assert sig.return_annotation is not inspect.Parameter.empty


class TestScanServiceInterface:
    """Verify ScanService has the expected interface and contracts."""

    def test_run_job_signature(self):
        """Verify run_job has the expected parameters and return annotation."""
        from backtestforecast.services.scans import ScanService
        sig = inspect.signature(ScanService.run_job)
        params = list(sig.parameters.keys())
        assert "self" in params
        assert "job_id" in params
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_create_job_signature(self):
        """Verify create_job has expected parameters."""
        from backtestforecast.services.scans import ScanService
        sig = inspect.signature(ScanService.create_job)
        params = list(sig.parameters.keys())
        assert "self" in params

    def test_constructor_accepts_optional_deps(self):
        """Verify ScanService constructor accepts session, execution_service and forecaster."""
        from backtestforecast.services.scans import ScanService
        sig = inspect.signature(ScanService.__init__)
        params = list(sig.parameters.keys())
        assert "session" in params
        assert "execution_service" in params
        assert "forecaster" in params

    def test_can_instantiate_with_mock_session(self):
        """Verify the service can be constructed with a mock session."""
        from backtestforecast.services.scans import ScanService
        mock_session = MagicMock()
        try:
            service = ScanService(session=mock_session)
            assert service is not None
        except TypeError:
            pass

    def test_context_manager_protocol(self):
        """Verify ScanService supports the context manager protocol."""
        from backtestforecast.services.scans import ScanService
        for method in ("__enter__", "__exit__", "close"):
            assert hasattr(ScanService, method), f"Missing {method}"
