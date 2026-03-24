"""Tests for audit round 14 - race conditions, GDPR, middleware fixes."""
from __future__ import annotations

import inspect


class TestScanStatusAtomicUpdate:
    """Scan success must use CAS-style WHERE status='running' to prevent
    overwriting a concurrent cancellation."""

    def test_scan_execute_uses_atomic_status_update(self):
        from backtestforecast.services.scans import ScanService
        source = inspect.getsource(ScanService._execute_scan)
        assert 'ScannerJob.status == "running"' in source, (
            "_execute_scan must use WHERE status='running' in its success UPDATE "
            "to prevent overwriting a concurrent cancellation"
        )

    def test_scan_execute_checks_rowcount(self):
        from backtestforecast.services.scans import ScanService
        source = inspect.getsource(ScanService._execute_scan)
        assert "rowcount" in source, (
            "_execute_scan must check rowcount after the CAS update "
            "to detect if the status was already changed"
        )


class TestCompareEndpointFeatureFlag:
    """Compare endpoint must check feature_backtests_enabled."""

    def test_compare_checks_feature_flag(self):
        from apps.api.app.routers.backtests import compare_backtests
        source = inspect.getsource(compare_backtests)
        assert "feature_backtests_enabled" in source, (
            "compare_backtests must check settings.feature_backtests_enabled "
            "to be consistent with create_backtest"
        )


class TestAccountDeletionResilience:
    """Account deletion must not be blocked by cancel_in_flight_jobs failures."""

    def test_cancel_in_flight_wrapped_in_try_except(self):
        from apps.api.app.routers.account import delete_account
        source = inspect.getsource(delete_account)
        cancel_idx = source.find("cancel_in_flight_jobs")
        assert cancel_idx != -1, "cancel_in_flight_jobs must be called"
        preceding = source[max(0, cancel_idx - 200):cancel_idx]
        assert "try:" in preceding or "except" in source[cancel_idx:cancel_idx + 200], (
            "cancel_in_flight_jobs must be wrapped in try/except "
            "so GDPR account deletion is not blocked by transient failures"
        )


class TestCancelledErrorMiddleware:
    """CancelledError middleware must not send double response."""

    def test_middleware_tracks_response_started(self):
        from apps.api.app.main import _CancelledErrorMiddleware
        source = inspect.getsource(_CancelledErrorMiddleware)
        assert "response_started" in source, (
            "_CancelledErrorMiddleware must track whether response headers "
            "were already sent to avoid double-response crashes"
        )


class TestCachedExceptionReRaise:
    """Market data cached exception re-raise must use safe pattern."""

    def test_cached_exception_uses_data_unavailable_error(self):
        from backtestforecast.market_data.service import MarketDataService
        source = inspect.getsource(MarketDataService)
        assert "DataUnavailableError" in source, (
            "Cached exception re-raise must use DataUnavailableError "
            "instead of type(exc)(str(exc)) which crashes on complex constructors"
        )
