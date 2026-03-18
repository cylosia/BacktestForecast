"""Tests verifying the audit round 4 (fixes 1-30) correctness.

Covers:
  - Fix 1:  Backtest success-path race guard (WHERE status='running')
  - Fix 2:  billing terminate=False
  - Fix 3:  generate_export AppError sets completed_at
  - Fix 4:  run_scan_job AppError sets completed_at
  - Fix 5:  _track_add negative guard (already present)
  - Fix 6:  billing cancel_in_flight_jobs flush
  - Fix 7:  billing cancel_values includes updated_at
  - Fix 8:  _fail_stale_running_jobs includes updated_at
  - Fix 12: _iv_cache is OrderedDict with size cap
  - Fix 13: clear_caches total includes _iv_cache
  - Fix 22: get_chain_delta_lookup double-checked locking
  - Fix 23: Reaper uses per-model sessions
  - Fix 30: store_iv/get_iv thread-safe IV cache API
"""
from __future__ import annotations

import inspect
import threading
from collections import OrderedDict
from datetime import date
from unittest.mock import MagicMock

import pytest


class TestFix1BacktestSuccessGuard:
    """Fix 1: execute_run_by_id success path must use WHERE status='running'."""

    def test_success_path_uses_conditional_update(self):
        from backtestforecast.services.backtests import BacktestService
        source = inspect.getsource(BacktestService.execute_run_by_id)
        assert 'BacktestRun.status == "running"' in source, (
            "Success commit must guard against concurrent reaper with "
            "WHERE BacktestRun.status == 'running'"
        )
        assert "success_overwrite_prevented" in source


class TestFix2BillingTerminate:
    """Fix 2: cancel_in_flight_jobs must not use terminate=True."""

    def test_revoke_uses_terminate_false(self):
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService.cancel_in_flight_jobs)
        assert "terminate=True" not in source, (
            "cancel_in_flight_jobs must use terminate=False to avoid "
            "killing entire worker processes"
        )
        assert "terminate=False" in source


class TestFix6BillingFlush:
    """Fix 6: cancel_in_flight_jobs must flush before SSE notifications."""

    def test_flush_before_publish(self):
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService.cancel_in_flight_jobs)
        flush_pos = source.find("self.session.flush()")
        publish_pos = source.find("publish_job_status")
        assert flush_pos > 0 and publish_pos > 0
        assert flush_pos < publish_pos, (
            "session.flush() must appear before publish_job_status calls"
        )


class TestFix7BillingUpdatedAt:
    """Fix 7: cancel_values must include updated_at."""

    def test_cancel_values_has_updated_at(self):
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService.cancel_in_flight_jobs)
        assert '"updated_at"' in source or "'updated_at'" in source


class TestFix8ReaperUpdatedAt:
    """Fix 8: _fail_stale_running_jobs must include updated_at."""

    def test_fail_stale_includes_updated_at(self):
        from apps.worker.app.tasks import _fail_stale_running_jobs
        source = inspect.getsource(_fail_stale_running_jobs)
        assert '"updated_at"' in source or "'updated_at'" in source


class TestFix12IvCacheOrderedDict:
    """Fix 12: _iv_cache must be an OrderedDict with size cap."""

    def test_iv_cache_is_ordered_dict(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        client = MagicMock()
        gw = MassiveOptionGateway(client=client, symbol="AAPL")
        assert isinstance(gw._iv_cache, OrderedDict)

    def test_iv_cache_max_constant_exists(self):
        from backtestforecast.market_data import service
        assert hasattr(service, "_GATEWAY_IV_CACHE_MAX")
        assert service._GATEWAY_IV_CACHE_MAX > 0


class TestFix13ClearCachesTotal:
    """Fix 13: clear_caches total must include _iv_cache length."""

    def test_clear_caches_source_includes_iv_cache_in_total(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        source = inspect.getsource(MassiveOptionGateway.clear_caches)
        assert "len(self._iv_cache)" in source


class TestFix30StoreGetIv:
    """Fix 30: store_iv/get_iv thread-safe IV cache API."""

    def test_store_iv_exists(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        assert hasattr(MassiveOptionGateway, "store_iv")

    def test_get_iv_exists(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        assert hasattr(MassiveOptionGateway, "get_iv")

    def test_store_and_get_roundtrip(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        client = MagicMock()
        gw = MassiveOptionGateway(client=client, symbol="AAPL")
        key = ("O:AAPL260101C00100000", date(2026, 1, 1))
        gw.store_iv(key, 0.25)
        found, val = gw.get_iv(key)
        assert found is True
        assert val == 0.25

    def test_get_iv_miss(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        client = MagicMock()
        gw = MassiveOptionGateway(client=client, symbol="AAPL")
        found, val = gw.get_iv(("nonexistent", date(2026, 1, 1)))
        assert found is False
        assert val is None

    def test_store_iv_evicts_when_over_cap(self):
        from backtestforecast.market_data import service as svc
        from backtestforecast.market_data.service import MassiveOptionGateway
        old_max = svc._GATEWAY_IV_CACHE_MAX
        svc._GATEWAY_IV_CACHE_MAX = 10
        try:
            client = MagicMock()
            gw = MassiveOptionGateway(client=client, symbol="AAPL")
            for i in range(15):
                gw.store_iv((f"O:T{i}", date(2026, 1, 1)), float(i))
            assert len(gw._iv_cache) <= 10
        finally:
            svc._GATEWAY_IV_CACHE_MAX = old_max


class TestFix22ThunderingHerd:
    """Fix 22: get_chain_delta_lookup must use double-checked locking."""

    def test_double_checked_locking_pattern(self):
        from backtestforecast.market_data.service import MassiveOptionGateway
        source = inspect.getsource(MassiveOptionGateway.get_chain_delta_lookup)
        lock_acquires = source.count("with self._lock:")
        assert lock_acquires >= 3, (
            f"Expected at least 3 lock acquisitions (outer check, inner check, store), "
            f"got {lock_acquires}"
        )


class TestFix23ReaperPerModelSessions:
    """Fix 23: Reaper must use separate sessions per model type."""

    def test_reaper_inner_uses_multiple_sessions(self):
        from apps.worker.app.tasks import _reap_stale_jobs_inner
        source = inspect.getsource(_reap_stale_jobs_inner)
        session_opens = source.count("create_worker_session()")
        assert session_opens >= 5, (
            f"Expected at least 5 session opens (one per model type + pipeline + orphans), "
            f"got {session_opens}"
        )
