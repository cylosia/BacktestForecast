"""Verification tests for the Performance Findings.

Covers all 9 findings: cache staleness, memory caps, Decimal caching,
session consolidation, query optimization, and S3 pagination.
"""
from __future__ import annotations

import inspect
import warnings

import pytest


# ---- PF1: Cache staleness detection wired in ----

def test_pf1_staleness_check_exists():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    assert hasattr(BacktestExecutionService, '_check_data_staleness')


# ---- PF2: CSV in-memory with size cap ----

def test_pf2_csv_has_size_cap():
    from backtestforecast.services.exports import _MAX_EXPORT_BYTES, _MAX_CSV_TRADES
    assert _MAX_EXPORT_BYTES == 10 * 1024 * 1024
    assert _MAX_CSV_TRADES == 10_000


# ---- PF3: PDF in-memory with page cap ----

def test_pf3_pdf_has_page_cap():
    from backtestforecast.services.exports import _MAX_PDF_PAGES
    assert _MAX_PDF_PAGES == 50


# ---- PF4: _D() cache for common values ----

def test_pf4_d_cache_has_common_values():
    from backtestforecast.backtests.engine import _D_CACHE
    assert 0 in _D_CACHE
    assert 1 in _D_CACHE
    assert -1 in _D_CACHE
    assert 100 in _D_CACHE
    assert 0.0 in _D_CACHE
    assert 1.0 in _D_CACHE


def test_pf4_d_cache_used_by_d_function():
    from backtestforecast.backtests.engine import _D, _D_CACHE
    from decimal import Decimal
    result = _D(1)
    assert result is _D_CACHE[1]


# ---- PF5: lazy='raise' is intentional ----

def test_pf5_lazy_raise_on_user_model():
    from backtestforecast.models import User
    for rel in User.__mapper__.relationships:
        assert rel.lazy in ("raise", "raise_on_sql")


# ---- PF6: Reaper sessions consolidated ----

def test_pf6_reaper_uses_single_session_per_model():
    """The reaper should use one session per model, not two separate ones."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _reap_stale_jobs_inner
    source = inspect.getsource(_reap_stale_jobs_inner)
    lines = source.split('\n')
    session_count = sum(1 for line in lines if 'create_worker_session()' in line)
    assert session_count <= 8, (
        f"Reaper opens {session_count} sessions; expected <= 8 "
        f"(5 models + pipeline + orphan + metrics)"
    )


def test_pf6_reaper_rollback_on_phase_failure():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _reap_stale_jobs_inner
    source = inspect.getsource(_reap_stale_jobs_inner)
    assert "session.rollback()" in source


# ---- PF7: Reconciliation capped at 100 users ----

def test_pf7_reconcile_has_limit():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService.reconcile_subscriptions)
    assert ".limit(100)" in source


# ---- PF8: Audit cleanup uses single-query delete ----

def test_pf8_audit_cleanup_single_query():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    source = inspect.getsource(cleanup_audit_events)
    assert "scalar_subquery" in source
    old_pattern_count = source.count("batch_events = list(session.execute")
    assert old_pattern_count == 0, (
        "Old two-query pattern (SELECT then DELETE) still present"
    )


# ---- PF9: S3 iter_keys uses lazy pagination ----

def test_pf9_iter_keys_is_generator():
    from backtestforecast.exports.storage import S3Storage
    source = inspect.getsource(S3Storage.iter_keys)
    assert "yield" in source
    assert "paginator" in source
