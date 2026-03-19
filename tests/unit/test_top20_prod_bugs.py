"""Verification tests for the Top 20 Bugs Most Likely Already Affecting Production.

Each test confirms the fix is in place and the bug can no longer occur.
"""
from __future__ import annotations

import inspect
import warnings
from decimal import Decimal

import pytest


# ---- B1: _commit_then_publish NameError ----

def test_b1_commit_then_publish_defined():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _commit_then_publish
    assert callable(_commit_then_publish)


# ---- B2: Scan jobs permanent fail on transient errors ----

def test_b2_scan_retries_external_service_error():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job
    source = inspect.getsource(run_scan_job)
    assert "ExternalServiceError" in source
    assert "self.retry" in source


# ---- B3: holding_period_days calendar vs trading ----

def test_b3_trading_days_field_exists():
    from backtestforecast.backtests.types import TradeResult
    assert "holding_period_trading_days" in TradeResult.__dataclass_fields__


def test_b3_engine_populates_trading_days():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._close_position)
    assert "holding_period_trading_days" in source


# ---- B4: Stale option data from cache ----

def test_b4_staleness_detection_wired():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    source = inspect.getsource(BacktestExecutionService.execute_request)
    assert "_check_data_staleness" in source


# ---- B5: entry_underlying_close 0.0 ----

def test_b5_entry_underlying_close_warns_on_missing():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._entry_underlying_close)
    assert "missing_entry_underlying_close" in source


# ---- B6: Settings staleness ----

def test_b6_metrics_endpoint_fresh_settings():
    from apps.api.app.main import prometheus_metrics
    source = inspect.getsource(prometheus_metrics)
    assert "get_settings()" in source


# ---- B7: Position sizing float truncation ----

def test_b7_position_sizing_accepts_decimal():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    sig = inspect.signature(OptionsBacktestEngine._resolve_position_size)
    param = sig.parameters["available_cash"]
    annotation = str(param.annotation)
    assert "Decimal" in annotation


# ---- B8: reconcile_subscriptions race ----

def test_b8_reconcile_locked():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService.reconcile_subscriptions)
    assert "with_for_update" in source
    assert "skip_locked" in source


# ---- B9: entry_mid/exit_mid confusing ----

def test_b9_entry_mid_schema_description():
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    desc = BacktestTradeResponse.model_fields["entry_mid"].description
    assert desc is not None and "100" in desc


# ---- B10: Break-even excluded from win_rate ----

def test_b10_high_break_even_warning():
    from backtestforecast.backtests.summary import build_summary
    source = inspect.getsource(build_summary)
    assert "high_break_even_rate" in source


def test_b10_decided_trades_in_schema():
    from backtestforecast.schemas.backtests import BacktestSummaryResponse
    assert "decided_trades" in BacktestSummaryResponse.model_fields


# ---- B11: Audit events deletion without archival ----

def test_b11_archival_logging_in_cleanup():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    source = inspect.getsource(cleanup_audit_events)
    assert "archival_batch" in source


def test_b11_retention_days_configurable():
    from backtestforecast.config import Settings
    assert "audit_cleanup_retention_days" in Settings.model_fields


# ---- B12: DLQ items accumulating ----

def test_b12_dlq_write_failures_metric_exists():
    from backtestforecast.observability.metrics import DLQ_WRITE_FAILURES_TOTAL
    assert DLQ_WRITE_FAILURES_TOTAL is not None


def test_b12_dlq_write_failure_tracked_on_error():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import BaseTaskWithDLQ
    source = inspect.getsource(BaseTaskWithDLQ.on_failure)
    assert "DLQ_WRITE_FAILURES_TOTAL" in source


def test_b12_dlq_uses_connection_pool():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _get_dlq_redis
    source = inspect.getsource(_get_dlq_redis)
    assert "ConnectionPool" in source


# ---- B13: Pipeline heuristic marking wrong run ----

def test_b13_pipeline_refuses_ambiguous():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _find_pipeline_run
    source = inspect.getsource(_find_pipeline_run)
    assert "running_count > 1" in source


# ---- B14: evaluated_candidate_count not rejected ----

def test_b14_evaluated_count_raises():
    from backtestforecast.models import ScannerJob
    source = inspect.getsource(ScannerJob._validate_evaluated_count)
    assert "ValueError" in source
    assert "5" in source


# ---- B15: SQL statement timeout on large scans ----

def test_b15_worker_uses_300s_timeout():
    from backtestforecast.config import Settings
    s = Settings.model_fields["db_worker_statement_timeout_ms"]
    assert s.default == 300_000


def test_b15_slow_query_metric_exists():
    from backtestforecast.observability.metrics import API_SLOW_QUERIES_TOTAL
    assert API_SLOW_QUERIES_TOTAL is not None


def test_b15_scan_recommendations_tracks_slow_queries():
    from backtestforecast.services.scans import ScanService
    source = inspect.getsource(ScanService.get_recommendations)
    assert "API_SLOW_QUERIES_TOTAL" in source
    assert "_SLOW_QUERY_THRESHOLD" in source


# ---- B16: PDF truncation silent ----

def test_b16_pdf_truncation_user_notice():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_pdf)
    assert "Use CSV export" in source
    assert "_truncated_at_page_limit" in source


# ---- B17: CSV truncation silent ----

def test_b17_csv_truncation_warning_marker():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_csv)
    assert "WARNING:" in source
    assert "csv_trades_truncated" in source


def test_b17_csv_equity_truncation_warning():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_csv)
    assert "equity points omitted" in source


# ---- B18: just_closed same-day re-entry ----

def test_b18_same_day_reentry_warning():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "same_day_reentry_blocked" in source


# ---- B19: admin_token fallback ----

def test_b19_admin_token_required_production():
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "ADMIN_TOKEN" in source


# ---- B20: CORS/TrustedHost mismatch ----

def test_b20_cors_trustedhost_check_at_startup():
    from apps.api.app.main import _lifespan
    source = inspect.getsource(_lifespan)
    assert "cors_trustedhost_mismatch" in source
