"""Verification tests for the Medium-Term Refactors (This Sprint).

Covers all 6 items:
  MT10: Split tasks.py — task_helpers.py exists, imports used
  MT11: Option cache staleness detection
  MT12: JSON schema validation for JSON_VARIANT columns
  MT13: Audit event archival before deletion
  MT14: Postgres CHECK constraint tests
  MT15: entry_mid/exit_mid documentation
"""
from __future__ import annotations

import inspect
import warnings
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ============================================================================
# MT10: Split tasks.py into per-domain modules
# ============================================================================

def test_mt10_task_helpers_module_exists():
    from apps.worker.app.task_helpers import (
        commit_then_publish,
        mark_job_failed,
        update_heartbeat,
        validate_task_ownership,
        handle_task_app_error,
    )
    for fn in (commit_then_publish, mark_job_failed, update_heartbeat, validate_task_ownership):
        assert callable(fn)


def test_mt10_tasks_imports_from_helpers():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks as tasks_mod
    src = inspect.getsource(tasks_mod)
    assert "from apps.worker.app.task_helpers import" in src


def test_mt10_no_duplicate_commit_then_publish():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks as tasks_mod
    src = inspect.getsource(tasks_mod)
    assert src.count("def _commit_then_publish(") == 0
    assert src.count("def commit_then_publish(") == 0


def test_mt10_task_helpers_has_docstring():
    from apps.worker.app import task_helpers
    assert task_helpers.__doc__ is not None


# ============================================================================
# MT11: Option cache staleness detection/reporting
# ============================================================================

def test_mt11_staleness_check_method_exists():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    assert hasattr(BacktestExecutionService, '_check_data_staleness')


def test_mt11_staleness_check_wired_into_execute():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    src = inspect.getsource(BacktestExecutionService.execute_request)
    assert "_check_data_staleness" in src


def test_mt11_staleness_emits_warning_code():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    src = inspect.getsource(BacktestExecutionService._check_data_staleness)
    assert "stale_option_cache" in src


def test_mt11_staleness_config_exists():
    from backtestforecast.config import Settings
    assert "option_cache_warn_age_seconds" in Settings.model_fields


def test_mt11_staleness_metric_exists():
    from backtestforecast.observability.metrics import OPTION_CACHE_AGE_SECONDS
    assert OPTION_CACHE_AGE_SECONDS is not None


# ============================================================================
# MT12: JSON schema validation for JSON_VARIANT columns
# ============================================================================

def test_mt12_summary_required_keys_defined():
    from backtestforecast.schemas.json_shapes import _SUMMARY_REQUIRED_KEYS
    assert "trade_count" in _SUMMARY_REQUIRED_KEYS
    assert "win_rate" in _SUMMARY_REQUIRED_KEYS
    assert "total_net_pnl" in _SUMMARY_REQUIRED_KEYS
    assert "starting_equity" in _SUMMARY_REQUIRED_KEYS
    assert len(_SUMMARY_REQUIRED_KEYS) >= 7


def test_mt12_backtest_validates_trade_detail():
    from backtestforecast.services.backtests import BacktestService
    src = inspect.getsource(BacktestService)
    assert "validate_json_shape" in src
    assert "_TRADE_DETAIL_REQUIRED_KEYS" in src


def test_mt12_scan_validates_summary_and_forecast():
    from backtestforecast.services.scans import ScanService
    src = inspect.getsource(ScanService)
    assert "validate_json_shape" in src
    assert "_FORECAST_REQUIRED_KEYS" in src


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_mt12_sweep_validates_summary():
    from backtestforecast.services.sweeps import SweepService
    src = inspect.getsource(SweepService)
    assert "_SUMMARY_REQUIRED_KEYS" in src
    assert "validate_json_shape" in src


def test_mt12_validate_json_shape_supports_strict_mode():
    from backtestforecast.schemas.json_shapes import validate_json_shape
    with pytest.raises(ValueError, match="missing required keys"):
        validate_json_shape({}, "test", required_keys=frozenset({"x"}), strict=True)


# ============================================================================
# MT13: Audit event archival before deletion
# ============================================================================

def test_mt13_cleanup_logs_before_delete():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    src = inspect.getsource(cleanup_audit_events)
    assert "archival_batch" in src


def test_mt13_retention_days_configurable():
    from backtestforecast.config import Settings
    field = Settings.model_fields["audit_cleanup_retention_days"]
    assert field.default == 90


def test_mt13_retention_days_used_in_cleanup():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    src = inspect.getsource(cleanup_audit_events)
    assert "audit_cleanup_retention_days" in src


def test_mt13_audit_cleanup_gated_by_enabled_flag():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    src = inspect.getsource(cleanup_audit_events)
    assert "audit_cleanup_enabled" in src


# ============================================================================
# MT14: Integration tests using Postgres for CHECK constraints
# ============================================================================

def test_mt14_check_constraint_test_file_exists():
    path = PROJECT_ROOT / "tests" / "unit" / "test_postgres_check_constraints.py"
    assert path.exists()


def test_mt14_check_constraint_tests_cover_key_models():
    import tests.unit.test_postgres_check_constraints as t
    test_names = [n for n in dir(t) if n.startswith("test_")]
    models_tested = set()
    for name in test_names:
        if "user" in name:
            models_tested.add("user")
        if "backtest_run" in name:
            models_tested.add("backtest_run")
        if "export" in name:
            models_tested.add("export")
        if "scanner" in name:
            models_tested.add("scanner")
        if "sweep" in name:
            models_tested.add("sweep")
        if "symbol" in name or "analysis" in name:
            models_tested.add("analysis")
        if "status" in name or "schema" in name:
            models_tested.add("schema")
    assert len(models_tested) >= 5, f"Only {len(models_tested)} model groups tested: {models_tested}"


# ============================================================================
# MT15: entry_mid/exit_mid documentation
# ============================================================================

def test_mt15_backtest_trade_entry_mid_documented():
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    desc = BacktestTradeResponse.model_fields["entry_mid"].description
    assert desc is not None
    assert "100" in desc
    assert "position value" in desc.lower() or "per-share" in desc.lower()


def test_mt15_backtest_trade_exit_mid_documented():
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    desc = BacktestTradeResponse.model_fields["exit_mid"].description
    assert desc is not None
    assert "entry_mid" in desc


def test_mt15_trade_json_response_entry_mid_documented():
    from backtestforecast.schemas.backtests import TradeJsonResponse
    desc = TradeJsonResponse.model_fields["entry_mid"].description
    assert desc is not None
    assert "100" in desc


def test_mt15_csv_export_has_entry_mid_note():
    from backtestforecast.services.exports import ExportService
    src = inspect.getsource(ExportService._build_csv)
    assert "entry_value_per_share" in src or "per-unit position value" in src.lower() or "contract multiplier" in src.lower()


def test_mt15_typescript_schema_has_entry_mid_description():
    ts_path = PROJECT_ROOT / "packages" / "api-client" / "src" / "schema.d.ts"
    content = ts_path.read_text()
    assert "Per-share position value" in content
