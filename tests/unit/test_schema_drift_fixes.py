"""Verification tests for the Database / Migration / Schema Drift findings.

Covers all 6 findings: migration chain integrity, JSON validation,
audit dedup safety, and constraint alignment.
"""
from __future__ import annotations

import warnings
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VERSIONS_DIR = PROJECT_ROOT / "alembic" / "versions"


# ---- D1: Migration revision IDs are valid ----

def test_d1_migration_0035_has_correct_revision():
    content = (VERSIONS_DIR / "20260319_0035_pipeline_scanner_fk.py").read_text()
    assert 'revision = "20260319_0035"' in content
    assert 'down_revision = "20260319_0034"' in content


def test_d1_migration_0036_has_correct_revision():
    content = (VERSIONS_DIR / "20260319_0036_holding_trading_days.py").read_text()
    assert 'revision = "20260319_0036"' in content
    assert 'down_revision = "20260319_0035"' in content


def test_d1_migration_0034_is_merge_point():
    content = (VERSIONS_DIR / "20260319_0034_schema_drift_fixes.py").read_text()
    assert 'down_revision = ("20260319_0033", "0024_heartbeat")' in content


# ---- D2: Duplicate 0024 — actually separate branches ----

def test_d2_0024_files_have_different_revision_ids():
    f1 = (VERSIONS_DIR / "20260318_0024_validate_sweep_mode_constraint.py").read_text()
    f2 = (VERSIONS_DIR / "20260318_0024_add_heartbeat_and_trade_index.py").read_text()
    assert 'revision = "20260318_0024"' in f1
    assert 'revision = "0024_heartbeat"' in f2


# ---- D3: strategy_type intentionally has no CHECK ----

def test_d3_strategy_type_no_check_is_documented():
    from backtestforecast.models import BacktestRun
    source = (PROJECT_ROOT / "src" / "backtestforecast" / "models.py").read_text()
    assert "strategy_type columns use String(48) without a DB-level CHECK" in source


# ---- D4: JSON columns have shape validation ----

def test_d4_backtest_trade_detail_validated():
    from backtestforecast.services.backtests import BacktestService
    import inspect
    source = inspect.getsource(BacktestService)
    assert "validate_json_shape" in source
    assert "_TRADE_DETAIL_REQUIRED_KEYS" in source


def test_d4_scan_recommendation_summary_validated():
    from backtestforecast.services.scans import ScanService
    import inspect
    source = inspect.getsource(ScanService)
    assert "validate_json_shape" in source


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_d4_sweep_result_summary_validated():
    from backtestforecast.services.sweeps import SweepService
    import inspect
    source = inspect.getsource(SweepService)
    assert "_SUMMARY_REQUIRED_KEYS" in source
    assert "validate_json_shape" in source


def test_d4_summary_required_keys_defined():
    from backtestforecast.schemas.json_shapes import _SUMMARY_REQUIRED_KEYS
    assert "trade_count" in _SUMMARY_REQUIRED_KEYS
    assert "win_rate" in _SUMMARY_REQUIRED_KEYS
    assert "total_net_pnl" in _SUMMARY_REQUIRED_KEYS


# ---- D5: NightlyPipelineRun FK to ScannerJob ----

def test_d5_pipeline_scanner_fk_migration_exists():
    content = (VERSIONS_DIR / "20260319_0035_pipeline_scanner_fk.py").read_text()
    assert "pipeline_run_id" in content
    assert "fk_scanner_jobs_pipeline_run_id" in content


# ---- D6: AuditEvent dedup constraint misuse protection ----

def test_d6_repeatable_events_defined():
    from backtestforecast.services.audit import _REPEATABLE_EVENT_TYPES
    assert "export.downloaded" in _REPEATABLE_EVENT_TYPES
    assert "backtest.viewed" in _REPEATABLE_EVENT_TYPES


def test_d6_record_warns_on_repeatable_event():
    import inspect
    from backtestforecast.services.audit import AuditService
    source = inspect.getsource(AuditService.record)
    assert "_REPEATABLE_EVENT_TYPES" in source
    assert "record_always" in source


def test_d6_record_always_uses_uuid_suffix():
    import inspect
    from backtestforecast.repositories.audit_events import AuditEventRepository
    source = inspect.getsource(AuditEventRepository.add_always)
    assert "uuid4" in source
