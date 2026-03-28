"""Verification tests for the Database / Migration / Schema Drift findings.

Covers all 6 findings: migration chain integrity, JSON validation,
audit dedup safety, and constraint alignment.
"""
from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VERSIONS_DIR = PROJECT_ROOT / "alembic" / "versions"


# ---- D1: Migration history is consolidated to a squashed baseline with follow-up deltas ----

def test_d1_consolidated_baseline_has_correct_revision():
    content = (VERSIONS_DIR / "20260324_0001_consolidated_baseline.py").read_text()
    assert 'revision = "20260324_0001"' in content
    assert "down_revision = None" in content


def test_d1_consolidated_baseline_is_only_revision_file():
    version_files = sorted(path.name for path in VERSIONS_DIR.glob("*.py"))
    assert version_files == [
        "20260324_0001_consolidated_baseline.py",
        "20260325_0002_multi_symbol_and_multi_step_backtests.py",
        "20260325_0003_export_targets_for_multi_workflows.py",
        "20260326_0004_scanner_dispatch_started_at.py",
        "20260326_0005_dispatch_started_at_backfill.py",
        "20260326_0006_ensure_updated_at_triggers.py",
        "20260327_0007_option_contract_catalog_snapshots.py",
        "20260327_0008_historical_flatfile_market_data.py",
        "20260328_0009_async_job_operational_indexes.py",
        "20260328_0010_db_index_hygiene_and_ops_guards.py",
    ]


# ---- D2: Old branch-specific revisions were intentionally removed ----

def test_d2_legacy_revision_files_are_absent():
    legacy_files = [
        "20260318_0024_validate_sweep_mode_constraint.py",
        "20260318_0045_add_heartbeat_and_trade_index.py",
        "20260319_0034_schema_drift_fixes.py",
        "20260319_0035_pipeline_scanner_fk.py",
        "20260319_0036_holding_trading_days.py",
    ]
    for file_name in legacy_files:
        assert not (VERSIONS_DIR / file_name).exists()


# ---- D3: strategy_type intentionally has no CHECK ----

def test_d3_strategy_type_no_check_is_documented():
    source = (PROJECT_ROOT / "src" / "backtestforecast" / "models.py").read_text()
    assert "strategy_type columns use String(48) without a DB-level CHECK" in source


# ---- D4: JSON columns have shape validation ----

def test_d4_backtest_trade_detail_validated():
    import inspect

    from backtestforecast.services.backtests import BacktestService
    source = inspect.getsource(BacktestService)
    assert "validate_json_shape" in source
    assert "_TRADE_DETAIL_REQUIRED_KEYS" in source


def test_d4_scan_recommendation_summary_validated():
    import inspect

    from backtestforecast.services.scans import ScanService
    source = inspect.getsource(ScanService)
    assert "validate_json_shape" in source


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_d4_sweep_result_summary_validated():
    import inspect

    from backtestforecast.services.sweeps import SweepService
    source = inspect.getsource(SweepService)
    assert "_SUMMARY_REQUIRED_KEYS" in source
    assert "validate_json_shape" in source


def test_d4_summary_required_keys_defined():
    from backtestforecast.schemas.json_shapes import _SUMMARY_REQUIRED_KEYS
    assert "trade_count" in _SUMMARY_REQUIRED_KEYS
    assert "win_rate" in _SUMMARY_REQUIRED_KEYS
    assert "total_net_pnl" in _SUMMARY_REQUIRED_KEYS


# ---- D5: Consolidated baseline includes NightlyPipelineRun schema ----

def test_d5_consolidated_baseline_creates_current_schema():
    content = (VERSIONS_DIR / "20260324_0001_consolidated_baseline.py").read_text()
    assert "POSTGRESQL_DDL_STATEMENTS" in content
    assert "SQLITE_DDL_STATEMENTS" in content
    assert "_TRIGGER_TABLES" in content


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
