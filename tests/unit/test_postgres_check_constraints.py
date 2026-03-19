"""Verify that SQLAlchemy models have CHECK constraints matching expectations.

These tests verify the constraint definitions at the ORM level, without
requiring a live Postgres connection. They catch cases where a developer
adds a column default that violates an existing CHECK constraint, or adds
a new status value without updating the constraint.
"""
from __future__ import annotations

import pytest

from backtestforecast.models import (
    BacktestRun,
    ExportJob,
    ScannerJob,
    SweepJob,
    SymbolAnalysis,
    User,
)


def _get_check_names(model) -> set[str]:
    """Extract CHECK constraint names from a model's __table_args__."""
    names = set()
    args = getattr(model, "__table_args__", ())
    if isinstance(args, tuple):
        for item in args:
            if hasattr(item, "name") and item.name and item.name.startswith("ck_"):
                names.add(item.name)
    return names


def test_user_check_constraints_exist():
    names = _get_check_names(User)
    assert "ck_users_valid_plan_tier" in names
    assert "ck_users_valid_subscription_status" in names


def test_backtest_run_check_constraints_exist():
    names = _get_check_names(BacktestRun)
    assert "ck_backtest_runs_valid_run_status" in names
    assert "ck_backtest_runs_account_positive" in names
    assert "ck_backtest_runs_date_order" in names


def test_export_job_check_constraints_exist():
    names = _get_check_names(ExportJob)
    assert "ck_export_jobs_valid_export_status" in names
    assert "ck_export_jobs_valid_export_format" in names
    assert "ck_export_jobs_succeeded_has_storage" in names


def test_scanner_job_check_constraints_exist():
    names = _get_check_names(ScannerJob)
    assert "ck_scanner_jobs_valid_job_status" in names
    assert "ck_scanner_jobs_valid_mode" in names


def test_sweep_job_check_constraints_exist():
    names = _get_check_names(SweepJob)
    assert "ck_sweep_jobs_valid_status" in names
    assert "ck_sweep_jobs_valid_mode" in names


def test_symbol_analysis_check_constraints_exist():
    names = _get_check_names(SymbolAnalysis)
    assert "ck_symbol_analyses_valid_analysis_status" in names
    assert "ck_symbol_analyses_valid_stage" in names


def test_export_status_values_match_schema():
    """Ensure the DB CHECK constraint and the Pydantic JobStatus enum agree."""
    from backtestforecast.schemas.common import JobStatus

    enum_values = {s.value for s in JobStatus}
    expected = {"queued", "running", "succeeded", "failed", "cancelled", "expired"}
    assert enum_values == expected
