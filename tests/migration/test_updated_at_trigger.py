"""Verify that updated_at DB trigger fires on job tables.

These tests require a live PostgreSQL database and are skipped when DATABASE_URL
is not set. They validate that the set_updated_at trigger function correctly
updates the updated_at column on row modification.
"""
from __future__ import annotations

import os
import time

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.skipif(
    "DATABASE_URL" not in os.environ,
    reason="requires PostgreSQL",
)


def test_updated_at_trigger_fires_on_backtest_runs(db_session):
    """Insert a backtest_runs row, update it via raw SQL, and verify updated_at changed."""
    from backtestforecast.models import BacktestRun

    run = BacktestRun(
        user_id="test-trigger-user",
        symbol="AAPL",
        strategy_type="long_call",
        status="pending",
        date_from="2024-01-01",
        date_to="2024-06-01",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=1,
        engine_version="options-multileg-v2",
        data_source="massive",
    )
    db_session.add(run)
    db_session.flush()

    original_updated_at = run.updated_at
    assert original_updated_at is not None

    time.sleep(0.05)

    db_session.execute(
        text("UPDATE backtest_runs SET status = 'running' WHERE id = :id"),
        {"id": str(run.id)},
    )
    db_session.expire(run)

    assert run.updated_at > original_updated_at, (
        "updated_at trigger did not fire: value unchanged after UPDATE"
    )


def test_updated_at_trigger_fires_on_scanner_jobs(db_session):
    """Insert a scanner_jobs row, update it, and verify updated_at changed."""
    from backtestforecast.models import ScannerJob

    job = ScannerJob(
        user_id="test-trigger-user",
        status="pending",
        symbols=["AAPL", "MSFT"],
        strategy_type="long_call",
        date_from="2024-01-01",
        date_to="2024-06-01",
    )
    db_session.add(job)
    db_session.flush()

    original_updated_at = job.updated_at
    assert original_updated_at is not None

    time.sleep(0.05)

    db_session.execute(
        text("UPDATE scanner_jobs SET status = 'running' WHERE id = :id"),
        {"id": str(job.id)},
    )
    db_session.expire(job)

    assert job.updated_at > original_updated_at, (
        "updated_at trigger did not fire: value unchanged after UPDATE"
    )
