"""Verify that updated_at DB trigger fires on job tables.

These tests require a live PostgreSQL database and are skipped when DATABASE_URL
is not set. They validate that the set_updated_at trigger function correctly
updates the updated_at column on row modification.
"""
from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.skipif(
    "TEST_DATABASE_URL" not in os.environ,
    reason="requires isolated PostgreSQL test database",
)


def test_updated_at_trigger_fires_on_backtest_runs(db_session):
    """Insert a backtest_runs row, update it via raw SQL, and verify updated_at changed."""
    from backtestforecast.models import BacktestRun, User

    user = User(
        clerk_user_id=f"clerk_trigger_{uuid4()}",
        email=f"trigger-{uuid4()}@example.com",
        plan_tier="free",
        subscription_status=None,
    )
    db_session.add(user)
    db_session.flush()

    run = BacktestRun(
        user_id=user.id,
        symbol="AAPL",
        strategy_type="long_call",
        status="queued",
        date_from=datetime(2024, 1, 1, tzinfo=UTC).date(),
        date_to=datetime(2024, 6, 1, tzinfo=UTC).date(),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        engine_version="options-multileg-v2",
        data_source="massive",
        input_snapshot_json={},
        warnings_json=[],
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
    from backtestforecast.models import ScannerJob, User

    user = User(
        clerk_user_id=f"clerk_trigger_{uuid4()}",
        email=f"trigger-{uuid4()}@example.com",
        plan_tier="free",
        subscription_status=None,
    )
    db_session.add(user)
    db_session.flush()

    job = ScannerJob(
        user_id=user.id,
        status="queued",
        mode="basic",
        plan_tier_snapshot="free",
        request_hash=f"migration-trigger-{uuid4().hex}",
        request_snapshot_json={
            "symbols": ["AAPL", "MSFT"],
            "strategy_type": "long_call",
            "date_from": str(datetime(2024, 1, 1, tzinfo=UTC).date()),
            "date_to": str(datetime(2024, 6, 1, tzinfo=UTC).date()),
        },
        warnings_json=[],
        candidate_count=2,
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
