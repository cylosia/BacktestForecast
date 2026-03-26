"""Verify workflow jobs are cancelled when a user's subscription is revoked."""
from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest

from backtestforecast.models import BacktestRun, MultiStepRun, MultiSymbolRun, SweepJob, User
from backtestforecast.services.billing import BillingService


@pytest.fixture()
def billing_service(db_session):
    return BillingService(db_session)


def _make_user(db_session, **overrides):
    from uuid import uuid4

    defaults = dict(
        clerk_user_id=f"clerk_{uuid4().hex[:8]}",
        plan_tier="pro",
        subscription_status="active",
    )
    defaults.update(overrides)
    user = User(**defaults)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def test_cancel_in_flight_includes_sweep_jobs(db_session, billing_service):
    """SweepJob must be cancelled alongside other job types."""
    user = _make_user(db_session)
    sweep = SweepJob(
        user_id=user.id,
        symbol="SPY",
        status="running",
        request_snapshot_json={},
        started_at=datetime.now(UTC),
    )
    db_session.add(sweep)
    db_session.commit()

    with patch("backtestforecast.events.publish_job_status"):
        billing_service.cancel_in_flight_jobs(user.id)
    db_session.commit()

    db_session.refresh(sweep)
    assert sweep.status == "cancelled"
    assert sweep.completed_at is not None
    assert sweep.error_code == "subscription_revoked"


def test_cancel_in_flight_sets_completed_at(db_session, billing_service):
    """All cancelled jobs should have completed_at set."""
    user = _make_user(db_session)
    bt = BacktestRun(
        user_id=user.id,
        symbol="AAPL",
        strategy_type="long_call",
        status="queued",
        date_from=datetime(2025, 1, 1).date(),
        date_to=datetime(2025, 6, 1).date(),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=2,
        commission_per_contract=1,
        input_snapshot_json={},
    )
    db_session.add(bt)
    db_session.commit()

    with patch("backtestforecast.events.publish_job_status"):
        billing_service.cancel_in_flight_jobs(user.id)
    db_session.commit()

    db_session.refresh(bt)
    assert bt.status == "cancelled"
    assert bt.completed_at is not None
    assert bt.error_code == "subscription_revoked"


def test_cancel_in_flight_includes_multi_workflow_runs(db_session, billing_service):
    """Multi-workflow runs must be cancelled alongside the legacy jobs."""
    user = _make_user(db_session)
    multi_symbol = MultiSymbolRun(
        user_id=user.id,
        name="Portfolio run",
        status="queued",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 3, 1),
        account_size=10000,
        capital_allocation_mode="equal_weight",
        commission_per_contract=0.65,
        slippage_pct=0,
        input_snapshot_json={},
        warnings_json=[],
        starting_equity=10000,
        ending_equity=10000,
    )
    multi_step = MultiStepRun(
        user_id=user.id,
        name="Step run",
        symbol="SPY",
        workflow_type="sequential",
        status="running",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 3, 1),
        account_size=10000,
        risk_per_trade_pct=2,
        commission_per_contract=0.65,
        slippage_pct=0,
        input_snapshot_json={},
        warnings_json=[],
        starting_equity=10000,
        ending_equity=10000,
        started_at=datetime.now(UTC),
    )
    db_session.add_all([multi_symbol, multi_step])
    db_session.commit()

    with patch("backtestforecast.events.publish_job_status"):
        cancelled = billing_service.cancel_in_flight_jobs(user.id)
    db_session.commit()

    db_session.refresh(multi_symbol)
    db_session.refresh(multi_step)

    assert multi_symbol.status == "cancelled"
    assert multi_symbol.completed_at is not None
    assert multi_symbol.error_code == "subscription_revoked"

    assert multi_step.status == "cancelled"
    assert multi_step.completed_at is not None
    assert multi_step.error_code == "subscription_revoked"

    cancelled_types = {job_type for job_type, _ in cancelled}
    assert "multi_symbol_backtest" in cancelled_types
    assert "multi_step_backtest" in cancelled_types
