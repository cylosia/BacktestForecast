"""Verify SweepJob is cancelled when a user's subscription is revoked."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backtestforecast.models import BacktestRun, SweepJob, User
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
