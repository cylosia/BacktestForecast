"""Test the billing webhook -> cancellation -> SSE publish flow end-to-end."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, SweepJob, User
from backtestforecast.services.billing import BillingService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    s = factory()
    try:
        yield s
    finally:
        s.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def _make_user(session: Session, **overrides) -> User:
    defaults = dict(
        clerk_user_id=f"clerk_{uuid4().hex[:8]}",
        plan_tier="pro",
        subscription_status="active",
    )
    defaults.update(overrides)
    user = User(**defaults)
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


class TestBillingCancellationFlow:
    def test_cancel_in_flight_cancels_backtest_and_sweep(self, session: Session):
        user = _make_user(session)

        bt = BacktestRun(
            user_id=user.id,
            symbol="AAPL",
            strategy_type="long_call",
            status="running",
            date_from=datetime(2025, 1, 1).date(),
            date_to=datetime(2025, 6, 1).date(),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=1,
            input_snapshot_json={},
            started_at=datetime.now(UTC),
        )
        sweep = SweepJob(
            user_id=user.id,
            symbol="SPY",
            status="queued",
            request_snapshot_json={},
        )
        session.add_all([bt, sweep])
        session.commit()

        svc = BillingService(session)
        cancelled_job_ids = svc.cancel_in_flight_jobs(user.id)
        session.commit()

        session.refresh(bt)
        session.refresh(sweep)

        assert bt.status == "cancelled"
        assert bt.error_code == "subscription_revoked"
        assert bt.completed_at is not None

        assert sweep.status == "cancelled"
        assert sweep.error_code == "subscription_revoked"
        assert sweep.completed_at is not None

        cancelled_types = {job_type for job_type, _ in cancelled_job_ids}
        assert "backtest" in cancelled_types
        assert "sweep" in cancelled_types
        assert len(cancelled_job_ids) == 2

    def test_sse_event_published_for_each_cancelled_job(self, session: Session):
        user = _make_user(session)

        bt1 = BacktestRun(
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
        bt2 = BacktestRun(
            user_id=user.id,
            symbol="MSFT",
            strategy_type="long_call",
            status="running",
            date_from=datetime(2025, 1, 1).date(),
            date_to=datetime(2025, 6, 1).date(),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=1,
            input_snapshot_json={},
            started_at=datetime.now(UTC),
        )
        sweep = SweepJob(
            user_id=user.id,
            symbol="TSLA",
            status="running",
            request_snapshot_json={},
            started_at=datetime.now(UTC),
        )
        session.add_all([bt1, bt2, sweep])
        session.commit()

        publish_calls: list[tuple] = []

        def capture(job_type, job_id, status, **kwargs):
            publish_calls.append((job_type, job_id, status))

        svc = BillingService(session)
        cancelled_job_ids = svc.cancel_in_flight_jobs(user.id)
        session.commit()
        with patch("backtestforecast.events.publish_job_status", side_effect=capture):
            BillingService.publish_cancellation_events(cancelled_job_ids)

        assert len(publish_calls) == 3
        assert all(call[2] == "cancelled" for call in publish_calls)

    def test_already_completed_jobs_not_cancelled(self, session: Session):
        user = _make_user(session)

        bt = BacktestRun(
            user_id=user.id,
            symbol="AAPL",
            strategy_type="long_call",
            status="succeeded",
            date_from=datetime(2025, 1, 1).date(),
            date_to=datetime(2025, 6, 1).date(),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=1,
            input_snapshot_json={},
            completed_at=datetime.now(UTC),
        )
        session.add(bt)
        session.commit()

        svc = BillingService(session)
        svc.cancel_in_flight_jobs(user.id)
        session.commit()

        session.refresh(bt)
        assert bt.status == "succeeded"
