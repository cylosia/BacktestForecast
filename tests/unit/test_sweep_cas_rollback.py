"""Fix 20: Sweep CAS rollback prevents orphaned results.

Same pattern as Fix 19 (scan) but for SweepJob / SweepResult.

When a sweep job is concurrently cancelled/failed by the reaper while the
sweep service is writing SweepResult rows, the CAS UPDATE returns
rowcount == 0.  The session must be rolled back so the results are NOT
committed to a job in a terminal state.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import SweepJob, SweepResult, User
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="sweep_cas_rollback_user", email="sweep_rb@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_sweep_job(session: Session, user: User, *, status: str = "running") -> SweepJob:
    job = SweepJob(
        user_id=user.id,
        symbol="AAPL",
        status=status,
        plan_tier_snapshot="pro",
        candidate_count=50,
        request_snapshot_json={},
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


class TestSweepCASRollback:
    """Verify that SweepResult rows are rolled back when the CAS UPDATE
    on SweepJob.status returns rowcount==0."""

    def test_results_not_committed_when_cas_fails(self, db_session: Session):
        """Reaper sets job to 'failed' via second session -> CAS returns 0 -> results rolled back."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="running")

        second_session = sessionmaker(bind=db_session.get_bind(), autoflush=False, expire_on_commit=False)()
        try:
            second_session.execute(
                update(SweepJob)
                .where(SweepJob.id == job.id)
                .values(status="failed", error_code="stale_running")
            )
            second_session.commit()
        finally:
            second_session.close()

        result = SweepResult(
            sweep_job_id=job.id,
            rank=1,
            score=Decimal("92.5"),
            strategy_type="covered_call",
            parameter_snapshot_json={"target_dte": 30},
            summary_json={"trade_count": 10},
            warnings_json=[],
            trades_json=[],
            equity_curve_json=[],
        )
        db_session.add(result)

        success_rows = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "running")
            .values(
                status="succeeded",
                result_count=1,
                completed_at=datetime.now(UTC),
            )
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 0

        db_session.expire_all()
        refreshed_job = db_session.get(SweepJob, job.id)
        assert refreshed_job.status == "failed"

        from sqlalchemy import func, select
        result_count = db_session.scalar(
            select(func.count()).select_from(SweepResult)
            .where(SweepResult.sweep_job_id == job.id)
        )
        assert result_count == 0, (
            "SweepResult rows must be rolled back when CAS fails - "
            "they should NOT be attached to a failed/cancelled job"
        )

    def test_results_committed_when_cas_succeeds(self, db_session: Session):
        """When CAS succeeds (job still running), results are persisted normally."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="running")

        result = SweepResult(
            sweep_job_id=job.id,
            rank=1,
            score=Decimal("88.0"),
            strategy_type="iron_condor",
            parameter_snapshot_json={"target_dte": 45},
            summary_json={"trade_count": 20},
            warnings_json=[],
            trades_json=[],
            equity_curve_json=[],
        )
        db_session.add(result)

        success_rows = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "running")
            .values(
                status="succeeded",
                result_count=1,
                completed_at=datetime.now(UTC),
            )
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 1

        db_session.expire_all()
        assert db_session.get(SweepJob, job.id).status == "succeeded"

        from sqlalchemy import func, select
        result_count = db_session.scalar(
            select(func.count()).select_from(SweepResult)
            .where(SweepResult.sweep_job_id == job.id)
        )
        assert result_count == 1

    def test_multiple_results_all_rolled_back(self, db_session: Session):
        """All pending SweepResult rows - not just the last - must be rolled back."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="running")

        second_session = sessionmaker(bind=db_session.get_bind(), autoflush=False, expire_on_commit=False)()
        try:
            second_session.execute(
                update(SweepJob)
                .where(SweepJob.id == job.id)
                .values(status="cancelled", error_code="subscription_revoked")
            )
            second_session.commit()
        finally:
            second_session.close()

        for rank in range(1, 6):
            db_session.add(SweepResult(
                sweep_job_id=job.id,
                rank=rank,
                score=Decimal(str(100 - rank)),
                strategy_type="long_call",
                parameter_snapshot_json={"rank": rank},
                summary_json={},
                warnings_json=[],
                trades_json=[],
                equity_curve_json=[],
            ))

        success_rows = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "running")
            .values(status="succeeded", result_count=5, completed_at=datetime.now(UTC))
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 0

        from sqlalchemy import func, select
        result_count = db_session.scalar(
            select(func.count()).select_from(SweepResult)
            .where(SweepResult.sweep_job_id == job.id)
        )
        assert result_count == 0, "All 5 pending SweepResult rows must be rolled back"

    def test_genetic_mode_results_also_rolled_back(self, db_session: Session):
        """Genetic mode follows the same CAS pattern as grid mode."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="running")
        job.mode = "genetic"
        db_session.commit()

        second_session = sessionmaker(bind=db_session.get_bind(), autoflush=False, expire_on_commit=False)()
        try:
            second_session.execute(
                update(SweepJob)
                .where(SweepJob.id == job.id)
                .values(status="failed", error_code="time_limit_exceeded")
            )
            second_session.commit()
        finally:
            second_session.close()

        db_session.add(SweepResult(
            sweep_job_id=job.id,
            rank=1,
            score=Decimal("95.0"),
            strategy_type="custom_2_leg",
            parameter_snapshot_json={"generations": 20},
            summary_json={"trade_count": 15},
            warnings_json=[],
            trades_json=[],
            equity_curve_json=[],
        ))

        success_rows = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "running")
            .values(status="succeeded", result_count=1, completed_at=datetime.now(UTC))
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 0

        from sqlalchemy import func, select
        assert db_session.scalar(
            select(func.count()).select_from(SweepResult)
            .where(SweepResult.sweep_job_id == job.id)
        ) == 0
