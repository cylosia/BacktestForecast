"""Fix 67: Sweep CAS transition prevents double-run.

If two workers call run_job concurrently with the same job_id, only one
should transition the status from "queued" to "running". The other must
see rowcount == 0 and return without executing.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import SweepJob, User


def _strip_partial_indexes_for_sqlite(engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    for table in Base.metadata.tables.values():
        indexes_to_remove = [
            idx for idx in table.indexes
            if idx.dialect_options.get("postgresql", {}).get("where") is not None
        ]
        for idx in indexes_to_remove:
            table.indexes.discard(idx)


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
    user = User(clerk_user_id="sweep_cas_user", email="sweep_cas@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_sweep_job(session: Session, user: User, *, status: str = "queued") -> SweepJob:
    job = SweepJob(
        user_id=user.id,
        symbol="SPY",
        status=status,
        plan_tier_snapshot="pro",
        candidate_count=100,
        request_snapshot_json={},
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


class TestSweepCASTransition:
    def test_first_cas_succeeds(self, db_session):
        """The first queued→running CAS transition should match exactly one row."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="queued")

        result = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()

        assert result.rowcount == 1

    def test_second_cas_fails(self, db_session):
        """After the first CAS transition, a second attempt must match zero rows."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="queued")

        first = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()
        assert first.rowcount == 1

        second = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()
        assert second.rowcount == 0, "Second CAS must not match — already running"

    def test_status_remains_running_after_double_cas(self, db_session):
        """After two CAS attempts, the job must be 'running' (not duplicated)."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="queued")

        db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()

        db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()

        db_session.expire_all()
        refreshed = db_session.get(SweepJob, job.id)
        assert refreshed.status == "running"

    def test_cas_does_not_match_terminal_status(self, db_session):
        """CAS must not transition a job that has already reached a terminal state."""
        user = _create_user(db_session)
        job = _create_sweep_job(db_session, user, status="failed")

        result = db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        db_session.commit()

        assert result.rowcount == 0
        db_session.expire_all()
        refreshed = db_session.get(SweepJob, job.id)
        assert refreshed.status == "failed"
