"""Fix 67: Sweep CAS transition prevents double-run.

If two workers call run_job concurrently with the same job_id, only one
should transition the status from "queued" to "running". The other must
see rowcount == 0 and return without executing.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

from backtestforecast.models import SweepJob, User

pytestmark = pytest.mark.postgres


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
    def test_first_cas_succeeds(self, postgres_db_session: Session):
        """The first queued->running CAS transition should match exactly one row."""
        user = _create_user(postgres_db_session)
        job = _create_sweep_job(postgres_db_session, user, status="queued")

        result = postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()

        assert result.rowcount == 1

    def test_second_cas_fails(self, postgres_db_session: Session):
        """After the first CAS transition, a second attempt must match zero rows."""
        user = _create_user(postgres_db_session)
        job = _create_sweep_job(postgres_db_session, user, status="queued")

        first = postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()
        assert first.rowcount == 1

        second = postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()
        assert second.rowcount == 0, "Second CAS must not match - already running"

    def test_status_remains_running_after_double_cas(self, postgres_db_session: Session):
        """After two CAS attempts, the job must be 'running' (not duplicated)."""
        user = _create_user(postgres_db_session)
        job = _create_sweep_job(postgres_db_session, user, status="queued")

        postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()

        postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()

        postgres_db_session.expire_all()
        refreshed = postgres_db_session.get(SweepJob, job.id)
        assert refreshed.status == "running"

    def test_cas_does_not_match_terminal_status(self, postgres_db_session: Session):
        """CAS must not transition a job that has already reached a terminal state."""
        user = _create_user(postgres_db_session)
        job = _create_sweep_job(postgres_db_session, user, status="failed")

        result = postgres_db_session.execute(
            update(SweepJob)
            .where(SweepJob.id == job.id, SweepJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        postgres_db_session.commit()

        assert result.rowcount == 0
        postgres_db_session.expire_all()
        refreshed = postgres_db_session.get(SweepJob, job.id)
        assert refreshed.status == "failed"
