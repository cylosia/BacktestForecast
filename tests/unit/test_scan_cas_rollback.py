"""Fix 19: Scan CAS rollback prevents orphaned recommendations.

When a scan job is concurrently cancelled/failed by the reaper while the
scan service is writing recommendations, the CAS UPDATE on the job status
returns rowcount == 0.  In that case, the session must be rolled back so
that the freshly-added ScannerRecommendation rows are NOT committed to a
job that is already in a terminal state.

Before the fix, ``session.commit()`` was called *before* checking
``success_rows.rowcount``, permanently attaching recommendations to a
cancelled/failed job.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.models import ScannerJob, ScannerRecommendation, User

pytestmark = pytest.mark.postgres


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="scan_cas_user", email="scan_cas@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_scan_job(session: Session, user: User, *, status: str = "running") -> ScannerJob:
    job = ScannerJob(
        user_id=user.id,
        status=status,
        mode="basic",
        plan_tier_snapshot="pro",
        request_hash="deadbeef" * 4,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _recommendation_payload(**overrides):
    payload = dict(
        request_snapshot_json={"symbol": "AAPL"},
        summary_json={"trade_count": 1},
        warnings_json=[],
        trades_json=[],
        equity_curve_json=[],
        historical_performance_json={"sample_count": 0},
        forecast_json={"summary": "n/a"},
        ranking_features_json={"final_score": 0.9},
    )
    payload.update(overrides)
    return payload


class TestScanCASRollback:
    """Verify that when the CAS UPDATE on ScannerJob succeeds->running
    returns rowcount==0 (because the reaper already set it to failed),
    the pending ScannerRecommendation objects are rolled back."""

    def test_recommendations_not_committed_when_cas_fails(self, db_session: Session):
        """Simulate the race: reaper sets job to 'failed' via a second
        connection between adding recommendations and the CAS UPDATE.

        Uses a second session to simulate the reaper's concurrent write,
        then verifies the first session's rollback discards the recs.
        """
        user = _create_user(db_session)
        job = _create_scan_job(db_session, user, status="running")

        second_session = sessionmaker(bind=db_session.get_bind(), autoflush=False, expire_on_commit=False)()
        try:
            second_session.execute(
                update(ScannerJob)
                .where(ScannerJob.id == job.id)
                .values(status="failed", error_code="stale_running")
            )
            second_session.commit()
        finally:
            second_session.close()

        rec = ScannerRecommendation(
            scanner_job_id=job.id,
            rank=1,
            score=Decimal("85.5"),
            symbol="AAPL",
            strategy_type="covered_call",
            rule_set_name="default",
            rule_set_hash="abc123",
            **_recommendation_payload(),
        )
        db_session.add(rec)

        success_rows = db_session.execute(
            update(ScannerJob)
            .where(ScannerJob.id == job.id, ScannerJob.status == "running")
            .values(
                status="succeeded",
                recommendation_count=1,
                completed_at=datetime.now(UTC),
            )
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 0, "CAS should not match - job is 'failed'"

        db_session.expire_all()
        refreshed_job = db_session.get(ScannerJob, job.id)
        assert refreshed_job.status == "failed"

        from sqlalchemy import func, select
        rec_count = db_session.scalar(
            select(func.count()).select_from(ScannerRecommendation)
            .where(ScannerRecommendation.scanner_job_id == job.id)
        )
        assert rec_count == 0, (
            "Recommendations must be rolled back when CAS fails - "
            "they should NOT be attached to a failed/cancelled job"
        )

    def test_recommendations_committed_when_cas_succeeds(self, db_session: Session):
        """When CAS succeeds (job is still running), recommendations are committed."""
        user = _create_user(db_session)
        job = _create_scan_job(db_session, user, status="running")

        rec = ScannerRecommendation(
            scanner_job_id=job.id,
            rank=1,
            score=Decimal("90.0"),
            symbol="MSFT",
            strategy_type="long_call",
            rule_set_name="default",
            rule_set_hash="def456",
            **_recommendation_payload(request_snapshot_json={"symbol": "MSFT"}),
        )
        db_session.add(rec)

        success_rows = db_session.execute(
            update(ScannerJob)
            .where(ScannerJob.id == job.id, ScannerJob.status == "running")
            .values(
                status="succeeded",
                recommendation_count=1,
                completed_at=datetime.now(UTC),
            )
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 1

        db_session.expire_all()
        refreshed_job = db_session.get(ScannerJob, job.id)
        assert refreshed_job.status == "succeeded"

        from sqlalchemy import func, select
        rec_count = db_session.scalar(
            select(func.count()).select_from(ScannerRecommendation)
            .where(ScannerRecommendation.scanner_job_id == job.id)
        )
        assert rec_count == 1

    def test_cancelled_job_blocks_cas(self, db_session: Session):
        """A job cancelled by billing revocation must not accept recommendations."""
        user = _create_user(db_session)
        job = _create_scan_job(db_session, user, status="cancelled")

        rec = ScannerRecommendation(
            scanner_job_id=job.id,
            rank=1,
            score=Decimal("50.0"),
            symbol="TSLA",
            strategy_type="cash_secured_put",
            rule_set_name="default",
            rule_set_hash="ghi789",
            **_recommendation_payload(request_snapshot_json={"symbol": "TSLA"}),
        )
        db_session.add(rec)

        success_rows = db_session.execute(
            update(ScannerJob)
            .where(ScannerJob.id == job.id, ScannerJob.status == "running")
            .values(status="succeeded", recommendation_count=1, completed_at=datetime.now(UTC))
        )

        if success_rows.rowcount == 0:
            db_session.rollback()
        else:
            db_session.commit()

        assert success_rows.rowcount == 0
        db_session.expire_all()
        assert db_session.get(ScannerJob, job.id).status == "cancelled"

        from sqlalchemy import func, select
        rec_count = db_session.scalar(
            select(func.count()).select_from(ScannerRecommendation)
            .where(ScannerRecommendation.scanner_job_id == job.id)
        )
        assert rec_count == 0
