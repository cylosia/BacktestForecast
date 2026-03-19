from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, noload, selectinload

from backtestforecast.models import SweepJob, SweepResult

_MAX_PAGE_SIZE = 200


class SweepJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: SweepJob) -> SweepJob:
        self.session.add(job)
        self.session.flush()
        return job

    def list_for_user(self, user_id: UUID, limit: int = 50, offset: int = 0) -> list[SweepJob]:
        stmt = (
            select(SweepJob)
            .where(SweepJob.user_id == user_id)
            .options(noload(SweepJob.results))
            .order_by(desc(SweepJob.created_at))
            .offset(offset)
            .limit(min(limit, _MAX_PAGE_SIZE))
        )
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        stmt = select(func.count(SweepJob.id)).where(SweepJob.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)

    def count_for_user_created_between(
        self,
        user_id: UUID,
        *,
        start_inclusive: datetime,
        end_exclusive: datetime,
        exclude_id: UUID | None = None,
    ) -> int:
        stmt = select(func.count(SweepJob.id)).where(
            SweepJob.user_id == user_id,
            SweepJob.created_at >= start_inclusive,
            SweepJob.created_at < end_exclusive,
        )
        if exclude_id is not None:
            stmt = stmt.where(SweepJob.id != exclude_id)
        return int(self.session.scalar(stmt) or 0)

    def get_for_user(
        self,
        job_id: UUID,
        user_id: UUID,
        include_results: bool = False,
    ) -> SweepJob | None:
        stmt = select(SweepJob).where(SweepJob.id == job_id, SweepJob.user_id == user_id)
        if include_results:
            stmt = stmt.options(selectinload(SweepJob.results))
        return self.session.scalar(stmt)

    def get(self, job_id: UUID, include_results: bool = False, for_update: bool = False) -> SweepJob | None:
        stmt = select(SweepJob).where(SweepJob.id == job_id)
        if include_results:
            stmt = stmt.options(selectinload(SweepJob.results))
        if for_update:
            stmt = stmt.with_for_update()
        return self.session.scalar(stmt)

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> SweepJob | None:
        stmt = select(SweepJob).where(
            SweepJob.user_id == user_id,
            SweepJob.idempotency_key == idempotency_key,
            SweepJob.status.notin_(["failed", "cancelled"]),
        )
        return self.session.scalar(stmt)

    def count_results(self, job_id: UUID, *, user_id: UUID | None = None) -> int:
        stmt = select(func.count(SweepResult.id)).where(SweepResult.sweep_job_id == job_id)
        if user_id is not None:
            stmt = stmt.join(SweepJob, SweepResult.sweep_job_id == SweepJob.id).where(
                SweepJob.user_id == user_id,
            )
        return int(self.session.scalar(stmt) or 0)

    def list_results(
        self, job_id: UUID, *, limit: int = 100, offset: int = 0, user_id: UUID | None = None,
    ) -> list[SweepResult]:
        limit = min(limit, _MAX_PAGE_SIZE)
        stmt = (
            select(SweepResult)
            .where(SweepResult.sweep_job_id == job_id)
        )
        if user_id is not None:
            stmt = stmt.join(SweepJob, SweepResult.sweep_job_id == SweepJob.id).where(
                SweepJob.user_id == user_id,
            )
        stmt = stmt.order_by(SweepResult.rank).offset(offset).limit(limit)
        return list(self.session.scalars(stmt))

    def find_recent_duplicate(
        self,
        user_id: UUID,
        symbol: str,
        request_snapshot: dict,
        since: datetime,
    ) -> SweepJob | None:
        """Find a recently created sweep with matching parameters."""
        stmt = (
            select(SweepJob)
            .where(
                SweepJob.user_id == user_id,
                SweepJob.symbol == symbol,
                SweepJob.created_at >= since,
                SweepJob.status.in_(["queued", "running"]),
            )
            .order_by(desc(SweepJob.created_at))
            .limit(5)
        )
        for job in self.session.scalars(stmt):
            if job.request_snapshot_json == request_snapshot:
                return job
        return None

    def delete_results(self, job_id: UUID, *, user_id: UUID | None = None) -> None:
        """Remove all results for a sweep job (for re-run cleanup).

        When *user_id* is provided, the method first verifies that the job
        belongs to the given user.  Worker code may omit *user_id* because
        the task already validated ownership at dispatch time.
        """
        from sqlalchemy import delete as sa_delete, exists as sa_exists

        if user_id is not None:
            owns = self.session.scalar(
                select(sa_exists().where(SweepJob.id == job_id, SweepJob.user_id == user_id))
            )
            if not owns:
                return

        self.session.execute(
            sa_delete(SweepResult)
            .where(SweepResult.sweep_job_id == job_id)
            .execution_options(synchronize_session="fetch")
        )
