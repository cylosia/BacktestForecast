from __future__ import annotations

from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, noload, selectinload

from backtestforecast.models import SweepJob

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
        )
        return self.session.scalar(stmt)
