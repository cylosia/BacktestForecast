from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session, defer

from backtestforecast.models import ExportJob

_MAX_PAGE_SIZE = 200


class ExportJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: ExportJob) -> ExportJob:
        self.session.add(job)
        self.session.flush()
        return job

    def get(self, export_job_id: UUID, *, for_update: bool = False) -> ExportJob | None:
        """Fetch by ID without ownership check. WORKER-ONLY — never call from API routes."""
        stmt = select(ExportJob).where(ExportJob.id == export_job_id).options(defer(ExportJob.content_bytes))
        if for_update:
            stmt = stmt.with_for_update()
        return self.session.scalar(stmt)

    def get_for_user(
        self, export_job_id: UUID, user_id: UUID, *, include_content: bool = False,
    ) -> ExportJob | None:
        stmt = select(ExportJob).where(ExportJob.id == export_job_id, ExportJob.user_id == user_id)
        if not include_content:
            stmt = stmt.options(defer(ExportJob.content_bytes))
        return self.session.scalar(stmt)

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> ExportJob | None:
        stmt = (
            select(ExportJob)
            .where(
                ExportJob.user_id == user_id,
                ExportJob.idempotency_key == idempotency_key,
                ExportJob.status.notin_(["failed", "cancelled", "expired"]),
            )
            .options(defer(ExportJob.content_bytes))
            .with_for_update()
        )
        return self.session.scalar(stmt)

    def list_for_user(self, user_id: UUID, limit: int = 50, offset: int = 0) -> list[ExportJob]:
        limit = max(limit, 1)
        offset = max(offset, 0)
        stmt = (
            select(ExportJob)
            .where(ExportJob.user_id == user_id)
            .options(defer(ExportJob.content_bytes))
            .order_by(desc(ExportJob.created_at))
            .offset(offset)
            .limit(min(limit, _MAX_PAGE_SIZE))
        )
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        stmt = select(func.count(ExportJob.id)).where(ExportJob.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)

    def list_for_user_with_count(
        self, user_id: UUID, limit: int = 50, offset: int = 0,
    ) -> tuple[list[ExportJob], int]:
        """Return (items, total) in a single query using a window function."""
        count_col = func.count().over().label("_total")
        stmt = (
            select(ExportJob, count_col)
            .where(ExportJob.user_id == user_id)
            .options(defer(ExportJob.content_bytes))
            .order_by(desc(ExportJob.created_at))
            .offset(offset)
            .limit(min(limit, _MAX_PAGE_SIZE))
        )
        rows = list(self.session.execute(stmt))
        if not rows:
            return [], 0
        items = [row[0] for row in rows]
        total = rows[0][1]
        return items, total

    def list_expired_for_cleanup(self, before: datetime, limit: int) -> list[ExportJob]:
        from sqlalchemy import asc
        stmt = (
            select(ExportJob)
            .where(
                ExportJob.expires_at < before,
                ExportJob.status.in_(("succeeded", "failed", "expired")),
                or_(ExportJob.storage_key.isnot(None), ExportJob.content_bytes.isnot(None)),
            )
            .options(defer(ExportJob.content_bytes))
            .order_by(asc(ExportJob.created_at))
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(self.session.scalars(stmt))
