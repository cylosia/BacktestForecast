from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, defer

from backtestforecast.models import ExportJob
from backtestforecast.repositories.pagination import apply_cursor_window, list_with_total

_MAX_PAGE_SIZE = 200


class ExportJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: ExportJob) -> ExportJob:
        self.session.add(job)
        self.session.flush()
        return job

    def get(self, export_job_id: UUID, *, for_update: bool = False) -> ExportJob | None:
        """Fetch by ID without ownership check. WORKER-ONLY - never call from API routes."""
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

    def list_for_user(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> list[ExportJob]:
        stmt = (
            select(ExportJob)
            .where(ExportJob.user_id == user_id)
            .options(defer(ExportJob.content_bytes))
        )
        stmt = apply_cursor_window(
            stmt,
            model=ExportJob,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        stmt = select(func.count(ExportJob.id)).where(ExportJob.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)

    def list_for_user_with_count(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> tuple[list[ExportJob], int]:
        """Return (items, total) where total ignores cursor pagination."""
        stmt = (
            select(ExportJob)
            .where(ExportJob.user_id == user_id)
            .options(defer(ExportJob.content_bytes))
        )
        return list_with_total(
            self.session,
            base_stmt=stmt,
            count_stmt=select(func.count(ExportJob.id)).where(ExportJob.user_id == user_id),
            model=ExportJob,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )

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


    def get_content_bytes_for_user(self, export_job_id: UUID, user_id: UUID) -> bytes | None:
        stmt = select(ExportJob.content_bytes).where(ExportJob.id == export_job_id, ExportJob.user_id == user_id)
        return self.session.scalar(stmt)
