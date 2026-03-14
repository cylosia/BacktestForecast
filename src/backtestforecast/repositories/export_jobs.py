from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.orm import Session, defer

from backtestforecast.models import ExportJob


class ExportJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: ExportJob) -> ExportJob:
        self.session.add(job)
        self.session.flush()
        return job

    def get(self, export_job_id: UUID, *, for_update: bool = False) -> ExportJob | None:
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
            .where(ExportJob.user_id == user_id, ExportJob.idempotency_key == idempotency_key)
            .options(defer(ExportJob.content_bytes))
        )
        return self.session.scalar(stmt)

    def list_for_user(self, user_id: UUID, limit: int = 50) -> list[ExportJob]:
        stmt = (
            select(ExportJob)
            .where(ExportJob.user_id == user_id)
            .options(defer(ExportJob.content_bytes))
            .order_by(desc(ExportJob.created_at))
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_expired_for_cleanup(self, before: datetime, limit: int) -> list[ExportJob]:
        from sqlalchemy import asc
        stmt = (
            select(ExportJob)
            .where(
                ExportJob.expires_at < before,
                ExportJob.status == "succeeded",
                ExportJob.storage_key.isnot(None),
            )
            .options(defer(ExportJob.content_bytes))
            .order_by(asc(ExportJob.created_at))
            .limit(limit)
        )
        return list(self.session.scalars(stmt))
