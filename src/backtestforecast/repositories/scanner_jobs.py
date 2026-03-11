from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, desc, select, tuple_
from sqlalchemy.orm import Session, selectinload

from backtestforecast.models import ScannerJob, ScannerRecommendation


class ScannerJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: ScannerJob) -> ScannerJob:
        self.session.add(job)
        self.session.flush()
        return job

    def list_for_user(self, user_id: UUID, limit: int = 50) -> list[ScannerJob]:
        stmt = (
            select(ScannerJob).where(ScannerJob.user_id == user_id).order_by(desc(ScannerJob.created_at)).limit(limit)
        )
        return list(self.session.scalars(stmt))

    def get_for_user(
        self,
        job_id: UUID,
        user_id: UUID,
        include_recommendations: bool = False,
    ) -> ScannerJob | None:
        stmt = select(ScannerJob).where(ScannerJob.id == job_id, ScannerJob.user_id == user_id)
        if include_recommendations:
            stmt = stmt.options(selectinload(ScannerJob.recommendations))
        return self.session.scalar(stmt)

    def get(self, job_id: UUID, include_recommendations: bool = False, for_update: bool = False) -> ScannerJob | None:
        stmt = select(ScannerJob).where(ScannerJob.id == job_id)
        if include_recommendations:
            stmt = stmt.options(selectinload(ScannerJob.recommendations))
        if for_update:
            stmt = stmt.with_for_update()
        return self.session.scalar(stmt)

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> ScannerJob | None:
        stmt = select(ScannerJob).where(
            ScannerJob.user_id == user_id,
            ScannerJob.idempotency_key == idempotency_key,
        )
        return self.session.scalar(stmt)

    def find_recent_duplicate(
        self,
        user_id: UUID,
        request_hash: str,
        mode: str,
        since: datetime,
    ) -> ScannerJob | None:
        stmt = (
            select(ScannerJob)
            .where(
                ScannerJob.user_id == user_id,
                ScannerJob.request_hash == request_hash,
                ScannerJob.mode == mode,
                ScannerJob.created_at >= since,
                ScannerJob.status.in_(["queued", "running", "succeeded"]),
                ScannerJob.job_kind == "manual",
            )
            .order_by(desc(ScannerJob.created_at))
            .limit(1)
        )
        return self.session.scalar(stmt)

    def delete_recommendations(self, job_id: UUID) -> None:
        self.session.execute(delete(ScannerRecommendation).where(ScannerRecommendation.scanner_job_id == job_id))

    def list_refresh_sources(self, limit: int = 100) -> list[ScannerJob]:
        stmt = (
            select(ScannerJob)
            .where(
                ScannerJob.refresh_daily.is_(True),
                ScannerJob.status == "succeeded",
            )
            .order_by(desc(ScannerJob.refresh_priority), desc(ScannerJob.completed_at), desc(ScannerJob.created_at))
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_historical_recommendations(
        self,
        *,
        symbol: str,
        strategy_type: str,
        rule_set_hash: str,
        before: datetime,
        limit: int = 200,
    ) -> list[tuple[ScannerRecommendation, datetime | None]]:
        stmt = (
            select(ScannerRecommendation, ScannerJob.completed_at)
            .join(ScannerJob, ScannerRecommendation.scanner_job_id == ScannerJob.id)
            .where(
                ScannerRecommendation.symbol == symbol,
                ScannerRecommendation.strategy_type == strategy_type,
                ScannerRecommendation.rule_set_hash == rule_set_hash,
                ScannerJob.status == "succeeded",
                ScannerJob.completed_at.is_not(None),
                ScannerJob.completed_at < before,
            )
            .order_by(desc(ScannerJob.completed_at))
            .limit(limit)
        )
        return list(self.session.execute(stmt).all())

    def batch_list_historical_recommendations(
        self,
        *,
        keys: list[tuple[str, str, str]],
        before: datetime,
        limit_per_key: int = 200,
    ) -> dict[tuple[str, str, str], list[tuple[ScannerRecommendation, datetime | None]]]:
        if not keys:
            return {}
        col_triple = tuple_(
            ScannerRecommendation.symbol,
            ScannerRecommendation.strategy_type,
            ScannerRecommendation.rule_set_hash,
        )
        stmt = (
            select(ScannerRecommendation, ScannerJob.completed_at)
            .join(ScannerJob, ScannerRecommendation.scanner_job_id == ScannerJob.id)
            .where(
                col_triple.in_(keys),
                ScannerJob.status == "succeeded",
                ScannerJob.completed_at.is_not(None),
                ScannerJob.completed_at < before,
            )
            .order_by(desc(ScannerJob.completed_at))
            .limit(len(keys) * limit_per_key)
        )
        rows = list(self.session.execute(stmt).all())
        result: dict[tuple[str, str, str], list[tuple[ScannerRecommendation, datetime | None]]] = {
            k: [] for k in keys
        }
        for rec, completed_at in rows:
            key = (rec.symbol, rec.strategy_type, rec.rule_set_hash)
            bucket = result.get(key)
            if bucket is not None and len(bucket) < limit_per_key:
                bucket.append((rec, completed_at))
        return result
