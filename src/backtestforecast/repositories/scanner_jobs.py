from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, desc, func, select, tuple_
from sqlalchemy.orm import Session, noload, selectinload

from backtestforecast.models import ScannerJob, ScannerRecommendation

_MAX_PAGE_SIZE = 200


class ScannerJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: ScannerJob) -> ScannerJob:
        self.session.add(job)
        self.session.flush()
        return job

    def list_for_user(self, user_id: UUID, limit: int = 50, offset: int = 0) -> list[ScannerJob]:
        stmt = (
            select(ScannerJob)
            .where(ScannerJob.user_id == user_id)
            .options(noload(ScannerJob.recommendations))
            .order_by(desc(ScannerJob.created_at))
            .offset(offset)
            .limit(min(limit, _MAX_PAGE_SIZE))
        )
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        stmt = select(func.count(ScannerJob.id)).where(ScannerJob.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)

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
            ScannerJob.status.notin_(["failed", "cancelled"]),
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
                ScannerJob.status.in_(["queued", "running"]),
                ScannerJob.job_kind == "manual",
            )
            .order_by(desc(ScannerJob.created_at))
            .limit(1)
        )
        return self.session.scalar(stmt)

    def delete_recommendations(self, job_id: UUID) -> None:
        self.session.execute(
            delete(ScannerRecommendation)
            .where(ScannerRecommendation.scanner_job_id == job_id)
            .execution_options(synchronize_session="fetch")
        )

    def list_refresh_sources(self, limit: int = 100) -> list[ScannerJob]:
        deduped = (
            select(ScannerJob.id)
            .where(
                ScannerJob.refresh_daily.is_(True),
                ScannerJob.status == "succeeded",
            )
            .distinct(ScannerJob.user_id, ScannerJob.request_hash, ScannerJob.mode)
            .order_by(
                ScannerJob.user_id,
                ScannerJob.request_hash,
                ScannerJob.mode,
                desc(ScannerJob.completed_at),
            )
            .subquery()
        )
        stmt = (
            select(ScannerJob)
            .where(ScannerJob.id.in_(select(deduped.c.id)))
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
        limit: int = 100,
    ) -> list[tuple[ScannerRecommendation, datetime | None]]:
        """Fetch historical recommendations across ALL users for aggregate signal analysis.

        This intentionally crosses user boundaries to build platform-wide signal history.
        Do not expose raw ScannerRecommendation objects to end users.
        """
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
        limit_per_key: int = 100,
    ) -> dict[tuple[str, str, str], list[tuple[ScannerRecommendation, datetime | None]]]:
        """Fetch historical recommendations across ALL users for aggregate signal analysis.

        This intentionally crosses user boundaries to build platform-wide signal history.
        Do not expose raw ScannerRecommendation objects to end users.
        """
        if not keys:
            return {}
        col_triple = tuple_(
            ScannerRecommendation.symbol,
            ScannerRecommendation.strategy_type,
            ScannerRecommendation.rule_set_hash,
        )
        row_num = func.row_number().over(
            partition_by=[
                ScannerRecommendation.symbol,
                ScannerRecommendation.strategy_type,
                ScannerRecommendation.rule_set_hash,
            ],
            order_by=(desc(ScannerJob.completed_at), desc(ScannerRecommendation.id)),
        ).label("rn")
        subq = (
            select(
                ScannerRecommendation.id.label("rec_id"),
                ScannerJob.completed_at.label("completed_at"),
                row_num,
            )
            .join(ScannerJob, ScannerRecommendation.scanner_job_id == ScannerJob.id)
            .where(
                col_triple.in_(keys),
                ScannerJob.status == "succeeded",
                ScannerJob.completed_at.is_not(None),
                ScannerJob.completed_at < before,
            )
            .subquery()
        )
        stmt = (
            select(ScannerRecommendation, subq.c.completed_at)
            .join(subq, ScannerRecommendation.id == subq.c.rec_id)
            .where(subq.c.rn <= limit_per_key)
            .order_by(desc(subq.c.completed_at))
        )
        rows = list(self.session.execute(stmt).all())
        result: dict[tuple[str, str, str], list[tuple[ScannerRecommendation, datetime | None]]] = {
            k: [] for k in keys
        }
        for rec, completed_at in rows:
            key = (rec.symbol, rec.strategy_type, rec.rule_set_hash)
            bucket = result.get(key)
            if bucket is not None:
                bucket.append((rec, completed_at))
        return result
