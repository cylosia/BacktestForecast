"""Repository for NightlyPipelineRun and DailyRecommendation queries."""
from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from backtestforecast.models import DailyRecommendation, NightlyPipelineRun


class DailyPicksRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_latest_succeeded_run(
        self, trade_date: date | None = None,
    ) -> NightlyPipelineRun | None:
        stmt = select(NightlyPipelineRun).where(
            NightlyPipelineRun.status == "succeeded",
        )
        if trade_date is not None:
            stmt = stmt.where(NightlyPipelineRun.trade_date == trade_date)
        stmt = stmt.order_by(desc(NightlyPipelineRun.created_at)).limit(1)
        return self.session.scalar(stmt)

    def get_recommendations_for_run(
        self, pipeline_run_id: UUID, *, limit: int = 20, offset: int = 0,
    ) -> list[DailyRecommendation]:
        limit = min(limit, 200)
        stmt = (
            select(DailyRecommendation)
            .where(DailyRecommendation.pipeline_run_id == pipeline_run_id)
            .order_by(DailyRecommendation.rank)
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_pipeline_history(
        self,
        *,
        limit: int = 10,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> list[NightlyPipelineRun]:
        stmt = select(NightlyPipelineRun).order_by(
            desc(NightlyPipelineRun.created_at),
            desc(NightlyPipelineRun.id),
        )
        if cursor_before is not None:
            cursor_dt, cursor_id = cursor_before
            stmt = stmt.where(
                or_(
                    NightlyPipelineRun.created_at < cursor_dt,
                    and_(
                        NightlyPipelineRun.created_at == cursor_dt,
                        NightlyPipelineRun.id < cursor_id,
                    ),
                )
            )
        stmt = stmt.limit(min(limit, 200))
        return list(self.session.scalars(stmt))

    def count_pipeline_history(self) -> int:
        stmt = select(func.count()).select_from(NightlyPipelineRun)
        return int(self.session.scalar(stmt) or 0)

    def count_pipeline_history_before_cursor(
        self,
        *,
        cursor_before: tuple[datetime, UUID],
    ) -> int:
        cursor_dt, cursor_id = cursor_before
        stmt = select(func.count()).select_from(NightlyPipelineRun)
        stmt = stmt.where(
            or_(
                NightlyPipelineRun.created_at > cursor_dt,
                and_(
                    NightlyPipelineRun.created_at == cursor_dt,
                    NightlyPipelineRun.id > cursor_id,
                ),
            )
        )
        return int(self.session.scalar(stmt) or 0)
