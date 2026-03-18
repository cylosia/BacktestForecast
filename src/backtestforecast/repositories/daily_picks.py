"""Repository for NightlyPipelineRun and DailyRecommendation queries."""
from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import and_, desc, or_, select
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
        cursor_dt: datetime | None = None,
        cursor_id: UUID | None = None,
    ) -> list[NightlyPipelineRun]:
        stmt = select(NightlyPipelineRun).order_by(
            desc(NightlyPipelineRun.created_at),
            desc(NightlyPipelineRun.id),
        )
        if cursor_dt is not None:
            if cursor_id is not None:
                stmt = stmt.where(
                    or_(
                        NightlyPipelineRun.created_at < cursor_dt,
                        and_(
                            NightlyPipelineRun.created_at == cursor_dt,
                            NightlyPipelineRun.id < cursor_id,
                        ),
                    )
                )
            else:
                stmt = stmt.where(NightlyPipelineRun.created_at < cursor_dt)
        stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))
