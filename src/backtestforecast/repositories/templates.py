from __future__ import annotations

from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from backtestforecast.models import BacktestTemplate

_MAX_PAGE_SIZE = 200


class BacktestTemplateRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, template: BacktestTemplate) -> BacktestTemplate:
        self.session.add(template)
        self.session.flush()
        return template

    def get_for_user(self, template_id: UUID, user_id: UUID, *, for_update: bool = False) -> BacktestTemplate | None:
        stmt = select(BacktestTemplate).where(
            BacktestTemplate.id == template_id,
            BacktestTemplate.user_id == user_id,
        )
        if for_update:
            stmt = stmt.with_for_update()
        return self.session.scalar(stmt)

    def list_for_user(self, user_id: UUID, *, limit: int = 100, offset: int = 0) -> list[BacktestTemplate]:
        stmt = (
            select(BacktestTemplate)
            .where(BacktestTemplate.user_id == user_id)
            .order_by(desc(BacktestTemplate.updated_at))
            .offset(offset)
            .limit(min(limit, _MAX_PAGE_SIZE))
        )
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        stmt = select(func.count(BacktestTemplate.id)).where(
            BacktestTemplate.user_id == user_id,
        )
        return int(self.session.scalar(stmt) or 0)

    def delete(self, template: BacktestTemplate) -> None:
        self.session.delete(template)
        self.session.flush()
