from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from backtestforecast.models import SymbolAnalysis

_MAX_PAGE_SIZE = 200


class SymbolAnalysisRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, analysis: SymbolAnalysis) -> SymbolAnalysis:
        self.session.add(analysis)
        self.session.flush()
        return analysis

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> SymbolAnalysis | None:
        stmt = (
            select(SymbolAnalysis).where(
                SymbolAnalysis.user_id == user_id,
                SymbolAnalysis.idempotency_key == idempotency_key,
                SymbolAnalysis.status.notin_(["failed", "cancelled"]),
            )
            .with_for_update()
        )
        return self.session.scalar(stmt)

    def get_by_id_unfiltered(self, analysis_id: UUID) -> SymbolAnalysis | None:
        """Fetch an analysis without ownership check.  Only use from worker/internal code."""
        return self.session.get(SymbolAnalysis, analysis_id)

    def get_by_id(self, analysis_id: UUID, *, user_id: UUID) -> SymbolAnalysis | None:
        stmt = select(SymbolAnalysis).where(SymbolAnalysis.id == analysis_id, SymbolAnalysis.user_id == user_id)
        return self.session.scalar(stmt)

    def list_for_user(
        self,
        user_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        cursor_before: datetime | None = None,
    ) -> list[SymbolAnalysis]:
        if offset > 0 and cursor_before is not None:
            raise ValueError("Cannot combine offset and cursor_before pagination.")
        limit = max(limit, 1)
        offset = max(offset, 0)
        stmt = (
            select(SymbolAnalysis)
            .where(SymbolAnalysis.user_id == user_id)
        )
        if cursor_before is not None:
            stmt = stmt.where(SymbolAnalysis.created_at < cursor_before)
        stmt = stmt.order_by(desc(SymbolAnalysis.created_at)).offset(offset).limit(min(limit, _MAX_PAGE_SIZE))
        return list(self.session.scalars(stmt))

    def count_for_user(self, user_id: UUID) -> int:
        """Return the total number of analyses for a user."""
        from sqlalchemy import func
        stmt = select(func.count()).select_from(SymbolAnalysis).where(SymbolAnalysis.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)
