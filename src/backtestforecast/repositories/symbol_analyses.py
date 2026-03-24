from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backtestforecast.models import SymbolAnalysis
from backtestforecast.repositories.pagination import apply_cursor_window, list_with_total

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
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> list[SymbolAnalysis]:
        stmt = apply_cursor_window(
            select(SymbolAnalysis).where(SymbolAnalysis.user_id == user_id),
            model=SymbolAnalysis,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )
        return list(self.session.scalars(stmt))

    def list_for_user_with_count(
        self,
        user_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> tuple[list[SymbolAnalysis], int]:
        return list_with_total(
            self.session,
            base_stmt=select(SymbolAnalysis).where(SymbolAnalysis.user_id == user_id),
            count_stmt=select(func.count()).select_from(SymbolAnalysis).where(SymbolAnalysis.user_id == user_id),
            model=SymbolAnalysis,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )

    def count_for_user(self, user_id: UUID) -> int:
        """Return the total number of analyses for a user."""
        from sqlalchemy import func
        stmt = select(func.count()).select_from(SymbolAnalysis).where(SymbolAnalysis.user_id == user_id)
        return int(self.session.scalar(stmt) or 0)
