from __future__ import annotations

from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from backtestforecast.models import SymbolAnalysis


class SymbolAnalysisRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, analysis: SymbolAnalysis) -> None:
        self.session.add(analysis)
        self.session.flush()

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> SymbolAnalysis | None:
        stmt = select(SymbolAnalysis).where(
            SymbolAnalysis.user_id == user_id,
            SymbolAnalysis.idempotency_key == idempotency_key,
        )
        return self.session.scalar(stmt)

    def get_by_id(self, analysis_id: UUID) -> SymbolAnalysis | None:
        return self.session.get(SymbolAnalysis, analysis_id)

    def get_for_user(self, analysis_id: UUID, user_id: UUID) -> SymbolAnalysis | None:
        stmt = select(SymbolAnalysis).where(
            SymbolAnalysis.id == analysis_id,
            SymbolAnalysis.user_id == user_id,
        )
        return self.session.scalar(stmt)

    def list_for_user(self, user_id: UUID, *, limit: int = 50, offset: int = 0) -> list[SymbolAnalysis]:
        stmt = (
            select(SymbolAnalysis)
            .where(SymbolAnalysis.user_id == user_id)
            .order_by(desc(SymbolAnalysis.created_at))
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))
