from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.models import SymbolAnalysis


class SymbolAnalysisRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> SymbolAnalysis | None:
        stmt = select(SymbolAnalysis).where(
            SymbolAnalysis.user_id == user_id,
            SymbolAnalysis.idempotency_key == idempotency_key,
        )
        return self.session.scalar(stmt)

    def get_by_id(self, analysis_id: UUID) -> SymbolAnalysis | None:
        return self.session.get(SymbolAnalysis, analysis_id)
