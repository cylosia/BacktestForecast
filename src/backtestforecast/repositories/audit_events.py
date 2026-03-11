from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.observability.metrics import AUDIT_DEDUPE_CONFLICTS_TOTAL


class AuditEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, event: AuditEvent) -> AuditEvent:
        nested = self.session.begin_nested()
        self.session.add(event)
        try:
            nested.commit()
        except IntegrityError:
            nested.rollback()
            AUDIT_DEDUPE_CONFLICTS_TOTAL.inc()
        return event

    def exists(self, *, event_type: str, subject_type: str, subject_id: str | None) -> bool:
        if not subject_id:
            return False
        stmt = select(AuditEvent.id).where(
            AuditEvent.event_type == event_type,
            AuditEvent.subject_type == subject_type,
            AuditEvent.subject_id == subject_id,
        )
        return self.session.execute(stmt).first() is not None
