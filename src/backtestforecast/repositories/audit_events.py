from __future__ import annotations

from uuid import uuid4

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.observability.metrics import AUDIT_DEDUPE_CONFLICTS_TOTAL

logger = structlog.get_logger("audit_events")


class AuditEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, event: AuditEvent) -> tuple[AuditEvent, bool]:
        """Insert an audit event. Returns (event, was_inserted).

        ``was_inserted`` is ``False`` when the row was deduplicated against the
        unique constraint on (event_type, subject_type, subject_id).
        """
        nested = self.session.begin_nested()
        self.session.add(event)
        try:
            nested.commit()
            return event, True
        except IntegrityError:
            nested.rollback()
            AUDIT_DEDUPE_CONFLICTS_TOTAL.inc()
            return event, False

    def add_always(self, event: AuditEvent) -> AuditEvent:
        """Insert an audit event unconditionally (no dedup). Appends a UUID suffix to subject_id.

        WARNING: Events created via this method bypass the dedup unique constraint
        and will accumulate unboundedly for frequently-triggered event types
        (e.g. ``export.downloaded``). Consider periodic cleanup or archival for
        high-volume event types.
        """
        if event.subject_id is not None:
            combined = f"{event.subject_id}:{uuid4()}"
            event.subject_id = combined[:255]
        nested = self.session.begin_nested()
        self.session.add(event)
        try:
            nested.commit()
        except IntegrityError:
            nested.rollback()
            AUDIT_DEDUPE_CONFLICTS_TOTAL.inc()
            logger.warning(
                "audit.add_always_conflict",
                event_type=event.event_type,
                subject_type=event.subject_type,
                subject_id=event.subject_id,
            )
        return event

    def exists(self, *, event_type: str, subject_type: str, subject_id: str | None) -> bool:
        stmt = select(AuditEvent.id).where(
            AuditEvent.event_type == event_type,
            AuditEvent.subject_type == subject_type,
        )
        if subject_id is not None:
            stmt = stmt.where(AuditEvent.subject_id == subject_id)
        else:
            stmt = stmt.where(AuditEvent.subject_id.is_(None))
        return self.session.execute(stmt).first() is not None
