from __future__ import annotations

from uuid import UUID, uuid4

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

    def add_always(self, event: AuditEvent) -> tuple[AuditEvent, bool]:
        """Insert an audit event unconditionally (no dedup). Appends a UUID suffix to subject_id.

        WARNING: Events created via this method bypass the dedup unique constraint
        and will accumulate unboundedly for frequently-triggered event types
        (e.g. ``export.downloaded``). Consider periodic cleanup or archival for
        high-volume event types.
        """
        if event.subject_id is not None:
            suffix = f":{uuid4()}"
            max_base = 255 - len(suffix)
            base = str(event.subject_id)[:max_base]
            event.subject_id = f"{base}{suffix}"
        nested = self.session.begin_nested()
        self.session.add(event)
        try:
            nested.commit()
            return event, True
        except IntegrityError:
            nested.rollback()
            AUDIT_DEDUPE_CONFLICTS_TOTAL.inc()
            logger.warning(
                "audit.add_always_conflict",
                event_type=event.event_type,
                subject_type=event.subject_type,
                subject_id=event.subject_id,
            )
            return event, False

    def list_recent(self, *, limit: int = 50) -> list[AuditEvent]:
        """Return the most recent audit events, newest first."""
        stmt = (
            select(AuditEvent)
            .order_by(AuditEvent.created_at.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_for_user(self, user_id: UUID, *, limit: int = 50) -> list[AuditEvent]:
        """Return audit events for a specific user."""
        stmt = (
            select(AuditEvent)
            .where(AuditEvent.user_id == user_id)
            .order_by(AuditEvent.created_at.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_by_type(self, event_type: str, *, limit: int = 50) -> list[AuditEvent]:
        """Return audit events of a specific type."""
        stmt = (
            select(AuditEvent)
            .where(AuditEvent.event_type == event_type)
            .order_by(AuditEvent.created_at.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

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
