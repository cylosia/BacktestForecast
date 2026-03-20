from __future__ import annotations

from uuid import UUID, uuid4

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.observability.metrics import AUDIT_DEDUPE_CONFLICTS_TOTAL

logger = structlog.get_logger("audit_events")

_MAX_PAGE_SIZE = 200


def _is_unique_violation(exc: IntegrityError) -> bool:
    """Check if an IntegrityError is a unique constraint violation.

    Uses the DBAPI-level ``pgcode`` for PostgreSQL (``23505``) and falls back
    to message-based detection for SQLite/other backends used in tests.
    """
    orig = getattr(exc, "orig", None)
    if orig is not None:
        pgcode = getattr(orig, "pgcode", None)
        if pgcode is not None:
            return pgcode == "23505"
    orig_str = str(orig).lower() if orig else str(exc).lower()
    return "unique" in orig_str or "duplicate" in orig_str


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
        except IntegrityError as exc:
            nested.rollback()
            if not _is_unique_violation(exc):
                raise
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
            deduped_subject_id = f"{base}{suffix}"
        else:
            deduped_subject_id = str(uuid4())
        insert_event = AuditEvent(
            event_type=event.event_type,
            subject_type=event.subject_type,
            subject_id=deduped_subject_id,
            user_id=event.user_id,
            request_id=event.request_id,
            ip_hash=event.ip_hash,
            metadata_json=event.metadata_json,
        )
        nested = self.session.begin_nested()
        self.session.add(insert_event)
        try:
            nested.commit()
            return insert_event, True
        except IntegrityError as exc:
            nested.rollback()
            if not _is_unique_violation(exc):
                raise
            AUDIT_DEDUPE_CONFLICTS_TOTAL.inc()
            logger.warning(
                "audit.add_always_conflict",
                event_type=insert_event.event_type,
                subject_type=insert_event.subject_type,
                subject_id=insert_event.subject_id,
            )
            return insert_event, False

    def list_recent(self, *, user_id: UUID | None = None, limit: int = 50, offset: int = 0) -> list[AuditEvent]:
        """Return the most recent audit events, newest first.

        When *user_id* is provided the results are scoped to that user.
        """
        limit = max(min(limit, _MAX_PAGE_SIZE), 1)
        offset = max(offset, 0)
        stmt = select(AuditEvent)
        if user_id is not None:
            stmt = stmt.where(AuditEvent.user_id == user_id)
        stmt = stmt.order_by(AuditEvent.created_at.desc()).offset(offset).limit(limit)
        return list(self.session.scalars(stmt))

    def list_by_type(self, event_type: str, *, user_id: UUID | None = None, limit: int = 50, offset: int = 0) -> list[AuditEvent]:
        """Return audit events of a specific type, optionally scoped to a user."""
        limit = min(limit, _MAX_PAGE_SIZE)
        stmt = select(AuditEvent).where(AuditEvent.event_type == event_type)
        if user_id is not None:
            stmt = stmt.where(AuditEvent.user_id == user_id)
        stmt = stmt.order_by(AuditEvent.created_at.desc()).offset(offset).limit(limit)
        return list(self.session.scalars(stmt))

    def exists(self, *, event_type: str, subject_type: str, subject_id: str | None) -> bool:
        from sqlalchemy import exists as sa_exists
        conditions = [
            AuditEvent.event_type == event_type,
            AuditEvent.subject_type == subject_type,
        ]
        if subject_id is not None:
            conditions.append(AuditEvent.subject_id == subject_id)
        else:
            conditions.append(AuditEvent.subject_id.is_(None))
        return bool(self.session.scalar(select(sa_exists().where(*conditions))))
