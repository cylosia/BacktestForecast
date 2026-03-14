from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.observability import get_logger, hash_ip
from backtestforecast.repositories.audit_events import AuditEventRepository

logger = get_logger("audit")


class AuditService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = AuditEventRepository(session)

    def record(
        self,
        *,
        event_type: str,
        subject_type: str,
        subject_id: str | UUID | None,
        user_id: UUID | None = None,
        request_id: str | None = None,
        ip_address: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEvent | None:
        """Record an audit event. Returns ``None`` when the event was deduplicated."""
        subject_value = None if subject_id is None else str(subject_id)
        payload = metadata or {}
        event = AuditEvent(
            user_id=user_id,
            request_id=request_id,
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_value,
            ip_hash=hash_ip(ip_address),
            metadata_json=payload,
        )
        event, was_inserted = self.repository.add(event)
        if was_inserted:
            logger.info(
                "audit.event.recorded",
                event_type=event_type,
                subject_type=subject_type,
                subject_id=subject_value,
                user_id=str(user_id) if user_id is not None else None,
            )
            return event
        logger.debug(
            "audit.event.deduplicated",
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_value,
        )
        return None

    def record_always(
        self,
        *,
        event_type: str,
        subject_type: str,
        subject_id: str | UUID | None,
        user_id: UUID | None = None,
        request_id: str | None = None,
        ip_address: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEvent:
        """Record an audit event without deduplication (append-only)."""
        subject_value = None if subject_id is None else str(subject_id)
        payload = metadata or {}
        event = AuditEvent(
            user_id=user_id,
            request_id=request_id,
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_value,
            ip_hash=hash_ip(ip_address),
            metadata_json=payload,
        )
        event, _ = self.repository.add_always(event)
        logger.info(
            "audit.event.recorded_always",
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_value,
            user_id=str(user_id) if user_id is not None else None,
        )
        return event
