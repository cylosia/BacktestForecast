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

    @staticmethod
    def _build_event(
        *,
        event_type: str,
        subject_type: str,
        subject_id: str | UUID | None,
        user_id: UUID | None = None,
        request_id: str | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEvent:
        subject_value = None if subject_id is None else str(subject_id)
        payload = dict(metadata) if metadata else {}
        if user_agent:
            payload["user_agent"] = user_agent[:512]
        return AuditEvent(
            user_id=user_id,
            request_id=request_id,
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_value,
            ip_hash=hash_ip(ip_address),
            metadata_json=payload,
        )

    def record(
        self,
        *,
        event_type: str,
        subject_type: str,
        subject_id: str | UUID | None,
        user_id: UUID | None = None,
        request_id: str | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEvent | None:
        """Record an audit event. Returns ``None`` when the event was deduplicated."""
        event = self._build_event(
            event_type=event_type, subject_type=subject_type,
            subject_id=subject_id, user_id=user_id, request_id=request_id,
            ip_address=ip_address, user_agent=user_agent, metadata=metadata,
        )
        event, was_inserted = self.repository.add(event)
        if was_inserted:
            logger.info(
                "audit.event.recorded",
                event_type=event_type,
                subject_type=subject_type,
                subject_id=event.subject_id,
                user_id=str(user_id) if user_id is not None else None,
            )
            return event
        logger.debug(
            "audit.event.deduplicated",
            event_type=event_type,
            subject_type=subject_type,
            subject_id=event.subject_id,
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
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEvent:
        """Record an audit event without deduplication (append-only)."""
        event = self._build_event(
            event_type=event_type, subject_type=subject_type,
            subject_id=subject_id, user_id=user_id, request_id=request_id,
            ip_address=ip_address, user_agent=user_agent, metadata=metadata,
        )
        event, _ = self.repository.add_always(event)
        logger.info(
            "audit.event.recorded_always",
            event_type=event_type,
            subject_type=subject_type,
            subject_id=event.subject_id,
            user_id=str(user_id) if user_id is not None else None,
        )
        return event
