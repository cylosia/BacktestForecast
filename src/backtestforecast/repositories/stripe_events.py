from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import StripeEvent
from backtestforecast.observability.metrics import STRIPE_WEBHOOK_DEDUPE_TOTAL

logger = structlog.get_logger("stripe_events")


class StripeEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def claim(
        self,
        *,
        stripe_event_id: str,
        event_type: str,
        livemode: bool,
        user_id: UUID | None = None,
        request_id: str | None = None,
        ip_hash: str | None = None,
        payload_summary: dict[str, Any] | None = None,
    ) -> StripeEvent | None:
        """Atomically claim a Stripe event for processing.

        Returns the persisted ``StripeEvent`` on success, or ``None`` if
        this event was already claimed (duplicate delivery).
        """
        event = StripeEvent(
            stripe_event_id=stripe_event_id,
            event_type=event_type,
            livemode=livemode,
            idempotency_status="processed",
            user_id=user_id,
            request_id=request_id,
            ip_hash=ip_hash,
            payload_summary=payload_summary or {},
        )
        nested = self.session.begin_nested()
        self.session.add(event)
        try:
            nested.commit()
            return event
        except IntegrityError:
            nested.rollback()
            STRIPE_WEBHOOK_DEDUPE_TOTAL.inc()
            return None

    def mark_error(self, stripe_event_id: str, error_detail: str) -> None:
        """Update a previously claimed event to record a processing error."""
        from sqlalchemy import update

        self.session.execute(
            update(StripeEvent)
            .where(StripeEvent.stripe_event_id == stripe_event_id)
            .values(idempotency_status="error", error_detail=error_detail[:2000])
        )

    def list_recent(self, *, limit: int = 50) -> list[StripeEvent]:
        """Return the most recent Stripe events, newest first."""
        stmt = (
            select(StripeEvent)
            .order_by(StripeEvent.created_at.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def get_by_stripe_id(self, stripe_event_id: str) -> StripeEvent | None:
        stmt = select(StripeEvent).where(StripeEvent.stripe_event_id == stripe_event_id)
        return self.session.scalar(stmt)
