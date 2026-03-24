from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.models import StripeEvent
from backtestforecast.observability.metrics import STRIPE_WEBHOOK_DEDUPE_TOTAL

logger = structlog.get_logger("stripe_events")

# Longer TTL gives legitimate handlers more time; shorter TTL recovers stuck events faster.
# 15 minutes exceeds typical webhook processing time while allowing recovery of lost workers.
STALE_CLAIM_TTL = timedelta(minutes=15)
_MAX_PAGE_SIZE = 200


class StripeEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def _recover_stale_claim(self, stripe_event_id: str) -> bool:
        """Delete a stale claim (older than 15 minutes) to allow reprocessing.

        Targets events with ``idempotency_status`` of ``'processing'`` (stuck
        in-flight) or ``'error'`` (failed on a previous attempt - Stripe may
        legitimately retry).  Never deletes ``'processed'`` events.
        Returns True if a stale claim was recovered.
        """
        from sqlalchemy import delete as sa_delete

        cutoff = datetime.now(UTC) - STALE_CLAIM_TTL
        result = self.session.execute(
            sa_delete(StripeEvent)
            .where(
                StripeEvent.stripe_event_id == stripe_event_id,
                StripeEvent.idempotency_status.in_(("processing", "error")),
                StripeEvent.created_at < cutoff,
            )
        )
        if result.rowcount > 0:
            logger.warning(
                "stripe_event.stale_claim_recovered",
                stripe_event_id=stripe_event_id,
            )
            self.session.flush()
            return True
        return False

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

        Stale claims older than 15 minutes are automatically recovered to
        allow reprocessing of events that may have been lost.
        """
        nested = self.session.begin_nested()
        try:
            self._recover_stale_claim(stripe_event_id)

            event = StripeEvent(
                stripe_event_id=stripe_event_id,
                event_type=event_type,
                livemode=livemode,
                idempotency_status="processing",
                user_id=user_id,
                request_id=request_id,
                ip_hash=ip_hash,
                payload_summary=payload_summary or {},
            )
            self.session.add(event)
            self.session.flush()
            nested.commit()
            return event
        except IntegrityError:
            nested.rollback()
            STRIPE_WEBHOOK_DEDUPE_TOTAL.inc()
            return None

    def mark_processed(self, stripe_event_id: str) -> bool:
        """Mark a claimed event as successfully processed.

        Only transitions from ``processing`` to ``processed`` to avoid
        overwriting a concurrent ``error`` status.

        Returns True if the transition was applied, False if the event
        was already in a different state (e.g. concurrently marked as error).
        """
        result = self.session.execute(
            update(StripeEvent)
            .where(
                StripeEvent.stripe_event_id == stripe_event_id,
                StripeEvent.idempotency_status == "processing",
            )
            .values(idempotency_status="processed")
        )
        if result.rowcount == 0:
            logger.warning("stripe_event.mark_processed_noop", stripe_event_id=stripe_event_id)
            return False
        return True

    def mark_error(self, stripe_event_id: str, error_detail: str) -> bool:
        """Update a previously claimed event to record a processing error.

        Returns True if the row was updated, False if it was already in a
        non-processing state (no-op).
        """
        result = self.session.execute(
            update(StripeEvent)
            .where(
                StripeEvent.stripe_event_id == stripe_event_id,
                StripeEvent.idempotency_status == "processing",
            )
            .values(idempotency_status="error", error_detail=error_detail[:2000])
        )
        return result.rowcount > 0

    def list_recent(self, *, user_id: UUID | None = None, limit: int = 50, offset: int = 0) -> list[StripeEvent]:
        """Return the most recent Stripe events, newest first.

        When *user_id* is provided the results are scoped to that user.
        Without a user_id filter this returns ALL events - callers exposing
        this to non-admin endpoints MUST pass user_id for data isolation.
        """
        limit = min(limit, _MAX_PAGE_SIZE)
        stmt = select(StripeEvent)
        if user_id is not None:
            stmt = stmt.where(StripeEvent.user_id == user_id)
        stmt = stmt.order_by(StripeEvent.created_at.desc()).offset(offset).limit(limit)
        return list(self.session.scalars(stmt))

    def get_by_stripe_id(self, stripe_event_id: str) -> StripeEvent | None:
        stmt = select(StripeEvent).where(StripeEvent.stripe_event_id == stripe_event_id)
        return self.session.scalar(stmt)
