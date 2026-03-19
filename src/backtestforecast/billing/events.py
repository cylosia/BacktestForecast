"""Billing event log for audit trail and replay capability."""
from __future__ import annotations

from datetime import datetime, UTC
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

logger = structlog.get_logger("billing.events")

_BILLING_REDACT_KEYS = {"payment_method", "billing_address", "card", "bank_account"}

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _safe_state(state: dict | None) -> dict | None:
    if not state:
        return None
    result: dict = {}
    for k, v in state.items():
        if k.lower() in _BILLING_REDACT_KEYS:
            continue
        if isinstance(v, dict):
            result[k] = _safe_state(v)
        elif isinstance(v, list):
            result[k] = [_safe_state(item) if isinstance(item, dict) else item for item in v]
        else:
            result[k] = v
    return result


def log_billing_event(
    *,
    user_id: UUID,
    event_type: str,
    subscription_id: str | None = None,
    old_state: dict | None = None,
    new_state: dict | None = None,
    source: str = "webhook",
    request_id: str | None = None,
    session: "Session | None" = None,
) -> None:
    """Log a structured billing state change event.

    This provides an audit trail that can be used to debug webhook
    ordering issues and replay state changes if needed. When session
    is provided, the event is also persisted to the audit database.
    """
    logger.info(
        "billing.state_change",
        user_id=str(user_id),
        event_type=event_type,
        subscription_id=subscription_id,
        old_state=_safe_state(old_state),
        new_state=_safe_state(new_state),
        source=source,
        request_id=request_id,
        timestamp=datetime.now(UTC).isoformat(),
    )
    if session is not None:
        from backtestforecast.services.audit import AuditService

        audit = AuditService(session)
        audit.record_always(
            event_type=f"billing.{event_type}",
            subject_type="stripe_subscription",
            subject_id=subscription_id,
            user_id=user_id,
            request_id=request_id,
            metadata={
                "old_state": _safe_state(old_state),
                "new_state": _safe_state(new_state),
                "source": source,
            },
        )
