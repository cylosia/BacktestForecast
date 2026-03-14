"""Billing event log for audit trail and replay capability."""
from __future__ import annotations

import json
from datetime import datetime, UTC
from uuid import UUID

import structlog

logger = structlog.get_logger("billing.events")


def log_billing_event(
    *,
    user_id: UUID,
    event_type: str,
    subscription_id: str | None = None,
    old_state: dict | None = None,
    new_state: dict | None = None,
    source: str = "webhook",
    request_id: str | None = None,
) -> None:
    """Log a structured billing state change event.

    This provides an audit trail that can be used to debug webhook
    ordering issues and replay state changes if needed.
    """
    logger.info(
        "billing.state_change",
        user_id=str(user_id),
        event_type=event_type,
        subscription_id=subscription_id,
        old_state=json.dumps(old_state) if old_state else None,
        new_state=json.dumps(new_state) if new_state else None,
        source=source,
        request_id=request_id,
        timestamp=datetime.now(UTC).isoformat(),
    )
