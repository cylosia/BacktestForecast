"""Billing event log for audit trail and replay capability."""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from backtestforecast.config import get_settings

logger = structlog.get_logger("billing.events")
UTC = timezone.utc
_BILLING_AUDIT_FALLBACK_REDIS_KEY = "billing:audit:dead_letter"
_BILLING_AUDIT_FALLBACK_FILE = Path(tempfile.gettempdir()) / "backtestforecast-billing-audit-fallback.jsonl"

_BILLING_REDACT_KEYS = {
    "payment_method", "billing_address", "card", "bank_account",
    "email", "name", "phone", "tax_id", "ip_address",
    "last4", "exp_month", "exp_year",
    "address_line1", "address_line2", "address_city", "address_state",
    "address_zip", "address_country",
}

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

_ALLOWED_EVENT_PREFIXES = frozenset({
    "checkout", "subscription", "portal", "cancellation", "reconciliation",
    "customer", "invoice",
})


def _safe_state(state: dict | None, *, _depth: int = 0) -> dict | None:
    _MAX_DEPTH = 10
    if state is None:
        return None
    if not state:
        return {}
    if _depth >= _MAX_DEPTH:
        return {"_truncated": True}
    result: dict = {}
    for k, v in state.items():
        if k.lower() in _BILLING_REDACT_KEYS:
            result[k] = "<redacted>"
            continue
        if isinstance(v, dict):
            result[k] = _safe_state(v, _depth=_depth + 1)
        elif isinstance(v, list):
            result[k] = [_safe_state(item, _depth=_depth + 1) if isinstance(item, dict) else item for item in v]
        else:
            result[k] = v
    return result


def _persist_failed_billing_audit(payload: dict[str, object]) -> None:
    settings = get_settings()
    redis_url = settings.redis_cache_url or settings.redis_url
    if redis_url:
        try:
            import redis

            client = redis.Redis.from_url(redis_url, socket_timeout=5, socket_connect_timeout=5, decode_responses=True)
            client.lpush(_BILLING_AUDIT_FALLBACK_REDIS_KEY, json.dumps(payload, sort_keys=True, default=str))
            return
        except Exception:
            logger.error("billing.audit_fallback_redis_failed", exc_info=True)

    try:
        _BILLING_AUDIT_FALLBACK_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _BILLING_AUDIT_FALLBACK_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, default=str))
            fh.write("\n")
    except Exception:
        logger.critical("billing.audit_fallback_persist_failed", exc_info=True)


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
    if not any(event_type.startswith(prefix) for prefix in _ALLOWED_EVENT_PREFIXES):
        logger.warning(
            "billing.unknown_event_type",
            event_type=event_type,
            user_id=str(user_id),
        )

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
        fallback_payload = {
            "event_type": event_type,
            "user_id": str(user_id),
            "subscription_id": subscription_id,
            "request_id": request_id,
            "source": source,
            "old_state": _safe_state(old_state),
            "new_state": _safe_state(new_state),
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        try:
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
        except Exception:
            _persist_failed_billing_audit(fallback_payload)
            logger.error(
                "billing.audit_write_failed",
                event_type=event_type,
                user_id=str(user_id),
                exc_info=True,
            )
