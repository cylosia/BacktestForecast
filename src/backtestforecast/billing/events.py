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
from backtestforecast.observability.metrics import (
    BILLING_AUDIT_REPLAYED_TOTAL,
    BILLING_AUDIT_WRITE_FAILURES_TOTAL,
)

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


def _replay_payload(session: "Session", payload: dict[str, object]) -> None:
    from backtestforecast.services.audit import AuditService

    user_id_raw = payload.get("user_id")
    user_id = UUID(str(user_id_raw)) if user_id_raw else None
    subscription_id = payload.get("subscription_id")
    request_id = payload.get("request_id")
    source = str(payload.get("source") or "fallback")
    event_type = str(payload["event_type"])
    old_state = payload.get("old_state")
    new_state = payload.get("new_state")

    AuditService(session).record_always(
        event_type=f"billing.{event_type}",
        subject_type="stripe_subscription",
        subject_id=str(subscription_id) if subscription_id is not None else None,
        user_id=user_id,
        request_id=str(request_id) if request_id is not None else None,
        metadata={
            "old_state": old_state if isinstance(old_state, dict) else None,
            "new_state": new_state if isinstance(new_state, dict) else None,
            "source": source,
            "replayed_from_fallback": True,
            "fallback_recorded_at": payload.get("recorded_at"),
        },
    )


def drain_deferred_billing_audits(session: "Session", *, batch_size: int = 100) -> dict[str, int]:
    settings = get_settings()
    redis_url = settings.redis_cache_url or settings.redis_url
    drained = 0
    failed = 0
    scanned = 0

    if redis_url:
        try:
            import redis

            client = redis.Redis.from_url(redis_url, socket_timeout=5, socket_connect_timeout=5, decode_responses=True)
            while scanned < batch_size:
                raw_payload = client.lindex(_BILLING_AUDIT_FALLBACK_REDIS_KEY, -1)
                if raw_payload is None:
                    break
                scanned += 1
                payload = json.loads(raw_payload)
                try:
                    _replay_payload(session, payload)
                    session.flush()
                    session.commit()
                    client.rpop(_BILLING_AUDIT_FALLBACK_REDIS_KEY)
                    drained += 1
                    BILLING_AUDIT_REPLAYED_TOTAL.labels(source="redis").inc()
                except Exception:
                    session.rollback()
                    failed += 1
                    logger.error("billing.audit_replay_failed", source="redis", exc_info=True)
                    break
        except Exception:
            session.rollback()
            logger.error("billing.audit_replay_redis_unavailable", exc_info=True)

    if scanned >= batch_size:
        return {"drained": drained, "failed": failed, "scanned": scanned}

    remaining_capacity = batch_size - scanned
    if not _BILLING_AUDIT_FALLBACK_FILE.exists():
        return {"drained": drained, "failed": failed, "scanned": scanned}

    retained_lines: list[str] = []
    drained_from_file = 0
    try:
        original_lines = _BILLING_AUDIT_FALLBACK_FILE.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(original_lines):
            if not line:
                continue
            if drained_from_file >= remaining_capacity:
                retained_lines.append(line)
                continue
            scanned += 1
            payload = json.loads(line)
            try:
                _replay_payload(session, payload)
                session.flush()
                session.commit()
                drained += 1
                drained_from_file += 1
                BILLING_AUDIT_REPLAYED_TOTAL.labels(source="file").inc()
            except Exception:
                session.rollback()
                retained_lines.extend(original_lines[index:])
                failed += 1
                logger.error("billing.audit_replay_failed", source="file", exc_info=True)
                break
        if retained_lines:
            _BILLING_AUDIT_FALLBACK_FILE.write_text(
                "\n".join(retained_lines) + "\n",
                encoding="utf-8",
            )
        else:
            _BILLING_AUDIT_FALLBACK_FILE.unlink(missing_ok=True)
    except FileNotFoundError:
        pass

    return {"drained": drained, "failed": failed, "scanned": scanned}


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
            BILLING_AUDIT_WRITE_FAILURES_TOTAL.labels(source=source).inc()
            logger.error(
                "billing.audit_write_failed",
                event_type=event_type,
                user_id=str(user_id),
                exc_info=True,
            )
