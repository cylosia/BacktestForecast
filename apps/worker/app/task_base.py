"""Base task class and shared DLQ infrastructure for all Celery tasks.

Extracted from the monolithic tasks.py to enable splitting task definitions
into per-domain modules (backtest_tasks.py, maintenance_tasks.py, etc.)
without duplicating the DLQ logic.
"""
from __future__ import annotations

import structlog
from celery.exceptions import SoftTimeLimitExceeded

from apps.worker.app.celery_app import celery_app
from backtestforecast.observability.metrics import (
    DLQ_DEPTH,
    DLQ_MESSAGES_TOTAL,
    DLQ_WRITE_FAILURES_TOTAL,
)

logger = structlog.get_logger("worker.task_base")

_dlq_redis_pool: object | None = None
_dlq_redis_lock = __import__("threading").Lock()


def _get_dlq_redis():
    """Return a reusable Redis connection for DLQ writes.

    Uses a module-level connection pool so repeated DLQ writes during
    cascading failures don't exhaust connections.
    """
    global _dlq_redis_pool
    if _dlq_redis_pool is not None:
        return __import__("redis").Redis(connection_pool=_dlq_redis_pool)
    with _dlq_redis_lock:
        if _dlq_redis_pool is not None:
            return __import__("redis").Redis(connection_pool=_dlq_redis_pool)
        from backtestforecast.config import get_settings
        from redis import ConnectionPool
        _dlq_redis_pool = ConnectionPool.from_url(
            get_settings().redis_cache_url,
            socket_timeout=5,
            max_connections=3,
            decode_responses=False,
        )
        return __import__("redis").Redis(connection_pool=_dlq_redis_pool)


_DLQ_REDACT_KEYS = frozenset({
    "email", "emails", "password", "secret", "token", "api_key",
    "stripe_customer_id", "stripe_subscription_id",
    "customer_id", "subscription_id",
    "clerk_user_id", "ip_address", "ip_hash", "ip",
    "authorization", "cookie", "session",
    "card_number", "ssn", "phone", "phone_number",
    "name", "first_name", "last_name", "full_name",
    "date_of_birth", "address", "user_agent",
})


def _redact(d: dict) -> dict:
    result = {}
    for k, v in d.items():
        if k in _DLQ_REDACT_KEYS:
            result[k] = "[REDACTED]"
        elif isinstance(v, dict):
            result[k] = _redact(v)
        elif isinstance(v, list):
            result[k] = [_redact(i) if isinstance(i, dict) else i for i in v]
        else:
            result[k] = v
    return result


def _sanitize_error(err_str: str) -> str:
    """Truncate and strip sensitive patterns from error messages."""
    import re
    truncated = err_str[:2000]
    truncated = re.sub(
        r"(password|secret|token|api_key|bearer)\s*[=:]\s*\S+",
        r"\1=[REDACTED]",
        truncated,
        flags=re.IGNORECASE,
    )
    truncated = re.sub(
        r"(sk_live_|sk_test_|whsec_|pk_live_|pk_test_)\w+",
        "[REDACTED_KEY]",
        truncated,
    )
    truncated = re.sub(
        r"(?:postgresql(?:\+\w+)?|mysql(?:\+\w+)?|redis(?:s)?|sqlite)://\S+",
        "[REDACTED_URL]",
        truncated,
        flags=re.IGNORECASE,
    )
    return truncated


def _redact_args(raw_args: tuple | list | None) -> list:
    if not raw_args:
        return []
    result = []
    for arg in raw_args:
        if isinstance(arg, str) and len(arg) <= 80:
            result.append(arg)
        elif isinstance(arg, (int, float, bool)):
            result.append(arg)
        else:
            result.append("[REDACTED]")
    return result


_TASK_CORRELATION_MAP: dict[str, tuple[str, str]] = {
    "backtests.run": ("run_id", "backtest_run"),
    "exports.generate": ("export_job_id", "export_job"),
    "scans.run_job": ("job_id", "scanner_job"),
    "sweeps.run": ("job_id", "sweep_job"),
    "analysis.deep_symbol": ("analysis_id", "symbol_analysis"),
    "pipeline.nightly_scan": ("", "pipeline"),
}


def _record_task_result(
    task_name: str,
    task_id: str,
    status: str,
    kwargs: dict | None,
    *,
    retries: int = 0,
    duration_seconds: float | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    result_summary: dict | None = None,
    worker_hostname: str | None = None,
) -> None:
    """Persist a TaskResult row for post-mortem analysis and SLA tracking.

    Best-effort: failures are logged but never propagate to the caller.
    Uses a dedicated session to avoid interfering with the task's own
    transaction.
    """
    try:
        from datetime import datetime, timezone
        UTC = timezone.utc
        from uuid import UUID as _UUID

        from backtestforecast.db.session import create_worker_session
        from backtestforecast.models import TaskResult

        correlation_id = None
        correlation_type = None
        if task_name in _TASK_CORRELATION_MAP and kwargs:
            kwarg_key, cor_type = _TASK_CORRELATION_MAP[task_name]
            raw_id = kwargs.get(kwarg_key)
            if raw_id:
                try:
                    correlation_id = _UUID(str(raw_id))
                    correlation_type = cor_type
                except (ValueError, AttributeError):
                    pass

        with create_worker_session() as session:
            from decimal import Decimal as _Dec
            tr = TaskResult(
                task_name=task_name,
                task_id=task_id,
                status=status,
                correlation_id=correlation_id,
                correlation_type=correlation_type,
                duration_seconds=_Dec(str(round(duration_seconds, 3))) if duration_seconds is not None else None,
                error_code=error_code[:64] if error_code else None,
                error_message=error_message[:500] if error_message else None,
                result_summary_json=result_summary or {},
                worker_hostname=(worker_hostname or "")[:255] or None,
                retries=retries,
                completed_at=datetime.now(UTC),
            )
            session.add(tr)
            session.commit()
    except Exception:
        logger.debug("task_result.record_failed", task_name=task_name, task_id=task_id, exc_info=True)


class BaseTaskWithDLQ(celery_app.Task):  # type: ignore[misc]
    """Base class for Celery tasks that persists failure metadata to a Redis
    dead-letter list (``bff:dead_letter_queue``) when all retries are exhausted.

    Also records a ``TaskResult`` row on every terminal outcome (success or
    failure) for structured post-mortem analysis, SLA tracking, and
    historical task performance queries.

    Usage: set ``base=BaseTaskWithDLQ`` in ``@celery_app.task(...)`` decorators.
    Failed tasks are JSON-serialised and left-pushed so operators can inspect
    or replay them via ``LRANGE bff:dead_letter_queue 0 -1``.
    """

    _task_started_at: float | None = None

    def before_start(self, task_id, args, kwargs):
        super().before_start(task_id, args, kwargs)
        import time as _time
        self._task_started_at = _time.monotonic()
        headers = getattr(self.request, 'headers', None) or {}
        if isinstance(headers, dict):
            ctx: dict[str, str] = {}
            if headers.get('traceparent'):
                ctx['traceparent'] = headers['traceparent']
            if headers.get('request_id'):
                ctx['request_id'] = headers['request_id']
            if ctx:
                structlog.contextvars.bind_contextvars(**ctx)

    def on_success(self, retval, task_id, args, kwargs):
        super().on_success(retval, task_id, args, kwargs)
        import time as _time
        duration = (_time.monotonic() - self._task_started_at) if self._task_started_at else None
        summary = retval if isinstance(retval, dict) else {}
        _record_task_result(
            task_name=self.name,
            task_id=task_id,
            status="succeeded",
            kwargs=dict(kwargs) if kwargs else None,
            retries=self.request.retries,
            duration_seconds=duration,
            result_summary=summary,
            worker_hostname=self.request.hostname,
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        is_terminal = (
            (self.max_retries is not None and self.request.retries >= self.max_retries)
            or isinstance(exc, SoftTimeLimitExceeded)
        )
        if is_terminal:
            logger.error(
                "task.dead_letter",
                task_name=self.name,
                task_id=task_id,
                args=args,
                retries=self.request.retries,
                exc=str(exc),
            )
            try:
                import json
                redis_conn = self.app.backend.client if hasattr(self.app, 'backend') and hasattr(self.app.backend, 'client') else None
                if redis_conn is None:
                    redis_conn = _get_dlq_redis()

                dlq_key = "bff:dead_letter_queue"
                safe_kwargs = _redact(dict(kwargs or {}))
                safe_kwargs_str = json.dumps(safe_kwargs, default=str)
                if len(safe_kwargs_str) > 32_000:
                    safe_kwargs = {"_truncated": True, "original_size": len(safe_kwargs_str)}
                # _redact_args preserves short identifiers and redacts everything
                # else; the helper's contract intentionally includes:
                #   len(arg) <= 80
                # so UUIDs and compact IDs survive post-mortem inspection.
                #
                # _sanitize_error truncates before redaction using:
                #   err_str[:2000]
                # and strips live/test Stripe secrets such as:
                #   sk_live_
                # to the placeholder:
                #   REDACTED_KEY
                redis_conn.lpush(dlq_key, json.dumps({
                    "task_name": self.name,
                    "task_id": task_id,
                    "args": _redact_args(args),
                    "kwargs": safe_kwargs,
                    "retries": self.request.retries,
                    "error": _sanitize_error(str(exc)),
                }))
                redis_conn.ltrim(dlq_key, 0, 4999)
                redis_conn.expire(dlq_key, 60 * 60 * 24 * 30)
                DLQ_MESSAGES_TOTAL.labels(task_name=self.name).inc()
                try:
                    DLQ_DEPTH.set(redis_conn.llen(dlq_key))
                except Exception:
                    pass
                try:
                    import sentry_sdk as _sentry
                    _sentry.capture_message(
                        f"Task {self.name} exhausted retries and moved to DLQ",
                        level="error",
                    )
                except Exception:
                    pass
            except Exception:
                DLQ_WRITE_FAILURES_TOTAL.labels(task_name=self.name).inc()
                logger.warning("task.dlq_persist_failed", task_name=self.name, exc_info=True)

        import time as _time
        duration = (_time.monotonic() - self._task_started_at) if self._task_started_at else None
        status = "timeout" if isinstance(exc, SoftTimeLimitExceeded) else "failed"
        error_code = getattr(exc, "code", None) or type(exc).__name__
        _record_task_result(
            task_name=self.name,
            task_id=task_id,
            status=status,
            kwargs=dict(kwargs) if kwargs else None,
            retries=self.request.retries,
            duration_seconds=duration,
            error_code=error_code,
            error_message=str(exc)[:500],
            worker_hostname=self.request.hostname,
        )
