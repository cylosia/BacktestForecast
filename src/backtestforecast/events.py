from __future__ import annotations

import atexit
import json
import threading
from typing import Any
from uuid import UUID

from backtestforecast.config import get_settings, register_invalidation_callback
from backtestforecast.models import JobStatus
from backtestforecast.observability import get_logger

logger = get_logger("events")

_RESERVED_PAYLOAD_KEYS = frozenset({"v", "status", "job_id"})

_redis_client = None
_redis_lock = threading.Lock()
_atexit_registered = False


def _get_redis():
    """Return a lazily-initialised, reusable sync Redis client."""
    global _redis_client, _atexit_registered
    if _redis_client is not None:
        return _redis_client

    from redis import Redis

    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        settings = get_settings()
        # Use the dedicated cache/SSE Redis URL to isolate pub/sub traffic
        # from the Celery broker. The model validator guarantees
        # redis_cache_url is always populated (defaults to redis_url).
        _redis_client = Redis.from_url(
            settings.redis_cache_url,
            decode_responses=True,
            socket_timeout=settings.sse_redis_socket_timeout,
            socket_connect_timeout=settings.sse_redis_connect_timeout,
            retry_on_timeout=True,
            max_connections=settings.sse_redis_max_connections,
        )
        if not _atexit_registered:
            atexit.register(_shutdown_redis)
            _atexit_registered = True
        return _redis_client


def _reset_redis() -> None:
    """Close the current Redis client and clear the reference so next call reconnects."""
    global _redis_client
    with _redis_lock:
        client = _redis_client
        _redis_client = None
    if client is not None:
        try:
            client.close()
        except Exception:  # Intentional: best-effort cleanup; failure to close an old
            # connection must not prevent reconnection on the next call.
            pass


register_invalidation_callback(_reset_redis)


def _shutdown_redis() -> None:
    _reset_redis()


def publish_job_status(
    job_type: str,
    job_id: UUID,
    status: str,
    *,
    metadata: dict | None = None,
) -> None:
    """Publish a job status change to Redis Pub/Sub for SSE consumers.

    Call from Celery workers whenever a job transitions state.
    """
    from redis.exceptions import RedisError

    from backtestforecast.observability.metrics import REDIS_CONNECTION_ERRORS_TOTAL

    safe_meta = {k: v for k, v in (metadata or {}).items() if k not in _RESERVED_PAYLOAD_KEYS}
    meta_json = json.dumps(safe_meta, default=str)
    if len(meta_json) > 10_000:
        logger.warning("events.metadata_too_large", job_type=job_type, job_id=str(job_id))
        safe_meta = {"_truncated": True}

    channel = f"job:{job_type}:{job_id}:status"
    payload = json.dumps({"v": 1, "status": status, "job_id": str(job_id), **safe_meta}, default=str)

    try:
        for attempt in range(2):
            try:
                client = _get_redis()
                client.publish(channel, payload)
                return
            except RedisError:
                REDIS_CONNECTION_ERRORS_TOTAL.labels(operation="publish").inc()
                if attempt == 0:
                    _reset_redis()
                    continue
                logger.error(
                    "events.publish_failed",
                    channel=channel,
                    status=status,
                    job_type=job_type,
                    job_id=str(job_id),
                    exc_info=True,
                )
                _fallback_persist_status(job_type, job_id, status)
    except Exception:  # Intentional last-resort handler: event publishing is best-effort
        # and must never crash the calling task (which has its own error handling).
        from backtestforecast.observability.metrics import EVENT_PUBLISH_FAILURES_TOTAL
        EVENT_PUBLISH_FAILURES_TOTAL.inc()
        logger.error(
            "events.publish_unexpected_failure",
            channel=channel,
            status=status,
            job_type=job_type,
            job_id=str(job_id),
            exc_info=True,
        )


_JOB_TYPE_MODEL_MAP: dict[str, str] = {
    "backtest": "BacktestRun",
    "export": "ExportJob",
    "scan": "ScannerJob",
    "sweep": "SweepJob",
    "analysis": "SymbolAnalysis",
    "pipeline": "NightlyPipelineRun",
}

_VALID_TARGET_STATUSES = frozenset({
    JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED,
})
_EXPORT_VALID_TARGET_STATUSES = frozenset({
    JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.EXPIRED,
})


def _fallback_persist_status(
    job_type: str, job_id: UUID, status: str,
) -> None:
    """Write status directly to the job row so polling consumers can pick it up.

    Uses an atomic UPDATE … WHERE to avoid a TOCTOU race between the
    read-check and the write that could clobber a terminal status set
    by another worker in the meantime.
    """
    # Statuses that this fallback is allowed to write. Also used in WHERE to
    # protect rows that are already in one of these terminal states.
    target_statuses = _EXPORT_VALID_TARGET_STATUSES if job_type == "export" else _VALID_TARGET_STATUSES
    if status not in target_statuses:
        return
    try:
        from sqlalchemy import update

        from backtestforecast import models
        from backtestforecast.db.session import create_worker_session

        model_name = _JOB_TYPE_MODEL_MAP.get(job_type)
        if model_name is None:
            logger.error("events.fallback_unknown_job_type", job_type=job_type)
            return

        model_cls = getattr(models, model_name, None)
        if model_cls is None:
            logger.error("events.fallback_model_not_found", model_name=model_name, job_type=job_type)
            return

        with create_worker_session() as session:
            from sqlalchemy.sql import func as sa_func
            terminal_statuses = {str(s) for s in (_VALID_TARGET_STATUSES | _EXPORT_VALID_TARGET_STATUSES)}
            update_values: dict[str, Any] = {"status": status, "updated_at": sa_func.now()}
            if status in terminal_statuses:
                update_values["completed_at"] = sa_func.now()
                if hasattr(model_cls, "started_at"):
                    from sqlalchemy import case
                    update_values["started_at"] = case(
                        (model_cls.started_at.is_(None), sa_func.now()),
                        else_=model_cls.started_at,
                    )
            result = session.execute(
                update(model_cls)
                .where(
                    model_cls.id == job_id,
                    model_cls.status.notin_(target_statuses),
                )
                .values(**update_values)
            )
            affected = result.rowcount
            try:
                session.commit()
            except Exception:
                session.rollback()
                raise
            if affected == 0:
                logger.info(
                    "events.fallback_skipped_terminal",
                    job_type=job_type,
                    job_id=str(job_id),
                    requested_status=status,
                )
            else:
                logger.info(
                    "events.fallback_persisted",
                    job_type=job_type,
                    job_id=str(job_id),
                    status=status,
                )
    except Exception:  # Intentional: this is the last-chance fallback path. If even the
        # direct DB write fails, we can only log — there is nothing left to try.
        logger.error(
            "events.fallback_persist_failed",
            job_type=job_type,
            job_id=str(job_id),
            exc_info=True,
        )
