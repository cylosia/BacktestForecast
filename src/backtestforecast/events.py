from __future__ import annotations

import atexit
import json
import threading
from uuid import UUID

from backtestforecast.config import get_settings
from backtestforecast.observability import get_logger

logger = get_logger("events")

_redis_client = None
_redis_lock = threading.Lock()


def _get_redis():
    """Return a lazily-initialised, reusable sync Redis client."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    from redis import Redis

    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        settings = get_settings()
        _redis_client = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=2.0,
            retry_on_timeout=True,
        )
        atexit.register(_shutdown_redis)
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
        except Exception:
            pass


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

    channel = f"job:{job_type}:{job_id}:status"
    payload = json.dumps({"status": status, "job_id": str(job_id), **(metadata or {})})

    for attempt in range(2):
        try:
            client = _get_redis()
            client.publish(channel, payload)
            return
        except RedisError:
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


_JOB_TYPE_MODEL_MAP: dict[str, str] = {
    "backtest": "BacktestRun",
    "export": "ExportJob",
    "scan": "ScannerJob",
    "analysis": "SymbolAnalysis",
}


def _fallback_persist_status(job_type: str, job_id: UUID, status: str) -> None:
    """Write status directly to the job row so polling consumers can pick it up."""
    try:
        from backtestforecast import models
        from backtestforecast.db.session import SessionLocal

        model_name = _JOB_TYPE_MODEL_MAP.get(job_type)
        if model_name is None:
            logger.error("events.fallback_unknown_job_type", job_type=job_type)
            return

        model_cls = getattr(models, model_name, None)
        if model_cls is None:
            return

        with SessionLocal() as session:
            obj = session.get(model_cls, job_id)
            if obj is not None and hasattr(obj, "status"):
                obj.status = status
                session.commit()
                logger.info(
                    "events.fallback_persisted",
                    job_type=job_type,
                    job_id=str(job_id),
                    status=status,
                )
    except Exception:
        logger.error(
            "events.fallback_persist_failed",
            job_type=job_type,
            job_id=str(job_id),
            exc_info=True,
        )
