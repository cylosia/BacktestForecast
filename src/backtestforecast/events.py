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
            logger.warning("events.publish_failed", channel=channel, status=status, exc_info=True)
