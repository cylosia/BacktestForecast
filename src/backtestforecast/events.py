from __future__ import annotations

import json
from uuid import UUID

from backtestforecast.config import get_settings
from backtestforecast.observability import get_logger

logger = get_logger("events")


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
    from redis import Redis
    from redis.exceptions import RedisError

    channel = f"job:{job_type}:{job_id}:status"
    payload = json.dumps({"status": status, "job_id": str(job_id), **(metadata or {})})

    try:
        settings = get_settings()
        client = Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            client.publish(channel, payload)
        finally:
            client.close()
    except RedisError:
        logger.warning("events.publish_failed", channel=channel, status=status, exc_info=True)
