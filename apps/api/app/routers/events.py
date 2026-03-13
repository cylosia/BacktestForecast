from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from apps.api.app.dependencies import get_current_user
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import NotFoundError
from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SymbolAnalysis, User

router = APIRouter(prefix="/events", tags=["events"])
logger = structlog.get_logger("api.events")

SSE_TIMEOUT_SECONDS = 300
SSE_HEARTBEAT_SECONDS = 15

_async_redis_pool = None
_async_redis_lock = asyncio.Lock()


async def _get_async_redis():
    """Return a lazily-initialised shared async Redis connection pool.

    Validates pool health via ``ping()`` before reuse; recreates on failure.
    """
    from redis.exceptions import RedisError

    global _async_redis_pool
    if _async_redis_pool is not None:
        try:
            await _async_redis_pool.ping()
        except (RedisError, OSError):
            logger.warning("sse.redis_pool_stale", action="recreating")
            _async_redis_pool = None
    if _async_redis_pool is not None:
        return _async_redis_pool
    async with _async_redis_lock:
        if _async_redis_pool is not None:
            return _async_redis_pool
        import redis.asyncio as aioredis

        settings = get_settings()
        _async_redis_pool = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=50,
            socket_timeout=10.0,
            socket_connect_timeout=5.0,
        )
    return _async_redis_pool


async def _invalidate_async_redis() -> None:
    """Close and discard the shared pool so it is recreated on next request."""
    global _async_redis_pool
    async with _async_redis_lock:
        if _async_redis_pool is not None:
            try:
                await _async_redis_pool.aclose()
            except Exception:
                pass
            _async_redis_pool = None


async def shutdown_async_redis() -> None:
    """Close the shared async Redis pool. Called from app lifespan shutdown."""
    global _async_redis_pool
    if _async_redis_pool is not None:
        await _async_redis_pool.aclose()
        _async_redis_pool = None


def _verify_ownership(db: Session, model: type, resource_id: UUID, user_id: UUID) -> None:
    """Raise NotFoundError unless the resource belongs to the user."""
    stmt = select(model.id).where(model.id == resource_id, model.user_id == user_id)
    if db.execute(stmt).first() is None:
        raise NotFoundError("Resource not found.")


async def _subscribe_redis(channel: str) -> AsyncGenerator[str | None, None]:
    """Subscribe to a Redis Pub/Sub channel and yield messages.

    Yields ``None`` when no message arrives within the heartbeat window so
    callers can check deadlines / disconnections during quiet periods.
    """
    pool = await _get_async_redis()
    pubsub = pool.pubsub()
    await pubsub.subscribe(channel)
    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=SSE_HEARTBEAT_SECONDS,
            )
            if message and message["type"] == "message":
                yield message["data"]
            else:
                yield None
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()


async def _event_stream(
    channel: str,
    request: Request,
) -> AsyncGenerator[dict[str, str], None]:
    """Wrap Redis subscription in SSE event format with heartbeats and timeout."""
    from redis.exceptions import RedisError

    deadline = asyncio.get_running_loop().time() + SSE_TIMEOUT_SECONDS

    try:
        async for data in _subscribe_redis(channel):
            if await request.is_disconnected():
                break
            if asyncio.get_running_loop().time() > deadline:
                yield {"event": "timeout", "data": "Connection timed out"}
                break
            if data is not None:
                yield {"event": "status", "data": data}
    except (RedisError, OSError) as exc:
        logger.warning("sse.redis_error", channel=channel, error=str(exc))
        await _invalidate_async_redis()
        yield {"event": "error", "data": "Event stream unavailable. Please poll for status instead."}

    yield {"event": "done", "data": "stream_ended"}


@router.get("/backtests/{run_id}")
async def backtest_events(
    run_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EventSourceResponse:
    _verify_ownership(db, BacktestRun, run_id, user.id)
    db.close()
    channel = f"job:backtest:{run_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request),
        ping=SSE_HEARTBEAT_SECONDS,
    )


@router.get("/scans/{job_id}")
async def scan_events(
    job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EventSourceResponse:
    _verify_ownership(db, ScannerJob, job_id, user.id)
    db.close()
    channel = f"job:scan:{job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request),
        ping=SSE_HEARTBEAT_SECONDS,
    )


@router.get("/exports/{export_job_id}")
async def export_events(
    export_job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EventSourceResponse:
    _verify_ownership(db, ExportJob, export_job_id, user.id)
    db.close()
    channel = f"job:export:{export_job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request),
        ping=SSE_HEARTBEAT_SECONDS,
    )


@router.get("/analyses/{analysis_id}")
async def analysis_events(
    analysis_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EventSourceResponse:
    _verify_ownership(db, SymbolAnalysis, analysis_id, user.id)
    db.close()
    channel = f"job:analysis:{analysis_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request),
        ping=SSE_HEARTBEAT_SECONDS,
    )
