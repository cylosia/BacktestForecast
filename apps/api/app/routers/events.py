from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sse_starlette.sse import EventSourceResponse

from apps.api.app.dependencies import get_current_user
from backtestforecast.config import get_settings
from backtestforecast.errors import NotFoundError
from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SymbolAnalysis, User
from backtestforecast.observability.metrics import ACTIVE_SSE_CONNECTIONS
from backtestforecast.security import get_rate_limiter

router = APIRouter(prefix="/events", tags=["events"])
logger = structlog.get_logger("api.events")

_SSE_RESPONSES = {
    200: {
        "description": "Server-Sent Events stream. Events: status, timeout, error, done.",
        "content": {"text/event-stream": {"schema": {"type": "string"}}},
    },
}

SSE_TIMEOUT_SECONDS = 300
SSE_HEARTBEAT_SECONDS = 15
SSE_MAX_CONNECTIONS_PER_USER = 10

_SSE_CONN_KEY_PREFIX = "sse:connections:"
_SSE_CONN_TTL = 600

_async_redis_pool = None
_async_redis_lock = asyncio.Lock()
_async_redis_last_ping: float = 0.0

_REDIS_PING_INTERVAL = 60.0


async def _get_async_redis():
    """Return a lazily-initialised shared async Redis connection pool.

    Validates pool health via ``ping()`` at most once every 60 s; recreates on
    failure.  All checks and mutations are done under the lock to prevent
    races between concurrent coroutines.
    """
    from redis.exceptions import RedisError

    import time as _time

    global _async_redis_pool, _async_redis_last_ping
    now = _time.monotonic()
    # Fast-path read outside the lock is safe here because asyncio's event loop
    # is single-threaded: no concurrent mutation can occur between the check and
    # the return.  If this code were used with threads, a proper memory barrier
    # or lock would be required for the double-checked read.
    if _async_redis_pool is not None and (now - _async_redis_last_ping) < _REDIS_PING_INTERVAL:
        return _async_redis_pool
    async with _async_redis_lock:
        now = _time.monotonic()
        if _async_redis_pool is not None and (now - _async_redis_last_ping) < _REDIS_PING_INTERVAL:
            return _async_redis_pool
        if _async_redis_pool is not None:
            try:
                await _async_redis_pool.ping()
                _async_redis_last_ping = now
                return _async_redis_pool
            except (RedisError, OSError):
                logger.warning("sse.redis_pool_stale", action="recreating")
                try:
                    await _async_redis_pool.aclose()
                except Exception:
                    pass
                _async_redis_pool = None
        import redis.asyncio as aioredis

        settings = get_settings()
        _async_redis_pool = aioredis.from_url(
            settings.redis_cache_url,
            decode_responses=True,
            max_connections=settings.sse_redis_max_connections,
            socket_timeout=settings.sse_redis_socket_timeout,
            socket_connect_timeout=settings.sse_redis_connect_timeout,
        )
        _async_redis_last_ping = _time.monotonic()
    return _async_redis_pool


async def _close_async_redis(*, suppress_errors: bool = True) -> None:
    """Close and discard the shared pool so it is recreated on next request."""
    global _async_redis_pool, _async_redis_last_ping
    async with _async_redis_lock:
        if _async_redis_pool is not None:
            try:
                await _async_redis_pool.aclose()
            except Exception:
                if not suppress_errors:
                    raise
            _async_redis_pool = None
            _async_redis_last_ping = 0.0


async def _invalidate_async_redis() -> None:
    """Alias: close and discard the shared pool (suppresses errors)."""
    await _close_async_redis(suppress_errors=True)


async def shutdown_async_redis() -> None:
    """Close the shared async Redis pool. Called from app lifespan shutdown."""
    await _close_async_redis(suppress_errors=False)


def _check_sse_rate(user_id: UUID) -> None:
    """Enforce a per-user rate limit on SSE connection attempts."""
    settings = get_settings()
    get_rate_limiter().check(
        bucket="events:sse",
        actor_key=str(user_id),
        limit=settings.sse_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )


def _verify_ownership(model: type, resource_id: UUID, user_id: UUID) -> bool:
    """Return True if the resource belongs to the user, raise NotFoundError otherwise.

    Called *before* entering the SSE stream so the ownership check completes
    during the HTTP request phase, not inside the async generator. Uses the
    shared session factory to benefit from connection pooling.
    """
    from backtestforecast.db.session import SessionLocal

    db = SessionLocal()
    try:
        stmt = select(model.id).where(model.id == resource_id, model.user_id == user_id)
        if db.execute(stmt).first() is None:
            raise NotFoundError("Resource not found.")
        return True
    finally:
        db.close()


async def _subscribe_redis(channel: str, *, max_reconnects: int = 2) -> AsyncGenerator[str | None, None]:
    """Subscribe to a Redis Pub/Sub channel and yield messages.

    Yields ``None`` when no message arrives within the heartbeat window so
    callers can check deadlines / disconnections during quiet periods.
    Automatically retries subscription up to *max_reconnects* times on
    transient Redis errors.
    """
    from redis.exceptions import RedisError

    for attempt in range(max_reconnects + 1):
        pool = await _get_async_redis()
        pubsub = pool.pubsub()
        try:
            await pubsub.subscribe(channel)
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=SSE_HEARTBEAT_SECONDS,
                )
                if message and message["type"] == "message":
                    yield message["data"]
                else:
                    yield None
        except (RedisError, OSError):
            if attempt < max_reconnects:
                logger.warning("sse.redis_subscription_retry", channel=channel, attempt=attempt + 1)
                await _invalidate_async_redis()
                await asyncio.sleep(1)
                continue
            raise
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.close()
            except Exception:
                pass


_SSE_SLOT_ACQUIRE_LUA = """
local count = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
if count > tonumber(ARGV[2]) then
    redis.call('DECR', KEYS[1])
    return 0
end
return 1
"""

_SSE_SLOT_RELEASE_LUA = """
local count = redis.call('DECR', KEYS[1])
if count <= 0 then
    redis.call('DEL', KEYS[1])
end
return count
"""


async def _acquire_sse_slot(user_id: UUID) -> bool:
    """Try to acquire a per-user SSE connection slot atomically via Lua."""
    try:
        pool = await _get_async_redis()
        key = f"{_SSE_CONN_KEY_PREFIX}{user_id}"
        result = await pool.eval(
            _SSE_SLOT_ACQUIRE_LUA, 1, key, _SSE_CONN_TTL, SSE_MAX_CONNECTIONS_PER_USER,
        )
        return int(result) == 1
    except Exception:
        logger.warning("sse.acquire_slot_redis_error", user_id=str(user_id), exc_info=True)
        return True


async def _release_sse_slot(user_id: UUID) -> None:
    """Release a per-user SSE connection slot atomically via Lua.

    Uses a Lua script to make DECR + conditional DEL atomic, preventing a
    race where a concurrent acquire INCRs between DECR and DEL.
    """
    try:
        pool = await _get_async_redis()
        key = f"{_SSE_CONN_KEY_PREFIX}{user_id}"
        await pool.eval(_SSE_SLOT_RELEASE_LUA, 1, key)
    except Exception:
        logger.warning("sse.release_slot_redis_error", user_id=str(user_id), exc_info=True)


async def _event_stream(
    channel: str,
    request: Request,
    user_id: UUID,
) -> AsyncGenerator[dict[str, str], None]:
    """Wrap Redis subscription in SSE event format with heartbeats and timeout."""
    from redis.exceptions import RedisError

    acquired = await _acquire_sse_slot(user_id)
    if not acquired:
        yield {"event": "error", "data": "Too many active event streams. Close other tabs and retry."}
        yield {"event": "done", "data": "stream_ended"}
        return

    ACTIVE_SSE_CONNECTIONS.inc()
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
        yield {"event": "error", "data": "Event stream unavailable. Please poll for status instead."}
    finally:
        ACTIVE_SSE_CONNECTIONS.dec()
        await _release_sse_slot(user_id)

    yield {"event": "done", "data": "stream_ended"}


@router.get("/backtests/{run_id}", responses=_SSE_RESPONSES)
async def backtest_events(
    run_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, BacktestRun, run_id, user.id)
    channel = f"job:backtest:{run_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/scans/{job_id}", responses=_SSE_RESPONSES)
async def scan_events(
    job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, ScannerJob, job_id, user.id)
    channel = f"job:scan:{job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/exports/{export_job_id}", responses=_SSE_RESPONSES)
async def export_events(
    export_job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, ExportJob, export_job_id, user.id)
    channel = f"job:export:{export_job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/analyses/{analysis_id}", responses=_SSE_RESPONSES)
async def analysis_events(
    analysis_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, SymbolAnalysis, analysis_id, user.id)
    channel = f"job:analysis:{analysis_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
