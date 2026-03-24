from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import suppress
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sse_starlette.sse import EventSourceResponse

from apps.api.app.dependencies import get_current_user_readonly
from backtestforecast.config import get_settings, register_invalidation_callback
from backtestforecast.errors import NotFoundError
from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SweepJob, SymbolAnalysis, User
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
# Per-process limit on simultaneous SSE connections. With N uvicorn workers,
# the effective server-wide limit is N x SSE_MAX_CONNECTIONS_PROCESS.
# Per-user limits are enforced via Redis (see _acquire_sse_slot).
#
# IMPORTANT: Each SSE connection holds a Redis Pub/Sub subscription for its
# lifetime. This value MUST NOT exceed sse_redis_max_connections (default 50)
# or SSE subscribers will block waiting for a Redis connection.
SSE_MAX_CONNECTIONS_PROCESS = 45

_SSE_CONN_KEY_PREFIX = "sse:connections:"
_SSE_CONN_TTL = 45

_sse_process_connections = 0
_sse_process_async_lock = asyncio.Lock()

_sse_user_connections: dict[str, int] = {}
_sse_user_connections_lock = asyncio.Lock()


async def _sse_process_dec() -> None:
    global _sse_process_connections
    async with _sse_process_async_lock:
        _sse_process_connections = max(0, _sse_process_connections - 1)


_async_redis_pool = None
_async_redis_lock = asyncio.Lock()
_async_redis_last_ping: float = 0.0

_REDIS_PING_INTERVAL = 60.0


async def _get_async_redis():
    """Return the shared async Redis pool for slot acquire/release operations.

    This pool is used **only** for short-lived atomic Lua scripts that manage
    per-user connection slot counts.  Pub/Sub subscriptions use dedicated
    per-connection clients created by ``_create_subscription_client()`` so
    that a failure on one subscription never disrupts other SSE streams.

    Validates pool health via ``ping()`` at most once every 60 s; recreates on
    failure.
    """
    import time as _time

    from redis.exceptions import RedisError

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
                with suppress(Exception):
                    await _async_redis_pool.aclose()
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
    """Close and discard the shared slot-management pool (suppresses errors).

    The shared pool is only used for slot acquire/release Lua scripts.
    Per-connection subscription clients are managed within each
    ``_subscribe_redis`` generator and cleaned up independently.
    """
    await _close_async_redis(suppress_errors=True)


def _invalidate_async_redis_sync() -> None:
    global _async_redis_pool, _async_redis_last_ping
    pool = _async_redis_pool
    _async_redis_pool = None
    _async_redis_last_ping = 0.0
    if pool is None:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        with suppress(Exception):
            asyncio.run(pool.aclose())
        return
    with suppress(Exception):
        loop.create_task(pool.aclose())


async def shutdown_async_redis() -> None:
    """Close the shared async Redis pool. Called from app lifespan shutdown."""
    await _close_async_redis(suppress_errors=False)


register_invalidation_callback(_invalidate_async_redis_sync)


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
    from backtestforecast.db.session import create_session

    with create_session() as db:
        stmt = select(model.id).where(model.id == resource_id, model.user_id == user_id)
        if db.execute(stmt).first() is None:
            raise NotFoundError("Resource not found.")
        return True


async def _create_subscription_client():
    """Create a dedicated async Redis client for a single SSE subscription.

    Each SSE connection gets its own lightweight client so that a failure
    (or close) on one subscription does not disrupt any other concurrent
    SSE stream.  The shared pool (``_get_async_redis``) is still used for
    short-lived slot acquire/release operations.
    """
    import redis.asyncio as aioredis

    settings = get_settings()
    return aioredis.from_url(
        settings.redis_cache_url,
        decode_responses=True,
        max_connections=1,
        socket_timeout=settings.sse_redis_socket_timeout,
        socket_connect_timeout=settings.sse_redis_connect_timeout,
    )


async def _subscribe_redis(channel: str, *, max_reconnects: int = 2) -> AsyncGenerator[str | None, None]:
    """Subscribe to a Redis Pub/Sub channel and yield messages.

    Each call creates a **dedicated** Redis client so that reconnection
    or failure on one SSE stream never disrupts other concurrent streams.
    The client is closed when the generator exits.

    Yields ``None`` when no message arrives within the heartbeat window so
    callers can check deadlines / disconnections during quiet periods.
    Automatically retries subscription up to *max_reconnects* times on
    transient Redis errors.
    """
    from redis.exceptions import RedisError

    for attempt in range(max_reconnects + 1):
        client = await _create_subscription_client()
        pubsub = client.pubsub()
        try:
            await pubsub.subscribe(channel)
            while True:
                try:
                    async with asyncio.timeout(SSE_TIMEOUT_SECONDS):
                        message = await pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=SSE_HEARTBEAT_SECONDS,
                        )
                except TimeoutError:
                    logger.info("sse.subscription_timeout", channel=channel)
                    return
                if message and message["type"] == "message":
                    raw = message["data"]
                    if isinstance(raw, str) and len(raw) > 65_536:
                        logger.warning("sse.message_too_large", channel=channel, size=len(raw))
                        continue
                    if isinstance(raw, str):
                        import json as _json
                        try:
                            _json.loads(raw)
                        except (ValueError, TypeError):
                            logger.warning("sse.invalid_json", channel=channel)
                            continue
                    yield raw
                else:
                    yield None
        except (RedisError, OSError):
            if attempt < max_reconnects:
                logger.warning("sse.redis_subscription_retry", channel=channel, attempt=attempt + 1)
                await asyncio.sleep(1)
                continue
            raise
        finally:
            with suppress(Exception):
                await pubsub.unsubscribe(channel)
                await pubsub.close()
            with suppress(Exception):
                await client.aclose()


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

_SSE_SLOT_REFRESH_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    return 1
end
return 0
"""


async def _acquire_sse_slot_in_process(user_id: UUID) -> bool:
    """Fallback: acquire a per-user slot using an in-process dict when Redis is unavailable."""
    uid = str(user_id)
    async with _sse_user_connections_lock:
        current = _sse_user_connections.get(uid, 0)
        if current >= SSE_MAX_CONNECTIONS_PER_USER:
            return False
        _sse_user_connections[uid] = current + 1
        return True


async def _release_sse_slot_in_process(user_id: UUID) -> None:
    """Fallback: release a per-user slot from the in-process dict."""
    uid = str(user_id)
    async with _sse_user_connections_lock:
        current = _sse_user_connections.get(uid, 0)
        if current <= 1:
            _sse_user_connections.pop(uid, None)
        else:
            _sse_user_connections[uid] = current - 1


async def _acquire_sse_slot(user_id: UUID) -> tuple[bool, bool]:
    """Try to acquire a per-user SSE connection slot atomically via Lua.

    Returns (acquired, used_redis). In production/staging this fails closed
    when Redis coordination is unavailable; only dev/test use the in-process
    fallback path.
    """
    try:
        pool = await _get_async_redis()
        if pool is None:
            settings = get_settings()
            if settings.app_env in ("production", "staging"):
                logger.error("sse.acquire_slot_fail_closed", user_id=str(user_id))
                return False, False
            acquired = await _acquire_sse_slot_in_process(user_id)
            return acquired, False
        key = f"{_SSE_CONN_KEY_PREFIX}{user_id}"
        result = await pool.eval(
            _SSE_SLOT_ACQUIRE_LUA, 1, key, _SSE_CONN_TTL, SSE_MAX_CONNECTIONS_PER_USER,
        )
        return int(result) == 1, True
    except Exception:
        logger.warning("sse.acquire_slot_redis_error", user_id=str(user_id), exc_info=True)
        settings = get_settings()
        if settings.app_env in ("production", "staging"):
            return False, False
        acquired = await _acquire_sse_slot_in_process(user_id)
        return acquired, False


async def _release_sse_slot(user_id: UUID) -> None:
    """Release a per-user SSE connection slot atomically via Lua.

    Uses a Lua script to make DECR + conditional DEL atomic, preventing a
    race where a concurrent acquire INCRs between DECR and DEL.  Falls back
    to the in-process counter if Redis is unreachable so the slot is not
    leaked for the full TTL duration.
    """
    try:
        pool = await _get_async_redis()
        key = f"{_SSE_CONN_KEY_PREFIX}{user_id}"
        await pool.eval(_SSE_SLOT_RELEASE_LUA, 1, key)
    except Exception:
        from backtestforecast.observability.metrics import REDIS_CONNECTION_ERRORS_TOTAL
        REDIS_CONNECTION_ERRORS_TOTAL.labels(operation="sse_slot_release").inc()
        logger.error("sse.release_slot_redis_error", user_id=str(user_id), exc_info=True)
        await _release_sse_slot_in_process(user_id)


async def _refresh_sse_slot(user_id: UUID) -> None:
    try:
        pool = await _get_async_redis()
        if pool is None:
            return
        key = f"{_SSE_CONN_KEY_PREFIX}{user_id}"
        await pool.eval(_SSE_SLOT_REFRESH_LUA, 1, key, _SSE_CONN_TTL)
    except Exception:
        logger.warning("sse.refresh_slot_redis_error", user_id=str(user_id), exc_info=True)


async def _event_stream(
    channel: str,
    request: Request,
    user_id: UUID,
) -> AsyncGenerator[dict[str, str], None]:
    """Wrap Redis subscription in SSE event format with heartbeats and timeout."""
    from redis.exceptions import RedisError

    global _sse_process_connections
    over_process_limit = False
    async with _sse_process_async_lock:
        if _sse_process_connections >= SSE_MAX_CONNECTIONS_PROCESS:
            over_process_limit = True
        else:
            _sse_process_connections += 1

    if over_process_limit:
        yield {"event": "error", "data": "Server connection limit reached. Please try again later."}
        yield {"event": "done", "data": "stream_ended"}
        return

    process_slot_acquired = True
    user_slot_acquired = False
    used_redis_for_slot = False
    try:
        acquired, used_redis_for_slot = await _acquire_sse_slot(user_id)
        if not acquired:
            yield {"event": "error", "data": "Too many active event streams. Close other tabs and retry."}
            yield {"event": "done", "data": "stream_ended"}
            return
        user_slot_acquired = True

        ACTIVE_SSE_CONNECTIONS.inc()
        deadline = asyncio.get_running_loop().time() + SSE_TIMEOUT_SECONDS

        try:
            async for data in _subscribe_redis(channel):
                if await request.is_disconnected():
                    break
                await _refresh_sse_slot(user_id)
                if asyncio.get_running_loop().time() > deadline:
                    yield {"event": "timeout", "data": "Connection timed out"}
                    break
                if data is not None:
                    yield {"event": "status", "data": data}
            yield {"event": "done", "data": "stream_ended"}
        except (RedisError, OSError) as exc:
            logger.warning("sse.redis_error", channel=channel, error=str(exc))
            yield {"event": "error", "data": "Event stream unavailable. Please poll for status instead."}
        finally:
            ACTIVE_SSE_CONNECTIONS.dec()
    finally:
        if process_slot_acquired:
            await _sse_process_dec()
        if user_slot_acquired:
            if used_redis_for_slot:
                await _release_sse_slot(user_id)
            else:
                await _release_sse_slot_in_process(user_id)


@router.get("/backtests/{run_id}", responses=_SSE_RESPONSES)
async def backtest_events(
    run_id: UUID,
    request: Request,
    user: User = Depends(get_current_user_readonly),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, BacktestRun, run_id, user.id)
    channel = f"job:backtest:{run_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache, no-store", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/scans/{job_id}", responses=_SSE_RESPONSES)
async def scan_events(
    job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user_readonly),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, ScannerJob, job_id, user.id)
    channel = f"job:scan:{job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache, no-store", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/sweeps/{job_id}", responses=_SSE_RESPONSES)
async def sweep_events(
    job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user_readonly),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, SweepJob, job_id, user.id)
    channel = f"job:sweep:{job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache, no-store", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/exports/{export_job_id}", responses=_SSE_RESPONSES)
async def export_events(
    export_job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user_readonly),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, ExportJob, export_job_id, user.id)
    channel = f"job:export:{export_job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache, no-store", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/analyses/{analysis_id}", responses=_SSE_RESPONSES)
async def analysis_events(
    analysis_id: UUID,
    request: Request,
    user: User = Depends(get_current_user_readonly),
) -> EventSourceResponse:
    await asyncio.to_thread(_check_sse_rate, user.id)
    await asyncio.to_thread(_verify_ownership, SymbolAnalysis, analysis_id, user.id)
    channel = f"job:analysis:{analysis_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request, user.id),
        ping=SSE_HEARTBEAT_SECONDS,
        headers={"Cache-Control": "no-cache, no-store", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
