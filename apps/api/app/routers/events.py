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
from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, User

router = APIRouter(prefix="/events", tags=["events"])
logger = structlog.get_logger("api.events")

SSE_TIMEOUT_SECONDS = 300
SSE_HEARTBEAT_SECONDS = 15


def _verify_ownership(db: Session, model: type, resource_id: UUID, user_id: UUID) -> None:
    """Raise NotFoundError unless the resource belongs to the user."""
    stmt = select(model.id).where(model.id == resource_id, model.user_id == user_id)
    if db.execute(stmt).first() is None:
        raise NotFoundError("Resource not found.")


async def _subscribe_redis(channel: str) -> AsyncGenerator[str, None]:
    """Subscribe to a Redis Pub/Sub channel and yield messages."""
    import redis.asyncio as aioredis

    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        pubsub = client.pubsub()
        await pubsub.subscribe(channel)
        try:
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=SSE_HEARTBEAT_SECONDS,
                )
                if message and message["type"] == "message":
                    yield message["data"]
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
    finally:
        await client.aclose()


async def _event_stream(
    channel: str,
    request: Request,
) -> AsyncGenerator[dict[str, str], None]:
    """Wrap Redis subscription in SSE event format with heartbeats and timeout."""
    deadline = asyncio.get_running_loop().time() + SSE_TIMEOUT_SECONDS

    async for data in _subscribe_redis(channel):
        if await request.is_disconnected():
            break
        if asyncio.get_running_loop().time() > deadline:
            yield {"event": "timeout", "data": "Connection timed out"}
            break
        yield {"event": "status", "data": data}

    yield {"event": "done", "data": "stream_ended"}


@router.get("/backtests/{run_id}")
async def backtest_events(
    run_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EventSourceResponse:
    _verify_ownership(db, BacktestRun, run_id, user.id)
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
    channel = f"job:export:{export_job_id}:status"
    logger.info("sse.subscribe", channel=channel, user_id=str(user.id))
    return EventSourceResponse(
        _event_stream(channel, request),
        ping=SSE_HEARTBEAT_SECONDS,
    )
