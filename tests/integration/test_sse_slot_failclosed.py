"""Test 69: SSE slot fail-closed behavior.

Verifies that _acquire_sse_slot returns False when Redis raises an exception,
ensuring the system fails closed (denies the connection) rather than failing
open.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4

import pytest


@pytest.fixture()
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_acquire_sse_slot_returns_false_on_redis_error(event_loop):
    """_acquire_sse_slot must return False when Redis raises any exception."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=ConnectionError("Redis unavailable"))

    async def _run():
        with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
            return await _acquire_sse_slot(user_id)

    result = event_loop.run_until_complete(_run())
    assert result is False, "Should fail closed (return False) when Redis errors"


def test_acquire_sse_slot_returns_false_on_timeout(event_loop):
    """_acquire_sse_slot must return False on timeout."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=TimeoutError("Redis timeout"))

    async def _run():
        with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
            return await _acquire_sse_slot(user_id)

    result = event_loop.run_until_complete(_run())
    assert result is False, "Should fail closed on timeout"


def test_acquire_sse_slot_returns_false_on_os_error(event_loop):
    """_acquire_sse_slot must return False on OSError."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=OSError("Network unreachable"))

    async def _run():
        with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
            return await _acquire_sse_slot(user_id)

    result = event_loop.run_until_complete(_run())
    assert result is False, "Should fail closed on OSError"


def test_acquire_sse_slot_returns_true_on_success(event_loop):
    """Control: _acquire_sse_slot returns True when Redis returns 1."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=1)

    async def _run():
        with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
            return await _acquire_sse_slot(user_id)

    result = event_loop.run_until_complete(_run())
    assert result is True


def test_acquire_sse_slot_returns_false_when_over_limit(event_loop):
    """_acquire_sse_slot returns False when Lua returns 0 (over limit)."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=0)

    async def _run():
        with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
            return await _acquire_sse_slot(user_id)

    result = event_loop.run_until_complete(_run())
    assert result is False, "Should deny when over connection limit"
