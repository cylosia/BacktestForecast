"""Test 69: SSE slot fail-closed behavior.

Verifies that _acquire_sse_slot returns False when Redis raises an exception,
ensuring the system fails closed (denies the connection) rather than failing
open.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_false_on_redis_error():
    """_acquire_sse_slot must return False when Redis raises any exception."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=ConnectionError("Redis unavailable"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        result = await _acquire_sse_slot(user_id)
    assert result is False, "Should fail closed (return False) when Redis errors"


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_false_on_timeout():
    """_acquire_sse_slot must return False on timeout."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=TimeoutError("Redis timeout"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        result = await _acquire_sse_slot(user_id)
    assert result is False, "Should fail closed on timeout"


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_false_on_os_error():
    """_acquire_sse_slot must return False on OSError."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=OSError("Network unreachable"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        result = await _acquire_sse_slot(user_id)
    assert result is False, "Should fail closed on OSError"


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_true_on_success():
    """Control: _acquire_sse_slot returns True when Redis returns 1."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=1)

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        result = await _acquire_sse_slot(user_id)
    assert result is True


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_false_when_over_limit():
    """_acquire_sse_slot returns False when Lua returns 0 (over limit)."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=0)

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        result = await _acquire_sse_slot(user_id)
    assert result is False, "Should deny when over connection limit"
