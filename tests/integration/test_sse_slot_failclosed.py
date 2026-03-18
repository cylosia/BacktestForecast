"""Test 69: SSE slot Redis-failure behavior.

Verifies that _acquire_sse_slot falls back to in-process tracking when Redis
raises an exception, and that the Lua-based acquire/release works correctly
when Redis is available.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_acquire_sse_slot_falls_back_on_redis_error():
    """_acquire_sse_slot must fall back to in-process tracking when Redis errors."""
    from apps.api.app.routers.events import _acquire_sse_slot, _sse_user_connections

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=ConnectionError("Redis unavailable"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        acquired, used_redis = await _acquire_sse_slot(user_id)
    assert acquired is True, "Should succeed via in-process fallback"
    assert used_redis is False, "Should indicate Redis was NOT used"


@pytest.mark.asyncio
async def test_acquire_sse_slot_falls_back_on_timeout():
    """_acquire_sse_slot must fall back to in-process tracking on timeout."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=TimeoutError("Redis timeout"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        acquired, used_redis = await _acquire_sse_slot(user_id)
    assert acquired is True, "Should succeed via in-process fallback"
    assert used_redis is False, "Should indicate Redis was NOT used"


@pytest.mark.asyncio
async def test_acquire_sse_slot_falls_back_on_os_error():
    """_acquire_sse_slot must fall back to in-process tracking on OSError."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(side_effect=OSError("Network unreachable"))

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        acquired, used_redis = await _acquire_sse_slot(user_id)
    assert acquired is True, "Should succeed via in-process fallback"
    assert used_redis is False, "Should indicate Redis was NOT used"


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_true_on_success():
    """Control: _acquire_sse_slot returns (True, True) when Lua returns 1."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=1)

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        acquired, used_redis = await _acquire_sse_slot(user_id)
    assert acquired is True
    assert used_redis is True


@pytest.mark.asyncio
async def test_acquire_sse_slot_returns_false_when_over_limit():
    """_acquire_sse_slot returns (False, True) when Lua returns 0 (over limit)."""
    from apps.api.app.routers.events import _acquire_sse_slot

    user_id = uuid4()

    mock_pool = AsyncMock()
    mock_pool.eval = AsyncMock(return_value=0)

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        acquired, used_redis = await _acquire_sse_slot(user_id)
    assert acquired is False, "Should deny when over connection limit"
    assert used_redis is True
