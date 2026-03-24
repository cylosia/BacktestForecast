"""Test SSE slot coordination stays bounded during Redis failures."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


def test_sse_slot_ttl_is_short_lived():
    from apps.api.app.routers import events

    assert events._SSE_CONN_TTL == 45


@pytest.mark.asyncio
async def test_release_slot_handles_redis_error():
    """_release_sse_slot should not raise when Redis fails."""
    import uuid

    from apps.api.app.routers.events import _release_sse_slot

    with patch("apps.api.app.routers.events._get_async_redis", new_callable=AsyncMock) as mock_redis:
        mock_pool = AsyncMock()
        mock_pool.eval.side_effect = ConnectionError("Redis down")
        mock_redis.return_value = mock_pool
        await _release_sse_slot(uuid.uuid4())


@pytest.mark.asyncio
async def test_refresh_slot_renews_distributed_ttl():
    import uuid

    from apps.api.app.routers.events import _SSE_CONN_TTL, _refresh_sse_slot

    with patch("apps.api.app.routers.events._get_async_redis", new_callable=AsyncMock) as mock_redis:
        mock_pool = AsyncMock()
        mock_redis.return_value = mock_pool

        user_id = uuid.uuid4()
        await _refresh_sse_slot(user_id)

        mock_pool.eval.assert_awaited_once()
        _, key_count, key, ttl = mock_pool.eval.await_args.args
        assert key_count == 1
        assert str(user_id) in key
        assert ttl == _SSE_CONN_TTL
