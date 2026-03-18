"""Test SSE slot release handles Redis failure gracefully."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_release_slot_handles_redis_error():
    """_release_sse_slot should not raise when Redis fails."""
    from apps.api.app.routers.events import _release_sse_slot
    import uuid

    with patch("apps.api.app.routers.events._get_async_redis", new_callable=AsyncMock) as mock_redis:
        mock_pool = AsyncMock()
        mock_pool.eval.side_effect = ConnectionError("Redis down")
        mock_redis.return_value = mock_pool
        await _release_sse_slot(uuid.uuid4())
