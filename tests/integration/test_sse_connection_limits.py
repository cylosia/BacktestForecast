"""Tests for SSE connection limit constants and in-process slot management."""
from __future__ import annotations

from uuid import uuid4

import pytest

from apps.api.app.routers.events import (
    SSE_MAX_CONNECTIONS_PER_USER,
    SSE_MAX_CONNECTIONS_PROCESS,
    _acquire_sse_slot_in_process,
    _release_sse_slot_in_process,
    _sse_user_connections,
)


class TestSSEConstants:
    def test_process_limit_stays_below_redis_pool_limit(self):
        from backtestforecast.config import get_settings

        settings = get_settings()
        assert SSE_MAX_CONNECTIONS_PROCESS > 0
        assert settings.sse_redis_max_connections > SSE_MAX_CONNECTIONS_PROCESS

    def test_per_user_limit_is_10(self):
        assert SSE_MAX_CONNECTIONS_PER_USER == 10

    def test_process_limit_exceeds_per_user_limit(self):
        assert SSE_MAX_CONNECTIONS_PROCESS > SSE_MAX_CONNECTIONS_PER_USER


@pytest.fixture(autouse=True)
def _clear_user_connections():
    """Reset in-process user connection state between tests."""
    _sse_user_connections.clear()
    yield
    _sse_user_connections.clear()


class TestInProcessSlotAcquireRelease:
    @pytest.mark.asyncio
    async def test_acquire_slot_succeeds(self):
        user_id = uuid4()
        result = await _acquire_sse_slot_in_process(user_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_respects_per_user_limit(self):
        user_id = uuid4()
        for _ in range(SSE_MAX_CONNECTIONS_PER_USER):
            acquired = await _acquire_sse_slot_in_process(user_id)
            assert acquired is True

        over_limit = await _acquire_sse_slot_in_process(user_id)
        assert over_limit is False

    @pytest.mark.asyncio
    async def test_release_frees_a_slot(self):
        user_id = uuid4()
        for _ in range(SSE_MAX_CONNECTIONS_PER_USER):
            await _acquire_sse_slot_in_process(user_id)

        assert await _acquire_sse_slot_in_process(user_id) is False

        await _release_sse_slot_in_process(user_id)

        assert await _acquire_sse_slot_in_process(user_id) is True

    @pytest.mark.asyncio
    async def test_release_all_removes_user_entry(self):
        user_id = uuid4()
        await _acquire_sse_slot_in_process(user_id)
        await _release_sse_slot_in_process(user_id)

        assert str(user_id) not in _sse_user_connections

    @pytest.mark.asyncio
    async def test_independent_users_have_independent_slots(self):
        user_a = uuid4()
        user_b = uuid4()

        for _ in range(SSE_MAX_CONNECTIONS_PER_USER):
            await _acquire_sse_slot_in_process(user_a)

        result = await _acquire_sse_slot_in_process(user_b)
        assert result is True

    @pytest.mark.asyncio
    async def test_release_below_zero_is_safe(self):
        user_id = uuid4()
        await _release_sse_slot_in_process(user_id)
        assert str(user_id) not in _sse_user_connections
