from __future__ import annotations

import asyncio


def test_sse_invalidation_callback_drops_cached_pool_and_schedules_close(monkeypatch) -> None:
    from apps.api.app.routers import events as module

    closed = False

    class _Pool:
        async def aclose(self) -> None:
            nonlocal closed
            closed = True

    scheduled = []

    class _Loop:
        def create_task(self, coro):
            scheduled.append(coro)
            return coro

    monkeypatch.setattr(module, "_async_redis_pool", _Pool())
    monkeypatch.setattr(module, "_async_redis_last_ping", 123.0)
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _Loop())

    module._invalidate_async_redis_sync()

    assert module._async_redis_pool is None
    assert module._async_redis_last_ping == 0.0
    assert len(scheduled) == 1
    asyncio.run(scheduled[0])
    assert closed is True
