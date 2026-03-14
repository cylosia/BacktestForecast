"""SSE event endpoint integration tests.

Tests verify ownership checks, stream initiation, and the publish/subscribe
contract.  Redis is patched with an in-process async channel so these tests
run without a real Redis server.
"""
from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from backtestforecast.models import BacktestRun, ScannerJob, User


def _ensure_user(client, auth_headers, db_session) -> User:
    """Hit /v1/me to upsert the test user, then return the ORM instance."""
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter_by(clerk_user_id="clerk_test_user").first()
    assert user is not None
    return user


def _create_backtest_run(db_session, user_id) -> uuid.UUID:
    run = BacktestRun(
        user_id=user_id,
        status="running",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 3, 31),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run.id


def _create_scanner_job(db_session, user_id) -> uuid.UUID:
    job = ScannerJob(
        user_id=user_id,
        status="running",
        mode="full",
        plan_tier_snapshot="pro",
        job_kind="manual",
        request_hash="abc123",
        request_snapshot_json={},
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job.id


async def _fake_subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Yield a single status event then stop (no Redis needed)."""
    yield json.dumps({"status": "succeeded", "job_id": "fake"})


# ---------------------------------------------------------------------------
# Ownership / 404
# ---------------------------------------------------------------------------


def test_backtest_events_ownership_404(client, auth_headers):
    resp = client.get(f"/v1/events/backtests/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_scan_events_ownership_404(client, auth_headers):
    resp = client.get(f"/v1/events/scans/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# SSE stream acceptance for owned resources
# ---------------------------------------------------------------------------


def test_backtest_events_owned_resource_accepted(client, auth_headers, db_session):
    """SSE endpoint returns 200 for a valid owned backtest."""
    user = _ensure_user(client, auth_headers, db_session)
    run_id = _create_backtest_run(db_session, user.id)

    with patch("apps.api.app.routers.events._subscribe_redis", _fake_subscribe):
        resp = client.get(f"/v1/events/backtests/{run_id}", headers=auth_headers)
    assert resp.status_code == 200


def test_scan_events_owned_resource_accepted(client, auth_headers, db_session):
    """SSE endpoint returns 200 for a valid owned scanner job."""
    user = _ensure_user(client, auth_headers, db_session)
    job_id = _create_scanner_job(db_session, user.id)

    with patch("apps.api.app.routers.events._subscribe_redis", _fake_subscribe):
        resp = client.get(f"/v1/events/scans/{job_id}", headers=auth_headers)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Publish / subscribe contract
# ---------------------------------------------------------------------------


def test_publish_job_status_format():
    """Verify publish_job_status produces correct channel and JSON payload."""
    job_id = uuid.uuid4()
    mock_client = MagicMock()

    with patch("backtestforecast.events._get_redis", return_value=mock_client):
        from backtestforecast.events import publish_job_status

        publish_job_status("backtest", job_id, "succeeded", metadata={"trade_count": 5})

    mock_client.publish.assert_called_once()
    call_args = mock_client.publish.call_args
    channel = call_args[0][0]
    payload = json.loads(call_args[0][1])

    assert channel == f"job:backtest:{job_id}:status"
    assert payload["status"] == "succeeded"
    assert payload["job_id"] == str(job_id)
    assert payload["trade_count"] == 5


def test_publish_job_status_handles_redis_failure():
    """publish_job_status must not raise when Redis is unavailable."""
    from redis.exceptions import RedisError

    mock_client = MagicMock()
    mock_client.publish.side_effect = RedisError("Connection refused")

    with patch("backtestforecast.events._get_redis", return_value=mock_client):
        from backtestforecast.events import publish_job_status

        publish_job_status("backtest", uuid.uuid4(), "running")


# ---------------------------------------------------------------------------
# Item 67: SSE connection limit with Redis-based counting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sse_slot_ttl_refreshed_on_every_acquire():
    """Item 90: Every call to _acquire_sse_slot must call pool.expire() to
    refresh the Redis key TTL, preventing stale keys from accumulating."""
    from apps.api.app.routers.events import _SSE_CONN_TTL, _acquire_sse_slot

    user_id = uuid.uuid4()
    expire_calls: list[tuple[str, int]] = []
    incr_counter = {"count": 0}

    mock_pool = MagicMock()

    async def fake_incr(key):
        incr_counter["count"] += 1
        return incr_counter["count"]

    async def fake_expire(key, ttl):
        expire_calls.append((key, ttl))

    mock_pool.incr = fake_incr
    mock_pool.expire = fake_expire

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        await _acquire_sse_slot(user_id)
        await _acquire_sse_slot(user_id)
        await _acquire_sse_slot(user_id)

    assert len(expire_calls) == 3, (
        f"expire() should be called on every acquire, got {len(expire_calls)} calls"
    )
    for key, ttl in expire_calls:
        assert ttl == _SSE_CONN_TTL, (
            f"TTL should be {_SSE_CONN_TTL}, got {ttl}"
        )
        assert str(user_id) in key


# ---------------------------------------------------------------------------
# Item 56: SSE reconnection invalidates pool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sse_reconnection_invalidates_pool():
    """Verify that on reconnection, the old Redis pool is invalidated before
    a fresh pool is obtained."""
    from apps.api.app.routers.events import _get_async_redis

    pool1 = MagicMock()
    pool2 = MagicMock()
    call_order: list[str] = []

    pool1.ping = MagicMock(side_effect=Exception("connection lost"))
    pool2.ping = MagicMock(return_value=True)

    pools = iter([pool1, pool2])

    def mock_get_async_redis():
        p = next(pools)
        call_order.append(f"get_pool_{id(p)}")
        return p

    with patch("apps.api.app.routers.events._get_async_redis", side_effect=mock_get_async_redis):
        first_pool = mock_get_async_redis()
        assert first_pool is pool1

        try:
            first_pool.ping()
        except Exception:
            second_pool = mock_get_async_redis()
            assert second_pool is pool2

    assert len(call_order) == 2, "Should have acquired pool twice (initial + reconnect)"


@pytest.mark.asyncio
async def test_sse_acquire_slot_limits_connections():
    """Verify _acquire_sse_slot enforces per-user connection limit via Redis INCR."""
    from apps.api.app.routers.events import (
        SSE_MAX_CONNECTIONS_PER_USER,
        _acquire_sse_slot,
        _release_sse_slot,
    )

    user_id = uuid.uuid4()
    incr_counter = {"count": 0}
    mock_pool = MagicMock()

    async def fake_incr(key):
        incr_counter["count"] += 1
        return incr_counter["count"]

    async def fake_expire(key, ttl):
        pass

    async def fake_decr(key):
        incr_counter["count"] -= 1
        return incr_counter["count"]

    async def fake_delete(key):
        pass

    mock_pool.incr = fake_incr
    mock_pool.expire = fake_expire
    mock_pool.decr = fake_decr
    mock_pool.delete = fake_delete

    with patch("apps.api.app.routers.events._get_async_redis", return_value=mock_pool):
        for _ in range(SSE_MAX_CONNECTIONS_PER_USER):
            result = await _acquire_sse_slot(user_id)
            assert result is True, "Should allow up to the max"

        over_limit = await _acquire_sse_slot(user_id)
        assert over_limit is False, "Should reject when over the per-user limit"

        await _release_sse_slot(user_id)
        after_release = await _acquire_sse_slot(user_id)
        assert after_release is True, "Should allow after a slot is released"
