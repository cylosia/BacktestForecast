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
