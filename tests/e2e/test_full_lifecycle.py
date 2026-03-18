"""Full-lifecycle smoke test: create → execute → read → list → compare.

This is a thin wrapper that exercises the synchronous backtest path
end-to-end via the HTTP API. It reuses the integration fixtures
(Postgres, TestClient, Celery stub) and verifies the most critical
user journey without needing a running Celery broker.

For the async (Celery-inclusive) E2E path, see the ``e2e-tests`` CI job
which runs Playwright against a live API + worker + web stack.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL is not set — requires a real Postgres instance",
)


def _backtest_payload(symbol: str = "AAPL") -> dict:
    return {
        "symbol": symbol,
        "strategy_type": "long_call",
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
    }


def test_backtest_full_lifecycle(client, auth_headers, db_session, immediate_backtest_execution):
    """HTTP → DB → response lifecycle: create, execute, read, list, compare."""
    # 1. Create and execute a backtest synchronously
    resp = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
    assert resp.status_code == 202
    created = resp.json()
    assert created["status"] == "succeeded"
    run_id = created["id"]

    # 2. Verify the result via GET
    detail = client.get(f"/v1/backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 200
    detail_data = detail.json()
    assert detail_data["id"] == run_id
    assert detail_data["summary"]["trade_count"] >= 1
    assert len(detail_data["trades"]) >= 1

    # 3. List runs and confirm it appears
    listing = client.get("/v1/backtests", headers=auth_headers)
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert any(item["id"] == run_id for item in items)

    # 4. Status endpoint returns terminal state
    status = client.get(f"/v1/backtests/{run_id}/status", headers=auth_headers)
    assert status.status_code == 200
    assert status.json()["status"] == "succeeded"
