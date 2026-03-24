"""Full-lifecycle smoke tests for both inline and real-worker execution."""
from __future__ import annotations

import time
from uuid import UUID

import pytest

from apps.worker.app.celery_app import celery_app
from backtestforecast.models import BacktestTrade, ExportJob
from tests.integration.test_endpoint_coverage import _set_user_plan


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


def _wait_for_status(
    client,
    path: str,
    headers: dict[str, str],
    *,
    terminal_statuses: set[str],
    timeout_seconds: int = 60,
):
    deadline = time.time() + timeout_seconds
    payload = None
    while time.time() < deadline:
        response = client.get(path, headers=headers)
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in terminal_statuses:
            return payload
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for terminal status on {path}: last payload={payload!r}")


def test_backtest_full_lifecycle(
    client,
    auth_headers,
    db_session,
    immediate_backtest_execution,
    immediate_export_execution,
):
    """Fast HTTP -> DB lifecycle using inline task execution."""
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    resp = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
    assert resp.status_code == 202
    created = resp.json()
    assert created["status"] == "succeeded"
    run_id = created["id"]

    resp2 = client.post("/v1/backtests", json=_backtest_payload("MSFT"), headers=auth_headers)
    assert resp2.status_code == 202
    created2 = resp2.json()
    assert created2["status"] == "succeeded"
    run_id_2 = created2["id"]

    detail = client.get(f"/v1/backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 200
    detail_data = detail.json()
    assert detail_data["id"] == run_id
    assert detail_data["summary"]["trade_count"] >= 1
    assert detail_data["summary"]["decided_trades"] <= detail_data["summary"]["trade_count"]
    assert len(detail_data["trades"]) >= 1
    assert float(detail_data["risk_free_rate"]) >= 0.0

    listing = client.get("/v1/backtests", headers=auth_headers)
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert any(item["id"] == run_id for item in items)

    status = client.get(f"/v1/backtests/{run_id}/status", headers=auth_headers)
    assert status.status_code == 200
    assert status.json()["status"] == "succeeded"

    compare = client.post(
        "/v1/backtests/compare",
        json={"run_ids": [run_id, run_id_2]},
        headers=auth_headers,
    )
    assert compare.status_code == 200
    compare_body = compare.json()
    assert [item["id"] for item in compare_body["items"]] == [run_id, run_id_2]
    for item in compare_body["items"]:
        assert item["summary"]["decided_trades"] <= item["summary"]["trade_count"]

    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    export_body = export.json()
    assert export_body["status"] == "succeeded"

    export_status = client.get(f"/v1/exports/{export_body['id']}/status", headers=auth_headers)
    assert export_status.status_code == 200
    assert export_status.json()["run_id"] == run_id

    download = client.get(f"/v1/exports/{export_body['id']}", headers=auth_headers)
    assert download.status_code == 200
    assert "text/csv" in download.headers.get("content-type", "")


@pytest.mark.real_worker
def test_backtest_full_lifecycle_with_real_worker(
    client,
    auth_headers,
    db_session,
    real_worker_stack,
):
    """Authoritative async lifecycle using the real Celery worker and Redis broker."""
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
    assert create.status_code == 202
    run = create.json()
    run_id = run["id"]
    assert run["status"] in {"queued", "running", "succeeded"}

    terminal = _wait_for_status(
        client,
        f"/v1/backtests/{run_id}/status",
        auth_headers,
        terminal_statuses={"succeeded", "failed", "cancelled"},
    )
    assert terminal["status"] == "succeeded"

    detail = client.get(f"/v1/backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["id"] == run_id
    assert detail_body["summary"]["trade_count"] >= 1


@pytest.mark.real_worker
def test_export_lifecycle_with_real_worker_and_real_storage(
    client,
    auth_headers,
    db_session,
    session_factory,
    real_worker_stack,
):
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
    assert create.status_code == 202
    run_id = create.json()["id"]
    UUID(run_id)
    run_status = _wait_for_status(
        client,
        f"/v1/backtests/{run_id}/status",
        auth_headers,
        terminal_statuses={"succeeded", "failed", "cancelled"},
    )
    assert run_status["status"] == "succeeded"

    export_create = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export_create.status_code == 202
    export_id = export_create.json()["id"]
    export_status = _wait_for_status(
        client,
        f"/v1/exports/{export_id}/status",
        auth_headers,
        terminal_statuses={"succeeded", "failed", "cancelled", "expired"},
    )
    assert export_status["status"] == "succeeded"

    with session_factory() as session:
        job = session.get(ExportJob, UUID(export_id))
        assert job is not None
        assert job.status == "succeeded"
        assert job.sha256_hex
        assert job.size_bytes > 0
        assert job.content_bytes is not None or job.storage_key is not None

    download = client.get(f"/v1/exports/{export_id}", headers=auth_headers)
    assert download.status_code == 200
    assert "text/csv" in download.headers.get("content-type", "")
    assert "risk_free_rate_source" in download.text


@pytest.mark.real_worker
def test_real_worker_duplicate_delivery_does_not_replay_terminal_backtest_or_export(
    client,
    auth_headers,
    db_session,
    session_factory,
    real_worker_stack,
):
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
    assert create.status_code == 202
    run_id = create.json()["id"]
    run_uuid = UUID(run_id)
    run_status = _wait_for_status(
        client,
        f"/v1/backtests/{run_id}/status",
        auth_headers,
        terminal_statuses={"succeeded", "failed", "cancelled"},
    )
    assert run_status["status"] == "succeeded"

    with session_factory() as session:
        trade_count_before = session.query(BacktestTrade).filter(BacktestTrade.run_id == run_uuid).count()

    celery_app.send_task("backtests.run", kwargs={"run_id": run_id})
    time.sleep(3)

    with session_factory() as session:
        trade_count_after = session.query(BacktestTrade).filter(BacktestTrade.run_id == run_uuid).count()
        assert trade_count_after == trade_count_before

    export_create = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export_create.status_code == 202
    export_id = export_create.json()["id"]
    export_status = _wait_for_status(
        client,
        f"/v1/exports/{export_id}/status",
        auth_headers,
        terminal_statuses={"succeeded", "failed", "cancelled", "expired"},
    )
    assert export_status["status"] == "succeeded"

    with session_factory() as session:
        before = session.get(ExportJob, UUID(export_id))
        assert before is not None
        before_tuple = (before.status, before.size_bytes, before.sha256_hex, before.storage_key, before.content_bytes)

    celery_app.send_task("exports.generate", kwargs={"export_job_id": export_id})
    time.sleep(3)

    with session_factory() as session:
        after = session.get(ExportJob, UUID(export_id))
        assert after is not None
        after_tuple = (after.status, after.size_bytes, after.sha256_hex, after.storage_key, after.content_bytes)

    assert after_tuple == before_tuple


@pytest.mark.real_worker
def test_real_worker_backtest_export_cancel_delete_lifecycle(
    client,
    auth_headers,
    db_session,
    real_worker_launcher,
):
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    with real_worker_launcher():
        create = client.post("/v1/backtests", json=_backtest_payload("AAPL"), headers=auth_headers)
        assert create.status_code == 202
        run_id = create.json()["id"]
        run_status = _wait_for_status(
            client,
            f"/v1/backtests/{run_id}/status",
            auth_headers,
            terminal_statuses={"succeeded", "failed", "cancelled"},
        )
        assert run_status["status"] == "succeeded"

    export_create = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export_create.status_code == 202
    export_body = export_create.json()
    export_id = export_body["id"]
    assert export_body["status"] == "queued"

    export_cancel = client.post(f"/v1/exports/{export_id}/cancel", headers=auth_headers)
    assert export_cancel.status_code == 200
    assert export_cancel.json()["status"] == "cancelled"

    export_delete = client.delete(f"/v1/exports/{export_id}", headers=auth_headers)
    assert export_delete.status_code == 204

    run_delete = client.delete(f"/v1/backtests/{run_id}", headers=auth_headers)
    assert run_delete.status_code == 204
