from __future__ import annotations

from backtestforecast.models import AuditEvent, BacktestRun, ExportJob, User


def test_prod_like_backtest_lifecycle_fixture(
    client,
    auth_headers,
    prod_like_backtest_run,
):
    first = prod_like_backtest_run(symbol="AAPL")
    second = prod_like_backtest_run(symbol="MSFT")

    detail = client.get(f"/v1/backtests/{first['id']}", headers=auth_headers)
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["summary_provenance"] == "persisted_run_aggregates"
    assert float(detail_body["risk_free_rate"]) >= 0.0
    assert detail_body["summary"]["trade_count"] >= 1
    assert detail_body["summary"]["decided_trades"] <= detail_body["summary"]["trade_count"]

    compare = client.post(
        "/v1/backtests/compare",
        json={"run_ids": [first["id"], second["id"]]},
        headers=auth_headers,
    )
    assert compare.status_code == 200
    compare_body = compare.json()
    assert compare_body["trades_truncated"] is False
    assert [item["id"] for item in compare_body["items"]] == [first["id"], second["id"]]
    assert {item["summary_provenance"] for item in compare_body["items"]} == {"persisted_run_aggregates"}


def test_prod_like_export_lifecycle_fixture(
    client,
    auth_headers,
    prod_like_backtest_run,
    prod_like_export_job,
):
    run = prod_like_backtest_run(symbol="NVDA")
    export = prod_like_export_job(run["id"], "csv")

    status = client.get(f"/v1/exports/{export['id']}/status", headers=auth_headers)
    assert status.status_code == 200
    status_body = status.json()
    assert status_body["run_id"] == run["id"]
    assert status_body["risk_free_rate_source"] is not None

    listing = client.get("/v1/exports", headers=auth_headers)
    assert listing.status_code == 200
    assert any(item["id"] == export["id"] for item in listing.json()["items"])

    download = client.get(f"/v1/exports/{export['id']}", headers=auth_headers)
    assert download.status_code == 200
    assert "text/csv" in download.headers.get("content-type", "")
    assert "risk_free_rate_source" in download.text


def test_prod_like_account_deletion_lifecycle_fixture(
    client,
    auth_headers,
    session_factory,
    prod_like_backtest_run,
    prod_like_export_job,
    prod_like_account_cleanup,
):
    run = prod_like_backtest_run(symbol="AAPL")
    export = prod_like_export_job(run["id"], "csv")

    with session_factory() as session:
        user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
        user.plan_tier = "pro"
        user.subscription_status = "active"
        user.stripe_subscription_id = "sub_prod_like_123"
        user.stripe_customer_id = "cus_prod_like_123"
        session.add(user)
        session.commit()
        deleted_user_id = user.id

    response = client.delete(
        "/v1/account/me",
        headers={**auth_headers, "X-Confirm-Delete": "permanently-delete-my-account"},
    )
    assert response.status_code == 204

    with session_factory() as session:
        assert session.query(User).filter(User.id == deleted_user_id).one_or_none() is None
        assert session.query(BacktestRun).filter(BacktestRun.id == run["id"]).one_or_none() is None
        assert session.query(ExportJob).filter(ExportJob.id == export["id"]).one_or_none() is None
        event_types = {
            event.event_type
            for event in session.query(AuditEvent).filter(AuditEvent.subject_type == "user").all()
        }
        assert "account.deleted" in event_types
        assert "account.external_cleanup_finished" in event_types

    assert prod_like_account_cleanup["cleanup"]
    assert prod_like_account_cleanup["cleanup"][0]["subscription_id"] == "sub_prod_like_123"
    assert prod_like_account_cleanup["retry"] == []
