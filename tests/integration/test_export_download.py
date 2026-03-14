from __future__ import annotations

from tests.integration.test_api_critical_flows import _create_backtest, _set_user_plan


def test_export_download_csv_content_type_and_disposition(
    client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]
    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    ej = export.json()
    assert ej["status"] == "succeeded"

    download = client.get(f"/v1/exports/{ej['id']}", headers=auth_headers)
    assert download.status_code == 200

    content_type = download.headers.get("content-type", "")
    assert "text/csv" in content_type
    assert "charset=utf-8" in content_type

    content_disposition = download.headers.get("content-disposition", "")
    assert "attachment" in content_disposition.lower()
    assert "filename=" in content_disposition.lower()
    assert ".csv" in content_disposition


def test_export_download_pdf_content_type_and_disposition(
    client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="premium", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]
    export = client.post("/v1/exports", json={"run_id": run_id, "format": "pdf"}, headers=auth_headers)
    assert export.status_code == 202
    ej = export.json()
    assert ej["status"] == "succeeded"

    download = client.get(f"/v1/exports/{ej['id']}", headers=auth_headers)
    assert download.status_code == 200

    content_type = download.headers.get("content-type", "")
    assert content_type.startswith("application/pdf")

    content_disposition = download.headers.get("content-disposition", "")
    assert "attachment" in content_disposition.lower()
    assert "filename=" in content_disposition.lower()
    assert ".pdf" in content_disposition
