"""Tests for empty/null/boundary inputs on critical endpoints."""
from __future__ import annotations

import uuid

from tests.integration.test_api_critical_flows import _set_user_plan


def test_compare_empty_run_ids(client, auth_headers, db_session):
    """Empty run_ids should be rejected."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.post("/v1/backtests/compare", headers=auth_headers, json={"run_ids": []})
    assert resp.status_code in (400, 422)


def test_compare_single_run_id(client, auth_headers, db_session):
    """Single run_id should be rejected or handled."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.post(
        "/v1/backtests/compare",
        headers=auth_headers,
        json={"run_ids": [str(uuid.uuid4())]},
    )
    assert resp.status_code in (400, 404, 422)


def test_malformed_uuid_returns_422(client, auth_headers):
    """Malformed UUID should return 422, not 500."""
    client.get("/v1/me", headers=auth_headers)
    resp = client.get("/v1/backtests/not-a-uuid", headers=auth_headers)
    assert resp.status_code in (404, 422)


def test_nonexistent_uuid_returns_404(client, auth_headers):
    """Nonexistent UUID should return 404."""
    client.get("/v1/me", headers=auth_headers)
    resp = client.get(f"/v1/backtests/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_scanner_empty_symbols(client, auth_headers, db_session):
    """Scanner with no symbols should be rejected."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.post(
        "/v1/scans",
        headers=auth_headers,
        json={
            "symbols": [],
            "strategy_types": ["long_call"],
            "mode": "basic",
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 7,
            "max_holding_days": 21,
            "account_size": 10000,
            "risk_per_trade_pct": 2.0,
            "commission_per_contract": 0.65,
            "rule_sets": [{"entry_rules": []}],
        },
    )
    assert resp.status_code in (400, 422)


def test_export_nonexistent_download(client, auth_headers):
    """Downloading a nonexistent export should return 404."""
    client.get("/v1/me", headers=auth_headers)
    resp = client.get(f"/v1/exports/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404
