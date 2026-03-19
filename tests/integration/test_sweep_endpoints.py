"""Integration tests for sweep endpoints."""
from __future__ import annotations

import pytest
from uuid import uuid4


def test_list_sweep_jobs(client, auth_headers):
    """GET /v1/sweeps returns a paginated list."""
    resp = client.get("/v1/sweeps", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert "total" in data


def test_create_sweep_job(client, auth_headers, db_session):
    """POST /v1/sweeps validates the payload and dispatches a job."""
    resp = client.post(
        "/v1/sweeps",
        json={
            "symbol": "AAPL",
            "strategy_types": ["long_call"],
            "start_date": "2025-01-01",
            "end_date": "2025-06-01",
            "target_dte": 45,
            "max_holding_days": 30,
            "account_size": "50000",
            "risk_per_trade_pct": "2",
            "commission_per_contract": "0.65",
            "entry_rule_sets": [
                {
                    "name": "default",
                    "entry_rules": [
                        {"type": "rsi", "operator": "lt", "threshold": "30"}
                    ],
                }
            ],
        },
        headers=auth_headers,
    )
    # 202 on success, 422 if required fields are missing/invalid,
    # 403 if the test user lacks entitlement
    assert resp.status_code == 202, f"Expected 202 Accepted, got {resp.status_code}: {resp.text}"


def test_get_nonexistent_sweep(client, auth_headers):
    """GET /v1/sweeps/{id} returns 404 for an unknown ID."""
    resp = client.get(f"/v1/sweeps/{uuid4()}", headers=auth_headers)
    assert resp.status_code == 404
