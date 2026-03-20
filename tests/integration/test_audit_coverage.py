"""Integration tests covering gaps identified in the production audit.

Each test class addresses a specific gap:
- DELETE endpoints for every resource type
- GET list endpoints for exports and scans
- Status endpoints for all job types
- Health and meta endpoints
- Cross-user isolation for sweeps and analysis
- Account export endpoint
"""
from __future__ import annotations

import contextlib
from typing import Generator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.auth.verification import AuthenticatedPrincipal


def _create_backtest(client: TestClient, auth_headers: dict[str, str]) -> dict:
    resp = client.post(
        "/v1/backtests",
        headers=auth_headers,
        json={
            "symbol": "SPY",
            "strategy_type": "long_call",
            "start_date": "2025-01-01",
            "end_date": "2025-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 30,
            "account_size": 10000,
            "risk_per_trade_pct": 5,
            "commission_per_contract": 0.65,
            "entry_rules": [{"rule_type": "dte_range"}],
        },
    )
    assert resp.status_code in (201, 202), resp.text
    return resp.json()


@contextlib.contextmanager
def _as_other_user(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    """Temporarily switch the auth identity to a different user."""
    from apps.api.app.dependencies import get_token_verifier as _get_tv

    verifier = _get_tv()
    original = verifier.verify_bearer_token

    def _alt(_token: str) -> AuthenticatedPrincipal:
        return AuthenticatedPrincipal(
            clerk_user_id="clerk_other_user",
            session_id="sess_other",
            email="other@example.com",
            claims={"sub": "clerk_other_user", "email": "other@example.com"},
        )

    monkeypatch.setattr(verifier, "verify_bearer_token", _alt)
    try:
        yield client
    finally:
        monkeypatch.setattr(verifier, "verify_bearer_token", original)


# ---------------------------------------------------------------------------
# DELETE endpoints
# ---------------------------------------------------------------------------


class TestDeleteBacktest:
    """Verify DELETE /v1/backtests/{run_id}."""

    def test_delete_succeeded(self, client: TestClient, auth_headers: dict, immediate_backtest_execution: None):
        run = _create_backtest(client, auth_headers)
        resp = client.delete(f"/v1/backtests/{run['id']}", headers=auth_headers)
        assert resp.status_code == 204

    def test_delete_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.delete(f"/v1/backtests/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404

    def test_delete_cross_user(
        self,
        client: TestClient,
        auth_headers: dict,
        immediate_backtest_execution: None,
        monkeypatch: pytest.MonkeyPatch,
    ):
        run = _create_backtest(client, auth_headers)
        with _as_other_user(client, monkeypatch):
            resp = client.delete(f"/v1/backtests/{run['id']}", headers=auth_headers)
            assert resp.status_code == 404


class TestDeleteExport:
    """Verify DELETE /v1/exports/{export_job_id}."""

    def test_delete_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.delete(f"/v1/exports/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404


class TestDeleteScan:
    """Verify DELETE /v1/scans/{job_id}."""

    def test_delete_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.delete(f"/v1/scans/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404


class TestDeleteSweep:
    """Verify DELETE /v1/sweeps/{job_id}."""

    def test_delete_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.delete(f"/v1/sweeps/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404


class TestDeleteAnalysis:
    """Verify DELETE /v1/analysis/{analysis_id}."""

    def test_delete_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.delete(f"/v1/analysis/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# List endpoints
# ---------------------------------------------------------------------------


class TestListEndpoints:
    """Verify GET list endpoints return correct structure."""

    def test_list_exports_empty(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/exports", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_list_scans_empty(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/scans", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_list_sweeps_empty(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/sweeps", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_list_analysis_empty(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/analysis", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data


# ---------------------------------------------------------------------------
# Status endpoints
# ---------------------------------------------------------------------------


class TestStatusEndpoints:
    """Verify status endpoints return 404 for non-existent resources."""

    def test_backtest_status_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/backtests/{uuid4()}/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_scan_status_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/scans/{uuid4()}/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_sweep_status_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/sweeps/{uuid4()}/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_export_status_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/exports/{uuid4()}/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_analysis_status_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/analysis/{uuid4()}/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_backtest_status_found(self, client: TestClient, auth_headers: dict, immediate_backtest_execution: None):
        run = _create_backtest(client, auth_headers)
        resp = client.get(f"/v1/backtests/{run['id']}/status", headers=auth_headers)
        assert resp.status_code == 200
        assert "status" in resp.json()


# ---------------------------------------------------------------------------
# Health and Meta
# ---------------------------------------------------------------------------


class TestHealthEndpoints:
    """Verify health endpoints work without auth."""

    def test_live(self, client: TestClient, _fake_celery):
        resp = client.get("/health/live")
        assert resp.status_code == 200

    def test_ready(self, client: TestClient, _fake_celery):
        resp = client.get("/health/ready")
        assert resp.status_code in (200, 503)

    def test_no_auth_required(self, client: TestClient, _fake_celery):
        resp = client.get("/health/live")
        assert resp.status_code == 200


class TestMetaEndpoint:
    """Verify /v1/meta returns service info."""

    def test_unauthenticated(self, client: TestClient, _fake_celery):
        resp = client.get("/v1/meta")
        assert resp.status_code == 200
        data = resp.json()
        assert "service" in data
        assert "version" in data

    def test_authenticated(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/meta", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "service" in data


# ---------------------------------------------------------------------------
# Cross-user isolation: Sweeps
# ---------------------------------------------------------------------------


class TestSweepCrossUserIsolation:
    """Verify sweeps cannot be accessed by another user."""

    def test_get_sweep_cross_user(self, client: TestClient, auth_headers: dict, _fake_celery, monkeypatch):
        resp = client.post(
            "/v1/sweeps",
            headers=auth_headers,
            json={
                "symbol": "SPY",
                "mode": "grid",
                "start_date": "2025-01-01",
                "end_date": "2025-06-01",
                "target_dte": 30,
                "dte_tolerance_days": 5,
                "max_holding_days": 30,
                "account_size": 10000,
                "risk_per_trade_pct": 5,
                "commission_per_contract": 0.65,
                "strategy_types": ["bull_put_credit_spread"],
                "entry_rule_sets": [{"name": "no_filter", "entry_rules": []}],
            },
        )
        if resp.status_code not in (200, 201, 202):
            pytest.skip(f"Sweep creation returned {resp.status_code}: feature may be locked")
        job_id = resp.json()["id"]

        with _as_other_user(client, monkeypatch):
            resp2 = client.get(f"/v1/sweeps/{job_id}", headers=auth_headers)
            assert resp2.status_code == 404


# ---------------------------------------------------------------------------
# Cross-user isolation: Analysis
# ---------------------------------------------------------------------------


class TestAnalysisCrossUserIsolation:
    """Verify analyses cannot be accessed by another user."""

    def test_get_analysis_cross_user(self, client: TestClient, auth_headers: dict, _fake_celery, monkeypatch):
        resp = client.post(
            "/v1/analysis",
            headers=auth_headers,
            json={"symbol": "AAPL"},
        )
        if resp.status_code not in (200, 201, 202):
            pytest.skip(f"Analysis creation returned {resp.status_code}: feature may be locked")
        analysis_id = resp.json()["id"]

        with _as_other_user(client, monkeypatch):
            resp2 = client.get(f"/v1/analysis/{analysis_id}", headers=auth_headers)
            assert resp2.status_code == 404


# ---------------------------------------------------------------------------
# Account export
# ---------------------------------------------------------------------------


class TestAccountExport:
    """Verify GET /v1/account/me/export returns user data."""

    def test_requires_auth(self, client: TestClient, _fake_celery):
        resp = client.get("/v1/account/me/export")
        assert resp.status_code in (401, 403, 422)

    def test_basic_structure(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get("/v1/account/me/export", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "user" in data
        assert data["pagination"]["limit"] == 200
        assert data["pagination"]["templates_offset"] == 0
        assert data["pagination"]["scans_offset"] == 0
        assert data["pagination"]["sweeps_offset"] == 0
        assert data["pagination"]["exports_offset"] == 0
        assert data["pagination"]["analyses_offset"] == 0
        assert data["pagination"]["audit_offset"] == 0

    def test_with_backtest_data(self, client: TestClient, auth_headers: dict, immediate_backtest_execution: None):
        _create_backtest(client, auth_headers)
        resp = client.get("/v1/account/me/export", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "user" in data
        assert "backtests" in data
        assert len(data["backtests"]) >= 1


# ---------------------------------------------------------------------------
# Auth required on all deletes
# ---------------------------------------------------------------------------


class TestDeleteRequiresAuth:
    """Verify all DELETE endpoints require authentication."""

    @pytest.mark.parametrize("path", [
        f"/v1/backtests/{uuid4()}",
        f"/v1/scans/{uuid4()}",
        f"/v1/sweeps/{uuid4()}",
        f"/v1/exports/{uuid4()}",
        f"/v1/analysis/{uuid4()}",
    ])
    def test_delete_no_auth(self, client: TestClient, path: str, _fake_celery):
        resp = client.delete(path)
        assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Sweep results
# ---------------------------------------------------------------------------


class TestSweepResults:
    """Verify sweep results endpoint."""

    def test_results_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/sweeps/{uuid4()}/results", headers=auth_headers)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# SSE events endpoints
# ---------------------------------------------------------------------------


class TestSSEEndpoints:
    """Verify SSE event endpoints return 404 for non-existent resources."""

    def test_sweep_events_not_found(self, client: TestClient, auth_headers: dict, _fake_celery):
        resp = client.get(f"/v1/events/sweeps/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404
