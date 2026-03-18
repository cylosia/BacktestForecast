"""Comprehensive endpoint coverage tests for all previously untested API routes.

Covers DELETE, status, list, SSE, health, meta, root, and cross-user isolation
endpoints that were identified as gaps during the audit.
"""
from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from apps.api.app.dependencies import get_token_verifier
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.models import User
from backtestforecast.services.scans import ScanService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USER_B_CLERK_ID = "clerk_other_user_coverage"
_USER_B_EMAIL = "other-coverage@example.com"
_AS_USER_LOCK = threading.Lock()


@contextmanager
def _as_user(clerk_id: str, email: str):
    """Temporarily switch the authenticated user returned by token verification."""
    verifier = get_token_verifier()
    with _AS_USER_LOCK:
        original = verifier.verify_bearer_token

        def _verify(_token: str) -> AuthenticatedPrincipal:
            return AuthenticatedPrincipal(
                clerk_user_id=clerk_id,
                session_id=f"sess_{clerk_id}",
                email=email,
                claims={"sub": clerk_id, "email": email},
            )

        verifier.verify_bearer_token = _verify
    try:
        yield
    finally:
        with _AS_USER_LOCK:
            verifier.verify_bearer_token = original


def _set_user_plan(session, *, clerk_id="clerk_test_user", tier="pro", subscription_status="active"):
    user = session.query(User).filter(User.clerk_user_id == clerk_id).one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _backtest_payload(**overrides):
    payload = {
        "symbol": "SPY",
        "strategy_type": "long_call",
        "start_date": "2024-01-01",
        "end_date": "2024-06-01",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 20,
        "account_size": "10000",
        "risk_per_trade_pct": "2.0",
        "commission_per_contract": "0.65",
        "entry_rules": [{"type": "always_enter"}],
    }
    payload.update(overrides)
    return payload


def _scan_payload(**overrides):
    payload = {
        "mode": "basic",
        "symbols": ["SPY"],
        "strategy_types": ["long_call"],
        "rule_sets": [{"name": "test", "entry_rules": [{"type": "always_enter"}]}],
        "start_date": "2024-01-01",
        "end_date": "2024-06-01",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 20,
        "account_size": "10000",
        "risk_per_trade_pct": "2.0",
        "commission_per_contract": "0.65",
    }
    payload.update(overrides)
    return payload


def _sweep_payload(**overrides):
    payload = {
        "mode": "grid",
        "symbol": "SPY",
        "strategy_types": ["bull_put_credit_spread"],
        "start_date": "2024-01-01",
        "end_date": "2024-06-01",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 20,
        "account_size": "10000",
        "risk_per_trade_pct": "2.0",
        "commission_per_contract": "0.65",
        "entry_rule_sets": [{"name": "no_filter", "entry_rules": []}],
    }
    payload.update(overrides)
    return payload


def _analysis_payload(**overrides):
    payload = {
        "symbol": "SPY",
        "idempotency_key": f"test-analysis-{uuid.uuid4().hex[:8]}",
    }
    payload.update(overrides)
    return payload


def _ensure_user(client, auth_headers):
    """Ensure the test user exists by hitting /v1/me."""
    client.get("/v1/me", headers=auth_headers)


@pytest.fixture()
def immediate_scan_execution(_fake_celery, session_factory, stub_execution):
    def _run(name: str, kwargs: dict[str, str]) -> None:
        assert name == "scans.run_job"
        with session_factory() as session:
            ScanService(session).run_job(uuid.UUID(kwargs["job_id"]))

    _fake_celery.register("scans.run_job", _run)


# ===========================================================================
# 1. DELETE /v1/backtests/{run_id}
# ===========================================================================


class TestDeleteBacktest:
    def test_delete_backtest_returns_204(self, client, auth_headers, immediate_backtest_execution):
        created = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers)
        assert created.status_code == 202
        run_id = created.json()["id"]

        resp = client.delete(f"/v1/backtests/{run_id}", headers=auth_headers)
        assert resp.status_code == 204

        get_resp = client.get(f"/v1/backtests/{run_id}", headers=auth_headers)
        assert get_resp.status_code == 404

    def test_delete_nonexistent_backtest_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.delete(f"/v1/backtests/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 2. GET /v1/backtests/{run_id}/status
# ===========================================================================


class TestBacktestStatus:
    def test_get_backtest_status(self, client, auth_headers, immediate_backtest_execution):
        created = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers)
        assert created.status_code == 202
        run_id = created.json()["id"]

        resp = client.get(f"/v1/backtests/{run_id}/status", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == run_id
        assert body["status"] == "succeeded"

    def test_status_nonexistent_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.get(f"/v1/backtests/{fake_id}/status", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 3. GET /v1/exports (list)
# ===========================================================================


class TestListExports:
    def test_list_exports_empty(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        resp = client.get("/v1/exports", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_list_exports_after_creation(
        self, client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution
    ):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        run_id = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers).json()["id"]
        export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
        assert export.status_code == 202

        resp = client.get("/v1/exports", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] >= 1
        assert len(body["items"]) >= 1


# ===========================================================================
# 4. DELETE /v1/exports/{export_job_id}
# ===========================================================================


class TestDeleteExport:
    def test_delete_export_returns_204(
        self, client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution
    ):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        run_id = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers).json()["id"]
        export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
        assert export.status_code == 202
        export_id = export.json()["id"]

        resp = client.delete(f"/v1/exports/{export_id}", headers=auth_headers)
        assert resp.status_code == 204

    def test_delete_nonexistent_export_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.delete(f"/v1/exports/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 5. GET /v1/scans (list)
# ===========================================================================


class TestListScans:
    def test_list_scans_shape(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        resp = client.get("/v1/scans", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert "total" in body
        assert isinstance(body["items"], list)


# ===========================================================================
# 6. GET /v1/scans/{job_id}/status
# ===========================================================================


class TestScanStatus:
    def test_get_scan_status(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/scans", json=_scan_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        resp = client.get(f"/v1/scans/{job_id}/status", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == job_id
        assert body["status"] in ("queued", "running", "succeeded", "failed")

    def test_scan_status_nonexistent_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.get(f"/v1/scans/{fake_id}/status", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 7. DELETE /v1/scans/{job_id}
# ===========================================================================


class TestDeleteScan:
    def test_delete_scan_returns_204(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/scans", json=_scan_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        resp = client.delete(f"/v1/scans/{job_id}", headers=auth_headers)
        assert resp.status_code == 204

        get_resp = client.get(f"/v1/scans/{job_id}", headers=auth_headers)
        assert get_resp.status_code == 404

    def test_delete_nonexistent_scan_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.delete(f"/v1/scans/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 8. DELETE /v1/sweeps/{job_id}
# ===========================================================================


class TestDeleteSweep:
    def test_delete_sweep_returns_204(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        resp = client.delete(f"/v1/sweeps/{job_id}", headers=auth_headers)
        assert resp.status_code == 204

        get_resp = client.get(f"/v1/sweeps/{job_id}", headers=auth_headers)
        assert get_resp.status_code == 404

    def test_delete_nonexistent_sweep_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.delete(f"/v1/sweeps/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 9. GET /v1/sweeps/{job_id}/status
# ===========================================================================


class TestSweepStatus:
    def test_get_sweep_status(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        resp = client.get(f"/v1/sweeps/{job_id}/status", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == job_id
        assert body["status"] in ("queued", "running", "succeeded", "failed")

    def test_sweep_status_nonexistent_returns_404(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.get(f"/v1/sweeps/{fake_id}/status", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 10. DELETE /v1/analysis/{analysis_id}
# ===========================================================================


class TestDeleteAnalysis:
    def test_delete_analysis_returns_204(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/analysis", json=_analysis_payload(), headers=auth_headers)
        assert created.status_code == 202
        analysis_id = created.json()["id"]

        resp = client.delete(f"/v1/analysis/{analysis_id}", headers=auth_headers)
        assert resp.status_code == 204

        get_resp = client.get(f"/v1/analysis/{analysis_id}", headers=auth_headers)
        assert get_resp.status_code == 404

    def test_delete_nonexistent_analysis_returns_404(self, client, auth_headers, db_session):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        fake_id = str(uuid.uuid4())
        resp = client.delete(f"/v1/analysis/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 11. GET /v1/events/sweeps/{job_id} (SSE)
# ===========================================================================


class TestSweepSSE:
    def test_sweep_sse_returns_200_for_valid_sweep(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        resp = client.get(f"/v1/events/sweeps/{job_id}", headers=auth_headers)
        assert resp.status_code == 200, f"SSE sweep endpoint returned {resp.status_code} instead of 200"

    def test_sweep_sse_returns_404_for_nonexistent(self, client, auth_headers):
        fake_id = str(uuid.uuid4())
        resp = client.get(f"/v1/events/sweeps/{fake_id}", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 12. GET /v1/meta
# ===========================================================================


class TestMeta:
    def test_meta_returns_service_info(self, client):
        resp = client.get("/v1/meta")
        assert resp.status_code == 200
        body = resp.json()
        assert body["service"] == "backtestforecast-api"
        assert "version" in body

    def test_meta_with_auth_includes_features(self, client, auth_headers):
        resp = client.get("/v1/meta", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["service"] == "backtestforecast-api"
        assert "features" in body


# ===========================================================================
# 13. GET /health/live
# ===========================================================================


class TestHealthLive:
    def test_health_live_returns_200(self, client):
        resp = client.get("/health/live")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["service"] == "api"
        assert "version" in body


# ===========================================================================
# 14. GET /health/ready
# ===========================================================================


class TestHealthReady:
    def test_health_ready_returns_200_when_healthy(self, client):
        with patch("apps.api.app.routers.health.ping_database"):
            resp = client.get("/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("ok", "degraded")
        assert "version" in body


# ===========================================================================
# 15. GET / (root)
# ===========================================================================


class TestRoot:
    def test_root_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.json()
        assert body["service"] == "backtestforecast-api"
        assert body["status"] == "ok"
        assert body["health"] == "/health/ready"


# ===========================================================================
# 16. Cross-user isolation for sweeps
# ===========================================================================


class TestSweepCrossUserIsolation:
    def test_user_b_cannot_access_user_a_sweep(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert created.status_code == 202
        job_id = created.json()["id"]

        with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
            _ensure_user(client, auth_headers)
            _set_user_plan(db_session, clerk_id=_USER_B_CLERK_ID, tier="pro")

            get_resp = client.get(f"/v1/sweeps/{job_id}", headers=auth_headers)
            assert get_resp.status_code == 404

            status_resp = client.get(f"/v1/sweeps/{job_id}/status", headers=auth_headers)
            assert status_resp.status_code == 404

            delete_resp = client.delete(f"/v1/sweeps/{job_id}", headers=auth_headers)
            assert delete_resp.status_code == 404

        # Verify User A still has access
        get_resp = client.get(f"/v1/sweeps/{job_id}", headers=auth_headers)
        assert get_resp.status_code == 200

    def test_user_b_sweep_list_excludes_user_a(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)

        with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
            _ensure_user(client, auth_headers)
            _set_user_plan(db_session, clerk_id=_USER_B_CLERK_ID, tier="pro")

            list_resp = client.get("/v1/sweeps", headers=auth_headers)
            assert list_resp.status_code == 200
            assert list_resp.json()["total"] == 0


# ===========================================================================
# 17. Cross-user isolation for analyses
# ===========================================================================


class TestAnalysisCrossUserIsolation:
    def test_user_b_cannot_access_user_a_analysis(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        created = client.post("/v1/analysis", json=_analysis_payload(), headers=auth_headers)
        assert created.status_code == 202
        analysis_id = created.json()["id"]

        with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
            _ensure_user(client, auth_headers)
            _set_user_plan(db_session, clerk_id=_USER_B_CLERK_ID, tier="pro")

            get_resp = client.get(f"/v1/analysis/{analysis_id}", headers=auth_headers)
            assert get_resp.status_code == 404

            status_resp = client.get(f"/v1/analysis/{analysis_id}/status", headers=auth_headers)
            assert status_resp.status_code == 404

            delete_resp = client.delete(f"/v1/analysis/{analysis_id}", headers=auth_headers)
            assert delete_resp.status_code == 404

        # Verify User A still has access
        get_resp = client.get(f"/v1/analysis/{analysis_id}", headers=auth_headers)
        assert get_resp.status_code == 200

    def test_user_b_analysis_list_excludes_user_a(self, client, auth_headers, db_session, _fake_celery):
        _ensure_user(client, auth_headers)
        _set_user_plan(db_session, tier="pro")

        client.post("/v1/analysis", json=_analysis_payload(), headers=auth_headers)

        with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
            _ensure_user(client, auth_headers)
            _set_user_plan(db_session, clerk_id=_USER_B_CLERK_ID, tier="pro")

            list_resp = client.get("/v1/analysis", headers=auth_headers)
            assert list_resp.status_code == 200
            assert list_resp.json()["total"] == 0
