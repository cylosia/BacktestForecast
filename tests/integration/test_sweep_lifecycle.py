"""Integration test for the sweep lifecycle via the HTTP API.

Covers: create sweep -> verify queued -> list sweeps -> get status -> get results.
Requires the integration fixtures (Postgres, TestClient, Celery stub).
"""
from __future__ import annotations

from uuid import uuid4

from backtestforecast.models import User


def _set_user_plan(session, *, tier: str, subscription_status: str | None = None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _sweep_payload(**overrides) -> dict:
    payload = {
        "symbol": "AAPL",
        "strategy_types": ["long_call"],
        "start_date": "2025-01-01",
        "end_date": "2025-06-01",
        "target_dte": 45,
        "dte_tolerance_days": 5,
        "max_holding_days": 30,
        "account_size": "50000",
        "risk_per_trade_pct": "2",
        "commission_per_contract": "0.65",
        "entry_rule_sets": [
            {
                "name": "default",
                "entry_rules": [
                    {"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}
                ],
            }
        ],
    }
    payload.update(overrides)
    return payload


class TestSweepLifecycle:
    def test_free_tier_blocked(self, client, auth_headers):
        """Free-tier users cannot create sweeps."""
        resp = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert resp.status_code == 403
        assert resp.json()["error"]["code"] == "feature_locked"

    def test_create_and_list(self, client, auth_headers, db_session, _fake_celery):
        """Pro users can create a sweep and see it in the list."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        create_resp = client.post("/v1/sweeps", json=_sweep_payload(), headers=auth_headers)
        assert create_resp.status_code in (200, 202), (
            f"Expected 200/202, got {create_resp.status_code}: {create_resp.text}"
        )
        sweep_id = create_resp.json()["id"]

        list_resp = client.get("/v1/sweeps", headers=auth_headers)
        assert list_resp.status_code == 200
        items = list_resp.json()["items"]
        assert any(item["id"] == sweep_id for item in items)

    def test_get_sweep_detail(self, client, auth_headers, db_session, _fake_celery):
        """GET /v1/sweeps/{id} returns the sweep detail."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        sweep_id = client.post(
            "/v1/sweeps", json=_sweep_payload(), headers=auth_headers,
        ).json()["id"]

        detail = client.get(f"/v1/sweeps/{sweep_id}", headers=auth_headers)
        assert detail.status_code == 200
        assert detail.json()["id"] == sweep_id
        assert detail.json()["symbol"] == "AAPL"

    def test_get_sweep_status(self, client, auth_headers, db_session, _fake_celery):
        """GET /v1/sweeps/{id}/status returns the current status."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        sweep_id = client.post(
            "/v1/sweeps", json=_sweep_payload(), headers=auth_headers,
        ).json()["id"]

        status = client.get(f"/v1/sweeps/{sweep_id}/status", headers=auth_headers)
        assert status.status_code == 200
        assert "status" in status.json()

    def test_get_nonexistent_sweep_returns_404(self, client, auth_headers):
        """GET /v1/sweeps/{id} returns 404 for an unknown sweep."""
        resp = client.get(f"/v1/sweeps/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404

    def test_delete_sweep(self, client, auth_headers, db_session, _fake_celery):
        """DELETE /v1/sweeps/{id} cancels/deletes the sweep."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        sweep_id = client.post(
            "/v1/sweeps", json=_sweep_payload(), headers=auth_headers,
        ).json()["id"]

        delete_resp = client.delete(f"/v1/sweeps/{sweep_id}", headers=auth_headers)
        assert delete_resp.status_code == 204

    def test_sweep_results_empty_for_queued_job(self, client, auth_headers, db_session, _fake_celery):
        """GET /v1/sweeps/{id}/results returns empty list for a queued sweep."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        sweep_id = client.post(
            "/v1/sweeps", json=_sweep_payload(), headers=auth_headers,
        ).json()["id"]

        results = client.get(f"/v1/sweeps/{sweep_id}/results", headers=auth_headers)
        assert results.status_code == 200
        assert results.json()["items"] == []

    def test_sweep_idempotency(self, client, auth_headers, db_session, _fake_celery):
        """Duplicate sweep creation with same idempotency key returns same job."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        payload = _sweep_payload(idempotency_key="sweep-idem-001")
        first = client.post("/v1/sweeps", json=payload, headers=auth_headers)
        second = client.post("/v1/sweeps", json=payload, headers=auth_headers)

        assert first.status_code in (200, 202), f"First request failed: {first.status_code} {first.text[:200]}"
        assert second.status_code in (200, 202), f"Second request failed: {second.status_code} {second.text[:200]}"
        assert first.json()["id"] == second.json()["id"], "Idempotency key should return the same job"
