"""Integration tests for DELETE /v1/analysis/{id}."""
from __future__ import annotations

from uuid import uuid4

from backtestforecast.models import SymbolAnalysis, User


def _set_user_plan(session, *, tier: str, subscription_status: str | None = None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _analysis_payload(**overrides):
    from uuid import uuid4 as _uuid4
    payload = {
        "symbol": "SPY",
        "idempotency_key": f"test-analysis-{_uuid4().hex[:8]}",
    }
    payload.update(overrides)
    return payload


class TestAnalysisDelete:
    def test_successful_deletion_of_own_analysis(
        self, client, auth_headers, db_session, _fake_celery
    ):
        """DELETE /v1/analysis/{id} returns 204 when deleting own analysis."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        create_resp = client.post(
            "/v1/analysis",
            json=_analysis_payload(),
            headers=auth_headers,
        )
        assert create_resp.status_code == 202
        analysis_id = create_resp.json()["id"]

        # Analysis starts as queued; only succeeded/failed/cancelled can be deleted.
        # Set to failed so we can test the delete path.
        analysis = db_session.query(SymbolAnalysis).filter(
            SymbolAnalysis.id == analysis_id
        ).one()
        analysis.status = "failed"
        db_session.add(analysis)
        db_session.commit()

        delete_resp = client.delete(
            f"/v1/analysis/{analysis_id}",
            headers=auth_headers,
        )
        assert delete_resp.status_code == 204

        get_resp = client.get(
            f"/v1/analysis/{analysis_id}",
            headers=auth_headers,
        )
        assert get_resp.status_code == 404

    def test_404_when_deleting_nonexistent_analysis(
        self, client, auth_headers, db_session
    ):
        """DELETE /v1/analysis/{id} returns 404 for nonexistent analysis."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        fake_id = uuid4()
        resp = client.delete(
            f"/v1/analysis/{fake_id}",
            headers=auth_headers,
        )
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "not_found"
