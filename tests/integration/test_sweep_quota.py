"""Integration tests for sweep quota enforcement."""
from __future__ import annotations

from backtestforecast.models import User


def _set_user_plan(session, *, tier: str, subscription_status: str | None = None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _sweep_payload(**overrides):
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


class TestSweepQuota:
    def test_sweep_enforces_monthly_quota(
        self, client, auth_headers, db_session, _fake_celery
    ):
        """POST /v1/sweeps enforces monthly quota for Pro tier (10 sweeps)."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        # Pro tier has 10 sweeps/month limit. Create 10 sweeps.
        for i in range(10):
            resp = client.post(
                "/v1/sweeps",
                json=_sweep_payload(idempotency_key=f"quota-test-{i}"),
                headers=auth_headers,
            )
            assert resp.status_code == 202, (
                f"Expected 202 for sweep {i+1}, got {resp.status_code}: {resp.text}"
            )

        # 11th sweep should exceed quota
        resp = client.post(
            "/v1/sweeps",
            json=_sweep_payload(idempotency_key="quota-test-11"),
            headers=auth_headers,
        )
        assert resp.status_code == 403
        assert resp.json()["error"]["code"] == "quota_exceeded"

    def test_exceeding_quota_returns_quota_exceeded_error(
        self, client, auth_headers, db_session, _fake_celery
    ):
        """Exceeding sweep quota returns quota_exceeded error."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        # Exhaust quota (10 for Pro)
        for i in range(10):
            client.post(
                "/v1/sweeps",
                json=_sweep_payload(idempotency_key=f"exceed-test-{i}"),
                headers=auth_headers,
            )

        resp = client.post(
            "/v1/sweeps",
            json=_sweep_payload(idempotency_key="exceed-test-11"),
            headers=auth_headers,
        )
        assert resp.status_code == 403
        data = resp.json()
        assert data["error"]["code"] == "quota_exceeded"
        assert "quota" in data["error"]["message"].lower() or "used" in data["error"]["message"].lower()
