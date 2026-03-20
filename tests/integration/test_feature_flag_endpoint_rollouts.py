from __future__ import annotations

import pytest

from backtestforecast.config import Settings
from backtestforecast.models import User
from tests.integration.test_api_critical_flows import _backtest_payload, _set_user_plan


@pytest.fixture(autouse=True)
def _reset_feature_flag_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Settings, "feature_backtests_rollout_pct", 100)
    monkeypatch.setattr(Settings, "feature_backtests_allow_user_ids", "")
    monkeypatch.setattr(Settings, "feature_backtests_tiers", "")
    monkeypatch.setattr(Settings, "feature_backtests_enabled", True)
    monkeypatch.setattr(Settings, "feature_billing_rollout_pct", 100)
    monkeypatch.setattr(Settings, "feature_billing_allow_user_ids", "")
    monkeypatch.setattr(Settings, "feature_billing_tiers", "")
    monkeypatch.setattr(Settings, "feature_billing_enabled", True)


def test_backtest_create_respects_partial_rollout(monkeypatch, client, auth_headers, db_session):
    import backtestforecast.feature_flags as feature_flags

    client.get("/v1/me", headers=auth_headers)
    monkeypatch.setattr(Settings, "feature_backtests_rollout_pct", 50)
    monkeypatch.setattr(feature_flags, "_deterministic_bucket", lambda user_id, feature_name: 99)

    resp = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers)

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


def test_backtest_create_allow_list_overrides_rollout(monkeypatch, client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    monkeypatch.setattr(Settings, "feature_backtests_rollout_pct", 0)
    monkeypatch.setattr(Settings, "feature_backtests_allow_user_ids", str(user.id))

    resp = client.post("/v1/backtests", json=_backtest_payload(), headers=auth_headers)

    assert resp.status_code == 202
    assert resp.json()["symbol"] == "AAPL"


def test_billing_create_uses_effective_plan_tier(client, auth_headers, db_session, monkeypatch):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="premium", subscription_status="canceled")
    monkeypatch.setattr(Settings, "feature_billing_tiers", "premium")

    resp = client.post(
        "/v1/billing/portal-session",
        headers=auth_headers,
        json={"return_path": "/app/settings/billing"},
    )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"
