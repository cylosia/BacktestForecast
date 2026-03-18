"""Integration tests for sweep entitlement enforcement."""
from __future__ import annotations


def test_free_tier_cannot_create_sweeps(client, auth_headers):
    """Free-tier users must receive 403 when attempting to create sweeps."""
    resp = client.post(
        "/v1/sweeps",
        json={
            "symbol": "AAPL",
            "strategy_types": ["long_call"],
            "start_date": "2024-01-01",
            "end_date": "2024-02-15",
            "target_dte": 45,
            "dte_tolerance_days": 5,
            "max_holding_days": 30,
            "account_size": "10000",
            "risk_per_trade_pct": "5",
            "commission_per_contract": "0.65",
            "entry_rule_sets": [{"name": "no_filter", "entry_rules": []}],
        },
        headers=auth_headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"
