"""Integration tests for GET /v1/strategy-catalog."""
from __future__ import annotations


class TestStrategyCatalog:
    def test_happy_path_returns_catalog(self, client, auth_headers):
        """GET /v1/strategy-catalog returns the strategy catalog."""
        resp = client.get("/v1/strategy-catalog", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "groups" in data
        assert "total_strategies" in data
        assert "user_tier" in data

    def test_catalog_is_non_empty(self, client, auth_headers):
        """GET /v1/strategy-catalog returns a non-empty catalog."""
        resp = client.get("/v1/strategy-catalog", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_strategies"] > 0
        assert len(data["groups"]) > 0
        for group in data["groups"]:
            assert "category" in group
            assert "strategies" in group
            assert len(group["strategies"]) > 0


    def test_calendar_catalog_exposes_contract_metadata(self, client, auth_headers):
        resp = client.get("/v1/strategy-catalog", headers=auth_headers)
        assert resp.status_code == 200
        calendar = next(
            strategy
            for group in resp.json()["groups"]
            for strategy in group["strategies"]
            if strategy["strategy_type"] == "calendar_spread"
        )
        assert calendar["supported_contract_types"] == ["call", "put"]
        assert calendar["default_contract_type"] == "call"
