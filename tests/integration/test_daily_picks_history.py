"""Integration tests for GET /v1/daily-picks/history."""
from __future__ import annotations

import pytest

from backtestforecast.models import User


def _set_user_plan(session, *, tier: str, subscription_status: str | None = None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


class TestDailyPicksHistory:
    def test_happy_path_returns_paginated_items(
        self, client, auth_headers, db_session
    ):
        """GET /v1/daily-picks/history returns paginated items."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        resp = client.get("/v1/daily-picks/history", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data or "items" in data

    def test_with_cursor_parameter(
        self, client, auth_headers, db_session
    ):
        """GET /v1/daily-picks/history accepts cursor for pagination."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        # Valid ISO 8601 timestamp with timezone
        cursor = "2025-01-15T12:00:00+00:00"
        resp = client.get(
            "/v1/daily-picks/history",
            params={"cursor": cursor, "limit": 5},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data

    def test_invalid_cursor_returns_422(
        self, client, auth_headers, db_session
    ):
        """GET /v1/daily-picks/history returns 422 for invalid cursor format."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        resp = client.get(
            "/v1/daily-picks/history",
            params={"cursor": "not-a-valid-timestamp"},
            headers=auth_headers,
        )
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
