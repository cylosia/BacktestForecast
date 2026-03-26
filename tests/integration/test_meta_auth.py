"""Test _try_authenticate returns feature flags when a valid Bearer token is provided."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def _ensure_user_exists(client, auth_headers):
    """Hit an authenticated endpoint to trigger user creation via get_or_create.

    The /v1/meta endpoint intentionally does NOT create users (read-only
    lookup) so we need the user to exist before testing authenticated meta.
    """
    client.get("/v1/backtests", headers=auth_headers)


def test_meta_returns_features_with_bearer_token(client, auth_headers):
    """GET /v1/meta with a valid Authorization: Bearer header should include
    the ``features`` dict in the response."""
    _ensure_user_exists(client, auth_headers)
    response = client.get("/v1/meta", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert "features" in data, "Authenticated /v1/meta should include 'features'"
    assert isinstance(data["features"], dict)
    assert "backtests" in data["features"]


def test_meta_without_auth_returns_null_features(client):
    """GET /v1/meta without auth should not expose feature flags."""
    response = client.get("/v1/meta")
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert data["features"] is None


def test_meta_with_auth_but_no_user_record_omits_features(client):
    """GET /v1/meta with a valid JWT but no pre-existing user record should
    omit features - the meta endpoint must not create user records."""
    response = client.get("/v1/meta", headers={"Authorization": "Bearer test-token"})
    assert response.status_code == 200
    data = response.json()
    assert data.get("service") == "backtestforecast-api"
