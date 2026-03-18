"""Test _try_authenticate returns feature flags when a valid Bearer token is provided."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_meta_returns_features_with_bearer_token(client, auth_headers):
    """GET /v1/meta with a valid Authorization: Bearer header should include
    the ``features`` dict in the response."""
    response = client.get("/v1/meta", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert "features" in data, "Authenticated /v1/meta should include 'features'"
    assert isinstance(data["features"], dict)
    assert "backtests" in data["features"]


def test_meta_without_auth_omits_features(client):
    """GET /v1/meta without auth should NOT include features."""
    response = client.get("/v1/meta")
    assert response.status_code == 200
    data = response.json()
    assert "features" not in data, "Unauthenticated /v1/meta should omit 'features'"
