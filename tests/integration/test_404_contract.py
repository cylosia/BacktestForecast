"""Contract tests for 404 error response shape."""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

_FAKE_ID = str(uuid.uuid4())

_ENDPOINTS_404 = [
    f"/v1/backtests/{_FAKE_ID}",
    f"/v1/backtests/{_FAKE_ID}/status",
    f"/v1/scans/{_FAKE_ID}",
    f"/v1/scans/{_FAKE_ID}/recommendations",
    f"/v1/exports/{_FAKE_ID}/status",
    f"/v1/analysis/{_FAKE_ID}",
    f"/v1/analysis/{_FAKE_ID}/status",
]


@pytest.mark.parametrize("path", _ENDPOINTS_404)
def test_resource_not_found_returns_404_with_error_envelope(
    client: TestClient,
    path: str,
) -> None:
    response = client.get(path, headers={"Authorization": "Bearer test-token"})
    assert response.status_code == 404
    body = response.json()
    assert "error" in body
    assert "code" in body["error"]
    assert "message" in body["error"]
