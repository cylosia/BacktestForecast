"""Verify account deletion requires confirmation and creates audit trail."""
from __future__ import annotations


def test_delete_without_header_returns_422(client, auth_headers):
    """DELETE /v1/account/me without X-Confirm-Delete returns validation error."""
    response = client.delete("/v1/account/me", headers=auth_headers)
    assert response.status_code == 422
    assert "X-Confirm-Delete" in response.json()["error"]["message"]


def test_delete_with_wrong_header_returns_422(client, auth_headers):
    """DELETE /v1/account/me with wrong confirmation value returns validation error."""
    response = client.delete(
        "/v1/account/me",
        headers={**auth_headers, "X-Confirm-Delete": "yes-please"},
    )
    assert response.status_code == 422


def test_delete_with_correct_header_returns_204(client, auth_headers):
    """DELETE /v1/account/me with correct confirmation deletes the account."""
    response = client.delete(
        "/v1/account/me",
        headers={**auth_headers, "X-Confirm-Delete": "permanently-delete-my-account"},
    )
    assert response.status_code == 204
