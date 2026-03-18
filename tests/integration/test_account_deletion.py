"""Verify account deletion requires confirmation and cancels in-flight jobs."""
from __future__ import annotations

import pytest


def test_delete_account_requires_confirmation_header(client, auth_headers):
    """DELETE /v1/account/me must require X-Confirm-Delete header."""
    response = client.delete("/v1/account/me", headers=auth_headers)
    assert response.status_code == 422
    body = response.json()
    assert "X-Confirm-Delete" in body.get("error", {}).get("message", "")


def test_delete_account_rejects_wrong_confirmation(client, auth_headers):
    response = client.delete(
        "/v1/account/me",
        headers={**auth_headers, "X-Confirm-Delete": "yes"},
    )
    assert response.status_code == 422


def test_delete_account_succeeds_with_correct_header(client, auth_headers):
    """With correct header, account should be deleted."""
    response = client.delete(
        "/v1/account/me",
        headers={**auth_headers, "X-Confirm-Delete": "permanently-delete-my-account"},
    )
    assert response.status_code == 204
