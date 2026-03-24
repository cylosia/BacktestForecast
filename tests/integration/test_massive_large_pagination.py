from __future__ import annotations

import pytest

from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient

pytestmark = pytest.mark.integration


def test_massive_client_collects_large_valid_pagination_chains(monkeypatch: pytest.MonkeyPatch) -> None:
    total_pages = 75

    def fake_get_json(self, path, params=None):
        page = 1 if params is not None else int(path.split("page=")[1])
        payload = {
            "results": [{"page": page}],
            "next_url": f"/v3/reference/dividends?page={page + 1}",
        }
        if page >= total_pages:
            payload["next_url"] = None
        return payload

    monkeypatch.setattr(MassiveClient, "_get_json", fake_get_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        rows = client._get_paginated_json("/v3/reference/dividends", params={"limit": 1})
    finally:
        client.close()

    assert len(rows) == total_pages
    assert rows[0] == {"page": 1}
    assert rows[-1] == {"page": total_pages}


def test_massive_client_rejects_invalid_next_url_in_large_pagination_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(self, path, params=None):
        page = 1 if params is not None else int(path.split("page=")[1])
        if page == 40:
            return {
                "results": [{"page": page}],
                "next_url": "https://evil.example.com/v3/reference/dividends?page=41",
            }
        return {
            "results": [{"page": page}],
            "next_url": f"/v3/reference/dividends?page={page + 1}",
        }

    monkeypatch.setattr(MassiveClient, "_get_json", fake_get_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        with pytest.raises(ExternalServiceError, match="invalid pagination continuation URL"):
            client._get_paginated_json("/v3/reference/dividends", params={"limit": 1})
    finally:
        client.close()
