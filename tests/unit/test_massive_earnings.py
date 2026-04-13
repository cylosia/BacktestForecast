from __future__ import annotations

from datetime import date

from backtestforecast.integrations.massive_client import AsyncMassiveClient, MassiveClient


def test_parse_earnings_records_maps_benzinga_rows_to_announcement_dates() -> None:
    rows = [
        {
            "benzinga_id": "benz-1",
            "ticker": "AAPL",
            "date": "2023-05-04",
            "time": "16:30:00",
            "estimated_eps": 1.43,
        }
    ]

    parsed = MassiveClient.parse_earnings_records(rows)

    assert len(parsed) == 1
    assert parsed[0].event_date == date(2023, 5, 4)
    assert parsed[0].event_type == "earnings_announcement_date"
    assert parsed[0].provider_event_id == "benz-1"


def test_parse_earnings_records_dedupes_same_day_rows() -> None:
    rows = [
        {
            "benzinga_id": "benz-old",
            "ticker": "AMBO",
            "date": "2022-05-02",
        },
        {
            "benzinga_id": "benz-new",
            "ticker": "AMBO",
            "date": "2022-05-02",
        },
    ]

    parsed = MassiveClient.parse_earnings_records(rows)

    assert len(parsed) == 1
    assert parsed[0].event_date == date(2022, 5, 2)
    assert parsed[0].event_type == "earnings_announcement_date"
    assert parsed[0].provider_event_id == "benz-old"


def test_sync_client_uses_benzinga_earnings_endpoint(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_paginated_json(self, path: str, params: dict[str, object]):
        captured["path"] = path
        captured["params"] = dict(params)
        return [
            {
                "benzinga_id": "benz-1",
                "ticker": "AAPL",
                "date": "2023-05-04",
            }
        ]

    monkeypatch.setattr(MassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")

    rows = client.list_earnings_event_records("AAPL", date(2023, 1, 1), date(2023, 5, 31))

    assert captured["path"] == "/benzinga/v1/earnings"
    assert captured["params"] == {
        "ticker": "AAPL",
        "date.gte": "2023-01-01",
        "date.lte": "2023-05-31",
        "sort": "date.asc",
        "limit": 1000,
    }
    assert len(rows) == 1
    assert rows[0].provider_event_id == "benz-1"


async def test_async_client_uses_benzinga_earnings_endpoint(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_get_paginated_json(self, path: str, params: dict[str, object]):
        captured["path"] = path
        captured["params"] = dict(params)
        return [
            {
                "benzinga_id": "benz-async-1",
                "ticker": "MSFT",
                "date": "2023-04-25",
            }
        ]

    monkeypatch.setattr(AsyncMassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")

    rows = await client.list_earnings_event_records("MSFT", date(2023, 1, 1), date(2023, 5, 31))

    assert captured["path"] == "/benzinga/v1/earnings"
    assert captured["params"] == {
        "ticker": "MSFT",
        "date.gte": "2023-01-01",
        "date.lte": "2023-05-31",
        "sort": "date.asc",
        "limit": 1000,
    }
    assert len(rows) == 1
    assert rows[0].provider_event_id == "benz-async-1"
