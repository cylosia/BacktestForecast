from __future__ import annotations

from datetime import date

import pytest
import httpx

from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MAX_PAGINATION_PAGES, AsyncMassiveClient, MassiveClient


def test_parse_treasury_yield_average_converts_percent_to_decimal_fraction() -> None:
    rows = [
        {"date": "2024-01-02", "yield_3_month": 5.10},
        {"date": "2024-01-03", "yield_3_month": 5.30},
        {"date": "2024-01-04", "yield_3_month": None},
    ]

    average = MassiveClient.parse_treasury_yield_average(rows, field_name="yield_3_month")

    assert average == pytest.approx(0.052)


def test_get_average_treasury_yield_uses_massive_endpoint(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_paginated_json(self, path, params):
        captured["path"] = path
        captured["params"] = params
        return [
            {"date": "2024-01-02", "yield_3_month": 5.0},
            {"date": "2024-01-03", "yield_3_month": 5.2},
        ]

    monkeypatch.setattr(MassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        average = client.get_average_treasury_yield(date(2024, 1, 2), date(2024, 1, 3))
    finally:
        client.close()

    assert captured["path"] == "/fed/v1/treasury-yields"
    assert captured["params"] == {
        "date.gte": "2024-01-02",
        "date.lte": "2024-01-03",
        "sort": "date.asc",
        "limit": 50000,
    }
    assert average == pytest.approx(0.051)


def test_list_ex_dividend_dates_uses_dividends_endpoint(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_paginated_json(self, path, params):
        captured["path"] = path
        captured["params"] = params
        return [
            {"ticker": "AAPL", "ex_dividend_date": "2024-01-12"},
            {"ticker": "AAPL", "ex_dividend_date": "2024-02-09"},
        ]

    monkeypatch.setattr(MassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        ex_dates = client.list_ex_dividend_dates("AAPL", date(2024, 1, 1), date(2024, 2, 29))
    finally:
        client.close()

    assert captured["path"] == "/v3/reference/dividends"
    assert captured["params"] == {
        "ticker": "AAPL",
        "ex_dividend_date.gte": "2024-01-01",
        "ex_dividend_date.lte": "2024-02-29",
        "sort": "ex_dividend_date",
        "order": "asc",
        "limit": 1000,
    }
    assert ex_dates == {date(2024, 1, 12), date(2024, 2, 9)}


def test_list_option_contracts_uses_active_as_of_window(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_paginated_json(self, path, params):
        captured["path"] = path
        captured["params"] = params
        return [
            {
                "ticker": "O:SPY250430C00375000",
                "expiration_date": "2025-04-30",
                "strike_price": 375,
                "contract_type": "call",
                "shares_per_contract": 100,
            }
        ]

    monkeypatch.setattr(MassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        contracts = client.list_option_contracts(
            "SPY",
            date(2025, 4, 1),
            "call",
            date(2025, 4, 26),
            date(2025, 5, 6),
        )
    finally:
        client.close()

    assert captured["path"] == "/v3/reference/options/contracts"
    assert captured["params"] == {
        "underlying_ticker": "SPY",
        "contract_type": "call",
        "as_of": "2025-04-01",
        "expired": "false",
        "expiration_date.gte": "2025-04-26",
        "expiration_date.lte": "2025-05-06",
        "sort": "expiration_date",
        "order": "asc",
        "limit": 1000,
    }
    assert len(contracts) == 1


@pytest.mark.asyncio
async def test_async_list_option_contracts_uses_active_as_of_window(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_get_paginated_json(self, path, params):
        captured["path"] = path
        captured["params"] = params
        return [
            {
                "ticker": "O:SPY250430C00375000",
                "expiration_date": "2025-04-30",
                "strike_price": 375,
                "contract_type": "call",
                "shares_per_contract": 100,
            }
        ]

    monkeypatch.setattr(AsyncMassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        contracts = await client.list_option_contracts(
            "SPY",
            date(2025, 4, 1),
            "call",
            date(2025, 4, 26),
            date(2025, 5, 6),
        )
    finally:
        await client.close()

    assert captured["path"] == "/v3/reference/options/contracts"
    assert captured["params"] == {
        "underlying_ticker": "SPY",
        "contract_type": "call",
        "as_of": "2025-04-01",
        "expired": "false",
        "expiration_date.gte": "2025-04-26",
        "expiration_date.lte": "2025-05-06",
        "sort": "expiration_date",
        "order": "asc",
        "limit": 1000,
    }
    assert len(contracts) == 1


def test_massive_client_uses_explicit_transport_timeouts(monkeypatch) -> None:
    class _Settings:
        massive_api_key = "test-key"
        massive_base_url = "https://api.test.com"
        massive_timeout_seconds = 60.0
        massive_max_retries = 4
        massive_retry_backoff_seconds = 1.0
        app_env = "test"

    monkeypatch.setattr(
        "backtestforecast.integrations.massive_client.get_settings",
        lambda: _Settings(),
    )
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        timeout = client._http.timeout
        assert isinstance(timeout, httpx.Timeout)
        assert timeout.read == 60.0
        assert timeout.write == 60.0
        assert timeout.connect == 10.0
        assert timeout.pool == 10.0
    finally:
        client.close()


def test_sync_pagination_raises_when_page_limit_is_exceeded(monkeypatch) -> None:
    call_count = 0

    def fake_get_json(self, path, params=None):
        nonlocal call_count
        call_count += 1
        return {
            "results": [{"page": call_count}],
            "next_url": "/v3/reference/options/contracts?page=next",
        }

    monkeypatch.setattr(MassiveClient, "_get_json", fake_get_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        with pytest.raises(
            ExternalServiceError,
            match=f"safety limit of {MAX_PAGINATION_PAGES} pages",
        ):
            client._get_paginated_json("/v3/reference/options/contracts", params={"limit": 1})
    finally:
        client.close()

    assert call_count == MAX_PAGINATION_PAGES


@pytest.mark.asyncio
async def test_async_pagination_raises_when_page_limit_is_exceeded(monkeypatch) -> None:
    call_count = 0

    async def fake_get_json(self, path, params=None):
        nonlocal call_count
        call_count += 1
        return {
            "results": [{"page": call_count}],
            "next_url": "/v3/reference/options/contracts?page=next",
        }

    monkeypatch.setattr(AsyncMassiveClient, "_get_json", fake_get_json)
    client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        with pytest.raises(
            ExternalServiceError,
            match=f"safety limit of {MAX_PAGINATION_PAGES} pages",
        ):
            await client._get_paginated_json("/v3/reference/options/contracts", params={"limit": 1})
    finally:
        await client.close()

    assert call_count == MAX_PAGINATION_PAGES
