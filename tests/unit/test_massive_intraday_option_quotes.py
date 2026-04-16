from __future__ import annotations

import shutil
import uuid
from datetime import date
from pathlib import Path

import pytest

from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.intraday_option_quotes import (
    IntradayOptionQuoteCache,
    _session_bounds_ns,
)
from backtestforecast.market_data.types import OptionQuoteRecord


def test_parse_quotes_filters_invalid_rows_and_sorts_by_timestamp() -> None:
    quotes = MassiveClient.parse_quotes(
        [
            {"bid_price": 1.2, "ask_price": 1.4, "participant_timestamp": 20},
            {"bid_price": 0.0, "ask_price": 1.0, "participant_timestamp": 10},
            {"bid_price": 1.1, "ask_price": 1.3, "participant_timestamp": 15},
            {"bid_price": 1.5, "ask_price": 1.4, "participant_timestamp": 25},
        ],
        date(2026, 1, 5),
    )

    assert [quote.participant_timestamp for quote in quotes] == [15, 20]
    assert [quote.mid_price for quote in quotes] == pytest.approx([1.2, 1.3])


def test_list_option_quotes_for_date_uses_quotes_endpoint(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_paginated_json(self, path, params):
        captured["path"] = path
        captured["params"] = params
        return [{"bid_price": 1.0, "ask_price": 1.2, "participant_timestamp": 123}]

    monkeypatch.setattr(MassiveClient, "_get_paginated_json", fake_get_paginated_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        quotes = client.list_option_quotes_for_date("O:SPY260105C00500000", date(2026, 1, 5))
    finally:
        client.close()

    assert captured["path"] == "/v3/quotes/O%3ASPY260105C00500000"
    assert captured["params"] == {
        "timestamp": "2026-01-05",
        "sort": "participant_timestamp.asc",
        "limit": 50000,
    }
    assert len(quotes) == 1
    assert quotes[0].mid_price == 1.1


def test_intraday_option_quote_cache_filters_regular_session_and_uses_disk_cache() -> None:
    trade_date = date(2026, 1, 5)
    session_open_ns, session_close_ns = _session_bounds_ns(trade_date)
    fetch_count = {"value": 0}

    class _FakeClient:
        def list_option_quotes_for_date(self, option_ticker: str, requested_trade_date: date) -> list[OptionQuoteRecord]:
            assert option_ticker == "O:SPY260105C00500000"
            assert requested_trade_date == trade_date
            fetch_count["value"] += 1
            return [
                OptionQuoteRecord(
                    trade_date=trade_date,
                    bid_price=0.9,
                    ask_price=1.1,
                    participant_timestamp=session_open_ns - 1,
                ),
                OptionQuoteRecord(
                    trade_date=trade_date,
                    bid_price=1.0,
                    ask_price=1.2,
                    participant_timestamp=session_open_ns + 1,
                ),
                OptionQuoteRecord(
                    trade_date=trade_date,
                    bid_price=1.1,
                    ask_price=1.3,
                    participant_timestamp=session_close_ns + 1,
                ),
            ]

        def close(self) -> None:
            return None

    cache_root = Path.cwd() / "logs" / f"test_intraday_cache_{uuid.uuid4().hex}"
    try:
        cache = IntradayOptionQuoteCache(cache_root, client_factory=_FakeClient)
        first = cache.get_regular_session_quotes("O:SPY260105C00500000", trade_date)
        second = cache.get_regular_session_quotes("O:SPY260105C00500000", trade_date)

        assert fetch_count["value"] == 1
        assert len(first) == 1
        assert first[0].mid_price == pytest.approx(1.1)
        assert second[0].participant_timestamp == session_open_ns + 1

        class _UnexpectedFetchClient:
            def list_option_quotes_for_date(self, option_ticker: str, requested_trade_date: date):
                raise RuntimeError("unexpected fetch")

            def close(self) -> None:
                return None

        cache_from_disk = IntradayOptionQuoteCache(cache_root, client_factory=_UnexpectedFetchClient)
        disk_quotes = cache_from_disk.get_regular_session_quotes("O:SPY260105C00500000", trade_date)

        assert len(disk_quotes) == 1
        assert disk_quotes[0].mid_price == pytest.approx(1.1)
    finally:
        shutil.rmtree(cache_root, ignore_errors=True)
