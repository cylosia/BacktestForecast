from __future__ import annotations

import json
import threading
from datetime import date, datetime, time
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.types import OptionQuoteRecord


_EASTERN_TZ = ZoneInfo("America/New_York")
_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(16, 0)


def _session_bounds_ns(trade_date: date) -> tuple[int, int]:
    session_open = datetime.combine(trade_date, _MARKET_OPEN, tzinfo=_EASTERN_TZ)
    session_close = datetime.combine(trade_date, _MARKET_CLOSE, tzinfo=_EASTERN_TZ)
    return int(session_open.timestamp() * 1_000_000_000), int(session_close.timestamp() * 1_000_000_000)


def _sanitize_option_ticker(option_ticker: str) -> str:
    return option_ticker.replace(":", "_")


def filter_regular_session_quotes(
    quotes: list[OptionQuoteRecord],
    trade_date: date,
) -> list[OptionQuoteRecord]:
    session_open_ns, session_close_ns = _session_bounds_ns(trade_date)
    filtered: list[OptionQuoteRecord] = []
    for quote in quotes:
        timestamp = quote.participant_timestamp
        if timestamp is None:
            continue
        if timestamp < session_open_ns or timestamp > session_close_ns:
            continue
        if quote.mid_price is None or quote.mid_price <= 0:
            continue
        filtered.append(quote)
    filtered.sort(key=lambda item: item.participant_timestamp or -1)
    return filtered


class IntradayOptionQuoteCache:
    def __init__(
        self,
        cache_root: Path,
        *,
        client_factory: Callable[[], MassiveClient] | None = None,
    ) -> None:
        self._cache_root = Path(cache_root)
        self._client_factory = client_factory or MassiveClient
        self._lock = threading.RLock()
        self._memory_cache: dict[tuple[str, date], tuple[OptionQuoteRecord, ...]] = {}

    def get_regular_session_quotes(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> list[OptionQuoteRecord]:
        cache_key = (option_ticker, trade_date)
        with self._lock:
            cached = self._memory_cache.get(cache_key)
            if cached is not None:
                return list(cached)

        quotes = self._load_from_disk(option_ticker, trade_date)
        if quotes is None:
            quotes = self._fetch_and_store(option_ticker, trade_date)

        with self._lock:
            self._memory_cache[cache_key] = tuple(quotes)
        return list(quotes)

    def get_open_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        quotes = self.get_regular_session_quotes(option_ticker, trade_date)
        return quotes[0] if quotes else None

    def get_close_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        quotes = self.get_regular_session_quotes(option_ticker, trade_date)
        return quotes[-1] if quotes else None

    def _cache_path(self, option_ticker: str, trade_date: date) -> Path:
        return (
            self._cache_root
            / trade_date.isoformat()
            / f"{_sanitize_option_ticker(option_ticker)}.json"
        )

    def _load_from_disk(self, option_ticker: str, trade_date: date) -> list[OptionQuoteRecord] | None:
        path = self._cache_path(option_ticker, trade_date)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        rows = payload.get("quotes")
        if not isinstance(rows, list):
            return None
        quotes: list[OptionQuoteRecord] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                quotes.append(
                    OptionQuoteRecord(
                        trade_date=trade_date,
                        bid_price=float(row["bid_price"]),
                        ask_price=float(row["ask_price"]),
                        participant_timestamp=int(row["participant_timestamp"])
                        if row.get("participant_timestamp") is not None
                        else None,
                        source_option_ticker=option_ticker,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return quotes

    def _fetch_and_store(self, option_ticker: str, trade_date: date) -> list[OptionQuoteRecord]:
        client = self._client_factory()
        try:
            quotes = filter_regular_session_quotes(
                client.list_option_quotes_for_date(option_ticker, trade_date),
                trade_date,
            )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

        path = self._cache_path(option_ticker, trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "option_ticker": option_ticker,
                    "trade_date": trade_date.isoformat(),
                    "quotes": [
                        {
                            "bid_price": quote.bid_price,
                            "ask_price": quote.ask_price,
                            "participant_timestamp": quote.participant_timestamp,
                        }
                        for quote in quotes
                    ],
                },
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        return quotes
