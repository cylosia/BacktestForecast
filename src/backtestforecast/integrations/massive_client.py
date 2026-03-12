from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import UTC, date, datetime
from typing import Any, Self
from urllib.parse import quote, urlparse

import httpx
import structlog

from backtestforecast.config import get_settings
from backtestforecast.errors import ConfigurationError, ExternalServiceError
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord

logger = structlog.get_logger("massive_client")

MAX_PAGINATION_PAGES = 100


class MassiveClient:
    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.massive_api_key
        if not self.api_key:
            raise ConfigurationError("MASSIVE_API_KEY is required.")
        self.base_url = (base_url or settings.massive_base_url).rstrip("/")
        self._base_netloc = urlparse(self.base_url).netloc
        self.timeout = settings.massive_timeout_seconds
        self.max_retries = settings.massive_max_retries
        self.retry_backoff_seconds = settings.massive_retry_backoff_seconds
        self._http = httpx.Client(timeout=self.timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get_stock_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        payload = self._get_json(
            f"/v2/aggs/ticker/{quote(symbol, safe='')}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}",
            params={
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
            },
        )

        results = payload.get("results", [])
        bars: list[DailyBar] = []
        for row in results:
            try:
                timestamp = row.get("t")
                if timestamp is None:
                    continue
                trade_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC).date()
                bars.append(
                    DailyBar(
                        trade_date=trade_date,
                        open_price=float(row["o"]),
                        high_price=float(row["h"]),
                        low_price=float(row["l"]),
                        close_price=float(row["c"]),
                        volume=float(row.get("v", 0)),
                    )
                )
            except (KeyError, ValueError, TypeError):
                logger.debug("massive_client.bar_parse_skipped", symbol=symbol, row=row)
        return bars

    def list_option_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_gte: date,
        expiration_lte: date,
    ) -> list[OptionContractRecord]:
        rows = self._get_paginated_json(
            "/v3/reference/options/contracts",
            params={
                "underlying_ticker": symbol,
                "contract_type": contract_type,
                "as_of": as_of_date.isoformat(),
                "expired": "true",
                "expiration_date.gte": expiration_gte.isoformat(),
                "expiration_date.lte": expiration_lte.isoformat(),
                "sort": "expiration_date.asc",
                "limit": 1000,
            },
        )

        contracts: list[OptionContractRecord] = []
        for row in rows:
            ticker = row.get("ticker")
            expiration_text = row.get("expiration_date")
            strike_price = row.get("strike_price")
            row_contract_type = row.get("contract_type")
            if not isinstance(ticker, str) or not isinstance(expiration_text, str):
                continue
            if strike_price is None or not isinstance(row_contract_type, str):
                continue
            strike = float(strike_price)
            if not math.isfinite(strike) or strike <= 0:
                continue
            contracts.append(
                OptionContractRecord(
                    ticker=ticker,
                    contract_type=row_contract_type,
                    expiration_date=date.fromisoformat(expiration_text),
                    strike_price=strike,
                    shares_per_contract=float(row.get("shares_per_contract", 100)),
                )
            )
        return contracts

    def get_option_quote_for_date(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> OptionQuoteRecord | None:
        payload = self._get_json(
            f"/v3/quotes/{quote(option_ticker, safe='')}",
            params={
                "timestamp": trade_date.isoformat(),
                "sort": "participant_timestamp.desc",
                "limit": 10,
            },
        )

        results = payload.get("results", [])
        for row in results:
            bid_price = row.get("bid_price")
            ask_price = row.get("ask_price")
            if bid_price is None or ask_price is None:
                continue
            bid = float(bid_price)
            ask = float(ask_price)
            if bid <= 0 or ask <= 0:
                continue
            if not math.isfinite(bid) or not math.isfinite(ask):
                continue
            if bid > ask:
                continue
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=bid,
                ask_price=ask,
                participant_timestamp=self._pick_quote_timestamp(row),
            )
        return None

    def list_earnings_event_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        attempts = [
            {
                "ticker": symbol,
                "type": "earnings_announcement_date,earnings_conference_call",
                "date.gte": start_date.isoformat(),
                "date.lte": end_date.isoformat(),
                "sort": "date.asc",
                "limit": 1000,
            },
            {
                "ticker": symbol,
                "type": "earnings_announcement_date,earnings_conference_call",
                "date_gte": start_date.isoformat(),
                "date_lte": end_date.isoformat(),
                "sort": "date.asc",
                "limit": 1000,
            },
        ]

        last_error: ExternalServiceError | None = None
        for params in attempts:
            try:
                rows = self._get_paginated_json("/tmx/v1/corporate-events", params=params)
            except ExternalServiceError as exc:
                last_error = exc
                continue

            dates: set[date] = set()
            for row in rows:
                raw_date = row.get("date")
                event_type = row.get("type")
                if not isinstance(raw_date, str) or not isinstance(event_type, str):
                    continue
                if event_type not in {"earnings_announcement_date", "earnings_conference_call"}:
                    continue
                dates.add(date.fromisoformat(raw_date))
            return dates

        if last_error is not None:
            raise last_error
        return set()

    def _get_paginated_json(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        next_path: str | None = path
        next_params: dict[str, Any] | None = params.copy()
        page = 0

        while next_path:
            if page >= MAX_PAGINATION_PAGES:
                break
            payload = self._get_json(next_path, params=next_params)
            rows.extend(payload.get("results", []))
            next_url = payload.get("next_url")
            if not isinstance(next_url, str) or not next_url:
                break
            if next_url.startswith("http"):
                if urlparse(next_url).netloc != self._base_netloc:
                    break

            next_path = next_url
            next_params = None
            page += 1

        return rows

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        retryable_message: str | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._http.get(
                    url,
                    params=params,
                    headers=headers,
                )
            except httpx.HTTPError as exc:
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code in {401, 403}:
                raise ExternalServiceError("Massive rejected the request. Verify API key and entitlements.")
            if response.status_code == 404:
                raise ExternalServiceError("Required Massive endpoint or data was not found.")
            if response.status_code == 429:
                retryable_message = "Massive rate limit reached. Retry later."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 500:
                retryable_message = "Massive is currently unavailable."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 400:
                logger.warning(
                    "massive_client_error",
                    status=response.status_code,
                    detail=response.text[:500],
                    url=url,
                )
                raise ExternalServiceError(
                    f"Massive returned {response.status_code}. The request could not be completed."
                )

            data = response.json()
            if not isinstance(data, dict):
                raise ExternalServiceError("Massive returned an unexpected response payload.")
            return data

        raise ExternalServiceError(retryable_message or "Massive request failed.")

    _MAX_RETRY_AFTER_SECONDS = 120.0

    def _sleep_before_retry(self, attempt: int, retry_after_header: str | None) -> None:
        if retry_after_header:
            try:
                retry_after_seconds = min(max(float(retry_after_header), 0.0), self._MAX_RETRY_AFTER_SECONDS)
                time.sleep(retry_after_seconds)
                return
            except ValueError:
                pass
        if self.retry_backoff_seconds <= 0:
            return
        base_delay = self.retry_backoff_seconds * (2 ** attempt)
        jitter = random.uniform(0, base_delay * 0.5)
        time.sleep(min(base_delay + jitter, self._MAX_RETRY_AFTER_SECONDS))

    @staticmethod
    def _pick_quote_timestamp(row: dict[str, Any]) -> int | None:
        for key in ("participant_timestamp", "sip_timestamp", "timestamp"):
            raw = row.get(key)
            if isinstance(raw, int):
                return raw
        return None


class AsyncMassiveClient:
    """Async variant of MassiveClient for use in async FastAPI endpoints."""

    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.massive_api_key
        if not self.api_key:
            raise ConfigurationError("MASSIVE_API_KEY is required.")
        self.base_url = (base_url or settings.massive_base_url).rstrip("/")
        self._base_netloc = urlparse(self.base_url).netloc
        self.timeout = settings.massive_timeout_seconds
        self.max_retries = settings.massive_max_retries
        self.retry_backoff_seconds = settings.massive_retry_backoff_seconds
        self._http = httpx.AsyncClient(timeout=self.timeout)

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def get_stock_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        payload = await self._get_json(
            f"/v2/aggs/ticker/{quote(symbol, safe='')}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}",
            params={"adjusted": "true", "sort": "asc", "limit": 50000},
        )
        results = payload.get("results", [])
        bars: list[DailyBar] = []
        for row in results:
            try:
                timestamp = row.get("t")
                if timestamp is None:
                    continue
                trade_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC).date()
                bars.append(
                    DailyBar(
                        trade_date=trade_date,
                        open_price=float(row["o"]),
                        high_price=float(row["h"]),
                        low_price=float(row["l"]),
                        close_price=float(row["c"]),
                        volume=float(row.get("v", 0)),
                    )
                )
            except (KeyError, ValueError, TypeError):
                logger.debug("massive_client.async_bar_parse_skipped", symbol=symbol, row=row)
        return bars

    async def list_option_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_gte: date,
        expiration_lte: date,
    ) -> list[OptionContractRecord]:
        rows = await self._get_paginated_json(
            "/v3/reference/options/contracts",
            params={
                "underlying_ticker": symbol,
                "contract_type": contract_type,
                "as_of": as_of_date.isoformat(),
                "expired": "true",
                "expiration_date.gte": expiration_gte.isoformat(),
                "expiration_date.lte": expiration_lte.isoformat(),
                "sort": "expiration_date.asc",
                "limit": 1000,
            },
        )
        contracts: list[OptionContractRecord] = []
        for row in rows:
            ticker = row.get("ticker")
            expiration_text = row.get("expiration_date")
            strike_price = row.get("strike_price")
            row_contract_type = row.get("contract_type")
            if not isinstance(ticker, str) or not isinstance(expiration_text, str):
                continue
            if strike_price is None or not isinstance(row_contract_type, str):
                continue
            strike = float(strike_price)
            if not math.isfinite(strike) or strike <= 0:
                continue
            contracts.append(
                OptionContractRecord(
                    ticker=ticker,
                    contract_type=row_contract_type,
                    expiration_date=date.fromisoformat(expiration_text),
                    strike_price=strike,
                    shares_per_contract=float(row.get("shares_per_contract", 100)),
                )
            )
        return contracts

    async def get_option_quote_for_date(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> OptionQuoteRecord | None:
        payload = await self._get_json(
            f"/v3/quotes/{quote(option_ticker, safe='')}",
            params={"timestamp": trade_date.isoformat(), "sort": "participant_timestamp.desc", "limit": 10},
        )
        results = payload.get("results", [])
        for row in results:
            bid_price = row.get("bid_price")
            ask_price = row.get("ask_price")
            if bid_price is None or ask_price is None:
                continue
            bid = float(bid_price)
            ask = float(ask_price)
            if bid <= 0 or ask <= 0:
                continue
            if not math.isfinite(bid) or not math.isfinite(ask):
                continue
            if bid > ask:
                continue
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=bid,
                ask_price=ask,
                participant_timestamp=MassiveClient._pick_quote_timestamp(row),
            )
        return None

    async def _get_paginated_json(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        next_path: str | None = path
        next_params: dict[str, Any] | None = params.copy()
        page = 0

        while next_path:
            if page >= MAX_PAGINATION_PAGES:
                break
            payload = await self._get_json(next_path, params=next_params)
            rows.extend(payload.get("results", []))
            next_url = payload.get("next_url")
            if not isinstance(next_url, str) or not next_url:
                break
            if next_url.startswith("http"):
                if urlparse(next_url).netloc != self._base_netloc:
                    break
            next_path = next_url
            next_params = None
            page += 1

        return rows

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        retryable_message: str | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = await self._http.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code in {401, 403}:
                raise ExternalServiceError("Massive rejected the request. Verify API key and entitlements.")
            if response.status_code == 404:
                raise ExternalServiceError("Required Massive endpoint or data was not found.")
            if response.status_code == 429:
                retryable_message = "Massive rate limit reached. Retry later."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 500:
                retryable_message = "Massive is currently unavailable."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 400:
                logger.warning(
                    "massive_client_error",
                    status=response.status_code,
                    detail=response.text[:500],
                    url=url,
                )
                raise ExternalServiceError(
                    f"Massive returned {response.status_code}. The request could not be completed."
                )

            data = response.json()
            if not isinstance(data, dict):
                raise ExternalServiceError("Massive returned an unexpected response payload.")
            return data

        raise ExternalServiceError(retryable_message or "Massive request failed.")

    _MAX_RETRY_AFTER_SECONDS = 120.0

    async def _async_sleep_before_retry(self, attempt: int, retry_after_header: str | None) -> None:
        if retry_after_header:
            try:
                retry_after_seconds = min(max(float(retry_after_header), 0.0), self._MAX_RETRY_AFTER_SECONDS)
                await asyncio.sleep(retry_after_seconds)
                return
            except ValueError:
                pass
        if self.retry_backoff_seconds <= 0:
            return
        base_delay = self.retry_backoff_seconds * (2 ** attempt)
        jitter = random.uniform(0, base_delay * 0.5)
        await asyncio.sleep(min(base_delay + jitter, self._MAX_RETRY_AFTER_SECONDS))
