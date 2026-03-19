# TODO: MassiveClient (sync) and AsyncMassiveClient (async) duplicate nearly
# identical logic. Refactor to a shared implementation — e.g., a sync base
# with async wrappers using asyncio.to_thread, or generate both from a
# common template.
#
# Approach notes:
#   Option A (recommended): Extract a _MassiveClientCore class that holds all
#   configuration, parsing, and response-handling logic. MassiveClient and
#   AsyncMassiveClient become thin wrappers that only differ in HTTP transport
#   (httpx.Client vs httpx.AsyncClient) and sleep (time.sleep vs asyncio.sleep).
#   Each wrapper delegates _get_json/_get_paginated_json to the core, passing
#   the transport as a callable.
#
#   Option B: Keep MassiveClient as the canonical implementation and have
#   AsyncMassiveClient delegate via asyncio.to_thread for each public method.
#   Simpler but adds thread-hop latency per call and loses true async I/O.
#
#   Either way, _parse_snapshot_result, _pick_quote_timestamp, and bar/contract
#   parsing are already static and can be shared immediately.

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
from backtestforecast.market_data.types import (
    DailyBar,
    OptionContractRecord,
    OptionGreeks,
    OptionQuoteRecord,
    OptionSnapshotRecord,
)
from backtestforecast.resilience.circuit_breaker import CircuitBreaker

logger = structlog.get_logger("massive_client")

MAX_PAGINATION_PAGES = 100


def _get_traceparent_from_context() -> str | None:
    """Get traceparent from structlog context vars if bound (e.g. from Celery task headers)."""
    ctx = structlog.contextvars.get_contextvars()
    return ctx.get("traceparent")


def _parse_finite_float(value: object, field: str) -> float:
    """Convert *value* to float, raising ValueError on inf/nan."""
    result = float(value)  # type: ignore[arg-type]
    if math.isinf(result) or math.isnan(result):
        raise ValueError(f"Invalid market data value for {field}: {result}")
    return result

# Circuit breakers protect against cascading failures when the Massive API
# is down. Both sync and async clients share independent breakers so that
# a failure in one path does not block the other.
_massive_sync_circuit = CircuitBreaker(name="massive_sync_api", failure_threshold=5, recovery_timeout=30.0)
_massive_async_circuit = CircuitBreaker(name="massive_async_api", failure_threshold=5, recovery_timeout=30.0)


class MassiveClient:
    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.massive_api_key
        if not self.api_key:
            raise ConfigurationError("MASSIVE_API_KEY is required.")
        self.base_url = (base_url or settings.massive_base_url).rstrip("/")
        if settings.app_env in ("production", "staging") and not self.base_url.startswith("https://"):
            raise ConfigurationError("MASSIVE_BASE_URL must use HTTPS in production/staging.")
        self._base_netloc = urlparse(self.base_url).netloc
        self.timeout = settings.massive_timeout_seconds
        self.max_retries = settings.massive_max_retries
        self.retry_backoff_seconds = settings.massive_retry_backoff_seconds
        # NOTE: This client instance should be shared across threads when used in
        # concurrent contexts (e.g., scan ThreadPoolExecutor). httpx.Client is
        # thread-safe for read operations.
        self._http = httpx.Client(
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            ),
        )
        self._circuit = _massive_sync_circuit

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
                        open_price=_parse_finite_float(row["o"], "open"),
                        high_price=_parse_finite_float(row["h"], "high"),
                        low_price=_parse_finite_float(row["l"], "low"),
                        close_price=_parse_finite_float(row["c"], "close"),
                        volume=_parse_finite_float(row.get("v", 0), "volume"),
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

    def get_option_snapshot(
        self,
        underlying: str,
        option_ticker: str,
    ) -> OptionSnapshotRecord | None:
        """Fetch real-time greeks for a single option contract via the snapshot endpoint."""
        try:
            payload = self._get_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}/{quote(option_ticker, safe='')}",
            )
        except ExternalServiceError:
            logger.debug("massive_client.snapshot_unavailable", option_ticker=option_ticker)
            return None
        results = payload.get("results")
        if not isinstance(results, dict):
            return None
        return self._parse_snapshot_result(results)

    def get_option_chain_snapshot(
        self,
        underlying: str,
    ) -> list[OptionSnapshotRecord]:
        """Fetch real-time snapshots for all contracts on an underlying.

        Returns a list of snapshot records with greeks attached.
        """
        try:
            rows = self._get_paginated_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}",
                params={"limit": 250},
            )
        except ExternalServiceError:
            logger.debug("massive_client.chain_snapshot_unavailable", underlying=underlying)
            return []
        snapshots: list[OptionSnapshotRecord] = []
        for item in rows:
            parsed = self._parse_snapshot_result(item)
            if parsed is not None:
                snapshots.append(parsed)
        return snapshots

    @staticmethod
    def _parse_snapshot_result(result: dict[str, Any]) -> OptionSnapshotRecord | None:
        details = result.get("details", {})
        ticker = details.get("ticker") or result.get("ticker")
        if not isinstance(ticker, str):
            return None
        underlying = result.get("underlying_asset", {})
        underlying_ticker = underlying.get("ticker", "") if isinstance(underlying, dict) else ""

        greeks_raw = result.get("greeks")
        greeks: OptionGreeks | None = None
        if isinstance(greeks_raw, dict):
            greeks = OptionGreeks(
                delta=greeks_raw.get("delta"),
                gamma=greeks_raw.get("gamma"),
                theta=greeks_raw.get("theta"),
                vega=greeks_raw.get("vega"),
            )

        iv_raw = result.get("implied_volatility")
        implied_volatility = _parse_finite_float(iv_raw, "implied_volatility") if iv_raw is not None else None

        last_quote = result.get("last_quote", {})
        bid = last_quote.get("bid") if isinstance(last_quote, dict) else None
        ask = last_quote.get("ask") if isinstance(last_quote, dict) else None
        bid_f = _parse_finite_float(bid, "snapshot_bid") if bid is not None else None
        ask_f = _parse_finite_float(ask, "snapshot_ask") if ask is not None else None

        break_even_raw = result.get("break_even_price")
        open_interest_raw = result.get("open_interest")

        return OptionSnapshotRecord(
            ticker=ticker,
            underlying_ticker=underlying_ticker,
            greeks=greeks,
            implied_volatility=implied_volatility,
            break_even_price=_parse_finite_float(break_even_raw, "break_even_price") if break_even_raw is not None else None,
            open_interest=int(open_interest_raw) if open_interest_raw is not None else None,
            bid_price=bid_f,
            ask_price=ask_f,
        )

    def get_market_holidays(self) -> list[date]:
        """Fetch upcoming NYSE full-closure dates from /v1/marketstatus/upcoming.

        Returns only dates where the NYSE is fully closed (status="closed"),
        not early-close sessions.  The endpoint is forward-looking; past
        holidays are not included.
        """
        if not self._circuit.allow_request():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = f"{self.base_url}/v1/marketstatus/upcoming"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        retryable_message: str | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._http.get(url, headers=headers)
            except httpx.HTTPError as exc:
                self._circuit.record_failure(is_transient=True)
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code == 429 or response.status_code >= 500:
                self._circuit.record_failure(is_transient=True)
                retryable_message = (
                    "Massive rate limit reached. Retry later."
                    if response.status_code == 429
                    else "Massive is currently unavailable."
                )
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code == 404:
                logger.debug("massive_client.market_holidays_not_found", status=404)
                return []
            if response.status_code >= 400:
                self._circuit.record_failure(is_transient=False)
                raise ExternalServiceError(
                    f"Massive returned {response.status_code} for market holidays."
                )

            self._circuit.record_success()
            data = response.json()
            if not isinstance(data, list):
                raise ExternalServiceError("Massive market holidays returned an unexpected payload.")

            holidays: list[date] = []
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                if entry.get("exchange") != "NYSE":
                    continue
                if entry.get("status") != "closed":
                    continue
                raw_date = entry.get("date")
                if isinstance(raw_date, str):
                    try:
                        holidays.append(date.fromisoformat(raw_date))
                    except ValueError:
                        logger.debug("massive_client.holiday_date_parse_skipped", raw=raw_date)
            return holidays

        raise ExternalServiceError(retryable_message or "Massive request failed.")

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
                try:
                    dates.add(date.fromisoformat(raw_date))
                except ValueError:
                    logger.debug("massive.earnings.invalid_date", raw_date=raw_date)
                    continue
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
                parsed_next = urlparse(next_url)
                parsed_base = urlparse(self.base_url)
                if parsed_next.netloc != self._base_netloc or parsed_next.scheme != parsed_base.scheme:
                    logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
                    break
            elif not next_url.startswith("/"):
                logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
                break

            next_path = next_url
            next_params = None
            page += 1

        return rows

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self._circuit.allow_request():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = path if path.startswith("http") else f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        traceparent = _get_traceparent_from_context()
        if traceparent:
            headers["traceparent"] = traceparent
        retryable_message: str | None = None
        deadline = time.monotonic() + self.timeout * (self.max_retries + 1)

        for attempt in range(self.max_retries + 1):
            if time.monotonic() > deadline:
                raise ExternalServiceError("Massive request exceeded aggregate retry deadline.")
            try:
                response = self._http.get(
                    url,
                    params=params,
                    headers=headers,
                )
            except httpx.HTTPError as exc:
                self._circuit.record_failure(is_transient=True)
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code in {401, 403}:
                raise ExternalServiceError("Massive rejected the request. Verify API key and entitlements.")
            if response.status_code == 404:
                # 404 does not trip circuit breaker: data-not-found is not a service failure.
                raise ExternalServiceError("Required Massive endpoint or data was not found.")
            if response.status_code == 429:
                self._circuit.record_failure(is_transient=True)
                retryable_message = "Massive rate limit reached. Retry later."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 500:
                self._circuit.record_failure(is_transient=True)
                retryable_message = "Massive is currently unavailable."
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 400:
                safe_url = urlparse(url)._replace(query="").geturl()
                logger.warning(
                    "massive_client_error",
                    status=response.status_code,
                    detail=response.text[:500],
                    url=safe_url,
                )
                self._circuit.record_failure(is_transient=False)
                raise ExternalServiceError(
                    f"Massive returned {response.status_code}. The request could not be completed."
                )

            try:
                data = response.json()
            except Exception as exc:
                self._circuit.record_failure(is_transient=True)
                raise ExternalServiceError(f"Invalid JSON response from Massive API: {exc}") from exc
            self._circuit.record_success()
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
        if settings.app_env in ("production", "staging") and not self.base_url.startswith("https://"):
            raise ConfigurationError("MASSIVE_BASE_URL must use HTTPS in production/staging.")
        self._base_netloc = urlparse(self.base_url).netloc
        self.timeout = settings.massive_timeout_seconds
        self.max_retries = settings.massive_max_retries
        self.retry_backoff_seconds = settings.massive_retry_backoff_seconds
        self._http = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            ),
        )
        self._circuit = _massive_async_circuit

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
                        open_price=_parse_finite_float(row["o"], "open"),
                        high_price=_parse_finite_float(row["h"], "high"),
                        low_price=_parse_finite_float(row["l"], "low"),
                        close_price=_parse_finite_float(row["c"], "close"),
                        volume=_parse_finite_float(row.get("v", 0), "volume"),
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

    async def get_market_holidays(self) -> list[date]:
        """Async variant of MassiveClient.get_market_holidays."""
        if not await self._circuit.allow_request_async():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = f"{self.base_url}/v1/marketstatus/upcoming"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        retryable_message: str | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = await self._http.get(url, headers=headers)
            except httpx.HTTPError as exc:
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code == 429 or response.status_code >= 500:
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = (
                    "Massive rate limit reached. Retry later."
                    if response.status_code == 429
                    else "Massive is currently unavailable."
                )
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 400:
                await self._circuit.record_failure_async(is_transient=False)
                raise ExternalServiceError(
                    f"Massive returned {response.status_code} for market holidays."
                )

            await self._circuit.record_success_async()
            data = response.json()
            if not isinstance(data, list):
                raise ExternalServiceError("Massive market holidays returned an unexpected payload.")

            holidays: list[date] = []
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                if entry.get("exchange") != "NYSE":
                    continue
                if entry.get("status") != "closed":
                    continue
                raw_date = entry.get("date")
                if isinstance(raw_date, str):
                    try:
                        holidays.append(date.fromisoformat(raw_date))
                    except ValueError:
                        logger.debug("massive_client.holiday_date_parse_skipped", raw=raw_date)
            return holidays

        raise ExternalServiceError(retryable_message or "Massive request failed.")

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

    async def get_option_snapshot(
        self,
        underlying: str,
        option_ticker: str,
    ) -> OptionSnapshotRecord | None:
        try:
            payload = await self._get_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}/{quote(option_ticker, safe='')}",
            )
        except ExternalServiceError:
            logger.debug("massive_client.async_snapshot_unavailable", option_ticker=option_ticker)
            return None
        results = payload.get("results")
        if not isinstance(results, dict):
            return None
        return MassiveClient._parse_snapshot_result(results)

    async def get_option_chain_snapshot(
        self,
        underlying: str,
    ) -> list[OptionSnapshotRecord]:
        try:
            rows = await self._get_paginated_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}",
                params={"limit": 250},
            )
        except ExternalServiceError:
            logger.debug("massive_client.async_chain_snapshot_unavailable", underlying=underlying)
            return []
        snapshots: list[OptionSnapshotRecord] = []
        for item in rows:
            parsed = MassiveClient._parse_snapshot_result(item)
            if parsed is not None:
                snapshots.append(parsed)
        return snapshots

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
                parsed_next = urlparse(next_url)
                parsed_base = urlparse(self.base_url)
                if parsed_next.netloc != self._base_netloc or parsed_next.scheme != parsed_base.scheme:
                    logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
                    break
            elif not next_url.startswith("/"):
                logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
                break
            next_path = next_url
            next_params = None
            page += 1

        return rows

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not await self._circuit.allow_request_async():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = path if path.startswith("http") else f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        traceparent = _get_traceparent_from_context()
        if traceparent:
            headers["traceparent"] = traceparent
        retryable_message: str | None = None
        deadline = time.monotonic() + self.timeout * (self.max_retries + 1)

        for attempt in range(self.max_retries + 1):
            if time.monotonic() > deadline:
                raise ExternalServiceError("Massive request exceeded aggregate retry deadline.")
            try:
                response = await self._http.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, None)
                    continue
                raise ExternalServiceError(retryable_message) from exc

            if response.status_code in {401, 403}:
                raise ExternalServiceError("Massive rejected the request. Verify API key and entitlements.")
            if response.status_code == 404:
                # 404 does not trip circuit breaker: data-not-found is not a service failure.
                raise ExternalServiceError("Required Massive endpoint or data was not found.")
            if response.status_code == 429:
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = "Massive rate limit reached. Retry later."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 500:
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = "Massive is currently unavailable."
                if attempt < self.max_retries:
                    await self._async_sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue
                raise ExternalServiceError(retryable_message)
            if response.status_code >= 400:
                safe_url = urlparse(url)._replace(query="").geturl()
                logger.warning(
                    "massive_client_error",
                    status=response.status_code,
                    detail=response.text[:500],
                    url=safe_url,
                )
                await self._circuit.record_failure_async(is_transient=False)
                raise ExternalServiceError(
                    f"Massive returned {response.status_code}. The request could not be completed."
                )

            try:
                data = response.json()
            except Exception as exc:
                await self._circuit.record_failure_async(is_transient=True)
                raise ExternalServiceError(f"Invalid JSON response from Massive API: {exc}") from exc
            await self._circuit.record_success_async()
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
