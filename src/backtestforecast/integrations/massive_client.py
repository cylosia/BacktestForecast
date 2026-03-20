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


_massive_circuit = CircuitBreaker(name="massive_api", failure_threshold=5, recovery_timeout=30.0)
_massive_sync_circuit = _massive_circuit


# ---------------------------------------------------------------------------
# Shared core: configuration, URL building, response/data parsing
# ---------------------------------------------------------------------------

class _MassiveClientCore:
    """Holds all configuration and pure-logic methods shared by sync and async clients.

    Transport-agnostic: no HTTP calls or sleeps happen in this class.
    """

    _MAX_RETRY_AFTER_SECONDS = 120.0

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
        self._circuit = _massive_circuit

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, api_key=[REDACTED])"

    def _build_url(self, path: str) -> str:
        return path if path.startswith("http") else f"{self.base_url}{path}"

    def _build_headers(self) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        traceparent = _get_traceparent_from_context()
        if traceparent:
            headers["traceparent"] = traceparent
        return headers

    def _compute_retry_delay(self, attempt: int, retry_after_header: str | None) -> float:
        """Return how many seconds to sleep before the next retry."""
        if retry_after_header:
            try:
                return min(max(float(retry_after_header), 0.0), self._MAX_RETRY_AFTER_SECONDS)
            except ValueError:
                pass
        if self.retry_backoff_seconds <= 0:
            return 0.0
        base_delay = self.retry_backoff_seconds * (2 ** attempt)
        jitter = random.uniform(0, base_delay * 0.5)
        return min(base_delay + jitter, self._MAX_RETRY_AFTER_SECONDS)

    def _validate_pagination_url(self, next_url: str) -> bool:
        if next_url.startswith("http"):
            parsed_next = urlparse(next_url)
            parsed_base = urlparse(self.base_url)
            if parsed_next.netloc != self._base_netloc or parsed_next.scheme != parsed_base.scheme:
                logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
                return False
            path = parsed_next.path
        elif next_url.startswith("/"):
            path = next_url.split("?")[0].split("#")[0]
        else:
            logger.debug("massive_client.pagination_next_url_rejected", next_url=next_url)
            return False
        from posixpath import normpath
        normalized = normpath(path)
        if not normalized.startswith("/") or ".." in normalized:
            logger.warning(
                "massive_client.pagination_path_traversal_blocked",
                next_url=next_url,
                normalized=normalized,
            )
            return False
        return True

    # -- Response status handling (returns action) --------------------------

    def _handle_response_status(
        self,
        status_code: int,
        response_text: str,
        url: str,
        retry_after_header: str | None,
        attempt: int,
    ) -> tuple[str, str | None]:
        """Evaluate an HTTP response status and return (action, retryable_message).

        Actions:
        - "ok": response is good, proceed with parsing
        - "retry": transient error, caller should sleep and retry
        - "raise": permanent error, caller should raise immediately
        - "raise_404": 404 specifically (some endpoints treat this as empty data)
        """
        if status_code in {401, 403}:
            return "raise", "Massive rejected the request. Verify API key and entitlements."
        if status_code == 404:
            return "raise_404", "Required Massive endpoint or data was not found."
        if status_code == 429:
            msg = "Massive rate limit reached. Retry later."
            if attempt < self.max_retries:
                return "retry", msg
            return "raise", msg
        if status_code >= 500:
            msg = "Massive is currently unavailable."
            if attempt < self.max_retries:
                return "retry", msg
            return "raise", msg
        if status_code >= 400:
            safe_url = urlparse(url)._replace(query="").geturl()
            logger.warning(
                "massive_client_error",
                status=status_code,
                detail=response_text[:500],
                url=safe_url,
            )
            return "raise", f"Massive returned {status_code}. The request could not be completed."
        return "ok", None

    # -- Data parsing (pure functions) --------------------------------------

    @staticmethod
    def parse_bars(results: list[dict[str, Any]], symbol: str) -> list[DailyBar]:
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

    @staticmethod
    def parse_contracts(rows: list[dict[str, Any]]) -> list[OptionContractRecord]:
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

    @staticmethod
    def parse_quote(results: list[dict[str, Any]], trade_date: date) -> OptionQuoteRecord | None:
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
                participant_timestamp=_MassiveClientCore._pick_quote_timestamp(row),
            )
        return None

    @staticmethod
    def parse_snapshot_result(result: dict[str, Any]) -> OptionSnapshotRecord | None:
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

    @staticmethod
    def parse_holidays(data: list[Any]) -> list[date]:
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

    @staticmethod
    def parse_earnings(rows: list[dict[str, Any]]) -> set[date]:
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
                logger.debug("massive_client.earnings.invalid_date", raw_date=raw_date)
        return dates

    @staticmethod
    def _pick_quote_timestamp(row: dict[str, Any]) -> int | None:
        for key in ("participant_timestamp", "sip_timestamp", "timestamp"):
            raw = row.get(key)
            if isinstance(raw, int):
                return raw
        return None

    @staticmethod
    def _earnings_param_variants(
        symbol: str, start_date: date, end_date: date,
    ) -> list[dict[str, Any]]:
        return [
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


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------

class MassiveClient(_MassiveClientCore):
    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        super().__init__(api_key=api_key, base_url=base_url)
        self._http = httpx.Client(
            timeout=self.timeout,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30),
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- Public API ---------------------------------------------------------

    def get_stock_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        payload = self._get_json(
            f"/v2/aggs/ticker/{quote(symbol, safe='')}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}",
            params={"adjusted": "true", "sort": "asc", "limit": 50000},
        )
        return self.parse_bars(payload.get("results", []), symbol)

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
        return self.parse_contracts(rows)

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        payload = self._get_json(
            f"/v3/quotes/{quote(option_ticker, safe='')}",
            params={"timestamp": trade_date.isoformat(), "sort": "participant_timestamp.desc", "limit": 10},
        )
        return self.parse_quote(payload.get("results", []), trade_date)

    def get_option_snapshot(self, underlying: str, option_ticker: str) -> OptionSnapshotRecord | None:
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
        return self.parse_snapshot_result(results)

    def get_option_chain_snapshot(self, underlying: str) -> list[OptionSnapshotRecord]:
        try:
            rows = self._get_paginated_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}",
                params={"limit": 250},
            )
        except ExternalServiceError:
            logger.debug("massive_client.chain_snapshot_unavailable", underlying=underlying)
            return []
        return [s for item in rows if (s := self.parse_snapshot_result(item)) is not None]

    def get_market_holidays(self) -> list[date]:
        data = self._request_with_retry("/v1/marketstatus/upcoming", not_found_returns_none=True)
        if data is None:
            logger.debug("massive_client.market_holidays_not_found", status=404)
            return []
        if not isinstance(data, list):
            raise ExternalServiceError("Massive market holidays returned an unexpected payload.")
        return self.parse_holidays(data)

    def list_earnings_event_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        last_error: ExternalServiceError | None = None
        for params in self._earnings_param_variants(symbol, start_date, end_date):
            try:
                rows = self._get_paginated_json("/tmx/v1/corporate-events", params=params)
            except ExternalServiceError as exc:
                last_error = exc
                continue
            return self.parse_earnings(rows)
        if last_error is not None:
            raise last_error
        return set()

    # -- Transport ----------------------------------------------------------

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
            if not self._validate_pagination_url(next_url):
                break
            next_path = next_url
            next_params = None
            page += 1
        return rows

    def _request_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        not_found_returns_none: bool = False,
    ) -> Any:
        """Execute a GET request with retry, circuit-breaker, and deadline.

        Returns the parsed JSON body (``dict`` or ``list``).  Raises
        ``ExternalServiceError`` on exhausted retries or non-retryable errors.
        When *not_found_returns_none* is True, 404 returns ``None`` instead of
        raising.
        """
        if not self._circuit.allow_request():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = self._build_url(path)
        headers = self._build_headers()
        retryable_message: str | None = None
        deadline = time.monotonic() + self.timeout * (self.max_retries + 1)

        for attempt in range(self.max_retries + 1):
            if time.monotonic() > deadline:
                raise ExternalServiceError("Massive request exceeded aggregate retry deadline.")
            try:
                response = self._http.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                self._circuit.record_failure(is_transient=True)
                retryable_message = "Massive request failed due to a network error."
                if attempt < self.max_retries:
                    time.sleep(self._compute_retry_delay(attempt, None))
                    continue
                raise ExternalServiceError(retryable_message) from exc

            action, msg = self._handle_response_status(
                response.status_code, response.text, url,
                response.headers.get("Retry-After"), attempt,
            )
            if action == "retry":
                self._circuit.record_failure(is_transient=True)
                retryable_message = msg
                time.sleep(self._compute_retry_delay(attempt, response.headers.get("Retry-After")))
                continue
            if action == "raise_404":
                if not_found_returns_none:
                    return None
                raise ExternalServiceError(msg or "Not found.")
            if action == "raise":
                is_transient = response.status_code >= 500 or response.status_code == 429
                self._circuit.record_failure(is_transient=is_transient)
                raise ExternalServiceError(msg or "Request failed.")

            try:
                data = response.json()
            except Exception as exc:
                self._circuit.record_failure(is_transient=True)
                raise ExternalServiceError(f"Invalid JSON response from Massive API: {exc}") from exc
            self._circuit.record_success()
            return data

        raise ExternalServiceError(retryable_message or "Massive request failed.")

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        data = self._request_with_retry(path, params)
        if not isinstance(data, dict):
            raise ExternalServiceError("Massive returned an unexpected response payload.")
        return data


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------

class AsyncMassiveClient(_MassiveClientCore):
    """Async variant of MassiveClient for use in async FastAPI endpoints."""

    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        super().__init__(api_key=api_key, base_url=base_url)
        self._http = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30),
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- Public API ---------------------------------------------------------

    async def get_stock_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        payload = await self._get_json(
            f"/v2/aggs/ticker/{quote(symbol, safe='')}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}",
            params={"adjusted": "true", "sort": "asc", "limit": 50000},
        )
        return self.parse_bars(payload.get("results", []), symbol)

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
        return self.parse_contracts(rows)

    async def get_option_quote_for_date(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        payload = await self._get_json(
            f"/v3/quotes/{quote(option_ticker, safe='')}",
            params={"timestamp": trade_date.isoformat(), "sort": "participant_timestamp.desc", "limit": 10},
        )
        return self.parse_quote(payload.get("results", []), trade_date)

    async def get_option_snapshot(self, underlying: str, option_ticker: str) -> OptionSnapshotRecord | None:
        try:
            payload = await self._get_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}/{quote(option_ticker, safe='')}",
            )
        except ExternalServiceError:
            logger.debug("massive_client.snapshot_unavailable", option_ticker=option_ticker)
            return None
        results = payload.get("results")
        if not isinstance(results, dict):
            return None
        return self.parse_snapshot_result(results)

    async def get_option_chain_snapshot(self, underlying: str) -> list[OptionSnapshotRecord]:
        try:
            rows = await self._get_paginated_json(
                f"/v3/snapshot/options/{quote(underlying, safe='')}",
                params={"limit": 250},
            )
        except ExternalServiceError:
            logger.debug("massive_client.chain_snapshot_unavailable", underlying=underlying)
            return []
        return [s for item in rows if (s := self.parse_snapshot_result(item)) is not None]

    async def get_market_holidays(self) -> list[date]:
        data = await self._request_with_retry("/v1/marketstatus/upcoming", not_found_returns_none=True)
        if data is None:
            logger.debug("massive_client.market_holidays_not_found", status=404)
            return []
        if not isinstance(data, list):
            raise ExternalServiceError("Massive market holidays returned an unexpected payload.")
        return self.parse_holidays(data)

    async def list_earnings_event_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        last_error: ExternalServiceError | None = None
        for params in self._earnings_param_variants(symbol, start_date, end_date):
            try:
                rows = await self._get_paginated_json("/tmx/v1/corporate-events", params=params)
            except ExternalServiceError as exc:
                last_error = exc
                continue
            return self.parse_earnings(rows)
        if last_error is not None:
            raise last_error
        return set()

    # -- Transport ----------------------------------------------------------

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
            if not self._validate_pagination_url(next_url):
                break
            next_path = next_url
            next_params = None
            page += 1
        return rows

    async def _request_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        not_found_returns_none: bool = False,
    ) -> Any:
        """Async version of MassiveClient._request_with_retry."""
        if not await self._circuit.allow_request_async():
            raise ExternalServiceError("Massive API circuit breaker is open. Retry later.")

        url = self._build_url(path)
        headers = self._build_headers()
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
                    await asyncio.sleep(self._compute_retry_delay(attempt, None))
                    continue
                raise ExternalServiceError(retryable_message) from exc

            action, msg = self._handle_response_status(
                response.status_code, response.text, url,
                response.headers.get("Retry-After"), attempt,
            )
            if action == "retry":
                await self._circuit.record_failure_async(is_transient=True)
                retryable_message = msg
                await asyncio.sleep(self._compute_retry_delay(attempt, response.headers.get("Retry-After")))
                continue
            if action == "raise_404":
                if not_found_returns_none:
                    return None
                raise ExternalServiceError(msg or "Not found.")
            if action == "raise":
                is_transient = response.status_code >= 500 or response.status_code == 429
                await self._circuit.record_failure_async(is_transient=is_transient)
                raise ExternalServiceError(msg or "Request failed.")

            try:
                data = response.json()
            except Exception as exc:
                await self._circuit.record_failure_async(is_transient=True)
                raise ExternalServiceError(f"Invalid JSON response from Massive API: {exc}") from exc
            await self._circuit.record_success_async()
            return data

        raise ExternalServiceError(retryable_message or "Massive request failed.")

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        data = await self._request_with_retry(path, params)
        if not isinstance(data, dict):
            raise ExternalServiceError("Massive returned an unexpected response payload.")
        return data
