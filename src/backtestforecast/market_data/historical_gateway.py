from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import date

from backtestforecast.backtests.strategies.common import preferred_expiration_dates
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord, OptionSnapshotRecord


@dataclass(slots=True)
class HistoricalOptionGateway:
    store: HistoricalMarketDataStore
    symbol: str
    _ex_dividend_dates: set[date] = field(default_factory=set)
    _iv_cache: dict[tuple[str, date], float | None] = field(default_factory=dict)
    _contract_cache: dict[tuple[date, str, int, int], list[OptionContractRecord]] = field(default_factory=dict)
    _exact_contract_cache: dict[
        tuple[date, str, date, float | None, float | None],
        list[OptionContractRecord],
    ] = field(default_factory=dict)
    _quote_cache: dict[tuple[str, date], OptionQuoteRecord | None] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _contracts_inflight: dict[tuple[date, str, int, int], threading.Event] = field(default_factory=dict)
    _exact_contracts_inflight: dict[
        tuple[date, str, date, float | None, float | None],
        threading.Event,
    ] = field(default_factory=dict)
    _quotes_inflight: dict[tuple[str, date], threading.Event] = field(default_factory=dict)
    _inflight_errors: dict[tuple[str, object], Exception] = field(default_factory=dict)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        cache_key = (entry_date, contract_type, target_dte, dte_tolerance_days)
        with self._lock:
            contracts = self._contract_cache.get(cache_key)
            if contracts is not None:
                return contracts
            inflight_event = self._contracts_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._contracts_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._lock:
                contracts = self._contract_cache.get(cache_key)
                if contracts is not None:
                    return contracts
                error = self._inflight_errors.get(("contracts", cache_key))
            if error is not None:
                raise error

        lower = entry_date.fromordinal(entry_date.toordinal() + max(1, target_dte - dte_tolerance_days))
        upper = entry_date.fromordinal(entry_date.toordinal() + target_dte + dte_tolerance_days)
        try:
            contracts = self.store.list_option_contracts(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_gte=lower,
                expiration_lte=upper,
            )
            with self._lock:
                self._contract_cache[cache_key] = contracts
                self._inflight_errors.pop(("contracts", cache_key), None)
            return contracts
        except Exception as exc:
            with self._lock:
                self._inflight_errors[("contracts", cache_key)] = exc
            raise
        finally:
            with self._lock:
                self._contracts_inflight.pop(cache_key, None)
                inflight_event.set()

    def list_contracts_for_preferred_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        for expiration_date in preferred_expiration_dates(entry_date, target_dte, dte_tolerance_days):
            contracts = self._list_contracts_for_exact_expiration(
                entry_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_price_gte,
                strike_price_lte=strike_price_lte,
            )
            if contracts:
                return contracts
        raise DataUnavailableError("No eligible option expirations were available in local historical data.")

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        cache_key = (option_ticker, trade_date)
        with self._lock:
            if cache_key in self._quote_cache:
                return self._quote_cache[cache_key]
            inflight_event = self._quotes_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._quotes_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._lock:
                if cache_key in self._quote_cache:
                    return self._quote_cache[cache_key]
                error = self._inflight_errors.get(("quotes", cache_key))
            if error is not None:
                raise error

        try:
            quote = self.store.get_option_quote_for_date(option_ticker, trade_date)
            with self._lock:
                self._quote_cache[cache_key] = quote
                self._inflight_errors.pop(("quotes", cache_key), None)
            return quote
        except Exception as exc:
            with self._lock:
                self._inflight_errors[("quotes", cache_key)] = exc
            raise
        finally:
            with self._lock:
                self._quotes_inflight.pop(cache_key, None)
                inflight_event.set()

    def set_ex_dividend_dates(self, ex_dividend_dates: set[date]) -> None:
        self._ex_dividend_dates = set(ex_dividend_dates)

    def get_ex_dividend_dates(self, start_date: date, end_date: date) -> set[date]:
        return {item for item in self._ex_dividend_dates if start_date <= item <= end_date}

    def get_snapshot(self, option_ticker: str) -> OptionSnapshotRecord | None:
        return None

    def get_chain_delta_lookup(
        self,
        contracts: list[OptionContractRecord],
    ) -> dict[tuple[float, date], float]:
        return {}

    def get_iv(self, key: tuple[str, date]) -> tuple[bool, float | None]:
        with self._lock:
            if key in self._iv_cache:
                return True, self._iv_cache[key]
        return False, None

    def store_iv(self, key: tuple[str, date], value: float | None) -> None:
        with self._lock:
            self._iv_cache[key] = value

    def _list_contracts_for_exact_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        strike_floor = round(strike_price_gte, 4) if strike_price_gte is not None else None
        strike_ceiling = round(strike_price_lte, 4) if strike_price_lte is not None else None
        cache_key = (entry_date, contract_type, expiration_date, strike_floor, strike_ceiling)
        with self._lock:
            contracts = self._exact_contract_cache.get(cache_key)
            if contracts is not None:
                return contracts
            inflight_event = self._exact_contracts_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._exact_contracts_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._lock:
                contracts = self._exact_contract_cache.get(cache_key)
                if contracts is not None:
                    return contracts
                error = self._inflight_errors.get(("exact_contracts", cache_key))
            if error is not None:
                raise error

        try:
            contracts = self.store.list_option_contracts_for_expiration(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_floor,
                strike_price_lte=strike_ceiling,
            )
            with self._lock:
                self._exact_contract_cache[cache_key] = contracts
                self._inflight_errors.pop(("exact_contracts", cache_key), None)
            return contracts
        except Exception as exc:
            with self._lock:
                self._inflight_errors[("exact_contracts", cache_key)] = exc
            raise
        finally:
            with self._lock:
                self._exact_contracts_inflight.pop(cache_key, None)
                inflight_event.set()
