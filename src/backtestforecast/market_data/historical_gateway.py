from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, TypeVar

from backtestforecast.backtests.strategies.common import preferred_expiration_dates
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord, OptionSnapshotRecord

if TYPE_CHECKING:
    from backtestforecast.market_data.contract_catalog import OptionContractCatalogStore
    from backtestforecast.market_data.redis_cache import OptionDataRedisCache

_SHARED_SYMBOL_CACHE_MAX = 64
_CONTRACT_CACHE_MAX = 512
_EXACT_CONTRACT_CACHE_MAX = 1_024
_QUOTE_CACHE_MAX = 10_000
_IV_CACHE_MAX = 50_000
_SHARED_STORE_CACHE_MAX = 16
_STORE_SHARED_STATE_LOCK = threading.Lock()
_SHARED_STORE_STATES: OrderedDict[int, tuple[HistoricalMarketDataStore, OrderedDict[str, _HistoricalGatewaySharedState]]] = OrderedDict()


@dataclass(slots=True)
class _HistoricalGatewaySharedState:
    contract_cache: OrderedDict[tuple[date, str, int, int], list[OptionContractRecord]] = field(default_factory=OrderedDict)
    exact_contract_cache: OrderedDict[
        tuple[date, str, date, float | None, float | None],
        list[OptionContractRecord],
    ] = field(default_factory=OrderedDict)
    quote_cache: OrderedDict[tuple[str, date], OptionQuoteRecord | None] = field(default_factory=OrderedDict)
    iv_cache: OrderedDict[tuple[str, date], float | None] = field(default_factory=OrderedDict)
    lock: threading.RLock = field(default_factory=threading.RLock)
    contracts_inflight: dict[tuple[date, str, int, int], threading.Event] = field(default_factory=dict)
    exact_contracts_inflight: dict[
        tuple[date, str, date, float | None, float | None],
        threading.Event,
    ] = field(default_factory=dict)
    quotes_inflight: dict[tuple[str, date], threading.Event] = field(default_factory=dict)
    inflight_errors: dict[tuple[str, object], Exception] = field(default_factory=dict)


def _shared_state_for_store(
    store: HistoricalMarketDataStore,
    symbol: str,
) -> _HistoricalGatewaySharedState:
    with _STORE_SHARED_STATE_LOCK:
        store_id = id(store)
        store_entry = _SHARED_STORE_STATES.get(store_id)
        if store_entry is None or store_entry[0] is not store:
            states: OrderedDict[str, _HistoricalGatewaySharedState] = OrderedDict()
            _SHARED_STORE_STATES[store_id] = (store, states)
        else:
            states = store_entry[1]
            _SHARED_STORE_STATES.move_to_end(store_id)
        state = states.get(symbol)
        if state is None:
            state = _HistoricalGatewaySharedState()
            states[symbol] = state
        else:
            states.move_to_end(symbol)
        while len(states) > _SHARED_SYMBOL_CACHE_MAX:
            states.popitem(last=False)
        while len(_SHARED_STORE_STATES) > _SHARED_STORE_CACHE_MAX:
            _SHARED_STORE_STATES.popitem(last=False)
        return state


_CacheKeyT = TypeVar("_CacheKeyT")
_CacheValueT = TypeVar("_CacheValueT")


def _cache_hit(
    cache: OrderedDict[_CacheKeyT, _CacheValueT],
    key: _CacheKeyT,
) -> _CacheValueT | None:
    value = cache.get(key)
    if value is None:
        return None
    cache.move_to_end(key)
    return value


def _store_lru(
    cache: OrderedDict[_CacheKeyT, _CacheValueT],
    key: _CacheKeyT,
    value: _CacheValueT,
    *,
    max_size: int,
) -> None:
    if key in cache:
        cache.move_to_end(key)
        cache[key] = value
        return
    cache[key] = value
    while len(cache) > max_size:
        cache.popitem(last=False)


@dataclass(slots=True)
class HistoricalOptionGateway:
    store: HistoricalMarketDataStore
    symbol: str
    redis_cache: OptionDataRedisCache | None = None
    contract_catalog: OptionContractCatalogStore | None = None
    _ex_dividend_dates: set[date] = field(default_factory=set)
    _shared_state: _HistoricalGatewaySharedState = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._shared_state = _shared_state_for_store(self.store, self.symbol)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        cache_key = (entry_date, contract_type, target_dte, dte_tolerance_days)
        with self._shared_state.lock:
            contracts = _cache_hit(self._shared_state.contract_cache, cache_key)
            if contracts is not None:
                return contracts
            inflight_event = self._shared_state.contracts_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._shared_state.contracts_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._shared_state.lock:
                contracts = _cache_hit(self._shared_state.contract_cache, cache_key)
                if contracts is not None:
                    return contracts
                error = self._shared_state.inflight_errors.get(("contracts", cache_key))
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
            with self._shared_state.lock:
                _store_lru(
                    self._shared_state.contract_cache,
                    cache_key,
                    contracts,
                    max_size=_CONTRACT_CACHE_MAX,
                )
                self._shared_state.inflight_errors.pop(("contracts", cache_key), None)
            return contracts
        except Exception as exc:
            with self._shared_state.lock:
                self._shared_state.inflight_errors[("contracts", cache_key)] = exc
            raise
        finally:
            with self._shared_state.lock:
                self._shared_state.contracts_inflight.pop(cache_key, None)
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

    def list_contracts_for_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        return self._list_contracts_for_exact_expiration(
            entry_date=entry_date,
            contract_type=contract_type,
            expiration_date=expiration_date,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
        )

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        cache_key = (option_ticker, trade_date)
        with self._shared_state.lock:
            if cache_key in self._shared_state.quote_cache:
                self._shared_state.quote_cache.move_to_end(cache_key)
                return self._shared_state.quote_cache[cache_key]
            inflight_event = self._shared_state.quotes_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._shared_state.quotes_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._shared_state.lock:
                if cache_key in self._shared_state.quote_cache:
                    self._shared_state.quote_cache.move_to_end(cache_key)
                    return self._shared_state.quote_cache[cache_key]
                error = self._shared_state.inflight_errors.get(("quotes", cache_key))
            if error is not None:
                raise error

        try:
            if self.redis_cache is not None:
                from backtestforecast.market_data.redis_cache import CACHE_MISS

                redis_result = self.redis_cache.get_quote(option_ticker, trade_date)
                if redis_result is not CACHE_MISS:
                    with self._shared_state.lock:
                        _store_lru(
                            self._shared_state.quote_cache,
                            cache_key,
                            redis_result,
                            max_size=_QUOTE_CACHE_MAX,
                        )
                        self._shared_state.inflight_errors.pop(("quotes", cache_key), None)
                    return redis_result

            quote = self.store.get_option_quote_for_date(option_ticker, trade_date)
            if self.redis_cache is not None:
                from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

                self.redis_cache.set_quote(
                    option_ticker,
                    trade_date,
                    quote,
                    ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS if quote is None else None,
                )
            with self._shared_state.lock:
                _store_lru(
                    self._shared_state.quote_cache,
                    cache_key,
                    quote,
                    max_size=_QUOTE_CACHE_MAX,
                )
                self._shared_state.inflight_errors.pop(("quotes", cache_key), None)
            return quote
        except Exception as exc:
            with self._shared_state.lock:
                self._shared_state.inflight_errors[("quotes", cache_key)] = exc
            raise
        finally:
            with self._shared_state.lock:
                self._shared_state.quotes_inflight.pop(cache_key, None)
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
        with self._shared_state.lock:
            if key in self._shared_state.iv_cache:
                self._shared_state.iv_cache.move_to_end(key)
                return True, self._shared_state.iv_cache[key]
        return False, None

    def store_iv(self, key: tuple[str, date], value: float | None) -> None:
        with self._shared_state.lock:
            _store_lru(
                self._shared_state.iv_cache,
                key,
                value,
                max_size=_IV_CACHE_MAX,
            )

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
        with self._shared_state.lock:
            contracts = _cache_hit(self._shared_state.exact_contract_cache, cache_key)
            if contracts is not None:
                return contracts
            inflight_event = self._shared_state.exact_contracts_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._shared_state.exact_contracts_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._shared_state.lock:
                contracts = _cache_hit(self._shared_state.exact_contract_cache, cache_key)
                if contracts is not None:
                    return contracts
                error = self._shared_state.inflight_errors.get(("exact_contracts", cache_key))
            if error is not None:
                raise error

        try:
            if self.contract_catalog is not None:
                cached = self.contract_catalog.get_contracts(
                    symbol=self.symbol,
                    as_of_date=entry_date,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    strike_price_gte=strike_floor,
                    strike_price_lte=strike_ceiling,
                )
                if cached is not None:
                    with self._shared_state.lock:
                        _store_lru(
                            self._shared_state.exact_contract_cache,
                            cache_key,
                            cached,
                            max_size=_EXACT_CONTRACT_CACHE_MAX,
                        )
                        self._shared_state.inflight_errors.pop(("exact_contracts", cache_key), None)
                    return cached

            if self.redis_cache is not None:
                cached = self.redis_cache.get_exact_contracts(
                    self.symbol,
                    entry_date,
                    contract_type,
                    expiration_date,
                    strike_price_gte=strike_floor,
                    strike_price_lte=strike_ceiling,
                )
                if cached is not None:
                    with self._shared_state.lock:
                        _store_lru(
                            self._shared_state.exact_contract_cache,
                            cache_key,
                            cached,
                            max_size=_EXACT_CONTRACT_CACHE_MAX,
                        )
                        self._shared_state.inflight_errors.pop(("exact_contracts", cache_key), None)
                    return cached

            contracts = self.store.list_option_contracts_for_expiration(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_floor,
                strike_price_lte=strike_ceiling,
            )
            if self.contract_catalog is not None:
                self.contract_catalog.upsert_contracts(
                    symbol=self.symbol,
                    as_of_date=entry_date,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    strike_price_gte=strike_floor,
                    strike_price_lte=strike_ceiling,
                    contracts=contracts,
                )
            if self.redis_cache is not None:
                from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

                self.redis_cache.set_exact_contracts(
                    self.symbol,
                    entry_date,
                    contract_type,
                    expiration_date,
                    contracts,
                    strike_price_gte=strike_floor,
                    strike_price_lte=strike_ceiling,
                    ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS if not contracts else None,
                )
            with self._shared_state.lock:
                _store_lru(
                    self._shared_state.exact_contract_cache,
                    cache_key,
                    contracts,
                    max_size=_EXACT_CONTRACT_CACHE_MAX,
                )
                self._shared_state.inflight_errors.pop(("exact_contracts", cache_key), None)
            return contracts
        except Exception as exc:
            with self._shared_state.lock:
                self._shared_state.inflight_errors[("exact_contracts", cache_key)] = exc
            raise
        finally:
            with self._shared_state.lock:
                self._shared_state.exact_contracts_inflight.pop(cache_key, None)
                inflight_event.set()
