from __future__ import annotations

import bisect
import contextvars
import math
import threading
import time as _time
from collections import OrderedDict, defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

import structlog

from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import (
    SpreadWidthConfig,
    SpreadWidthMode,
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
)

_logger = structlog.get_logger("strategies.common")

if TYPE_CHECKING:
    from backtestforecast.backtests.types import OptionDataGateway


_CHAIN_CONTEXT_CACHE_MAX = 4_096
_CHAIN_CONTEXT_LOCK = threading.Lock()
_DELTA_LOOKUP_CACHE_MAX = 4_096
_DELTA_LOOKUP_CACHE_LOCK = threading.Lock()
_PREFERRED_EXPIRATION_SELECTION_CACHE_MAX = 16_384
_PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK = threading.Lock()
_COMMON_EXPIRATION_SELECTION_CACHE_MAX = 16_384
_COMMON_EXPIRATION_SELECTION_CACHE_LOCK = threading.Lock()


@dataclass(slots=True)
class BuildPositionProfiler:
    contract_fetch_ms: float = 0.0
    delta_lookup_ms: float = 0.0
    entry_quote_fetch_ms: float = 0.0


_BUILD_POSITION_PROFILER: contextvars.ContextVar[BuildPositionProfiler | None] = contextvars.ContextVar(
    "build_position_profiler",
    default=None,
)
_BUILD_POSITION_PHASE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "build_position_phase",
    default=None,
)


def activate_build_position_profiler(
    profiler: BuildPositionProfiler,
) -> contextvars.Token[BuildPositionProfiler | None]:
    return _BUILD_POSITION_PROFILER.set(profiler)


def reset_build_position_profiler(
    token: contextvars.Token[BuildPositionProfiler | None],
) -> None:
    _BUILD_POSITION_PROFILER.reset(token)


def current_build_position_phase() -> str | None:
    return _BUILD_POSITION_PHASE.get()


@contextmanager
def _profile_build_position_phase(phase: str):
    profiler = _BUILD_POSITION_PROFILER.get()
    if profiler is None:
        yield
        return
    active_phase = _BUILD_POSITION_PHASE.get()
    if active_phase == phase:
        yield
        return
    phase_token = _BUILD_POSITION_PHASE.set(phase)
    start = _time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (_time.perf_counter() - start) * 1000.0
        if phase == "contract_fetch":
            profiler.contract_fetch_ms += elapsed_ms
        elif phase == "delta_lookup":
            profiler.delta_lookup_ms += elapsed_ms
        _BUILD_POSITION_PHASE.reset(phase_token)


@dataclass(slots=True)
class _ChainContext:
    contract_count: int
    first_ticker: str | None
    last_ticker: str | None
    grouped_by_expiration: dict[date, list[OptionContractRecord]]
    expirations_sorted: tuple[date, ...]
    unique_strikes_sorted: tuple[float, ...]
    contracts_by_strike: dict[int, OptionContractRecord]


_CHAIN_CONTEXT_CACHE: OrderedDict[int, _ChainContext] = OrderedDict()
_DELTA_LOOKUP_CACHE: OrderedDict[tuple[object, ...], dict[tuple[float, date], float]] = OrderedDict()
_PREFERRED_EXPIRATION_SELECTION_CACHE: OrderedDict[
    tuple[object, ...],
    tuple[date, list[OptionContractRecord]],
] = OrderedDict()
_COMMON_EXPIRATION_SELECTION_CACHE: OrderedDict[
    tuple[object, ...],
    tuple[date, list[OptionContractRecord], list[OptionContractRecord]],
] = OrderedDict()


def _normalized_strike_key(strike: float) -> int:
    return int(round(strike * 10_000))


def _normalized_bound(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _gateway_cache_identity(option_gateway: object) -> tuple[object, ...]:
    shared_state = getattr(option_gateway, "_shared_state", None)
    if shared_state is not None:
        return (
            type(option_gateway),
            "shared_state",
            id(shared_state),
            getattr(option_gateway, "symbol", None),
        )
    store = getattr(option_gateway, "store", None)
    symbol = getattr(option_gateway, "symbol", None)
    if store is not None and symbol is not None:
        return (type(option_gateway), "store_symbol", id(store), symbol)
    return (type(option_gateway), id(option_gateway))


def _cache_get(cache: OrderedDict[tuple[object, ...], object], key: tuple[object, ...], *, lock: threading.Lock):
    with lock:
        cached = cache.get(key)
        if cached is None:
            return None
        cache.move_to_end(key)
        return cached


def _cache_store(
    cache: OrderedDict[tuple[object, ...], object],
    key: tuple[object, ...],
    value: object,
    *,
    lock: threading.Lock,
    max_size: int,
) -> None:
    with lock:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > max_size:
            cache.popitem(last=False)


def _expiration_priority(
    expiration: date,
    *,
    entry_date: date,
    target_dte: int,
) -> tuple[int, int, int]:
    return (
        abs((expiration - entry_date).days - target_dte),
        0 if (expiration - entry_date).days >= target_dte else 1,
        (expiration - entry_date).days,
    )


def _sequence_signature(contracts: list[OptionContractRecord] | tuple[OptionContractRecord, ...]) -> tuple[int, str | None, str | None]:
    if not contracts:
        return 0, None, None
    return len(contracts), contracts[0].ticker, contracts[-1].ticker


def _build_chain_context(
    contracts: list[OptionContractRecord] | tuple[OptionContractRecord, ...],
) -> _ChainContext:
    grouped: dict[date, list[OptionContractRecord]] = defaultdict(list)
    contracts_by_strike: dict[int, OptionContractRecord] = {}
    for contract in contracts:
        grouped[contract.expiration_date].append(contract)
        contracts_by_strike.setdefault(_normalized_strike_key(contract.strike_price), contract)
    contract_count, first_ticker, last_ticker = _sequence_signature(contracts)
    return _ChainContext(
        contract_count=contract_count,
        first_ticker=first_ticker,
        last_ticker=last_ticker,
        grouped_by_expiration=dict(grouped),
        expirations_sorted=tuple(sorted(grouped)),
        unique_strikes_sorted=tuple(sorted({contract.strike_price for contract in contracts})),
        contracts_by_strike=contracts_by_strike,
    )


def _materialize_contracts(
    contracts: Iterable[OptionContractRecord] | list[OptionContractRecord] | tuple[OptionContractRecord, ...],
) -> list[OptionContractRecord] | tuple[OptionContractRecord, ...]:
    if isinstance(contracts, (list, tuple)):
        return contracts
    return list(contracts)


def _contracts_with_context(
    contracts: Iterable[OptionContractRecord],
) -> tuple[list[OptionContractRecord] | tuple[OptionContractRecord, ...], _ChainContext]:
    if isinstance(contracts, (list, tuple)):
        cache_key = id(contracts)
        contract_count, first_ticker, last_ticker = _sequence_signature(contracts)
        with _CHAIN_CONTEXT_LOCK:
            cached = _CHAIN_CONTEXT_CACHE.get(cache_key)
            if cached is not None:
                if (
                    cached.contract_count == contract_count
                    and cached.first_ticker == first_ticker
                    and cached.last_ticker == last_ticker
                ):
                    _CHAIN_CONTEXT_CACHE.move_to_end(cache_key)
                    return contracts, cached
                _CHAIN_CONTEXT_CACHE.pop(cache_key, None)
        context = _build_chain_context(contracts)
        with _CHAIN_CONTEXT_LOCK:
            _CHAIN_CONTEXT_CACHE[cache_key] = context
            _CHAIN_CONTEXT_CACHE.move_to_end(cache_key)
            while len(_CHAIN_CONTEXT_CACHE) > _CHAIN_CONTEXT_CACHE_MAX:
                _CHAIN_CONTEXT_CACHE.popitem(last=False)
        return contracts, context

    materialized = _materialize_contracts(contracts)
    return materialized, _build_chain_context(materialized)


def group_contracts_by_expiration(contracts: Iterable[OptionContractRecord]) -> dict[date, list[OptionContractRecord]]:
    _contracts, context = _contracts_with_context(contracts)
    return context.grouped_by_expiration


def common_sorted_strikes(
    left_contracts: Iterable[OptionContractRecord],
    right_contracts: Iterable[OptionContractRecord],
) -> list[float]:
    _left_contracts, left_context = _contracts_with_context(left_contracts)
    _right_contracts, right_context = _contracts_with_context(right_contracts)
    if len(left_context.unique_strikes_sorted) <= len(right_context.unique_strikes_sorted):
        right_keys = {_normalized_strike_key(strike) for strike in right_context.unique_strikes_sorted}
        return [
            strike for strike in left_context.unique_strikes_sorted
            if _normalized_strike_key(strike) in right_keys
        ]
    left_keys = {_normalized_strike_key(strike) for strike in left_context.unique_strikes_sorted}
    return [
        strike for strike in right_context.unique_strikes_sorted
        if _normalized_strike_key(strike) in left_keys
    ]


def common_sorted_expirations(
    left_contracts: Iterable[OptionContractRecord],
    right_contracts: Iterable[OptionContractRecord],
    *,
    min_expiration_exclusive: date | None = None,
) -> list[date]:
    _left_contracts, left_context = _contracts_with_context(left_contracts)
    _right_contracts, right_context = _contracts_with_context(right_contracts)
    if len(left_context.expirations_sorted) <= len(right_context.expirations_sorted):
        right_expirations = set(right_context.expirations_sorted)
        expirations = [
            expiration for expiration in left_context.expirations_sorted
            if expiration in right_expirations
        ]
    else:
        left_expirations = set(left_context.expirations_sorted)
        expirations = [
            expiration for expiration in right_context.expirations_sorted
            if expiration in left_expirations
        ]
    if min_expiration_exclusive is None:
        return expirations
    return [expiration for expiration in expirations if expiration > min_expiration_exclusive]


def choose_primary_expiration(
    contracts: Iterable[OptionContractRecord],
    entry_date: date,
    target_dte: int,
) -> date:
    _contracts, context = _contracts_with_context(contracts)
    expirations = context.expirations_sorted
    if not expirations:
        raise DataUnavailableError("No eligible option expirations were available.")
    return choose_primary_expiration_date(expirations, entry_date=entry_date, target_dte=target_dte)


def choose_primary_expiration_date(
    expirations: Iterable[date],
    *,
    entry_date: date,
    target_dte: int,
) -> date:
    ordered_expirations = tuple(expirations)
    if not ordered_expirations:
        raise DataUnavailableError("No eligible option expirations were available.")
    return min(
        ordered_expirations,
        key=lambda expiration: _expiration_priority(expiration, entry_date=entry_date, target_dte=target_dte),
    )


def preferred_expiration_dates(
    entry_date: date,
    target_dte: int,
    dte_tolerance_days: int,
) -> list[date]:
    """Return exact expiration dates ordered by choose_primary_expiration priority."""
    lower = max(1, target_dte - dte_tolerance_days)
    upper = target_dte + dte_tolerance_days
    offsets = range(lower, upper + 1)
    return sorted(
        (entry_date + timedelta(days=offset) for offset in offsets),
        key=lambda expiration: _expiration_priority(expiration, entry_date=entry_date, target_dte=target_dte),
    )


def choose_secondary_expiration(
    contracts: Iterable[OptionContractRecord],
    entry_date: date,
    base_expiration: date,
    min_extra_days: int = 14,
) -> date | None:
    _contracts, context = _contracts_with_context(contracts)
    expirations = [expiration for expiration in context.expirations_sorted if expiration > base_expiration]
    if not expirations:
        return None
    minimum_target = (base_expiration - entry_date).days + min_extra_days
    later_candidates = [expiration for expiration in expirations if (expiration - entry_date).days >= minimum_target]
    if later_candidates:
        return later_candidates[0]
    return None


def contracts_for_expiration(contracts: Iterable[OptionContractRecord], expiration: date) -> list[OptionContractRecord]:
    _contracts, context = _contracts_with_context(contracts)
    return context.grouped_by_expiration.get(expiration, [])


def select_preferred_expiration_contracts(
    option_gateway: OptionDataGateway,
    *,
    entry_date: date,
    contract_type: str,
    target_dte: int,
    dte_tolerance_days: int,
    strike_price_gte: float | None = None,
    strike_price_lte: float | None = None,
) -> tuple[date, list[OptionContractRecord]]:
    cache_key = (
        _gateway_cache_identity(option_gateway),
        entry_date,
        contract_type,
        target_dte,
        dte_tolerance_days,
        _normalized_bound(strike_price_gte),
        _normalized_bound(strike_price_lte),
    )
    cached = _cache_get(
        _PREFERRED_EXPIRATION_SELECTION_CACHE,
        cache_key,
        lock=_PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK,
    )
    if cached is not None:
        return cached
    with _profile_build_position_phase("contract_fetch"):
        preferred_fetch = getattr(option_gateway, "list_contracts_for_preferred_expiration", None)
        if callable(preferred_fetch):
            contracts = _materialize_contracts(
                preferred_fetch(
                    entry_date=entry_date,
                    contract_type=contract_type,
                    target_dte=target_dte,
                    dte_tolerance_days=dte_tolerance_days,
                    strike_price_gte=strike_price_gte,
                    strike_price_lte=strike_price_lte,
                )
            )
            if not contracts:
                raise DataUnavailableError("No eligible option expirations were available.")
            result = (contracts[0].expiration_date, contracts)
            _cache_store(
                _PREFERRED_EXPIRATION_SELECTION_CACHE,
                cache_key,
                result,
                lock=_PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK,
                max_size=_PREFERRED_EXPIRATION_SELECTION_CACHE_MAX,
            )
            return result

        contracts = list(
            option_gateway.list_contracts(
                entry_date,
                contract_type,
                target_dte,
                dte_tolerance_days,
            )
        )
        expiration = choose_primary_expiration(contracts, entry_date, target_dte)
        result = (expiration, contracts_for_expiration(contracts, expiration))
        _cache_store(
            _PREFERRED_EXPIRATION_SELECTION_CACHE,
            cache_key,
            result,
            lock=_PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK,
            max_size=_PREFERRED_EXPIRATION_SELECTION_CACHE_MAX,
        )
        return result


def select_preferred_common_expiration_contracts(
    option_gateway: OptionDataGateway,
    *,
    entry_date: date,
    target_dte: int,
    dte_tolerance_days: int,
) -> tuple[date, list[OptionContractRecord], list[OptionContractRecord]]:
    cache_key = (
        _gateway_cache_identity(option_gateway),
        entry_date,
        target_dte,
        dte_tolerance_days,
    )
    cached = _cache_get(
        _COMMON_EXPIRATION_SELECTION_CACHE,
        cache_key,
        lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
    )
    if cached is not None:
        return cached
    with _profile_build_position_phase("contract_fetch"):
        availability_fetch_by_type = getattr(option_gateway, "list_available_expirations_by_type", None)
        batch_fetch_by_type = getattr(option_gateway, "list_contracts_for_expirations_by_type", None)
        batch_fetch = getattr(option_gateway, "list_contracts_for_expirations", None)
        exact_fetch = getattr(option_gateway, "list_contracts_for_expiration", None)
        ordered_expirations = preferred_expiration_dates(entry_date, target_dte, dte_tolerance_days)
        if callable(availability_fetch_by_type):
            available_by_type = availability_fetch_by_type(
                entry_date=entry_date,
                contract_types=["call", "put"],
                expiration_dates=ordered_expirations,
            )
            available_calls = set(available_by_type.get("call", []))
            available_puts = set(available_by_type.get("put", []))
            for expiration_date in ordered_expirations:
                if expiration_date not in available_calls or expiration_date not in available_puts:
                    continue
                if callable(batch_fetch_by_type):
                    fetched_by_type = batch_fetch_by_type(
                        entry_date=entry_date,
                        contract_types=["call", "put"],
                        expiration_dates=[expiration_date],
                    )
                    calls = _materialize_contracts(fetched_by_type.get("call", {}).get(expiration_date, []))
                    puts = _materialize_contracts(fetched_by_type.get("put", {}).get(expiration_date, []))
                elif callable(exact_fetch):
                    calls = _materialize_contracts(
                        exact_fetch(
                            entry_date=entry_date,
                            contract_type="call",
                            expiration_date=expiration_date,
                        )
                    )
                    puts = _materialize_contracts(
                        exact_fetch(
                            entry_date=entry_date,
                            contract_type="put",
                            expiration_date=expiration_date,
                        )
                    )
                else:
                    calls = []
                    puts = []
                if calls and puts:
                    result = (expiration_date, calls, puts)
                    _cache_store(
                        _COMMON_EXPIRATION_SELECTION_CACHE,
                        cache_key,
                        result,
                        lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
                        max_size=_COMMON_EXPIRATION_SELECTION_CACHE_MAX,
                    )
                    return result
        if callable(batch_fetch_by_type):
            fetched_by_type = batch_fetch_by_type(
                entry_date=entry_date,
                contract_types=["call", "put"],
                expiration_dates=ordered_expirations,
            )
            calls_by_expiration = fetched_by_type.get("call", {})
            puts_by_expiration = fetched_by_type.get("put", {})
            for expiration_date in ordered_expirations:
                calls = _materialize_contracts(calls_by_expiration.get(expiration_date, []))
                puts = _materialize_contracts(puts_by_expiration.get(expiration_date, []))
                if calls and puts:
                    result = (expiration_date, calls, puts)
                    _cache_store(
                        _COMMON_EXPIRATION_SELECTION_CACHE,
                        cache_key,
                        result,
                        lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
                        max_size=_COMMON_EXPIRATION_SELECTION_CACHE_MAX,
                    )
                    return result
        if callable(batch_fetch):
            calls_by_expiration = batch_fetch(
                entry_date=entry_date,
                contract_type="call",
                expiration_dates=ordered_expirations,
            )
            puts_by_expiration = batch_fetch(
                entry_date=entry_date,
                contract_type="put",
                expiration_dates=ordered_expirations,
            )
            for expiration_date in ordered_expirations:
                calls = _materialize_contracts(calls_by_expiration.get(expiration_date, []))
                puts = _materialize_contracts(puts_by_expiration.get(expiration_date, []))
                if calls and puts:
                    result = (expiration_date, calls, puts)
                    _cache_store(
                        _COMMON_EXPIRATION_SELECTION_CACHE,
                        cache_key,
                        result,
                        lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
                        max_size=_COMMON_EXPIRATION_SELECTION_CACHE_MAX,
                    )
                    return result

        if callable(exact_fetch):
            for expiration_date in ordered_expirations:
                calls = _materialize_contracts(
                    exact_fetch(
                        entry_date=entry_date,
                        contract_type="call",
                        expiration_date=expiration_date,
                    )
                )
                puts = _materialize_contracts(
                    exact_fetch(
                        entry_date=entry_date,
                        contract_type="put",
                        expiration_date=expiration_date,
                    )
                )
                if calls and puts:
                    result = (expiration_date, calls, puts)
                    _cache_store(
                        _COMMON_EXPIRATION_SELECTION_CACHE,
                        cache_key,
                        result,
                        lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
                        max_size=_COMMON_EXPIRATION_SELECTION_CACHE_MAX,
                    )
                    return result

        calls = list(option_gateway.list_contracts(entry_date, "call", target_dte, dte_tolerance_days))
        puts = list(option_gateway.list_contracts(entry_date, "put", target_dte, dte_tolerance_days))
        common_expirations = common_sorted_expirations(calls, puts)
        if not common_expirations:
            raise DataUnavailableError("No common call/put expiration was available for the selected strategy.")
        expiration = choose_primary_expiration_date(common_expirations, entry_date=entry_date, target_dte=target_dte)
        result = (
            expiration,
            contracts_for_expiration(calls, expiration),
            contracts_for_expiration(puts, expiration),
        )
        _cache_store(
            _COMMON_EXPIRATION_SELECTION_CACHE,
            cache_key,
            result,
            lock=_COMMON_EXPIRATION_SELECTION_CACHE_LOCK,
            max_size=_COMMON_EXPIRATION_SELECTION_CACHE_MAX,
        )
        return result


def sorted_unique_strikes(contracts: Iterable[OptionContractRecord]) -> list[float]:
    _contracts, context = _contracts_with_context(contracts)
    return list(context.unique_strikes_sorted)


def choose_atm_strike(strikes: list[float], underlying_close: float) -> float:
    if not strikes:
        raise DataUnavailableError("No strikes were available for the selected expiration.")
    return min(strikes, key=lambda strike: (abs(strike - underlying_close), strike))


def choose_call_otm_strike(strikes: list[float], underlying_close: float) -> float:
    if not strikes:
        raise DataUnavailableError("No strikes available for the selected expiration.")
    above = [strike for strike in strikes if strike >= underlying_close]
    if above:
        return min(above)
    _logger.warning("strike_selection.no_otm_call", underlying_close=underlying_close, fallback_strike=max(strikes))
    return max(strikes)


def choose_put_otm_strike(strikes: list[float], underlying_close: float) -> float:
    if not strikes:
        raise DataUnavailableError("No strikes available for the selected expiration.")
    below = [strike for strike in strikes if strike <= underlying_close]
    if below:
        return max(below)
    _logger.warning("strike_selection.no_otm_put", underlying_close=underlying_close, fallback_strike=min(strikes))
    return min(strikes)


def offset_strike(strikes: list[float], base_strike: float, steps: int, *, presorted: bool = False) -> float | None:
    ordered = list(strikes) if presorted else sorted(strikes)
    insert_pos = bisect.bisect_left(ordered, base_strike)
    # WARNING: If base_strike is not in the listed strikes, we temporarily
    # insert it as a phantom to find the correct offset position. The
    # returned strike is always validated against the original strikes list.
    phantom_inserted = False
    if insert_pos >= len(ordered) or ordered[insert_pos] != base_strike:
        bisect.insort(ordered, base_strike)
        phantom_inserted = True
    index = bisect.bisect_left(ordered, base_strike)
    target_index = index + steps
    if target_index < 0 or target_index >= len(ordered):
        return None
    result = ordered[target_index]
    if phantom_inserted and result == base_strike:
        return None
    return result


def require_contract_for_strike(contracts: Iterable[OptionContractRecord], strike: float) -> OptionContractRecord:
    contract_sequence, context = _contracts_with_context(contracts)
    cached = context.contracts_by_strike.get(_normalized_strike_key(strike))
    if cached is not None:
        tolerance = max(0.005, cached.strike_price * 0.0001)
        if abs(cached.strike_price - strike) < tolerance:
            return cached

    for contract in contract_sequence:
        tolerance = max(0.005, contract.strike_price * 0.0001)
        if abs(contract.strike_price - strike) < tolerance:
            return contract
    raise DataUnavailableError(f"No contract was available for strike {strike}.")


def choose_common_atm_strike(
    call_contracts: Iterable[OptionContractRecord],
    put_contracts: Iterable[OptionContractRecord],
    underlying_close: float,
) -> float:
    common_strikes = common_sorted_strikes(call_contracts, put_contracts)
    if not common_strikes:
        raise DataUnavailableError("No common call/put strike was available for the selected expiration.")
    return choose_atm_strike(common_strikes, underlying_close)


def synthetic_ticker(identifiers: list[str]) -> str:
    return "|".join(identifiers)


# ---------------------------------------------------------------------------
# Configurable strike resolution
# ---------------------------------------------------------------------------


def _norm_cdf(x: float) -> float:
    """Standard normal CDF using math.erf (exact, matches rules.normal_cdf)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _approx_bsm_delta(
    spot: float,
    strike: float,
    dte_days: int,
    contract_type: str,
    vol: float = 0.30,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    """Approximate Black-Scholes delta with continuous dividend yield.

    When *vol* is left at the default 0.30 it acts as a rough fallback.
    Callers should pass estimated implied volatility when available for
    significantly better accuracy.
    """
    if dte_days <= 0:
        if spot == strike:
            return 0.5 if contract_type == "call" else -0.5
        if contract_type == "call":
            return 1.0 if spot > strike else 0.0
        return -1.0 if spot < strike else 0.0

    t = dte_days / 365.0
    sqrt_t = math.sqrt(t)
    try:
        d1 = (math.log(spot / strike) + (risk_free_rate - dividend_yield + 0.5 * vol * vol) * t) / (vol * sqrt_t)
    except (ValueError, ZeroDivisionError):
        return 0.5 if contract_type == "call" else -0.5

    if contract_type == "call":
        return math.exp(-dividend_yield * t) * _norm_cdf(d1)
    return math.exp(-dividend_yield * t) * (_norm_cdf(d1) - 1.0)


def _estimate_iv_for_strike(
    strike: float,
    contract_type: str,
    underlying_close: float,
    dte_days: int,
    contracts: list[OptionContractRecord],
    option_gateway: OptionDataGateway,
    trade_date: date,
    risk_free_rate: float = 0.045,
    iv_cache: dict[tuple[str, date], float | None] | None = None,
) -> float | None:
    """Estimate implied volatility from the market quote for a given strike.

    Returns None if no usable quote or IV estimate is available.
    When *iv_cache* is provided, results are memoized by (ticker, date)
    so repeated calls for the same contract skip the BSM bisection.

    If the ``option_gateway`` exposes thread-safe ``get_iv``/``store_iv``
    methods (MassiveOptionGateway does), those are preferred over direct
    dict access for bounded LRU eviction and cache tracking.
    """
    from backtestforecast.backtests.rules import implied_volatility_from_price

    contract = None
    for c in contracts:
        tolerance = max(0.005, c.strike_price * 0.0001)
        if abs(c.strike_price - strike) < tolerance and c.contract_type == contract_type:
            contract = c
            break
    if contract is None:
        return None

    return _estimate_iv_for_contract(
        contract,
        underlying_close=underlying_close,
        dte_days=dte_days,
        option_gateway=option_gateway,
        trade_date=trade_date,
        risk_free_rate=risk_free_rate,
        iv_cache=iv_cache,
    )


def _estimate_iv_for_contract(
    contract: OptionContractRecord,
    *,
    underlying_close: float,
    dte_days: int,
    option_gateway: OptionDataGateway,
    trade_date: date,
    risk_free_rate: float = 0.045,
    iv_cache: dict[tuple[str, date], float | None] | None = None,
) -> float | None:
    """Estimate implied volatility for a specific contract."""
    from backtestforecast.backtests.rules import implied_volatility_from_price

    cache_key = (contract.ticker, trade_date)

    _get_iv = getattr(option_gateway, "get_iv", None)
    _store_iv = getattr(option_gateway, "store_iv", None)

    if _get_iv is not None:
        found, cached_val = _get_iv(cache_key)
        if found:
            return cached_val
    elif iv_cache is not None and cache_key in iv_cache:
        return iv_cache[cache_key]

    quote = option_gateway.get_quote(contract.ticker, trade_date)
    if quote is None or quote.mid_price <= 0:
        if _store_iv is not None:
            _store_iv(cache_key, None)
        elif iv_cache is not None:
            iv_cache[cache_key] = None
        return None

    iv = implied_volatility_from_price(
        option_price=quote.mid_price,
        underlying_price=underlying_close,
        strike_price=contract.strike_price,
        time_to_expiry_years=max(dte_days, 1) / 365.0,
        option_type=contract.contract_type,
        risk_free_rate=risk_free_rate,
    )
    if _store_iv is not None:
        _store_iv(cache_key, iv)
    elif iv_cache is not None:
        iv_cache[cache_key] = iv
    return iv


def build_contract_delta_lookup(
    *,
    contracts: list[OptionContractRecord],
    option_gateway: OptionDataGateway,
    trade_date: date,
    underlying_close: float,
    dte_days: int,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
    iv_cache: dict[tuple[str, date], float | None] | None = None,
    realized_vol: float | None = None,
) -> dict[tuple[float, date], float]:
    contract_sequence, context = _contracts_with_context(contracts)
    if not contract_sequence:
        return {}
    cache_key = (
        id(contract_sequence),
        context.contract_count,
        context.first_ticker,
        context.last_ticker,
        trade_date.toordinal(),
        round(underlying_close, 4),
        int(dte_days),
        round(risk_free_rate, 8),
        round(dividend_yield, 8),
        round(realized_vol, 8) if realized_vol is not None else None,
    )
    with _DELTA_LOOKUP_CACHE_LOCK:
        cached = _DELTA_LOOKUP_CACHE.get(cache_key)
        if cached is not None:
            _DELTA_LOOKUP_CACHE.move_to_end(cache_key)
            return cached

    gateway_lookup = getattr(option_gateway, "get_chain_delta_lookup", None)
    if callable(gateway_lookup):
        try:
            raw_lookup = gateway_lookup(contract_sequence) or {}
        except Exception:
            raw_lookup = {}
        else:
            normalized: dict[tuple[float, date], float] = {}
            for contract in contract_sequence:
                raw_delta = raw_lookup.get((contract.strike_price, contract.expiration_date))
                if raw_delta is None:
                    raw_delta = raw_lookup.get(contract.strike_price)
                if raw_delta is not None:
                    normalized[(contract.strike_price, contract.expiration_date)] = raw_delta
            if normalized:
                with _DELTA_LOOKUP_CACHE_LOCK:
                    _DELTA_LOOKUP_CACHE[cache_key] = normalized
                    _DELTA_LOOKUP_CACHE.move_to_end(cache_key)
                    while len(_DELTA_LOOKUP_CACHE) > _DELTA_LOOKUP_CACHE_MAX:
                        _DELTA_LOOKUP_CACHE.popitem(last=False)
                return normalized

    lookup: dict[tuple[float, date], float] = {}
    for contract in contract_sequence:
        iv = _estimate_iv_for_contract(
            contract,
            underlying_close=underlying_close,
            dte_days=dte_days,
            option_gateway=option_gateway,
            trade_date=trade_date,
            risk_free_rate=risk_free_rate,
            iv_cache=iv_cache,
        )
        if iv is not None:
            delta = _approx_bsm_delta(
                underlying_close,
                contract.strike_price,
                dte_days,
                contract.contract_type,
                vol=iv,
                risk_free_rate=risk_free_rate,
                dividend_yield=dividend_yield,
            )
        elif realized_vol is not None:
            delta = _approx_bsm_delta(
                underlying_close,
                contract.strike_price,
                dte_days,
                contract.contract_type,
                vol=realized_vol,
                risk_free_rate=risk_free_rate,
                dividend_yield=dividend_yield,
            )
        else:
            delta = _approx_bsm_delta(
                underlying_close,
                contract.strike_price,
                dte_days,
                contract.contract_type,
                risk_free_rate=risk_free_rate,
                dividend_yield=dividend_yield,
            )
        lookup[(contract.strike_price, contract.expiration_date)] = delta
    with _DELTA_LOOKUP_CACHE_LOCK:
        _DELTA_LOOKUP_CACHE[cache_key] = lookup
        _DELTA_LOOKUP_CACHE.move_to_end(cache_key)
        while len(_DELTA_LOOKUP_CACHE) > _DELTA_LOOKUP_CACHE_MAX:
            _DELTA_LOOKUP_CACHE.popitem(last=False)
    return lookup


def maybe_build_contract_delta_lookup(
    *,
    selection: StrikeSelection | None,
    contracts: list[OptionContractRecord],
    option_gateway: OptionDataGateway,
    trade_date: date,
    underlying_close: float,
    dte_days: int,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
    iv_cache: dict[tuple[str, date], float | None] | None = None,
    realized_vol: float | None = None,
) -> dict[tuple[float, date], float] | None:
    if selection is None or selection.mode != StrikeSelectionMode.DELTA_TARGET:
        return None
    with _profile_build_position_phase("delta_lookup"):
        return build_contract_delta_lookup(
            contracts=contracts,
            option_gateway=option_gateway,
            trade_date=trade_date,
            underlying_close=underlying_close,
            dte_days=dte_days,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            iv_cache=iv_cache,
            realized_vol=realized_vol,
        )


def _nearest_strike(strikes: list[float], target: float) -> float:
    """Find the listed strike closest to a target value."""
    if not strikes:
        raise DataUnavailableError("No strikes available.")
    return min(strikes, key=lambda s: (abs(s - target), s))


def resolve_strike(
    strikes: list[float],
    underlying_close: float,
    contract_type: str,
    selection: StrikeSelection | None,
    dte_days: int = 30,
    *,
    delta_lookup: dict[tuple[float, date], float] | dict[float, float] | None = None,
    contracts: list[OptionContractRecord] | None = None,
    option_gateway: OptionDataGateway | None = None,
    trade_date: date | None = None,
    expiration_date: date | None = None,
    iv_cache: dict[tuple[str, date], float | None] | None = None,
    realized_vol: float | None = None,
    risk_free_rate: float = 0.045,
) -> float:
    """Resolve a strike based on the selection config, or fall back to nearest OTM.

    For DELTA_TARGET mode, the resolution order is:
      1. *delta_lookup* - pre-built (strike, expiration)->delta map (from API chain snapshot)
      2. IV-improved BSM - estimate IV from the market quote for each candidate
      3. *realized_vol* - historical realized volatility (if available)
      4. Hardcoded 30% vol BSM - final fallback
    """
    with _profile_build_position_phase("delta_lookup"):
        if selection is None or selection.mode == StrikeSelectionMode.NEAREST_OTM:
            if contract_type == "call":
                return choose_call_otm_strike(strikes, underlying_close)
            return choose_put_otm_strike(strikes, underlying_close)

        val = float(selection.value) if selection.value is not None else 0.0

        if selection.mode == StrikeSelectionMode.PCT_FROM_SPOT:
            if contract_type == "call":
                target = underlying_close * (1.0 + val / 100.0)
            else:
                target = underlying_close * (1.0 - val / 100.0)
            return _nearest_strike(strikes, target)

        if selection.mode == StrikeSelectionMode.ATM_OFFSET_STEPS:
            steps = round(val)
            atm = choose_atm_strike(strikes, underlying_close)
            sorted_strikes = sorted(set(strikes))
            if contract_type == "call":
                resolved = offset_strike(sorted_strikes, atm, steps, presorted=True)
            else:
                resolved = offset_strike(sorted_strikes, atm, -steps, presorted=True)
            if resolved is None:
                raise DataUnavailableError(f"Strike offset {steps} out of range for {contract_type}.")
            return resolved

        if selection.mode == StrikeSelectionMode.DELTA_TARGET:
            if not strikes:
                raise DataUnavailableError("No strikes available for delta targeting.")
            target_delta = val / 100.0
            lookup_expiration = expiration_date
            if lookup_expiration is None and delta_lookup is not None:
                if contracts is not None:
                    _, context = _contracts_with_context(contracts)
                    if len(context.expirations_sorted) == 1:
                        lookup_expiration = context.expirations_sorted[0]
                if lookup_expiration is None:
                    tuple_expirations = {
                        key[1]
                        for key in delta_lookup
                        if isinstance(key, tuple)
                        and len(key) == 2
                        and isinstance(key[1], date)
                    }
                    if len(tuple_expirations) == 1:
                        lookup_expiration = next(iter(tuple_expirations))

            best_strike = strikes[0]
            best_diff = float("inf")
            for strike in strikes:
                delta: float | None = None

                if delta_lookup is not None:
                    raw: float | None = None
                    if lookup_expiration is not None:
                        raw = delta_lookup.get((strike, lookup_expiration))  # type: ignore[call-overload]
                    if raw is None:
                        raw = delta_lookup.get(strike)  # type: ignore[call-overload]
                    if raw is not None:
                        delta = raw

                if delta is None:
                    iv: float | None = None
                    if contracts is not None and option_gateway is not None and trade_date is not None:
                        iv = _estimate_iv_for_strike(
                            strike,
                            contract_type,
                            underlying_close,
                            dte_days,
                            contracts,
                            option_gateway,
                            trade_date,
                            risk_free_rate=risk_free_rate,
                            iv_cache=iv_cache,
                        )
                    if iv is not None:
                        delta = _approx_bsm_delta(
                            underlying_close,
                            strike,
                            dte_days,
                            contract_type,
                            vol=iv,
                            risk_free_rate=risk_free_rate,
                        )
                    elif realized_vol is not None:
                        delta = _approx_bsm_delta(
                            underlying_close,
                            strike,
                            dte_days,
                            contract_type,
                            vol=realized_vol,
                            risk_free_rate=risk_free_rate,
                        )
                    else:
                        delta = _approx_bsm_delta(
                            underlying_close,
                            strike,
                            dte_days,
                            contract_type,
                            risk_free_rate=risk_free_rate,
                        )

                diff = abs(abs(delta) - target_delta)
                if diff < best_diff:
                    best_diff = diff
                    best_strike = strike
            return best_strike

        # Fallback
        if contract_type == "call":
            return choose_call_otm_strike(strikes, underlying_close)
        return choose_put_otm_strike(strikes, underlying_close)


def resolve_wing_strike(
    strikes: list[float],
    short_strike: float,
    direction: int,
    underlying_close: float,
    width_config: SpreadWidthConfig | None,
) -> float | None:
    """Resolve a wing/protection strike relative to a short strike.

    Args:
        strikes: Available listed strikes.
        short_strike: The anchor (short leg) strike.
        direction: +1 for higher (call wing), -1 for lower (put wing).
        underlying_close: Current underlying price (for pct_width).
        width_config: Optional spread width configuration.

    Returns:
        The resolved wing strike, or None if no valid strike exists.
    """
    result: float | None = None
    unique_sorted = sorted(set(strikes))

    if width_config is None:
        result = offset_strike(unique_sorted, short_strike, direction, presorted=True)
    elif width_config.mode == SpreadWidthMode.STRIKE_STEPS:
        steps = int(float(width_config.value))
        result = offset_strike(unique_sorted, short_strike, direction * steps, presorted=True)
    elif width_config.mode == SpreadWidthMode.DOLLAR_WIDTH:
        val = float(width_config.value)
        target = short_strike + val if direction > 0 else short_strike - val
        result = _nearest_strike(strikes, target)
    elif width_config.mode == SpreadWidthMode.PCT_WIDTH:
        val = float(width_config.value)
        dollar_width = underlying_close * val / 100.0
        target = short_strike + dollar_width if direction > 0 else short_strike - dollar_width
        result = _nearest_strike(strikes, target)
    else:
        result = offset_strike(unique_sorted, short_strike, direction, presorted=True)

    if result is not None:
        wrong_side = (
            result == short_strike
            or (direction > 0 and result < short_strike)
            or (direction < 0 and result > short_strike)
        )
        if wrong_side:
            fallback = offset_strike(unique_sorted, short_strike, direction, presorted=True)
            result = fallback if fallback is not None and fallback != short_strike else None

    if result is None:
        _logger.debug(
            "resolve_wing_strike.no_valid_strike",
            short_strike=short_strike,
            direction=direction,
            width_mode=width_config.mode if width_config else None,
            num_strikes=len(strikes),
        )

    return result


def valid_entry_mids(*mids: float | None) -> bool:
    """Return True if every mid price is finite and positive."""
    return all(m is not None and math.isfinite(m) and m > 0 for m in mids)


def get_entry_quotes(
    option_gateway: OptionDataGateway,
    *,
    trade_date: date,
    contracts: Iterable[OptionContractRecord],
) -> dict[str, OptionQuoteRecord | None]:
    contract_list = list(contracts)
    if not contract_list:
        return {}
    tickers = list(dict.fromkeys(contract.ticker for contract in contract_list))
    quotes: dict[str, OptionQuoteRecord | None] = {}
    batch_fetch = getattr(option_gateway, "get_quotes", None)
    if callable(batch_fetch) and len(tickers) > 1:
        try:
            quotes.update(dict(batch_fetch(tickers, trade_date)))
        except Exception:
            _logger.debug(
                "entry_quotes.batch_fetch_failed",
                trade_date=str(trade_date),
                ticker_count=len(tickers),
                exc_info=True,
            )
    for ticker in tickers:
        if ticker not in quotes:
            quotes[ticker] = option_gateway.get_quote(ticker, trade_date)
    return quotes


def get_overrides(config_overrides: StrategyOverrides | None) -> StrategyOverrides:
    """Return the overrides or an empty default."""
    if config_overrides is not None:
        return config_overrides
    return StrategyOverrides()
