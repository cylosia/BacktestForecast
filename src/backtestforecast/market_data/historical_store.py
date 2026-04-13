from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager, suppress
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from functools import lru_cache
import threading
from typing import Callable, Iterator
from uuid import uuid4

import structlog
from sqlalchemy import bindparam
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import union_all
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.models import (
    HistoricalEarningsEvent,
    HistoricalExDividendDate,
    HistoricalOptionDayBar,
    HistoricalTreasuryYield,
    HistoricalUnderlyingDayBar,
    HistoricalUnderlyingRawDayBar,
)
from backtestforecast.utils.dates import is_trading_day

logger = structlog.get_logger("market_data.historical_store")
_POSTGRES_MAX_BIND_PARAMS = 65_000
_MAX_NUMERIC_ROOT_ALIASES = 9
_OPTION_DAY_BAR_COPY_COLUMNS = (
    "id",
    "option_ticker",
    "underlying_symbol",
    "trade_date",
    "expiration_date",
    "contract_type",
    "strike_price",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "source_file_date",
)


@lru_cache(maxsize=None)
def _contracts_for_expiration_stmt(
    *,
    has_expiration_date: bool,
    has_expiration_gte: bool,
    has_expiration_lte: bool,
    has_strike_gte: bool,
    has_strike_lte: bool,
):
    clauses = [
        "underlying_symbol IN :symbols",
        "trade_date = :as_of_date",
        "contract_type IN :contract_types",
    ]
    if has_expiration_date:
        clauses.append("expiration_date = :expiration_date")
    if has_expiration_gte:
        clauses.append("expiration_date >= :expiration_gte")
    if has_expiration_lte:
        clauses.append("expiration_date <= :expiration_lte")
    if has_strike_gte:
        clauses.append("strike_price >= :strike_price_gte")
    if has_strike_lte:
        clauses.append("strike_price <= :strike_price_lte")
    return text(
        f"""
        SELECT option_ticker, underlying_symbol, contract_type, expiration_date, strike_price, close_price
        FROM historical_option_day_bars
        WHERE {' AND '.join(clauses)}
        ORDER BY expiration_date, strike_price, underlying_symbol, option_ticker
        """
    ).bindparams(
        bindparam("symbols", expanding=True),
        bindparam("contract_types", expanding=True),
    )


@lru_cache(maxsize=None)
def _contracts_for_expirations_stmt(*, has_strike_gte: bool, has_strike_lte: bool):
    clauses = [
        "underlying_symbol IN :symbols",
        "trade_date = :as_of_date",
        "contract_type = :contract_type",
        "expiration_date IN :expiration_dates",
    ]
    if has_strike_gte:
        clauses.append("strike_price >= :strike_price_gte")
    if has_strike_lte:
        clauses.append("strike_price <= :strike_price_lte")
    return text(
        f"""
        SELECT option_ticker, underlying_symbol, contract_type, expiration_date, strike_price, close_price
        FROM historical_option_day_bars
        WHERE {' AND '.join(clauses)}
        ORDER BY expiration_date, strike_price, underlying_symbol, option_ticker
        """
    ).bindparams(
        bindparam("symbols", expanding=True),
        bindparam("expiration_dates", expanding=True),
    )


@lru_cache(maxsize=None)
def _contracts_for_expirations_by_type_stmt(*, has_strike_gte: bool, has_strike_lte: bool):
    clauses = [
        "underlying_symbol IN :symbols",
        "trade_date = :as_of_date",
        "contract_type IN :contract_types",
        "expiration_date IN :expiration_dates",
    ]
    if has_strike_gte:
        clauses.append("strike_price >= :strike_price_gte")
    if has_strike_lte:
        clauses.append("strike_price <= :strike_price_lte")
    return text(
        f"""
        SELECT option_ticker, underlying_symbol, contract_type, expiration_date, strike_price, close_price
        FROM historical_option_day_bars
        WHERE {' AND '.join(clauses)}
        ORDER BY contract_type, expiration_date, strike_price, underlying_symbol, option_ticker
        """
    ).bindparams(
        bindparam("symbols", expanding=True),
        bindparam("contract_types", expanding=True),
        bindparam("expiration_dates", expanding=True),
    )


@lru_cache(maxsize=None)
def _available_expirations_stmt(*, has_strike_gte: bool, has_strike_lte: bool):
    clauses = [
        "underlying_symbol IN :symbols",
        "trade_date = :as_of_date",
        "contract_type = :contract_type",
        "expiration_date IN :expiration_dates",
    ]
    if has_strike_gte:
        clauses.append("strike_price >= :strike_price_gte")
    if has_strike_lte:
        clauses.append("strike_price <= :strike_price_lte")
    return text(
        f"""
        SELECT DISTINCT expiration_date
        FROM historical_option_day_bars
        WHERE {' AND '.join(clauses)}
        ORDER BY expiration_date
        """
    ).bindparams(
        bindparam("symbols", expanding=True),
        bindparam("expiration_dates", expanding=True),
    )


@lru_cache(maxsize=None)
def _available_expirations_by_type_stmt(*, has_strike_gte: bool, has_strike_lte: bool):
    clauses = [
        "underlying_symbol IN :symbols",
        "trade_date = :as_of_date",
        "contract_type IN :contract_types",
        "expiration_date IN :expiration_dates",
    ]
    if has_strike_gte:
        clauses.append("strike_price >= :strike_price_gte")
    if has_strike_lte:
        clauses.append("strike_price <= :strike_price_lte")
    return text(
        f"""
        SELECT DISTINCT contract_type, expiration_date
        FROM historical_option_day_bars
        WHERE {' AND '.join(clauses)}
        ORDER BY contract_type, expiration_date
        """
    ).bindparams(
        bindparam("symbols", expanding=True),
        bindparam("contract_types", expanding=True),
        bindparam("expiration_dates", expanding=True),
    )


@lru_cache(maxsize=None)
def _quote_for_date_stmt():
    return text(
        """
        SELECT underlying_symbol, close_price
        FROM historical_option_day_bars
        WHERE option_ticker = :option_ticker
          AND trade_date = :trade_date
        """
    )


@lru_cache(maxsize=None)
def _quotes_for_date_stmt():
    return text(
        """
        SELECT option_ticker, underlying_symbol, close_price
        FROM historical_option_day_bars
        WHERE trade_date = :trade_date
          AND option_ticker IN :option_tickers
        """
    ).bindparams(bindparam("option_tickers", expanding=True))


@lru_cache(maxsize=None)
def _quote_series_stmt():
    return text(
        """
        SELECT option_ticker, trade_date, underlying_symbol, close_price
        FROM historical_option_day_bars
        WHERE option_ticker IN :option_tickers
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        """
    ).bindparams(bindparam("option_tickers", expanding=True))


def _normalize_text_params(params: dict[str, object], dialect_name: str) -> dict[str, object]:
    if dialect_name != "sqlite":
        return params
    normalized: dict[str, object] = {}
    for key, value in params.items():
        if isinstance(value, date):
            normalized[key] = value.isoformat()
        elif isinstance(value, (tuple, list)) and value and all(isinstance(item, date) for item in value):
            normalized[key] = [item.isoformat() for item in value]
        else:
            normalized[key] = value
    return normalized


def _coerce_row_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Unsupported date value from query row: {value!r}")


def parse_option_ticker_metadata(option_ticker: str) -> tuple[str, date, str, float] | None:
    ticker = option_ticker.strip().upper()
    if not ticker.startswith("O:"):
        return None
    body = ticker[2:]
    if len(body) < 16:
        return None
    suffix = body[-15:]
    underlying = body[:-15]
    if len(suffix) != 15 or not underlying:
        return None
    raw_date = suffix[:6]
    contract_flag = suffix[6]
    raw_strike = suffix[7:]
    if contract_flag not in {"C", "P"}:
        return None
    try:
        expiration = date(2000 + int(raw_date[:2]), int(raw_date[2:4]), int(raw_date[4:6]))
        strike = int(raw_strike) / 1000.0
    except ValueError:
        return None
    return underlying, expiration, "call" if contract_flag == "C" else "put", strike


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _base_root_symbol(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    stripped = normalized.rstrip("0123456789")
    return stripped or normalized


def _is_related_root_symbol(base_symbol: str, candidate_symbol: str) -> bool:
    normalized_candidate = _normalize_symbol(candidate_symbol)
    if normalized_candidate == base_symbol:
        return True
    if not normalized_candidate.startswith(base_symbol):
        return False
    suffix = normalized_candidate[len(base_symbol):]
    return bool(suffix) and suffix.isdigit()


def _root_sequence_index(base_symbol: str, symbol: str) -> int | None:
    normalized_symbol = _normalize_symbol(symbol)
    if normalized_symbol == base_symbol:
        return 0
    if not _is_related_root_symbol(base_symbol, normalized_symbol):
        return None
    suffix = normalized_symbol[len(base_symbol):]
    if not suffix:
        return 0
    if not suffix.isdigit():
        return None
    return int(suffix)


def _root_sort_key(base_symbol: str, symbol: str) -> tuple[int, str]:
    index = _root_sequence_index(base_symbol, symbol)
    if index is None:
        return (10_000, _normalize_symbol(symbol))
    return (index, _normalize_symbol(symbol))


def _related_root_symbol_candidates(base_symbol: str) -> list[str]:
    return [base_symbol, *[f"{base_symbol}{index}" for index in range(1, _MAX_NUMERIC_ROOT_ALIASES + 1)]]


def _family_root_symbol_candidates(symbol: str) -> list[str]:
    return _related_root_symbol_candidates(_base_root_symbol(symbol))


def _decimal_strike(strike_price: float) -> Decimal:
    return Decimal(str(strike_price))


_COMMON_SHARE_DELIVERABLE_FACTORS = (
    0.1,
    0.125,
    0.2,
    0.25,
    1.0 / 3.0,
    0.5,
    2.0 / 3.0,
    0.75,
    0.8,
    1.25,
    4.0 / 3.0,
    1.5,
    2.0,
    3.0,
    4.0,
    5.0,
    10.0,
)


def _normalize_deliverable_factor(raw_factor: float) -> float:
    nearest = min(_COMMON_SHARE_DELIVERABLE_FACTORS, key=lambda factor: abs(factor - raw_factor))
    if nearest > 0 and abs(nearest - raw_factor) / nearest <= 0.08:
        return nearest
    return 1.0


def _prefer_requested_root_rows(
    rows: list[tuple[object, ...]],
    *,
    requested_symbol: str,
    group_key: Callable[[tuple[object, ...]], object],
) -> list[tuple[object, ...]]:
    grouped_rows: dict[object, list[tuple[object, ...]]] = {}
    for row in rows:
        grouped_rows.setdefault(group_key(row), []).append(row)

    preferred_rows: list[tuple[object, ...]] = []
    for group_rows in grouped_rows.values():
        requested_rows = [
            row for row in group_rows
            if _normalize_symbol(str(row[1])) == requested_symbol
        ]
        preferred_rows.extend(requested_rows or group_rows)
    return preferred_rows


@dataclass(slots=True)
class _PinnedReadonlySessionContext(AbstractContextManager[Session]):
    session: Session

    def __enter__(self) -> Session:
        return self.session

    def __exit__(self, exc_type, exc, exc_tb) -> bool:
        # End the implicit read transaction after each store call while keeping
        # the underlying session/connection pinned for reuse by long-running
        # backtest workers.
        with suppress(Exception):
            if self.session.in_transaction():
                self.session.rollback()
        return False


@dataclass(slots=True)
class HistoricalMarketDataStore:
    session_factory: Callable[[], Session]
    readonly_session_factory: Callable[[], Session] | None = None
    _related_root_symbols_cache: dict[tuple[str, date | None, date | None, date | None], tuple[str, ...]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _root_first_dates_cache: dict[str, tuple[tuple[str, date], ...]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _deliverable_shares_cache: dict[tuple[str, str], float] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _cache_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    _session_local: threading.local = field(default_factory=threading.local, init=False, repr=False)

    def _get_pinned_readonly_session(self) -> Session | None:
        return getattr(self._session_local, "pinned_readonly_session", None)

    def _set_pinned_readonly_session(self, session: Session | None) -> None:
        self._session_local.pinned_readonly_session = session

    def _get_pinned_readonly_connection(self):
        return getattr(self._session_local, "pinned_readonly_connection", None)

    def _set_pinned_readonly_connection(self, connection) -> None:
        self._session_local.pinned_readonly_connection = connection

    def _get_pinned_readonly_depth(self) -> int:
        return int(getattr(self._session_local, "pinned_readonly_depth", 0))

    def _set_pinned_readonly_depth(self, depth: int) -> None:
        self._session_local.pinned_readonly_depth = depth

    def _session(self, *, readonly: bool) -> Session | AbstractContextManager[Session]:
        if readonly:
            pinned_session = self._get_pinned_readonly_session()
            if pinned_session is not None:
                return _PinnedReadonlySessionContext(pinned_session)
        factory = self.readonly_session_factory if readonly and self.readonly_session_factory is not None else self.session_factory
        return factory()

    @contextmanager
    def pinned_readonly_session(self) -> Iterator[Session]:
        existing_session = self._get_pinned_readonly_session()
        if existing_session is not None:
            self._set_pinned_readonly_depth(self._get_pinned_readonly_depth() + 1)
            try:
                yield existing_session
            finally:
                self._set_pinned_readonly_depth(max(self._get_pinned_readonly_depth() - 1, 0))
            return

        factory = self.readonly_session_factory if self.readonly_session_factory is not None else self.session_factory
        bootstrap_session = factory()
        connection = None
        session = bootstrap_session
        bind = bootstrap_session.get_bind()
        if bind is not None and hasattr(bind, "connect"):
            connection = bind.connect()
            try:
                connection = connection.execution_options(isolation_level="AUTOCOMMIT")
            except Exception:
                pass
            session = type(bootstrap_session)(
                bind=connection,
                autoflush=bootstrap_session.autoflush,
                expire_on_commit=bootstrap_session.expire_on_commit,
            )
            with suppress(Exception):
                bootstrap_session.close()
        self._set_pinned_readonly_session(session)
        self._set_pinned_readonly_connection(connection)
        self._set_pinned_readonly_depth(1)
        try:
            yield session
        finally:
            with suppress(Exception):
                if session.in_transaction():
                    session.rollback()
            with suppress(Exception):
                session.close()
            if connection is not None:
                with suppress(Exception):
                    connection.close()
            self._set_pinned_readonly_session(None)
            self._set_pinned_readonly_connection(None)
            self._set_pinned_readonly_depth(0)

    def _invalidate_option_cache_state(self) -> None:
        with self._cache_lock:
            self._related_root_symbols_cache.clear()
            self._root_first_dates_cache.clear()
            self._deliverable_shares_cache.clear()

    def _get_related_root_symbols(
        self,
        session: Session,
        symbol: str,
        *,
        on_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[str]:
        normalized_symbol = _normalize_symbol(symbol)
        cache_key = (normalized_symbol, on_date, start_date, end_date)
        with self._cache_lock:
            cached = self._related_root_symbols_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        related = self._list_related_root_symbols(
            session,
            normalized_symbol,
            on_date=on_date,
            start_date=start_date,
            end_date=end_date,
        )
        with self._cache_lock:
            self._related_root_symbols_cache[cache_key] = tuple(related)
        return related

    def _get_family_root_first_dates(
        self,
        session: Session,
        symbol: str,
    ) -> dict[str, date]:
        base_symbol = _base_root_symbol(symbol)
        with self._cache_lock:
            cached = self._root_first_dates_cache.get(base_symbol)
        if cached is not None:
            return {root_symbol: first_date for root_symbol, first_date in cached}

        candidates = _related_root_symbol_candidates(base_symbol)
        root_first_dates: dict[str, date] = {}
        for candidate in candidates:
            first_trade_date = session.scalars(
                select(HistoricalOptionDayBar.trade_date)
                .where(HistoricalOptionDayBar.underlying_symbol == candidate)
                .order_by(HistoricalOptionDayBar.trade_date)
                .limit(1)
            ).first()
            if first_trade_date is None:
                continue
            root_first_dates[candidate] = _coerce_row_date(first_trade_date)
        cached_rows = tuple(
            sorted(
                root_first_dates.items(),
                key=lambda item: _root_sort_key(base_symbol, item[0]),
            )
        )
        with self._cache_lock:
            self._root_first_dates_cache[base_symbol] = cached_rows
        return dict(cached_rows)

    def get_underlying_day_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        with self._session(readonly=True) as session:
            rows = list(
                session.execute(
                    select(
                        HistoricalUnderlyingDayBar.trade_date,
                        HistoricalUnderlyingDayBar.open_price,
                        HistoricalUnderlyingDayBar.high_price,
                        HistoricalUnderlyingDayBar.low_price,
                        HistoricalUnderlyingDayBar.close_price,
                        HistoricalUnderlyingDayBar.volume,
                    )
                    .where(
                        HistoricalUnderlyingDayBar.symbol == symbol,
                        HistoricalUnderlyingDayBar.trade_date >= start_date,
                        HistoricalUnderlyingDayBar.trade_date <= end_date,
                    )
                    .order_by(HistoricalUnderlyingDayBar.trade_date)
                )
            )
        return [
            DailyBar(
                trade_date=trade_date,
                open_price=float(open_price),
                high_price=float(high_price),
                low_price=float(low_price),
                close_price=float(close_price),
                volume=float(volume),
            )
            for trade_date, open_price, high_price, low_price, close_price, volume in rows
        ]

    def _get_underlying_trade_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        with self._session(readonly=True) as session:
            rows = list(
                session.scalars(
                    select(HistoricalUnderlyingDayBar.trade_date)
                    .where(
                        HistoricalUnderlyingDayBar.symbol == symbol,
                        HistoricalUnderlyingDayBar.trade_date >= start_date,
                        HistoricalUnderlyingDayBar.trade_date <= end_date,
                    )
                )
            )
        return set(rows)

    def has_underlying_coverage(self, symbol: str, start_date: date, end_date: date) -> bool:
        trade_dates = self._get_underlying_trade_dates(symbol, start_date, end_date)
        if not trade_dates:
            return False
        current = start_date
        while current <= end_date:
            if is_trading_day(current) and current not in trade_dates:
                return False
            current = current.fromordinal(current.toordinal() + 1)
        return True

    @staticmethod
    def _list_related_root_symbols(
        session: Session,
        symbol: str,
        *,
        on_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[str]:
        base_symbol = _base_root_symbol(symbol)
        stmt = select(HistoricalOptionDayBar.underlying_symbol).distinct().where(
            HistoricalOptionDayBar.underlying_symbol.in_(_related_root_symbol_candidates(base_symbol))
        )
        if on_date is not None:
            stmt = stmt.where(HistoricalOptionDayBar.trade_date == on_date)
        if start_date is not None:
            stmt = stmt.where(HistoricalOptionDayBar.trade_date >= start_date)
        if end_date is not None:
            stmt = stmt.where(HistoricalOptionDayBar.trade_date <= end_date)
        raw_symbols = list(session.scalars(stmt))
        related = {
            normalized
            for candidate in raw_symbols
            if candidate
            for normalized in (_normalize_symbol(str(candidate)),)
            if _is_related_root_symbol(base_symbol, normalized)
        }
        return sorted(related, key=lambda item: _root_sort_key(base_symbol, item))

    def _infer_deliverable_shares_per_contract(
        self,
        session: Session,
        *,
        base_symbol: str,
        underlying_symbol: str,
        cache: dict[tuple[str, str], float],
    ) -> float:
        normalized_underlying = _normalize_symbol(underlying_symbol)
        cache_key = (base_symbol, normalized_underlying)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        with self._cache_lock:
            persisted = self._deliverable_shares_cache.get(cache_key)
        if persisted is not None:
            cache[cache_key] = persisted
            return persisted
        if normalized_underlying == base_symbol:
            cache[cache_key] = 100.0
            with self._cache_lock:
                self._deliverable_shares_cache[cache_key] = 100.0
            return 100.0

        first_alias_date = session.scalar(
            select(HistoricalOptionDayBar.trade_date)
            .where(HistoricalOptionDayBar.underlying_symbol == normalized_underlying)
            .order_by(HistoricalOptionDayBar.trade_date)
            .limit(1)
        )
        if first_alias_date is None:
            cache[cache_key] = 100.0
            with self._cache_lock:
                self._deliverable_shares_cache[cache_key] = 100.0
            return 100.0

        previous_close = session.scalar(
            select(HistoricalUnderlyingDayBar.close_price)
            .where(
                HistoricalUnderlyingDayBar.symbol == base_symbol,
                HistoricalUnderlyingDayBar.trade_date < first_alias_date,
            )
            .order_by(HistoricalUnderlyingDayBar.trade_date.desc())
            .limit(1)
        )
        current_close = session.scalar(
            select(HistoricalUnderlyingDayBar.close_price).where(
                HistoricalUnderlyingDayBar.symbol == base_symbol,
                HistoricalUnderlyingDayBar.trade_date == first_alias_date,
            )
        )
        if previous_close is None or current_close is None:
            cache[cache_key] = 100.0
            with self._cache_lock:
                self._deliverable_shares_cache[cache_key] = 100.0
            return 100.0

        previous_close_value = float(previous_close)
        current_close_value = float(current_close)
        if previous_close_value <= 0 or current_close_value <= 0:
            cache[cache_key] = 100.0
            with self._cache_lock:
                self._deliverable_shares_cache[cache_key] = 100.0
            return 100.0

        raw_factor = previous_close_value / current_close_value
        normalized_factor = _normalize_deliverable_factor(raw_factor)
        deliverable_shares = round(100.0 * normalized_factor, 3)
        if deliverable_shares <= 0:
            deliverable_shares = 100.0
        cache[cache_key] = deliverable_shares
        with self._cache_lock:
            self._deliverable_shares_cache[cache_key] = deliverable_shares
        return deliverable_shares

    def _contract_record_from_row(
        self,
        session: Session,
        *,
        base_symbol: str,
        option_ticker: str,
        underlying_symbol: str,
        contract_type: str,
        expiration_date: date,
        strike_price: Decimal | float,
        close_price: Decimal | float | None,
        deliverable_cache: dict[tuple[str, str], float],
    ) -> OptionContractRecord:
        shares_per_contract = self._infer_deliverable_shares_per_contract(
            session,
            base_symbol=base_symbol,
            underlying_symbol=underlying_symbol,
            cache=deliverable_cache,
        )
        return OptionContractRecord(
            ticker=option_ticker,
            contract_type=contract_type,
            expiration_date=expiration_date,
            strike_price=float(strike_price),
            shares_per_contract=shares_per_contract,
            underlying_symbol=_normalize_symbol(underlying_symbol),
            as_of_mid_price=(
                float(close_price)
                if close_price is not None and float(close_price) > 0
                else None
            ),
        )

    def _quote_record_from_row(
        self,
        session: Session,
        *,
        base_symbol: str,
        trade_date: date,
        close_price: Decimal | float,
        option_ticker: str,
        underlying_symbol: str,
        deliverable_cache: dict[tuple[str, str], float],
    ) -> OptionQuoteRecord | None:
        close_value = float(close_price)
        if close_value <= 0:
            return None
        return OptionQuoteRecord(
            trade_date=trade_date,
            bid_price=close_value,
            ask_price=close_value,
            participant_timestamp=None,
            source_option_ticker=option_ticker,
            deliverable_shares_per_contract=self._infer_deliverable_shares_per_contract(
                session,
                base_symbol=base_symbol,
                underlying_symbol=underlying_symbol,
                cache=deliverable_cache,
            ),
        )

    @staticmethod
    def _fetch_contract_rows(
        session: Session,
        *,
        symbols: list[str],
        as_of_date: date,
        contract_types: list[str],
        expiration_date: date | None = None,
        expiration_gte: date | None = None,
        expiration_lte: date | None = None,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[tuple[str, str, str, date, Decimal, Decimal | float | None]]:
        if not symbols or not contract_types:
            return []
        stmt = _contracts_for_expiration_stmt(
            has_expiration_date=expiration_date is not None,
            has_expiration_gte=expiration_gte is not None,
            has_expiration_lte=expiration_lte is not None,
            has_strike_gte=strike_price_gte is not None,
            has_strike_lte=strike_price_lte is not None,
        )
        bind = session.get_bind()
        dialect_name = bind.dialect.name if bind is not None else ""
        params: dict[str, object] = {
            "symbols": list(dict.fromkeys(symbols)),
            "as_of_date": as_of_date,
            "contract_types": list(dict.fromkeys(contract_types)),
        }
        if expiration_date is not None:
            params["expiration_date"] = expiration_date
        if expiration_gte is not None:
            params["expiration_gte"] = expiration_gte
        if expiration_lte is not None:
            params["expiration_lte"] = expiration_lte
        if strike_price_gte is not None:
            params["strike_price_gte"] = _decimal_strike(strike_price_gte)
        if strike_price_lte is not None:
            params["strike_price_lte"] = _decimal_strike(strike_price_lte)
        return list(session.execute(stmt, _normalize_text_params(params, dialect_name)))

    def list_option_contracts(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_gte: date,
        expiration_lte: date,
    ) -> list[OptionContractRecord]:
        return self.list_option_contracts_for_expiration(
            symbol=symbol,
            as_of_date=as_of_date,
            contract_type=contract_type,
            expiration_date=None,
            expiration_gte=expiration_gte,
            expiration_lte=expiration_lte,
        )

    def list_option_contracts_for_expiration(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_date: date | None,
        expiration_gte: date | None = None,
        expiration_lte: date | None = None,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        normalized_symbol = _normalize_symbol(symbol)
        base_symbol = _base_root_symbol(normalized_symbol)
        family_symbols = _family_root_symbol_candidates(normalized_symbol)
        with self._session(readonly=True) as session:
            rows = self._fetch_contract_rows(
                session,
                symbols=family_symbols,
                as_of_date=as_of_date,
                contract_types=[contract_type],
                expiration_date=expiration_date,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
                strike_price_gte=strike_price_gte,
                strike_price_lte=strike_price_lte,
            )
            rows = _prefer_requested_root_rows(
                rows,
                requested_symbol=normalized_symbol,
                group_key=lambda row: _coerce_row_date(row[3]),
            )
            deliverable_cache: dict[tuple[str, str], float] = {}
            return [
                self._contract_record_from_row(
                    session,
                    base_symbol=base_symbol,
                    option_ticker=option_ticker,
                    underlying_symbol=underlying_symbol,
                    contract_type=row_contract_type,
                    expiration_date=_coerce_row_date(row_expiration_date),
                    strike_price=row_strike_price,
                    close_price=row_close_price,
                    deliverable_cache=deliverable_cache,
                )
                for option_ticker, underlying_symbol, row_contract_type, row_expiration_date, row_strike_price, row_close_price in rows
            ]

    def list_option_contracts_for_expirations(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_dates: list[date],
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> dict[date, list[OptionContractRecord]]:
        if not expiration_dates:
            return {}
        requested_expirations = tuple(dict.fromkeys(expiration_dates))
        contracts_by_expiration: dict[date, list[OptionContractRecord]] = {
            expiration_date: [] for expiration_date in requested_expirations
        }
        normalized_symbol = _normalize_symbol(symbol)
        base_symbol = _base_root_symbol(normalized_symbol)
        family_symbols = _family_root_symbol_candidates(normalized_symbol)
        with self._session(readonly=True) as session:
            deliverable_cache: dict[tuple[str, str], float] = {}
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            stmt = _contracts_for_expirations_stmt(
                has_strike_gte=strike_price_gte is not None,
                has_strike_lte=strike_price_lte is not None,
            )
            params: dict[str, object] = {
                "symbols": family_symbols,
                "as_of_date": as_of_date,
                "contract_type": contract_type,
                "expiration_dates": list(requested_expirations),
            }
            if strike_price_gte is not None:
                params["strike_price_gte"] = _decimal_strike(strike_price_gte)
            if strike_price_lte is not None:
                params["strike_price_lte"] = _decimal_strike(strike_price_lte)
            rows = _prefer_requested_root_rows(
                list(session.execute(stmt, _normalize_text_params(params, dialect_name))),
                requested_symbol=normalized_symbol,
                group_key=lambda row: _coerce_row_date(row[3]),
            )
            for option_ticker, underlying_symbol, row_contract_type, row_expiration_date, row_strike_price, row_close_price in rows:
                normalized_expiration = _coerce_row_date(row_expiration_date)
                if normalized_expiration not in contracts_by_expiration:
                    continue
                contracts_by_expiration[normalized_expiration].append(
                    self._contract_record_from_row(
                        session,
                        base_symbol=base_symbol,
                        option_ticker=option_ticker,
                        underlying_symbol=underlying_symbol,
                        contract_type=row_contract_type,
                        expiration_date=normalized_expiration,
                        strike_price=row_strike_price,
                        close_price=row_close_price,
                        deliverable_cache=deliverable_cache,
                    )
                )
        return contracts_by_expiration

    def list_option_contracts_for_expirations_by_type(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_types: list[str],
        expiration_dates: list[date],
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> dict[str, dict[date, list[OptionContractRecord]]]:
        if not contract_types or not expiration_dates:
            return {}
        requested_types = tuple(dict.fromkeys(contract_types))
        requested_expirations = tuple(dict.fromkeys(expiration_dates))
        contracts_by_type: dict[str, dict[date, list[OptionContractRecord]]] = {
            contract_type: {
                expiration_date: [] for expiration_date in requested_expirations
            }
            for contract_type in requested_types
        }
        normalized_symbol = _normalize_symbol(symbol)
        base_symbol = _base_root_symbol(normalized_symbol)
        family_symbols = _family_root_symbol_candidates(normalized_symbol)
        with self._session(readonly=True) as session:
            deliverable_cache: dict[tuple[str, str], float] = {}
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            stmt = _contracts_for_expirations_by_type_stmt(
                has_strike_gte=strike_price_gte is not None,
                has_strike_lte=strike_price_lte is not None,
            )
            params: dict[str, object] = {
                "symbols": family_symbols,
                "as_of_date": as_of_date,
                "contract_types": list(requested_types),
                "expiration_dates": list(requested_expirations),
            }
            if strike_price_gte is not None:
                params["strike_price_gte"] = _decimal_strike(strike_price_gte)
            if strike_price_lte is not None:
                params["strike_price_lte"] = _decimal_strike(strike_price_lte)
            rows = _prefer_requested_root_rows(
                list(session.execute(stmt, _normalize_text_params(params, dialect_name))),
                requested_symbol=normalized_symbol,
                group_key=lambda row: (row[2], _coerce_row_date(row[3])),
            )
            for option_ticker, underlying_symbol, row_contract_type, row_expiration_date, row_strike_price, row_close_price in rows:
                normalized_expiration = _coerce_row_date(row_expiration_date)
                if (
                    row_contract_type not in contracts_by_type
                    or normalized_expiration not in contracts_by_type[row_contract_type]
                ):
                    continue
                contracts_by_type[row_contract_type][normalized_expiration].append(
                    self._contract_record_from_row(
                        session,
                        base_symbol=base_symbol,
                        option_ticker=option_ticker,
                        underlying_symbol=underlying_symbol,
                        contract_type=row_contract_type,
                        expiration_date=normalized_expiration,
                        strike_price=row_strike_price,
                        close_price=row_close_price,
                        deliverable_cache=deliverable_cache,
                    )
                )
        return contracts_by_type

    def list_available_option_expirations(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_dates: list[date],
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[date]:
        if not expiration_dates:
            return []
        requested_expirations = tuple(dict.fromkeys(expiration_dates))
        normalized_symbol = _normalize_symbol(symbol)
        family_symbols = _family_root_symbol_candidates(normalized_symbol)
        with self._session(readonly=True) as session:
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            stmt = _available_expirations_stmt(
                has_strike_gte=strike_price_gte is not None,
                has_strike_lte=strike_price_lte is not None,
            )
            params: dict[str, object] = {
                "symbols": family_symbols,
                "as_of_date": as_of_date,
                "contract_type": contract_type,
                "expiration_dates": list(requested_expirations),
            }
            if strike_price_gte is not None:
                params["strike_price_gte"] = _decimal_strike(strike_price_gte)
            if strike_price_lte is not None:
                params["strike_price_lte"] = _decimal_strike(strike_price_lte)
            available_expirations = {
                _coerce_row_date(row_expiration_date)
                for (row_expiration_date,) in session.execute(
                    stmt,
                    _normalize_text_params(params, dialect_name),
                )
            }
        return [
            expiration_date
            for expiration_date in requested_expirations
            if expiration_date in available_expirations
        ]

    def list_available_option_expirations_by_type(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_types: list[str],
        expiration_dates: list[date],
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> dict[str, list[date]]:
        if not contract_types or not expiration_dates:
            return {}
        requested_types = tuple(dict.fromkeys(contract_types))
        requested_expirations = tuple(dict.fromkeys(expiration_dates))
        normalized_symbol = _normalize_symbol(symbol)
        family_symbols = _family_root_symbol_candidates(normalized_symbol)
        with self._session(readonly=True) as session:
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            stmt = _available_expirations_by_type_stmt(
                has_strike_gte=strike_price_gte is not None,
                has_strike_lte=strike_price_lte is not None,
            )
            params: dict[str, object] = {
                "symbols": family_symbols,
                "as_of_date": as_of_date,
                "contract_types": list(requested_types),
                "expiration_dates": list(requested_expirations),
            }
            if strike_price_gte is not None:
                params["strike_price_gte"] = _decimal_strike(strike_price_gte)
            if strike_price_lte is not None:
                params["strike_price_lte"] = _decimal_strike(strike_price_lte)

            available_by_type: dict[str, set[date]] = {contract_type: set() for contract_type in requested_types}
            for row_contract_type, row_expiration_date in session.execute(
                stmt,
                _normalize_text_params(params, dialect_name),
            ):
                if row_contract_type not in available_by_type:
                    continue
                available_by_type[row_contract_type].add(_coerce_row_date(row_expiration_date))
        return {
            contract_type: [
                expiration_date
                for expiration_date in requested_expirations
                if expiration_date in available_by_type[contract_type]
            ]
            for contract_type in requested_types
        }

    @staticmethod
    def _fetch_signature_rows_for_date(
        session: Session,
        *,
        symbols: list[str],
        trade_date: date,
        expiration_date: date,
        contract_type: str,
        strike_price: float,
    ) -> list[tuple[str, str, Decimal | float]]:
        if not symbols:
            return []
        stmt = select(
            HistoricalOptionDayBar.option_ticker,
            HistoricalOptionDayBar.underlying_symbol,
            HistoricalOptionDayBar.close_price,
        ).where(
            HistoricalOptionDayBar.underlying_symbol.in_(symbols),
            HistoricalOptionDayBar.trade_date == trade_date,
            HistoricalOptionDayBar.expiration_date == expiration_date,
            HistoricalOptionDayBar.contract_type == contract_type,
            HistoricalOptionDayBar.strike_price == _decimal_strike(strike_price),
        ).order_by(
            HistoricalOptionDayBar.underlying_symbol,
            HistoricalOptionDayBar.option_ticker,
        )
        return list(session.execute(stmt))

    @staticmethod
    def _fetch_signature_rows_for_range(
        session: Session,
        *,
        symbols: list[str],
        start_date: date,
        end_date: date,
        expiration_date: date,
        contract_type: str,
        strike_price: float,
    ) -> list[tuple[date, str, str, Decimal | float]]:
        if not symbols:
            return []
        stmt = select(
            HistoricalOptionDayBar.trade_date,
            HistoricalOptionDayBar.option_ticker,
            HistoricalOptionDayBar.underlying_symbol,
            HistoricalOptionDayBar.close_price,
        ).where(
            HistoricalOptionDayBar.underlying_symbol.in_(symbols),
            HistoricalOptionDayBar.trade_date >= start_date,
            HistoricalOptionDayBar.trade_date <= end_date,
            HistoricalOptionDayBar.expiration_date == expiration_date,
            HistoricalOptionDayBar.contract_type == contract_type,
            HistoricalOptionDayBar.strike_price == _decimal_strike(strike_price),
        ).order_by(
            HistoricalOptionDayBar.trade_date,
            HistoricalOptionDayBar.underlying_symbol,
            HistoricalOptionDayBar.option_ticker,
        )
        return list(session.execute(stmt))

    @staticmethod
    def _lineage_successor_roots(
        *,
        base_symbol: str,
        requested_root: str,
        start_date: date,
        root_first_dates: dict[str, date],
    ) -> list[tuple[date, str]]:
        requested_index = _root_sequence_index(base_symbol, requested_root)
        if requested_index is None:
            return []
        successors: list[tuple[date, str]] = []
        for root_symbol, first_date in root_first_dates.items():
            root_index = _root_sequence_index(base_symbol, root_symbol)
            if root_index is None or root_index <= requested_index or first_date <= start_date:
                continue
            successors.append((first_date, root_symbol))
        successors.sort(key=lambda item: (item[0], _root_sort_key(base_symbol, item[1])))
        return successors

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        with self._session(readonly=True) as session:
            deliverable_cache: dict[tuple[str, str], float] = {}
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            exact_row = session.execute(
                _quote_for_date_stmt(),
                _normalize_text_params(
                    {"option_ticker": option_ticker, "trade_date": trade_date},
                    dialect_name,
                ),
            ).first()
            metadata = parse_option_ticker_metadata(option_ticker)
            base_symbol = _base_root_symbol(metadata[0]) if metadata is not None else None
            if exact_row is not None and base_symbol is not None:
                quote = self._quote_record_from_row(
                    session,
                    base_symbol=base_symbol,
                    trade_date=trade_date,
                    close_price=exact_row[1],
                    option_ticker=option_ticker,
                    underlying_symbol=str(exact_row[0]),
                    deliverable_cache=deliverable_cache,
                )
                if quote is not None:
                    return quote

            if metadata is None:
                return None
            requested_root, expiration_date, contract_type, strike_price = metadata
            family_symbols = self._get_related_root_symbols(
                session,
                requested_root,
                on_date=trade_date,
            )
            fallback_symbols = [symbol for symbol in family_symbols if symbol != requested_root]
            if not fallback_symbols:
                return None
            fallback_rows = self._fetch_signature_rows_for_date(
                session,
                symbols=fallback_symbols,
                trade_date=trade_date,
                expiration_date=expiration_date,
                contract_type=contract_type,
                strike_price=strike_price,
            )
            for fallback_ticker, underlying_symbol, close_price in fallback_rows:
                quote = self._quote_record_from_row(
                    session,
                    base_symbol=_base_root_symbol(requested_root),
                    trade_date=trade_date,
                    close_price=close_price,
                    option_ticker=str(fallback_ticker),
                    underlying_symbol=str(underlying_symbol),
                    deliverable_cache=deliverable_cache,
                )
                if quote is not None:
                    return quote
        return None

    def get_option_quotes_for_date(
        self,
        option_tickers: list[str],
        trade_date: date,
    ) -> dict[str, OptionQuoteRecord | None]:
        if not option_tickers:
            return {}
        requested_tickers = tuple(dict.fromkeys(option_tickers))
        quotes: dict[str, OptionQuoteRecord | None] = {ticker: None for ticker in requested_tickers}
        with self._session(readonly=True) as session:
            deliverable_cache: dict[tuple[str, str], float] = {}
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            exact_rows = list(
                session.execute(
                    _quotes_for_date_stmt(),
                    _normalize_text_params(
                        {"trade_date": trade_date, "option_tickers": list(requested_tickers)},
                        dialect_name,
                    ),
                )
            )
            exact_hits = {
                str(option_ticker): (str(underlying_symbol), close_price)
                for option_ticker, underlying_symbol, close_price in exact_rows
            }
            for option_ticker in requested_tickers:
                metadata = parse_option_ticker_metadata(option_ticker)
                base_symbol = _base_root_symbol(metadata[0]) if metadata is not None else None
                exact_hit = exact_hits.get(option_ticker)
                if exact_hit is not None and base_symbol is not None:
                    quote = self._quote_record_from_row(
                        session,
                        base_symbol=base_symbol,
                        trade_date=trade_date,
                        close_price=exact_hit[1],
                        option_ticker=option_ticker,
                        underlying_symbol=exact_hit[0],
                        deliverable_cache=deliverable_cache,
                    )
                    if quote is not None:
                        quotes[option_ticker] = quote
                        continue
                if metadata is None:
                    continue
                requested_root, expiration_date, contract_type, strike_price = metadata
                fallback_symbols = [
                    symbol
                    for symbol in self._get_related_root_symbols(session, requested_root, on_date=trade_date)
                    if symbol != requested_root
                ]
                if not fallback_symbols:
                    continue
                fallback_rows = self._fetch_signature_rows_for_date(
                    session,
                    symbols=fallback_symbols,
                    trade_date=trade_date,
                    expiration_date=expiration_date,
                    contract_type=contract_type,
                    strike_price=strike_price,
                )
                for fallback_ticker, underlying_symbol, close_price in fallback_rows:
                    quote = self._quote_record_from_row(
                        session,
                        base_symbol=_base_root_symbol(requested_root),
                        trade_date=trade_date,
                        close_price=close_price,
                        option_ticker=str(fallback_ticker),
                        underlying_symbol=str(underlying_symbol),
                        deliverable_cache=deliverable_cache,
                    )
                    if quote is not None:
                        quotes[option_ticker] = quote
                        break
        return quotes

    def get_option_quote_series(
        self,
        option_tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[date, OptionQuoteRecord | None]]:
        if not option_tickers:
            return {}
        requested_tickers = tuple(dict.fromkeys(option_tickers))
        series: dict[str, dict[date, OptionQuoteRecord | None]] = {
            ticker: {} for ticker in requested_tickers
        }
        with self._session(readonly=True) as session:
            deliverable_cache: dict[tuple[str, str], float] = {}
            family_symbol_cache: dict[tuple[str, date, date], list[str]] = {}
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            for option_ticker in requested_tickers:
                exact_rows = list(
                    session.execute(
                        _quote_series_stmt(),
                        _normalize_text_params(
                            {
                                "option_tickers": [option_ticker],
                                "start_date": start_date,
                                "end_date": end_date,
                            },
                            dialect_name,
                        ),
                    )
                )
                metadata = parse_option_ticker_metadata(option_ticker)
                if metadata is None:
                    for _, trade_dt, underlying_symbol, close_price in exact_rows:
                        normalized_trade_date = _coerce_row_date(trade_dt)
                        quote = self._quote_record_from_row(
                            session,
                            base_symbol=_normalize_symbol(str(underlying_symbol)),
                            trade_date=normalized_trade_date,
                            close_price=close_price,
                            option_ticker=option_ticker,
                            underlying_symbol=str(underlying_symbol),
                            deliverable_cache=deliverable_cache,
                        )
                        if quote is not None:
                            series[option_ticker][normalized_trade_date] = quote
                    continue

                requested_root, expiration_date, contract_type, strike_price = metadata
                base_symbol = _base_root_symbol(requested_root)
                if exact_rows:
                    exact_series: dict[date, OptionQuoteRecord | None] = {}
                    for _, trade_dt, underlying_symbol, close_price in exact_rows:
                        normalized_trade_date = _coerce_row_date(trade_dt)
                        quote = self._quote_record_from_row(
                            session,
                            base_symbol=base_symbol,
                            trade_date=normalized_trade_date,
                            close_price=close_price,
                            option_ticker=option_ticker,
                            underlying_symbol=str(underlying_symbol),
                            deliverable_cache=deliverable_cache,
                        )
                        if quote is not None:
                            exact_series[normalized_trade_date] = quote
                    root_first_dates = self._get_family_root_first_dates(session, requested_root)
                    successor_roots = self._lineage_successor_roots(
                        base_symbol=base_symbol,
                        requested_root=requested_root,
                        start_date=start_date,
                        root_first_dates=root_first_dates,
                    )
                    if not any(first_date <= end_date for first_date, _ in successor_roots):
                        series[option_ticker] = exact_series
                        continue

                family_cache_key = (base_symbol, start_date, end_date)
                family_symbols = family_symbol_cache.get(family_cache_key)
                if family_symbols is None:
                    family_symbols = self._get_related_root_symbols(
                        session,
                        requested_root,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if requested_root not in family_symbols:
                        family_symbols = sorted(
                            {*family_symbols, requested_root},
                            key=lambda item: _root_sort_key(base_symbol, item),
                        )
                    family_symbol_cache[family_cache_key] = family_symbols
                signature_rows = self._fetch_signature_rows_for_range(
                    session,
                    symbols=family_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    expiration_date=expiration_date,
                    contract_type=contract_type,
                    strike_price=strike_price,
                )
                if not signature_rows:
                    continue

                rows_by_date: dict[date, dict[str, tuple[str, str, Decimal | float]]] = {}
                root_first_dates: dict[str, date] = {}
                for trade_dt, source_option_ticker, underlying_symbol, close_price in signature_rows:
                    normalized_trade_date = _coerce_row_date(trade_dt)
                    normalized_root = _normalize_symbol(str(underlying_symbol))
                    rows_by_date.setdefault(normalized_trade_date, {})[normalized_root] = (
                        str(source_option_ticker),
                        normalized_root,
                        close_price,
                    )
                    root_first_dates.setdefault(normalized_root, normalized_trade_date)

                current_root = requested_root
                successor_roots = self._lineage_successor_roots(
                    base_symbol=base_symbol,
                    requested_root=requested_root,
                    start_date=start_date,
                    root_first_dates=root_first_dates,
                )
                successor_index = 0
                for normalized_trade_date in sorted(rows_by_date):
                    while successor_index < len(successor_roots) and successor_roots[successor_index][0] <= normalized_trade_date:
                        current_root = successor_roots[successor_index][1]
                        successor_index += 1
                    day_rows = rows_by_date[normalized_trade_date]
                    selected_row = day_rows.get(current_root)
                    if selected_row is None:
                        current_index = _root_sequence_index(base_symbol, current_root)
                        ordered_roots = sorted(day_rows, key=lambda item: _root_sort_key(base_symbol, item))
                        for root_symbol in ordered_roots:
                            root_index = _root_sequence_index(base_symbol, root_symbol)
                            if current_index is not None and root_index is not None and root_index < current_index:
                                continue
                            selected_row = day_rows[root_symbol]
                            current_root = root_symbol
                            break
                    if selected_row is None:
                        continue
                    quote = self._quote_record_from_row(
                        session,
                        base_symbol=base_symbol,
                        trade_date=normalized_trade_date,
                        close_price=selected_row[2],
                        option_ticker=selected_row[0],
                        underlying_symbol=selected_row[1],
                        deliverable_cache=deliverable_cache,
                    )
                    if quote is not None:
                        series[option_ticker][normalized_trade_date] = quote
        return series

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        with self._session(readonly=True) as session:
            rows = list(
                session.scalars(
                    select(HistoricalExDividendDate.ex_dividend_date).distinct().where(
                        HistoricalExDividendDate.symbol == symbol,
                        HistoricalExDividendDate.ex_dividend_date >= start_date,
                        HistoricalExDividendDate.ex_dividend_date <= end_date,
                    )
                )
            )
        return set(rows)

    def list_earnings_event_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        with self._session(readonly=True) as session:
            rows = list(
                session.scalars(
                    select(HistoricalEarningsEvent.event_date).distinct().where(
                        HistoricalEarningsEvent.symbol == symbol,
                        HistoricalEarningsEvent.event_date >= start_date,
                        HistoricalEarningsEvent.event_date <= end_date,
                    )
                )
            )
        return set(rows)

    def list_imported_symbols_for_window(self, start_date: date, end_date: date) -> set[str]:
        symbol_union = union_all(
            select(HistoricalUnderlyingDayBar.symbol.label("symbol")).where(
                HistoricalUnderlyingDayBar.trade_date >= start_date,
                HistoricalUnderlyingDayBar.trade_date <= end_date,
            ),
            select(HistoricalOptionDayBar.underlying_symbol.label("symbol")).where(
                HistoricalOptionDayBar.trade_date >= start_date,
                HistoricalOptionDayBar.trade_date <= end_date,
            ),
        ).subquery()
        with self._session(readonly=True) as session:
            symbol_rows = session.execute(select(symbol_union.c.symbol).distinct())
            return {str(symbol).upper() for (symbol,) in symbol_rows if symbol}

    def get_freshness_summary(self) -> dict[str, dict[str, str | int | None]]:
        def _row_to_payload(
            latest_date: date | None,
            latest_source_file_date: date | None,
            *,
            row_estimate: int | None = None,
        ) -> dict[str, str | int | None]:
            return {
                "latest_date": latest_date.isoformat() if latest_date is not None else None,
                "latest_source_file_date": latest_source_file_date.isoformat() if latest_source_file_date is not None else None,
                "row_estimate": row_estimate,
            }

        with self._session(readonly=True) as session:
            latest_underlying = session.execute(
                select(
                    HistoricalUnderlyingDayBar.trade_date,
                    HistoricalUnderlyingDayBar.source_file_date,
                )
                .order_by(HistoricalUnderlyingDayBar.trade_date.desc())
                .limit(1)
            ).first()
            latest_option = session.execute(
                select(
                    HistoricalOptionDayBar.trade_date,
                    HistoricalOptionDayBar.source_file_date,
                )
                .order_by(HistoricalOptionDayBar.trade_date.desc())
                .limit(1)
            ).first()
            latest_dividend = session.execute(
                select(
                    HistoricalExDividendDate.ex_dividend_date,
                    HistoricalExDividendDate.source_file_date,
                )
                .order_by(HistoricalExDividendDate.ex_dividend_date.desc())
                .limit(1)
            ).first()
            latest_earnings = session.execute(
                select(
                    HistoricalEarningsEvent.event_date,
                    HistoricalEarningsEvent.source_file_date,
                )
                .order_by(HistoricalEarningsEvent.event_date.desc())
                .limit(1)
            ).first()
            latest_treasury = session.execute(
                select(
                    HistoricalTreasuryYield.trade_date,
                    HistoricalTreasuryYield.source_file_date,
                )
                .order_by(HistoricalTreasuryYield.trade_date.desc())
                .limit(1)
            ).first()
            row_estimates = {
                "historical_underlying_day_bars": None,
                "historical_option_day_bars": None,
                "historical_ex_dividend_dates": None,
                "historical_earnings_events": None,
                "historical_treasury_yields": None,
            }
            bind = session.get_bind()
            if bind is not None and getattr(bind.dialect, "name", "") == "postgresql":
                for table_name in tuple(row_estimates):
                    estimate = session.execute(
                        text(
                            "SELECT GREATEST(reltuples::bigint, 0) "
                            "FROM pg_class WHERE relname = :table_name"
                        ),
                        {"table_name": table_name},
                    ).scalar_one_or_none()
                    row_estimates[table_name] = int(estimate) if estimate is not None else None

        return {
            "underlying_day_bars": _row_to_payload(
                *(latest_underlying or (None, None)),
                row_estimate=row_estimates["historical_underlying_day_bars"],
            ),
            "option_day_bars": _row_to_payload(
                *(latest_option or (None, None)),
                row_estimate=row_estimates["historical_option_day_bars"],
            ),
            "ex_dividend_dates": _row_to_payload(
                *(latest_dividend or (None, None)),
                row_estimate=row_estimates["historical_ex_dividend_dates"],
            ),
            "earnings_events": _row_to_payload(
                *(latest_earnings or (None, None)),
                row_estimate=row_estimates["historical_earnings_events"],
            ),
            "treasury_yields": _row_to_payload(
                *(latest_treasury or (None, None)),
                row_estimate=row_estimates["historical_treasury_yields"],
            ),
        }

    def get_average_treasury_yield(
        self,
        start_date: date,
        end_date: date,
        *,
        field_name: str = "yield_3_month",
    ) -> float | None:
        series = self.get_treasury_yield_series(start_date, end_date, field_name=field_name)
        if not series:
            return None
        return sum(series.values()) / len(series)

    def get_treasury_yield_series(
        self,
        start_date: date,
        end_date: date,
        *,
        field_name: str = "yield_3_month",
    ) -> dict[date, float]:
        if field_name != "yield_3_month":
            return {}
        with self._session(readonly=True) as session:
            rows = list(
                session.execute(
                    select(
                        HistoricalTreasuryYield.trade_date,
                        HistoricalTreasuryYield.yield_3_month,
                    )
                    .where(
                        HistoricalTreasuryYield.trade_date >= start_date,
                        HistoricalTreasuryYield.trade_date <= end_date,
                    )
                    .order_by(HistoricalTreasuryYield.trade_date)
                )
            )
        return {trade_date: float(yield_3_month) for trade_date, yield_3_month in rows}

    def upsert_underlying_day_bars(self, bars: list[HistoricalUnderlyingDayBar]) -> int:
        return self._bulk_upsert(bars, HistoricalUnderlyingDayBar, ("symbol", "trade_date"))

    def upsert_underlying_raw_day_bars(self, bars: list[HistoricalUnderlyingRawDayBar]) -> int:
        return self._bulk_upsert(bars, HistoricalUnderlyingRawDayBar, ("symbol", "trade_date"))

    def upsert_option_day_bars(self, bars: list[HistoricalOptionDayBar]) -> int:
        return self._bulk_upsert(bars, HistoricalOptionDayBar, ("option_ticker", "trade_date"))

    def upsert_ex_dividend_dates(self, rows: list[HistoricalExDividendDate]) -> int:
        if not rows:
            return 0
        payloads = [self._normalize_payload(self._row_payload(row, HistoricalExDividendDate), HistoricalExDividendDate) for row in rows]
        return self.upsert_ex_dividend_payloads(payloads)

    def upsert_earnings_events(self, rows: list[HistoricalEarningsEvent]) -> int:
        if not rows:
            return 0
        payloads = [self._normalize_payload(self._row_payload(row, HistoricalEarningsEvent), HistoricalEarningsEvent) for row in rows]
        return self.upsert_earnings_event_payloads(payloads)

    def upsert_treasury_yields(self, rows: list[HistoricalTreasuryYield]) -> int:
        return self._bulk_upsert(rows, HistoricalTreasuryYield, ("trade_date",))

    def upsert_underlying_day_bar_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalUnderlyingDayBar, ("symbol", "trade_date"))

    def upsert_underlying_raw_day_bar_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalUnderlyingRawDayBar, ("symbol", "trade_date"))

    def upsert_option_day_bar_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalOptionDayBar, ("option_ticker", "trade_date"))

    def upsert_option_day_bar_records(self, rows: list[tuple[object, ...]]) -> int:
        if not rows:
            return 0
        rows = self._dedupe_option_records(rows)
        session = self._session(readonly=False)
        try:
            bind = session.get_bind()
            if bind is not None and bind.dialect.name == "postgresql" and self._can_use_postgres_copy_fast_path(session, HistoricalOptionDayBar):
                self._bulk_upsert_postgres_copy_rows(
                    session,
                    HistoricalOptionDayBar,
                    ("option_ticker", "trade_date"),
                    _OPTION_DAY_BAR_COPY_COLUMNS,
                    rows,
                )
            else:
                payloads = [
                    dict(zip(_OPTION_DAY_BAR_COPY_COLUMNS, row, strict=True))
                    for row in rows
                ]
                self._bulk_upsert_fallback(session, payloads, HistoricalOptionDayBar, ("option_ticker", "trade_date"))
            session.commit()
            self._invalidate_option_cache_state()
            return len(rows)
        except Exception:
            with suppress(Exception):
                session.rollback()
            logger.warning("historical_store.bulk_upsert_option_records_failed", exc_info=True)
            raise
        finally:
            with suppress(Exception):
                session.close()

    def upsert_ex_dividend_payloads(self, rows: list[dict[str, object]]) -> int:
        if not rows:
            return 0
        normalized_rows = [self._normalize_payload(row, HistoricalExDividendDate) for row in rows]
        provider_rows = [row for row in normalized_rows if row.get("provider_dividend_id")]
        legacy_rows = [row for row in normalized_rows if not row.get("provider_dividend_id")]
        stored = 0
        session = self._session(readonly=False)
        try:
            if provider_rows:
                provider_rows = self._dedupe_payloads(provider_rows, ("provider_dividend_id",))
                placeholder_pairs = sorted(
                    {
                        (str(row["symbol"]), row["ex_dividend_date"])
                        for row in provider_rows
                        if row.get("symbol") and row.get("ex_dividend_date") is not None
                    }
                )
                if placeholder_pairs:
                    session.execute(
                        delete(HistoricalExDividendDate).where(
                            HistoricalExDividendDate.provider_dividend_id.is_(None),
                            tuple_(HistoricalExDividendDate.symbol, HistoricalExDividendDate.ex_dividend_date).in_(placeholder_pairs),
                        )
                    )
                bind = session.get_bind()
                if bind is not None and bind.dialect.name == "postgresql":
                    self._bulk_upsert_postgres(session, provider_rows, HistoricalExDividendDate, ("provider_dividend_id",))
                else:
                    self._bulk_upsert_fallback(session, provider_rows, HistoricalExDividendDate, ("provider_dividend_id",))
                stored += len(provider_rows)
            if legacy_rows:
                legacy_rows = self._dedupe_payloads(legacy_rows, ("symbol", "ex_dividend_date"))
                bind = session.get_bind()
                if bind is not None and bind.dialect.name == "postgresql":
                    self._bulk_upsert_postgres(session, legacy_rows, HistoricalExDividendDate, ("symbol", "ex_dividend_date"))
                else:
                    self._bulk_upsert_fallback(session, legacy_rows, HistoricalExDividendDate, ("symbol", "ex_dividend_date"))
                stored += len(legacy_rows)
            session.commit()
            return stored
        except Exception:
            with suppress(Exception):
                session.rollback()
            logger.warning("historical_store.bulk_upsert_ex_dividend_failed", exc_info=True)
            raise
        finally:
            with suppress(Exception):
                session.close()

    def upsert_earnings_event_payloads(self, rows: list[dict[str, object]]) -> int:
        if not rows:
            return 0
        normalized_rows = [self._normalize_payload(row, HistoricalEarningsEvent) for row in rows]
        provider_rows = [row for row in normalized_rows if row.get("provider_event_id")]
        legacy_rows = [row for row in normalized_rows if not row.get("provider_event_id")]
        stored = 0
        session = self._session(readonly=False)
        try:
            if provider_rows:
                provider_rows = self._dedupe_payloads(provider_rows, ("provider_event_id",))
                placeholder_keys = sorted(
                    {
                        (str(row["symbol"]), row["event_date"], str(row["event_type"]))
                        for row in provider_rows
                        if row.get("symbol") and row.get("event_date") is not None and row.get("event_type")
                    }
                )
                if placeholder_keys:
                    session.execute(
                        delete(HistoricalEarningsEvent).where(
                            HistoricalEarningsEvent.provider_event_id.is_(None),
                            tuple_(
                                HistoricalEarningsEvent.symbol,
                                HistoricalEarningsEvent.event_date,
                                HistoricalEarningsEvent.event_type,
                            ).in_(placeholder_keys),
                        )
                    )
                bind = session.get_bind()
                if bind is not None and bind.dialect.name == "postgresql":
                    self._bulk_upsert_postgres(session, provider_rows, HistoricalEarningsEvent, ("provider_event_id",))
                else:
                    self._bulk_upsert_fallback(session, provider_rows, HistoricalEarningsEvent, ("provider_event_id",))
                stored += len(provider_rows)
            if legacy_rows:
                legacy_rows = self._dedupe_payloads(legacy_rows, ("symbol", "event_date", "event_type"))
                bind = session.get_bind()
                if bind is not None and bind.dialect.name == "postgresql":
                    self._bulk_upsert_postgres(session, legacy_rows, HistoricalEarningsEvent, ("symbol", "event_date", "event_type"))
                else:
                    self._bulk_upsert_fallback(session, legacy_rows, HistoricalEarningsEvent, ("symbol", "event_date", "event_type"))
                stored += len(legacy_rows)
            session.commit()
            return stored
        except Exception:
            with suppress(Exception):
                session.rollback()
            logger.warning("historical_store.bulk_upsert_earnings_failed", exc_info=True)
            raise
        finally:
            with suppress(Exception):
                session.close()

    def upsert_treasury_yield_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalTreasuryYield, ("trade_date",))

    def _bulk_upsert(self, rows: list[object], model: type[object], key_fields: tuple[str, ...]) -> int:
        if not rows:
            return 0
        payloads = [self._normalize_payload(self._row_payload(row, model), model) for row in rows]
        return self._bulk_upsert_payloads(payloads, model, key_fields)

    def _bulk_upsert_payloads(
        self,
        rows: list[dict[str, object]],
        model: type[object],
        key_fields: tuple[str, ...],
    ) -> int:
        if not rows:
            return 0
        rows = [self._normalize_payload(row, model) for row in rows]
        rows = self._dedupe_payloads(rows, key_fields)
        session = self._session(readonly=False)
        try:
            bind = session.get_bind()
            if bind is not None and bind.dialect.name == "postgresql":
                self._bulk_upsert_postgres(session, rows, model, key_fields)
            else:
                self._bulk_upsert_fallback(session, rows, model, key_fields)
            session.commit()
            if model in {HistoricalUnderlyingDayBar, HistoricalUnderlyingRawDayBar, HistoricalOptionDayBar}:
                self._invalidate_option_cache_state()
            return len(rows)
        except Exception:
            with suppress(Exception):
                session.rollback()
            logger.warning("historical_store.bulk_upsert_failed", model=model.__name__, exc_info=True)
            raise
        finally:
            with suppress(Exception):
                session.close()

    def _bulk_upsert_postgres(
        self,
        session: Session,
        rows: list[dict[str, object]],
        model: type[object],
        key_fields: tuple[str, ...],
    ) -> None:
        if self._can_use_postgres_copy_fast_path(session, model):
            self._bulk_upsert_postgres_copy(
                session,
                rows,
                model,
                key_fields,
            )
            return
        self._bulk_upsert_postgres_insert(session, rows, model, key_fields)

    def _bulk_upsert_postgres_insert(
        self,
        session: Session,
        rows: list[dict[str, object]],
        model: type[object],
        key_fields: tuple[str, ...],
    ) -> None:
        table = model.__table__
        batch_size = self._postgres_bulk_batch_size(table, rows)
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset:offset + batch_size]
            stmt = pg_insert(table).values(batch)
            update_columns = {
                column.name: stmt.excluded[column.name]
                for column in table.columns
                if column.name not in {"id", *key_fields}
            }
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[getattr(table.c, field) for field in key_fields],
                    set_=update_columns,
                )
            )

    def _bulk_upsert_postgres_copy(
        self,
        session: Session,
        rows: list[dict[str, object]],
        model: type[object],
        key_fields: tuple[str, ...],
    ) -> None:
        table = model.__table__
        columns = [column.name for column in table.columns if column.name in rows[0]]
        copy_rows = [tuple(row.get(column) for column in columns) for row in rows]
        self._bulk_upsert_postgres_copy_rows(session, model, key_fields, tuple(columns), copy_rows)

    def _bulk_upsert_postgres_copy_rows(
        self,
        session: Session,
        model: type[object],
        key_fields: tuple[str, ...],
        columns: tuple[str, ...],
        rows: list[tuple[object, ...]],
    ) -> None:
        table = model.__table__
        temp_table_name = f"tmp_{table.name}_{uuid4().hex}"
        column_sql = ", ".join(columns)
        key_sql = ", ".join(key_fields)
        update_columns = [column for column in columns if column not in {"id", *key_fields}]
        update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)

        session.execute(
            text(
                f"CREATE TEMP TABLE {temp_table_name} "
                f"(LIKE {table.name} INCLUDING DEFAULTS) ON COMMIT DROP"
            )
        )

        driver_connection = self._driver_connection(session)
        if driver_connection is None:
            raise RuntimeError("Postgres COPY fast path requires a psycopg driver connection.")

        with driver_connection.cursor() as cursor:
            with cursor.copy(f"COPY {temp_table_name} ({column_sql}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)

        session.execute(
            text(
                f"INSERT INTO {table.name} ({column_sql}) "
                f"SELECT {column_sql} FROM {temp_table_name} "
                f"ON CONFLICT ({key_sql}) DO UPDATE SET {update_sql}"
            )
        )

    def _bulk_upsert_fallback(
        self,
        session: Session,
        rows: list[dict[str, object]],
        model: type[object],
        key_fields: tuple[str, ...],
    ) -> None:
        for payload in rows:
            filters = [getattr(model, field) == payload[field] for field in key_fields]
            existing = session.scalar(select(model).where(*filters))
            if existing is None:
                session.add(model(**payload))
                continue
            for field, value in payload.items():
                if field == "id":
                    continue
                setattr(existing, field, value)

    @staticmethod
    def _dedupe_payloads(rows: list[dict[str, object]], key_fields: tuple[str, ...]) -> list[dict[str, object]]:
        if not rows:
            return rows
        deduped: dict[tuple[object, ...], dict[str, object]] = {}
        for row in rows:
            key = tuple(row[field] for field in key_fields)
            deduped[key] = row
        return list(deduped.values())

    @staticmethod
    def _row_payload(row: object, model: type[object]) -> dict[str, object]:
        mapper = sa_inspect(model)
        return {
            attr.key: getattr(row, attr.key)
            for attr in mapper.column_attrs
        }

    @staticmethod
    def _normalize_payload(payload: dict[str, object], model: type[object]) -> dict[str, object]:
        mapper = sa_inspect(model)
        normalized: dict[str, object] = {}
        for column in mapper.columns:
            key = column.key
            value = payload.get(key)
            if value is not None:
                normalized[key] = value
                continue
            if column.default is not None:
                default_value = HistoricalMarketDataStore._resolve_column_default(column.default.arg)
                if default_value is not None:
                    normalized[key] = default_value
                    continue
            if column.server_default is not None:
                continue
            if key in payload:
                normalized[key] = value
        return normalized

    @staticmethod
    def _resolve_column_default(default_arg: object) -> object | None:
        if callable(default_arg):
            try:
                return default_arg()
            except TypeError:
                return default_arg(None)
        return default_arg

    @staticmethod
    def _postgres_bulk_batch_size(table: object, rows: list[dict[str, object]]) -> int:
        if not rows:
            return 1
        sample = rows[0]
        bind_columns = max(1, sum(1 for column in table.columns if column.name in sample))
        return max(1, min(len(rows), _POSTGRES_MAX_BIND_PARAMS // bind_columns))

    @staticmethod
    def _dedupe_option_records(rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
        deduped: dict[tuple[object, object], tuple[object, ...]] = {}
        option_ticker_index = _OPTION_DAY_BAR_COPY_COLUMNS.index("option_ticker")
        trade_date_index = _OPTION_DAY_BAR_COPY_COLUMNS.index("trade_date")
        for row in rows:
            key = (row[option_ticker_index], row[trade_date_index])
            deduped[key] = row
        return list(deduped.values())

    @staticmethod
    def _driver_connection(session: Session):
        try:
            connection = session.connection()
            return getattr(connection.connection, "driver_connection", None)
        except Exception:
            return None

    @classmethod
    def _can_use_postgres_copy_fast_path(cls, session: Session, model: type[object]) -> bool:
        if model is not HistoricalOptionDayBar:
            return False
        driver_connection = cls._driver_connection(session)
        if driver_connection is None:
            return False
        try:
            with driver_connection.cursor() as cursor:
                return callable(getattr(cursor, "copy", None))
        except Exception:
            return False
