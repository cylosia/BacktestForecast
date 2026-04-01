from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Callable
from uuid import uuid4

import structlog
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
)
from backtestforecast.utils.dates import is_trading_day

logger = structlog.get_logger("market_data.historical_store")
_POSTGRES_MAX_BIND_PARAMS = 65_000
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


@dataclass(slots=True)
class HistoricalMarketDataStore:
    session_factory: Callable[[], Session]
    readonly_session_factory: Callable[[], Session] | None = None

    def _session(self, *, readonly: bool) -> Session:
        factory = self.readonly_session_factory if readonly and self.readonly_session_factory is not None else self.session_factory
        return factory()

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
        with self._session(readonly=True) as session:
            stmt = (
                select(
                    HistoricalOptionDayBar.option_ticker,
                    HistoricalOptionDayBar.contract_type,
                    HistoricalOptionDayBar.expiration_date,
                    HistoricalOptionDayBar.strike_price,
                )
                .where(
                    HistoricalOptionDayBar.underlying_symbol == symbol,
                    HistoricalOptionDayBar.trade_date == as_of_date,
                    HistoricalOptionDayBar.contract_type == contract_type,
                )
            )
            if expiration_date is not None:
                stmt = stmt.where(HistoricalOptionDayBar.expiration_date == expiration_date)
            if expiration_gte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.expiration_date >= expiration_gte)
            if expiration_lte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.expiration_date <= expiration_lte)
            if strike_price_gte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.strike_price >= Decimal(f"{strike_price_gte:.4f}"))
            if strike_price_lte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.strike_price <= Decimal(f"{strike_price_lte:.4f}"))
            rows = list(
                session.execute(
                    stmt.order_by(HistoricalOptionDayBar.expiration_date, HistoricalOptionDayBar.strike_price)
                )
            )
        seen: set[str] = set()
        contracts: list[OptionContractRecord] = []
        for option_ticker, row_contract_type, row_expiration_date, row_strike_price in rows:
            if option_ticker in seen:
                continue
            seen.add(option_ticker)
            contracts.append(
                OptionContractRecord(
                    ticker=option_ticker,
                    contract_type=row_contract_type,
                    expiration_date=row_expiration_date,
                    strike_price=float(row_strike_price),
                    shares_per_contract=100.0,
                )
            )
        return contracts

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
        with self._session(readonly=True) as session:
            stmt = (
                select(
                    HistoricalOptionDayBar.option_ticker,
                    HistoricalOptionDayBar.contract_type,
                    HistoricalOptionDayBar.expiration_date,
                    HistoricalOptionDayBar.strike_price,
                )
                .where(
                    HistoricalOptionDayBar.underlying_symbol == symbol,
                    HistoricalOptionDayBar.trade_date == as_of_date,
                    HistoricalOptionDayBar.contract_type == contract_type,
                    HistoricalOptionDayBar.expiration_date.in_(requested_expirations),
                )
            )
            if strike_price_gte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.strike_price >= Decimal(f"{strike_price_gte:.4f}"))
            if strike_price_lte is not None:
                stmt = stmt.where(HistoricalOptionDayBar.strike_price <= Decimal(f"{strike_price_lte:.4f}"))
            rows = list(
                session.execute(
                    stmt.order_by(HistoricalOptionDayBar.expiration_date, HistoricalOptionDayBar.strike_price)
                )
            )
        contracts_by_expiration: dict[date, list[OptionContractRecord]] = {
            expiration_date: [] for expiration_date in requested_expirations
        }
        seen_by_expiration: dict[date, set[str]] = {
            expiration_date: set() for expiration_date in requested_expirations
        }
        for option_ticker, row_contract_type, row_expiration_date, row_strike_price in rows:
            seen = seen_by_expiration.setdefault(row_expiration_date, set())
            if option_ticker in seen:
                continue
            seen.add(option_ticker)
            contracts_by_expiration.setdefault(row_expiration_date, []).append(
                OptionContractRecord(
                    ticker=option_ticker,
                    contract_type=row_contract_type,
                    expiration_date=row_expiration_date,
                    strike_price=float(row_strike_price),
                    shares_per_contract=100.0,
                )
            )
        return contracts_by_expiration

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        with self._session(readonly=True) as session:
            close_price = session.scalar(
                select(HistoricalOptionDayBar.close_price).where(
                    HistoricalOptionDayBar.option_ticker == option_ticker,
                    HistoricalOptionDayBar.trade_date == trade_date,
                )
            )
        if close_price is None:
            return None
        close_price = float(close_price)
        if close_price <= 0:
            return None
        return OptionQuoteRecord(
            trade_date=trade_date,
            bid_price=close_price,
            ask_price=close_price,
            participant_timestamp=None,
        )

    def get_option_quotes_for_date(
        self,
        option_tickers: list[str],
        trade_date: date,
    ) -> dict[str, OptionQuoteRecord | None]:
        if not option_tickers:
            return {}
        requested_tickers = tuple(dict.fromkeys(option_tickers))
        with self._session(readonly=True) as session:
            rows = list(
                session.execute(
                    select(
                        HistoricalOptionDayBar.option_ticker,
                        HistoricalOptionDayBar.close_price,
                    ).where(
                        HistoricalOptionDayBar.trade_date == trade_date,
                        HistoricalOptionDayBar.option_ticker.in_(requested_tickers),
                    )
                )
            )
        quotes: dict[str, OptionQuoteRecord | None] = {
            ticker: None for ticker in requested_tickers
        }
        for option_ticker, close_price in rows:
            if close_price is None:
                continue
            close_value = float(close_price)
            if close_value <= 0:
                continue
            quotes[option_ticker] = OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=close_value,
                ask_price=close_value,
                participant_timestamp=None,
            )
        return quotes

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
