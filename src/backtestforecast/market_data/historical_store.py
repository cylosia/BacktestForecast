from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Callable

import structlog
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.models import (
    HistoricalExDividendDate,
    HistoricalOptionDayBar,
    HistoricalTreasuryYield,
    HistoricalUnderlyingDayBar,
)
from backtestforecast.utils.dates import is_trading_day

logger = structlog.get_logger("market_data.historical_store")
_POSTGRES_MAX_BIND_PARAMS = 65_000


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
                session.scalars(
                    select(HistoricalUnderlyingDayBar)
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
                trade_date=row.trade_date,
                open_price=float(row.open_price),
                high_price=float(row.high_price),
                low_price=float(row.low_price),
                close_price=float(row.close_price),
                volume=float(row.volume),
            )
            for row in rows
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
                select(HistoricalOptionDayBar)
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
            rows = list(session.scalars(stmt.order_by(HistoricalOptionDayBar.expiration_date, HistoricalOptionDayBar.strike_price)))
        seen: set[str] = set()
        contracts: list[OptionContractRecord] = []
        for row in rows:
            if row.option_ticker in seen:
                continue
            seen.add(row.option_ticker)
            contracts.append(
                OptionContractRecord(
                    ticker=row.option_ticker,
                    contract_type=row.contract_type,
                    expiration_date=row.expiration_date,
                    strike_price=float(row.strike_price),
                    shares_per_contract=100.0,
                )
            )
        return contracts

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        with self._session(readonly=True) as session:
            row = session.scalar(
                select(HistoricalOptionDayBar).where(
                    HistoricalOptionDayBar.option_ticker == option_ticker,
                    HistoricalOptionDayBar.trade_date == trade_date,
                )
            )
        if row is None:
            return None
        close_price = float(row.close_price)
        if close_price <= 0:
            return None
        return OptionQuoteRecord(
            trade_date=trade_date,
            bid_price=close_price,
            ask_price=close_price,
            participant_timestamp=None,
        )

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        with self._session(readonly=True) as session:
            rows = list(
                session.scalars(
                    select(HistoricalExDividendDate.ex_dividend_date).where(
                        HistoricalExDividendDate.symbol == symbol,
                        HistoricalExDividendDate.ex_dividend_date >= start_date,
                        HistoricalExDividendDate.ex_dividend_date <= end_date,
                    )
                )
            )
        return set(rows)

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
                session.scalars(
                    select(HistoricalTreasuryYield)
                    .where(
                        HistoricalTreasuryYield.trade_date >= start_date,
                        HistoricalTreasuryYield.trade_date <= end_date,
                    )
                    .order_by(HistoricalTreasuryYield.trade_date)
                )
            )
        return {row.trade_date: float(row.yield_3_month) for row in rows}

    def upsert_underlying_day_bars(self, bars: list[HistoricalUnderlyingDayBar]) -> int:
        return self._bulk_upsert(bars, HistoricalUnderlyingDayBar, ("symbol", "trade_date"))

    def upsert_option_day_bars(self, bars: list[HistoricalOptionDayBar]) -> int:
        return self._bulk_upsert(bars, HistoricalOptionDayBar, ("option_ticker", "trade_date"))

    def upsert_ex_dividend_dates(self, rows: list[HistoricalExDividendDate]) -> int:
        return self._bulk_upsert(rows, HistoricalExDividendDate, ("symbol", "ex_dividend_date"))

    def upsert_treasury_yields(self, rows: list[HistoricalTreasuryYield]) -> int:
        return self._bulk_upsert(rows, HistoricalTreasuryYield, ("trade_date",))

    def upsert_underlying_day_bar_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalUnderlyingDayBar, ("symbol", "trade_date"))

    def upsert_option_day_bar_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalOptionDayBar, ("option_ticker", "trade_date"))

    def upsert_ex_dividend_payloads(self, rows: list[dict[str, object]]) -> int:
        return self._bulk_upsert_payloads(rows, HistoricalExDividendDate, ("symbol", "ex_dividend_date"))

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
