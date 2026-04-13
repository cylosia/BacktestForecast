from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Callable

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.market_data.types import OptionContractRecord
from backtestforecast.models import HistoricalOptionContractCatalogSnapshot, OptionContractCatalogSnapshot

logger = structlog.get_logger("market_data.contract_catalog")

_DB_FAILURE_COOLDOWN_SECONDS = 60.0


def _normalize_bound(value: float | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(f"{value:.4f}")


def _serialize_contracts(contracts: list[OptionContractRecord]) -> list[dict[str, object]]:
    return [
        {
            "ticker": contract.ticker,
            "contract_type": contract.contract_type,
            "expiration_date": contract.expiration_date.isoformat(),
            "strike_price": contract.strike_price,
            "shares_per_contract": contract.shares_per_contract,
            "underlying_symbol": contract.underlying_symbol,
            "as_of_mid_price": contract.as_of_mid_price,
        }
        for contract in contracts
    ]


def _deserialize_contracts(rows: list[dict[str, object]]) -> list[OptionContractRecord]:
    contracts: list[OptionContractRecord] = []
    for row in rows:
        ticker = row.get("ticker")
        contract_type = row.get("contract_type")
        expiration_date = row.get("expiration_date")
        strike_price = row.get("strike_price")
        shares_per_contract = row.get("shares_per_contract", 100.0)
        underlying_symbol = row.get("underlying_symbol")
        if not isinstance(ticker, str) or not isinstance(contract_type, str) or not isinstance(expiration_date, str):
            continue
        if strike_price is None:
            continue
        try:
            contracts.append(
                OptionContractRecord(
                    ticker=ticker,
                    contract_type=contract_type,
                    expiration_date=date.fromisoformat(expiration_date),
                    strike_price=float(strike_price),
                    shares_per_contract=float(shares_per_contract),
                    underlying_symbol=str(underlying_symbol) if isinstance(underlying_symbol, str) else None,
                    as_of_mid_price=(
                        float(row["as_of_mid_price"])
                        if row.get("as_of_mid_price") is not None
                        else None
                    ),
                )
            )
        except (TypeError, ValueError):
            logger.debug("contract_catalog.deserialize_skipped", row=row)
        continue
    return contracts


def _filter_contracts(
    contracts: list[OptionContractRecord],
    *,
    strike_price_gte: float | None,
    strike_price_lte: float | None,
) -> list[OptionContractRecord]:
    result = contracts
    if strike_price_gte is not None:
        result = [contract for contract in result if contract.strike_price >= strike_price_gte]
    if strike_price_lte is not None:
        result = [contract for contract in result if contract.strike_price <= strike_price_lte]
    return result


@dataclass(slots=True)
class OptionContractCatalogStore:
    session_factory: Callable[[], Session]
    readonly_session_factory: Callable[[], Session] | None = None
    snapshot_model: type[OptionContractCatalogSnapshot] | type[HistoricalOptionContractCatalogSnapshot] = OptionContractCatalogSnapshot
    _disabled_until_monotonic: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self._disabled_until_monotonic = 0.0

    def get_contracts(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord] | None:
        if self._is_temporarily_disabled():
            return None

        strike_floor = _normalize_bound(strike_price_gte)
        strike_ceiling = _normalize_bound(strike_price_lte)
        with self._session(readonly=True) as session:
            if session is None:
                return None
            try:
                snapshot = session.scalar(
                    select(self.snapshot_model).where(
                        self.snapshot_model.symbol == symbol,
                        self.snapshot_model.as_of_date == as_of_date,
                        self.snapshot_model.contract_type == contract_type,
                        self.snapshot_model.expiration_date == expiration_date,
                        self.snapshot_model.strike_price_gte == strike_floor,
                        self.snapshot_model.strike_price_lte == strike_ceiling,
                    )
                )
                if snapshot is not None:
                    return _deserialize_contracts(snapshot.contracts_json)
                if strike_floor is None and strike_ceiling is None:
                    return None

                full_snapshot = session.scalar(
                    select(self.snapshot_model).where(
                        self.snapshot_model.symbol == symbol,
                        self.snapshot_model.as_of_date == as_of_date,
                        self.snapshot_model.contract_type == contract_type,
                        self.snapshot_model.expiration_date == expiration_date,
                        self.snapshot_model.strike_price_gte.is_(None),
                        self.snapshot_model.strike_price_lte.is_(None),
                    )
                )
                if full_snapshot is None:
                    return None
                return _filter_contracts(
                    _deserialize_contracts(full_snapshot.contracts_json),
                    strike_price_gte=strike_price_gte,
                    strike_price_lte=strike_price_lte,
                )
            except Exception:
                self._trip_cooldown()
                logger.warning(
                    "contract_catalog.read_failed",
                    symbol=symbol,
                    as_of_date=as_of_date.isoformat(),
                    contract_type=contract_type,
                    expiration_date=expiration_date.isoformat(),
                    exc_info=True,
                )
                return None

    def upsert_contracts(
        self,
        *,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None,
        strike_price_lte: float | None,
        contracts: list[OptionContractRecord],
    ) -> None:
        if self._is_temporarily_disabled():
            return

        strike_floor = _normalize_bound(strike_price_gte)
        strike_ceiling = _normalize_bound(strike_price_lte)
        with self._session(readonly=False) as session:
            if session is None:
                return
            try:
                snapshot = session.scalar(
                    select(self.snapshot_model).where(
                        self.snapshot_model.symbol == symbol,
                        self.snapshot_model.as_of_date == as_of_date,
                        self.snapshot_model.contract_type == contract_type,
                        self.snapshot_model.expiration_date == expiration_date,
                        self.snapshot_model.strike_price_gte == strike_floor,
                        self.snapshot_model.strike_price_lte == strike_ceiling,
                    )
                )
                serialized = _serialize_contracts(contracts)
                if snapshot is None:
                    session.add(
                        self.snapshot_model(
                            symbol=symbol,
                            as_of_date=as_of_date,
                            contract_type=contract_type,
                            expiration_date=expiration_date,
                            strike_price_gte=strike_floor,
                            strike_price_lte=strike_ceiling,
                            contracts_json=serialized,
                            contract_count=len(serialized),
                        )
                    )
                else:
                    snapshot.contracts_json = serialized
                    snapshot.contract_count = len(serialized)
                session.commit()
            except Exception:
                with suppress(Exception):
                    session.rollback()
                self._trip_cooldown()
                logger.warning(
                    "contract_catalog.write_failed",
                    symbol=symbol,
                    as_of_date=as_of_date.isoformat(),
                    contract_type=contract_type,
                    expiration_date=expiration_date.isoformat(),
                    exc_info=True,
                )

    def _session(self, *, readonly: bool) -> _SessionContext:
        factory = self.readonly_session_factory if readonly and self.readonly_session_factory is not None else self.session_factory
        return _SessionContext(factory)

    def _is_temporarily_disabled(self) -> bool:
        import time
        return time.monotonic() < self._disabled_until_monotonic

    def _trip_cooldown(self) -> None:
        import time
        self._disabled_until_monotonic = time.monotonic() + _DB_FAILURE_COOLDOWN_SECONDS


class _SessionContext:
    def __init__(self, factory: Callable[[], Session]) -> None:
        self._factory = factory
        self._session: Session | None = None

    def __enter__(self) -> Session | None:
        self._session = self._factory()
        return self._session

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            with suppress(Exception):
                self._session.close()
