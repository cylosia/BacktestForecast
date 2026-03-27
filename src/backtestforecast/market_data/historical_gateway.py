from __future__ import annotations

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

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        lower = entry_date.fromordinal(entry_date.toordinal() + max(1, target_dte - dte_tolerance_days))
        upper = entry_date.fromordinal(entry_date.toordinal() + target_dte + dte_tolerance_days)
        return self.store.list_option_contracts(
            symbol=self.symbol,
            as_of_date=entry_date,
            contract_type=contract_type,
            expiration_gte=lower,
            expiration_lte=upper,
        )

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
            contracts = self.store.list_option_contracts_for_expiration(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_price_gte,
                strike_price_lte=strike_price_lte,
            )
            if contracts:
                return contracts
        raise DataUnavailableError("No eligible option expirations were available in local historical data.")

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return self.store.get_option_quote_for_date(option_ticker, trade_date)

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
        if key in self._iv_cache:
            return True, self._iv_cache[key]
        return False, None

    def store_iv(self, key: tuple[str, date], value: float | None) -> None:
        self._iv_cache[key] = value
