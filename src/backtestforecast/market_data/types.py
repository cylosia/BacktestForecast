from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DailyBar:
    trade_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float


@dataclass(frozen=True, slots=True)
class OptionContractRecord:
    ticker: str
    contract_type: str
    expiration_date: date
    strike_price: float
    shares_per_contract: float


@dataclass(frozen=True, slots=True)
class OptionQuoteRecord:
    trade_date: date
    bid_price: float
    ask_price: float
    participant_timestamp: int | None

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0
