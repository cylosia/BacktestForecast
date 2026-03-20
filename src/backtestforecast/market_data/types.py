from __future__ import annotations

import math
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
    strike_price: float  # Precision note: float cannot represent all decimals exactly. Use Decimal(str(strike_price)) at financial computation boundaries.
    shares_per_contract: float


@dataclass(frozen=True, slots=True)
class OptionGreeks:
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None


@dataclass(frozen=True, slots=True)
class OptionQuoteRecord:
    trade_date: date
    bid_price: float
    ask_price: float
    participant_timestamp: int | None

    @property
    def mid_price(self) -> float | None:
        result = (self.bid_price + self.ask_price) / 2.0
        if not math.isfinite(result):
            return None
        return result


@dataclass(frozen=True, slots=True)
class OptionSnapshotRecord:
    """Real-time snapshot from the Massive /v3/snapshot/options endpoint.

    Only available for current-day data — the API has no historical parameter.
    """

    ticker: str
    underlying_ticker: str
    greeks: OptionGreeks | None = None
    implied_volatility: float | None = None
    break_even_price: float | None = None
    open_interest: int | None = None
    bid_price: float | None = None
    ask_price: float | None = None

    @property
    def mid_price(self) -> float | None:
        if self.bid_price is not None and self.ask_price is not None:
            result = (self.bid_price + self.ask_price) / 2.0
            if not math.isfinite(result):
                return None
            return result
        return None
