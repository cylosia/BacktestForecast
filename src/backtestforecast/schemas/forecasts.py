from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from backtestforecast.schemas.backtests import StrategyType
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse


class ForecastRequestParams(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    strategy_type: StrategyType | None = None
    horizon_days: int = Field(default=20, ge=5, le=90)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()


class ForecastEnvelopeResponse(BaseModel):
    forecast: HistoricalAnalogForecastResponse
    probabilistic_note: str = Field(
        default=(
            "This range is probabilistic, derived from historical analog setups, "
            "and is not financial advice or a certainty of future results."
        )
    )
    expected_move_abs_pct: Decimal
