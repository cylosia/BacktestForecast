from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse


class ForecastEnvelopeResponse(BaseModel):
    forecast: HistoricalAnalogForecastResponse
    probabilistic_note: str = Field(
        default=(
            "This range is probabilistic, derived from historical analog setups, "
            "and is not financial advice or a certainty of future results."
        )
    )
    expected_move_abs_pct: Decimal = Field(ge=Decimal("0"), le=Decimal("500"))
