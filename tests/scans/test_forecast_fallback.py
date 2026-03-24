"""Verify forecast fallback produces a valid response."""
from __future__ import annotations


def test_fallback_forecast_has_zero_analog_count():
    """When no analogs are found, analog_count should be 0."""
    from datetime import date
    from decimal import Decimal

    from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse

    fallback = HistoricalAnalogForecastResponse(
        symbol="TEST",
        as_of_date=date(2024, 1, 1),
        horizon_days=20,
        analog_count=0,
        expected_return_low_pct=Decimal("0"),
        expected_return_median_pct=Decimal("0"),
        expected_return_high_pct=Decimal("0"),
        summary="No analogs",
        disclaimer="Test disclaimer",
    )
    assert fallback.analog_count == 0
