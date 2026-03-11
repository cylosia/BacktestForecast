from __future__ import annotations

from datetime import date, timedelta

from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
from backtestforecast.market_data.types import DailyBar


def test_historical_analog_forecast_returns_positive_median_for_uptrend_series() -> None:
    start = date(2023, 1, 1)
    bars: list[DailyBar] = []
    close = 100.0
    for index in range(220):
        close += 0.25 + ((index % 7) * 0.03)
        bars.append(
            DailyBar(
                trade_date=start + timedelta(days=index),
                open_price=close - 0.2,
                high_price=close + 0.5,
                low_price=close - 0.5,
                close_price=close,
                volume=1_000_000 + ((index % 10) * 25_000),
            )
        )

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="AAPL",
        bars=bars,
        horizon_days=15,
        strategy_type="long_call",
    )

    assert forecast.analog_count > 0
    assert float(forecast.expected_return_median_pct) > 0
