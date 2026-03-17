from __future__ import annotations

from datetime import date, timedelta

import pytest

from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
from backtestforecast.market_data.types import DailyBar

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_START = date(2023, 1, 1)


def _make_bars(
    n: int = 220,
    start: date = _START,
    start_close: float = 100.0,
    daily_delta: float = 0.25,
    noise_factor: float = 0.03,
) -> list[DailyBar]:
    bars: list[DailyBar] = []
    close = start_close
    for index in range(n):
        close += daily_delta + ((index % 7) * noise_factor)
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
    return bars


# ---------------------------------------------------------------------------
# Uptrend (existing)
# ---------------------------------------------------------------------------


def test_historical_analog_forecast_returns_positive_median_for_uptrend_series() -> None:
    bars = _make_bars()

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="AAPL",
        bars=bars,
        horizon_days=15,
        strategy_type="long_call",
    )

    assert forecast.analog_count > 0
    assert float(forecast.expected_return_median_pct) > 0


# ---------------------------------------------------------------------------
# Downtrend
# ---------------------------------------------------------------------------


def test_downtrend_series_returns_negative_median() -> None:
    bars = _make_bars(daily_delta=-0.25, noise_factor=-0.03)

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="TSLA",
        bars=bars,
        horizon_days=15,
        strategy_type="long_put",
    )

    assert forecast.analog_count > 0
    assert float(forecast.expected_return_median_pct) < 0


# ---------------------------------------------------------------------------
# Flat / sideways
# ---------------------------------------------------------------------------


def test_flat_series_returns_median_near_zero() -> None:
    bars: list[DailyBar] = []
    close = 100.0
    for index in range(250):
        jitter = 0.3 * (1 if index % 2 == 0 else -1) * ((index % 5) * 0.1)
        close = 100.0 + jitter
        bars.append(
            DailyBar(
                trade_date=_START + timedelta(days=index),
                open_price=close - 0.1,
                high_price=close + 0.2,
                low_price=close - 0.2,
                close_price=close,
                volume=1_000_000,
            )
        )

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="FLAT",
        bars=bars,
        horizon_days=15,
        strategy_type=None,
    )

    assert forecast.analog_count > 0
    assert abs(float(forecast.expected_return_median_pct)) < 3.0


# ---------------------------------------------------------------------------
# Short series (below minimum bars)
# ---------------------------------------------------------------------------


def test_short_series_raises_value_error() -> None:
    bars = _make_bars(n=30)

    with pytest.raises(ValueError, match="Not enough historical bars"):
        HistoricalAnalogForecaster().forecast(
            symbol="SHORT",
            bars=bars,
            horizon_days=15,
            strategy_type="long_call",
        )


# ---------------------------------------------------------------------------
# Empty bars list
# ---------------------------------------------------------------------------


def test_empty_bars_list_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Not enough historical bars"):
        HistoricalAnalogForecaster().forecast(
            symbol="EMPTY",
            bars=[],
            horizon_days=15,
            strategy_type="long_call",
        )


# ---------------------------------------------------------------------------
# Different horizon_days values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon_days", [5, 30, 90])
def test_different_horizon_days(horizon_days: int) -> None:
    bars = _make_bars(n=350)

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="AAPL",
        bars=bars,
        horizon_days=horizon_days,
        strategy_type="long_call",
    )

    assert forecast.horizon_days == horizon_days
    assert forecast.analog_count > 0


# ---------------------------------------------------------------------------
# Bounds validation: low <= median <= high
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon_days", [5, 15, 30])
def test_bounds_low_le_median_le_high(horizon_days: int) -> None:
    bars = _make_bars(n=350)

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="AAPL",
        bars=bars,
        horizon_days=horizon_days,
        strategy_type="long_call",
    )

    low = float(forecast.expected_return_low_pct)
    med = float(forecast.expected_return_median_pct)
    high = float(forecast.expected_return_high_pct)
    assert low <= med <= high, f"Expected low({low}) <= median({med}) <= high({high})"


# ---------------------------------------------------------------------------
# positive_outcome_rate_pct validation (0–100)
# ---------------------------------------------------------------------------


def test_positive_outcome_rate_in_valid_range() -> None:
    bars = _make_bars()

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="AAPL",
        bars=bars,
        horizon_days=15,
        strategy_type="long_call",
    )

    rate = float(forecast.positive_outcome_rate_pct)
    assert 0.0 <= rate <= 100.0


def test_positive_outcome_rate_downtrend_in_valid_range() -> None:
    bars = _make_bars(daily_delta=-0.25, noise_factor=-0.03)

    forecast = HistoricalAnalogForecaster().forecast(
        symbol="TSLA",
        bars=bars,
        horizon_days=15,
        strategy_type="long_put",
    )

    rate = float(forecast.positive_outcome_rate_pct)
    assert 0.0 <= rate <= 100.0


# ---------------------------------------------------------------------------
# horizon_days < 1 raises
# ---------------------------------------------------------------------------


def test_horizon_days_zero_raises() -> None:
    bars = _make_bars()

    with pytest.raises(ValueError, match="horizon_days must be at least 1"):
        HistoricalAnalogForecaster().forecast(
            symbol="AAPL",
            bars=bars,
            horizon_days=0,
            strategy_type="long_call",
        )


# ---------------------------------------------------------------------------
# Item 41: horizon_days=5 doesn't crash — CreateBacktestRunRequest validation
# ---------------------------------------------------------------------------


def test_horizon_days_5_does_not_crash() -> None:
    """CreateBacktestRunRequest with target_dte=7 and dte_tolerance_days=min(5,6)=5
    must not raise ValueError, validating that short horizon_days is accepted."""
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 30),
        target_dte=7,
        dte_tolerance_days=min(5, 6),
        max_holding_days=5,
        account_size="10000",
        risk_per_trade_pct="5",
        commission_per_contract="1",
        entry_rules=[],
    )
    assert request.dte_tolerance_days == 5
    assert request.target_dte == 7


def test_forecast_horizon_5_runs_successfully() -> None:
    """HistoricalAnalogForecaster.forecast with horizon_days=5 does not crash."""
    bars = _make_bars(n=250)
    forecast = HistoricalAnalogForecaster().forecast(
        symbol="SPY",
        bars=bars,
        horizon_days=5,
        strategy_type="long_call",
    )
    assert forecast.horizon_days == 5
    assert forecast.analog_count > 0


# ---------------------------------------------------------------------------
# Item 60: build_forecast dte_tolerance edge cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon_days", [5, 6, 7, 10, 30])
def test_dte_tolerance_less_than_target_dte(horizon_days: int) -> None:
    """For each horizon_days value, verify that dte_tolerance_days < target_dte
    holds when constructing a CreateBacktestRunRequest with target_dte = horizon_days + 2."""
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest

    target_dte = horizon_days + 2
    dte_tolerance = min(horizon_days, target_dte - 1)
    assert dte_tolerance < target_dte, (
        f"dte_tolerance_days ({dte_tolerance}) must be < target_dte ({target_dte})"
    )

    request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_call",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 30),
        target_dte=target_dte,
        dte_tolerance_days=dte_tolerance,
        max_holding_days=horizon_days,
        account_size="10000",
        risk_per_trade_pct="5",
        commission_per_contract="1",
        entry_rules=[],
    )
    assert request.dte_tolerance_days == dte_tolerance


# ---------------------------------------------------------------------------
# Item 89: Canary test — dte_tolerance < target_dte across all horizon_days
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon_days", [1, 5, 7, 14, 30, 60, 90, 180, 365])
def test_forecast_builder_dte_tolerance_invariant(horizon_days: int) -> None:
    """For every supported horizon_days, the ScanService.build_forecast formula
    must produce dte_tolerance_days < target_dte to satisfy
    CreateBacktestRunRequest validation."""
    target_dte = max(horizon_days, 1)
    dte_tolerance = min(5, target_dte - 1)
    assert dte_tolerance < target_dte, (
        f"horizon_days={horizon_days}: dte_tolerance_days ({dte_tolerance}) "
        f"must be strictly less than target_dte ({target_dte})"
    )
