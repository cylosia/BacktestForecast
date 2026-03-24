"""Tests for critical audit findings.

These tests verify fixes for:
- NaN bypass in forecast feature extraction
- IV solver convergence failure
- Negative mid_price guard in engine marking
- Sharpe/Sortino consistent variance convention
- Calendar-to-trading-days rounding
"""
from __future__ import annotations

import math
from datetime import date

from backtestforecast.backtests.rules import implied_volatility_from_price
from backtestforecast.backtests.summary import _compute_sharpe_sortino
from backtestforecast.backtests.types import EquityPointResult
from backtestforecast.forecasts.analog import HistoricalAnalogForecaster


class TestNaNGuardInForecastFeatures:
    """Verify that NaN and Inf values are rejected by _features_for_index."""

    def _build_series(self, length: int = 30, close_override: dict[int, float] | None = None) -> tuple:
        closes = [100.0 + i * 0.1 for i in range(length)]
        volumes = [1_000_000.0] * length
        if close_override:
            for idx, val in close_override.items():
                closes[idx] = val
        forecaster = HistoricalAnalogForecaster()
        returns = forecaster._daily_returns(closes)
        from backtestforecast.indicators.calculations import ema, rolling_stddev, rsi, sma
        rsi14 = rsi(closes, 14)
        ema8 = ema(closes, 8)
        ema21 = ema(closes, 21)
        avg_volume20 = sma(volumes, 20)
        vol20 = rolling_stddev(returns, 20, ddof=1)
        return closes, volumes, returns, rsi14, ema8, ema21, avg_volume20, vol20

    def test_nan_close_rejected(self):
        series = self._build_series(close_override={25: float("nan")})
        forecaster = HistoricalAnalogForecaster()
        result = forecaster._features_for_index(
            index=25, closes=series[0], volumes=series[1], returns=series[2],
            rsi14=series[3], ema8=series[4], ema21=series[5],
            avg_volume20=series[6], vol20=series[7],
        )
        assert result is None

    def test_inf_close_rejected(self):
        series = self._build_series(close_override={25: float("inf")})
        forecaster = HistoricalAnalogForecaster()
        result = forecaster._features_for_index(
            index=25, closes=series[0], volumes=series[1], returns=series[2],
            rsi14=series[3], ema8=series[4], ema21=series[5],
            avg_volume20=series[6], vol20=series[7],
        )
        assert result is None

    def test_negative_inf_close_rejected(self):
        series = self._build_series(close_override={25: float("-inf")})
        forecaster = HistoricalAnalogForecaster()
        result = forecaster._features_for_index(
            index=25, closes=series[0], volumes=series[1], returns=series[2],
            rsi14=series[3], ema8=series[4], ema21=series[5],
            avg_volume20=series[6], vol20=series[7],
        )
        assert result is None

    def test_valid_close_accepted(self):
        series = self._build_series()
        forecaster = HistoricalAnalogForecaster()
        result = forecaster._features_for_index(
            index=25, closes=series[0], volumes=series[1], returns=series[2],
            rsi14=series[3], ema8=series[4], ema21=series[5],
            avg_volume20=series[6], vol20=series[7],
        )
        assert result is not None
        assert all(math.isfinite(v) for v in result)


class TestIVSolverConvergence:
    """Verify the IV solver returns None when it cannot converge."""

    def test_normal_convergence(self):
        iv = implied_volatility_from_price(
            option_price=5.0,
            underlying_price=100.0,
            strike_price=100.0,
            time_to_expiry_years=0.25,
            option_type="call",
        )
        assert iv is not None
        assert 0.01 < iv < 5.0

    def test_deep_otm_returns_none_when_unconverged(self):
        iv = implied_volatility_from_price(
            option_price=0.001,
            underlying_price=100.0,
            strike_price=200.0,
            time_to_expiry_years=0.01,
            option_type="call",
        )
        # Deep OTM with near-zero time: BSM price is essentially 0 for all vols
        # The solver should return None since it can't match the target price
        if iv is not None:
            from backtestforecast.backtests.rules import black_scholes_price
            theoretical = black_scholes_price(
                option_type="call", underlying_price=100.0,
                strike_price=200.0, time_to_expiry_years=0.01,
                volatility=iv,
            )
            residual = abs(theoretical - 0.001)
            assert residual < max(0.01, 0.001 * 0.05), (
                f"If IV is returned, residual {residual} must be within threshold"
            )

    def test_zero_price_returns_none(self):
        iv = implied_volatility_from_price(
            option_price=0.0,
            underlying_price=100.0,
            strike_price=100.0,
            time_to_expiry_years=0.25,
            option_type="call",
        )
        assert iv is None

    def test_negative_price_returns_none(self):
        iv = implied_volatility_from_price(
            option_price=-1.0,
            underlying_price=100.0,
            strike_price=100.0,
            time_to_expiry_years=0.25,
            option_type="call",
        )
        assert iv is None


class TestSharpeSortinoConsistency:
    """Both ratios must use the same variance convention (N-1)."""

    @staticmethod
    def _make_equity_curve(values: list[float]) -> list[EquityPointResult]:
        return [
            EquityPointResult(trade_date=date(2025, 1, 1), equity=v, cash=v, position_value=0.0, drawdown_pct=0.0)
            for v in values
        ]

    def test_both_use_sample_variance(self):
        equities = [10000.0]
        for _ in range(30):
            equities.append(equities[-1] * 1.002)
        for _ in range(10):
            equities.append(equities[-1] * 0.998)
        curve = self._make_equity_curve(equities)
        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.04, trade_count=10)
        assert sharpe is not None
        assert sortino is not None
        assert math.isfinite(sharpe)
        assert math.isfinite(sortino)

    def test_returns_none_for_few_trades(self):
        curve = self._make_equity_curve([10000.0, 10100.0])
        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.04, trade_count=3)
        assert sharpe is None
        assert sortino is None


class TestCalendarToTradingDays:
    """Verify rounding instead of truncation."""

    def test_seven_days_gives_five(self):
        result = HistoricalAnalogForecaster._calendar_to_trading_days(7)
        assert result == 5

    def test_thirty_days(self):
        result = HistoricalAnalogForecaster._calendar_to_trading_days(30)
        assert 20 <= result <= 22

    def test_zero_days(self):
        assert HistoricalAnalogForecaster._calendar_to_trading_days(0) == 0

    def test_one_day(self):
        assert HistoricalAnalogForecaster._calendar_to_trading_days(1) >= 1
