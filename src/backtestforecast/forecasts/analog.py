from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from statistics import median

from backtestforecast.backtests.strategies.registry import BEARISH_STRATEGIES, BULLISH_STRATEGIES
from backtestforecast.indicators.calculations import ema, rolling_stddev, rsi, sma
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse


@dataclass(frozen=True, slots=True)
class ForecastCandidate:
    trade_date: date
    features: tuple[float, ...]
    forward_return_pct: float


class HistoricalAnalogForecaster:
    def forecast(
        self,
        *,
        symbol: str,
        bars: list[DailyBar],
        horizon_days: int,
        strategy_type: str | None = None,
        max_analogs: int = 20,
    ) -> HistoricalAnalogForecastResponse:
        if max_analogs <= 0:
            raise ValueError("max_analogs must be positive")
        if horizon_days < 1:
            raise ValueError("horizon_days must be at least 1.")
        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        calendar_horizon = horizon_days
        trading_horizon = self._calendar_to_trading_days(horizon_days)
        horizon_days = trading_horizon
        if len(sorted_bars) < max(80, horizon_days + 40):
            raise ValueError("Not enough historical bars were available to build a forecast.")

        closes = [bar.close_price for bar in sorted_bars]
        volumes = [bar.volume for bar in sorted_bars]
        daily_returns = self._daily_returns(closes)
        rsi14 = rsi(closes, 14)
        ema8 = ema(closes, 8)
        ema21 = ema(closes, 21)
        avg_volume20 = sma(volumes, 20)
        vol20 = rolling_stddev(daily_returns, 20, ddof=1)

        as_of_index = len(sorted_bars) - 1
        current_features = self._features_for_index(
            index=as_of_index,
            closes=closes,
            volumes=volumes,
            returns=daily_returns,
            rsi14=rsi14,
            ema8=ema8,
            ema21=ema21,
            avg_volume20=avg_volume20,
            vol20=vol20,
        )
        if current_features is None:
            raise ValueError("Not enough history was available to compute current forecast features.")

        candidates: list[ForecastCandidate] = []
        min_index = 30
        max_index = len(sorted_bars) - horizon_days
        for index in range(min_index, max_index):
            candidate_features = self._features_for_index(
                index=index,
                closes=closes,
                volumes=volumes,
                returns=daily_returns,
                rsi14=rsi14,
                ema8=ema8,
                ema21=ema21,
                avg_volume20=avg_volume20,
                vol20=vol20,
            )
            if candidate_features is None:
                continue
            start_close = closes[index]
            end_close = closes[index + horizon_days]
            if start_close <= 0:
                continue
            forward_return_pct = ((end_close - start_close) / start_close) * 100.0
            candidates.append(
                ForecastCandidate(
                    trade_date=sorted_bars[index].trade_date,
                    features=candidate_features,
                    forward_return_pct=forward_return_pct,
                )
            )

        if len(candidates) < 15:
            raise ValueError("Not enough comparable historical analogs were available.")

        ranked = sorted(
            candidates,
            key=lambda candidate: (
                self._distance(current_features, candidate.features),
                candidate.trade_date,
            ),
        )
        min_spacing = max(calendar_horizon // 2, 5)
        analogs: list[ForecastCandidate] = []
        for candidate in ranked:
            if any(abs((candidate.trade_date - s.trade_date).days) < min_spacing for s in analogs):
                continue
            analogs.append(candidate)
            if len(analogs) >= max_analogs:
                break
        if not analogs:
            analogs = ranked[: min(max_analogs, len(ranked))]
        returns = sorted(candidate.forward_return_pct for candidate in analogs)
        positive_rate = (sum(1 for value in returns if value > 0) / len(returns)) * 100.0
        low = self._percentile(returns, 0.25)
        med = median(returns)
        high = self._percentile(returns, 0.75)
        summary = self._build_summary(strategy_type, med, low, high, positive_rate, calendar_horizon)

        return HistoricalAnalogForecastResponse(
            symbol=symbol,
            strategy_type=strategy_type,
            as_of_date=sorted_bars[as_of_index].trade_date,
            horizon_days=calendar_horizon,
            trading_days_used=trading_horizon,
            analog_count=len(analogs),
            analogs_used=len(analogs),
            expected_return_low_pct=self._to_decimal(low),
            expected_return_median_pct=self._to_decimal(med),
            expected_return_high_pct=self._to_decimal(high),
            positive_outcome_rate_pct=self._to_decimal(positive_rate),
            summary=summary,
            disclaimer=(
                "This is a bounded probability range based on historical analogs under similar daily-bar conditions. "
                "It is not a prediction, certainty, or financial advice."
            ),
            analog_dates=[candidate.trade_date for candidate in analogs[:5]],
            analog_dates_shown=min(len(analogs), 5),
            analog_dates_total=len(analogs),
        )

    @staticmethod
    def _daily_returns(closes: list[float]) -> list[float]:
        """Compute daily returns as percentages (2.0 = 2% gain).

        Note: pipeline/regime.py uses raw decimal format (0.02 = 2% gain).
        These conventions are independent and should not be mixed.
        """
        returns: list[float] = [0.0]
        for index in range(1, len(closes)):
            prior = closes[index - 1]
            current = closes[index]
            returns.append(0.0 if prior <= 0 else ((current - prior) / prior) * 100.0)
        return returns

    def _features_for_index(
        self,
        *,
        index: int,
        closes: list[float],
        volumes: list[float],
        returns: list[float],
        rsi14: list[float | None],
        ema8: list[float | None],
        ema21: list[float | None],
        avg_volume20: list[float | None],
        vol20: list[float | None],
    ) -> tuple[float, ...] | None:
        if index < 21:
            return None
        close = closes[index]
        five_day_base = closes[index - 5]
        twenty_day_base = closes[index - 20]
        current_rsi = rsi14[index]
        current_ema8 = ema8[index]
        current_ema21 = ema21[index]
        current_avg_volume20 = avg_volume20[index]
        current_vol20 = vol20[index]

        numeric_inputs = (close, five_day_base, twenty_day_base)
        if any(not math.isfinite(v) or v <= 0 for v in numeric_inputs):
            return None
        optional_inputs = (current_rsi, current_ema8, current_ema21, current_avg_volume20, current_vol20)
        if any(v is None for v in optional_inputs):
            return None
        if not math.isfinite(current_avg_volume20) or current_avg_volume20 <= 0:
            return None
        if not all(math.isfinite(v) for v in optional_inputs):  # type: ignore[arg-type]
            return None

        return (
            ((close - five_day_base) / five_day_base) * 100.0,
            ((close - twenty_day_base) / twenty_day_base) * 100.0,
            current_rsi,
            ((current_ema8 - current_ema21) / close) * 100.0,
            (volumes[index] / current_avg_volume20),
            current_vol20 * math.sqrt(252.0),
            returns[index],
        )

    @staticmethod
    def _distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
        # Scale denominators normalise each feature into a comparable range so
        # that raw magnitude differences (e.g. RSI 0-100 vs volume ratio ~1)
        # don't dominate.  The values were calibrated empirically on US large-
        # cap daily bars:
        #   0: 5-day return %  (scale  8)   – typical swing ~±8%
        #   1: 20-day return % (scale 15)   – wider window, larger moves
        #   2: RSI-14          (scale 20)   – 0-100 range, 20 keeps it moderate
        #   3: EMA8/EMA21 gap% (scale  5)   – usually within a few percent
        #   4: Volume ratio    (scale 1.5)  – centered around 1.0
        #   5: Annualised vol  (scale 25)   – typically 10-40%
        #   6: 1-day return %  (scale  4)   – single-day shock
        #
        # Weights emphasise 20-day momentum (1.2) and volatility regime (1.1)
        # while down-weighting volume ratio (0.7) and single-day return (0.6).
        # TODO: Consider making these configurable via settings for A/B testing.
        # Approach notes:
        #   - Add `analog_distance_scales: list[float]` and
        #     `analog_distance_weights: list[float]` to Settings with these
        #     values as defaults. Validate len(scales) == len(weights) == 7
        #     at startup.
        #   - Pass them into the forecaster at construction time so different
        #     worker pools or feature-flag cohorts can run alternative configs.
        #   - Log the active scales/weights in forecast results so A/B
        #     comparisons can attribute performance differences to tuning.
        scales = (8.0, 15.0, 20.0, 5.0, 1.5, 25.0, 4.0)
        weights = (1.0, 1.2, 0.8, 1.0, 0.7, 1.1, 0.6)
        return sum(
            abs((a - b) / scale) * weight for a, b, scale, weight in zip(left, right, scales, weights, strict=True)
        )

    @staticmethod
    def _percentile(values: list[float], fraction: float) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]
        position = (len(values) - 1) * fraction
        lower_index = int(math.floor(position))
        upper_index = int(math.ceil(position))
        if lower_index == upper_index:
            return values[lower_index]
        lower_value = values[lower_index]
        upper_value = values[upper_index]
        blend = position - lower_index
        return lower_value + ((upper_value - lower_value) * blend)

    @staticmethod
    def _build_summary(
        strategy_type: str | None,
        median_return: float,
        low_return: float,
        high_return: float,
        positive_rate: float,
        horizon_days: int,
    ) -> str:
        direction_hint = "neutral"
        if strategy_type in BULLISH_STRATEGIES:
            direction_hint = "bullish"
        elif strategy_type in BEARISH_STRATEGIES:
            direction_hint = "bearish"

        if direction_hint == "bullish":
            alignment = (
                "supports bullish structures" if median_return > 0 else "does not strongly support bullish structures"
            )
        elif direction_hint == "bearish":
            alignment = (
                "supports bearish structures" if median_return < 0 else "does not strongly support bearish structures"
            )
        else:
            alignment = "suggests a mixed or neutral backdrop"

        return (
            f"Across {horizon_days}-day analogs, the middle historical outcome ranged "
            f"from {low_return:.2f}% to {high_return:.2f}% "
            f"with a median of {median_return:.2f}% and {positive_rate:.1f}% positive analogs; this {alignment}."
        )

    _SMALL_HORIZON_LOOKUP = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}

    @staticmethod
    def _calendar_to_trading_days(calendar_days: int) -> int:
        if calendar_days <= 0:
            return 0
        if calendar_days in HistoricalAnalogForecaster._SMALL_HORIZON_LOOKUP:
            return HistoricalAnalogForecaster._SMALL_HORIZON_LOOKUP[calendar_days]
        weekday_days = round(calendar_days * 5 / 7)
        estimated_holidays = calendar_days * 9.0 / 365.0  # ~9 NYSE holidays per year
        return max(1, round(weekday_days - estimated_holidays))

    _QUANT = Decimal("0.0001")

    @staticmethod
    def _to_decimal(value: float) -> Decimal:
        return Decimal(str(value)).quantize(
            HistoricalAnalogForecaster._QUANT, rounding=ROUND_HALF_UP,
        )
