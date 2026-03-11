from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from statistics import median

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
    ) -> HistoricalAnalogForecastResponse:
        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        horizon_days = self._calendar_to_trading_days(horizon_days)
        if len(sorted_bars) < max(80, horizon_days + 40):
            raise ValueError("Not enough historical bars were available to build a forecast.")

        closes = [bar.close_price for bar in sorted_bars]
        volumes = [bar.volume for bar in sorted_bars]
        daily_returns = self._daily_returns(closes)
        rsi14 = rsi(closes, 14)
        ema8 = ema(closes, 8)
        ema21 = ema(closes, 21)
        avg_volume20 = sma(volumes, 20)
        vol20 = rolling_stddev(daily_returns, 20)

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
        analogs = ranked[: min(20, len(ranked))]
        returns = sorted(candidate.forward_return_pct for candidate in analogs)
        positive_rate = (sum(1 for value in returns if value > 0) / len(returns)) * 100.0
        low = self._percentile(returns, 0.25)
        med = median(returns)
        high = self._percentile(returns, 0.75)
        summary = self._build_summary(strategy_type, med, low, high, positive_rate, horizon_days)

        return HistoricalAnalogForecastResponse(
            symbol=symbol,
            strategy_type=strategy_type,
            as_of_date=sorted_bars[as_of_index].trade_date,
            horizon_days=horizon_days,
            analog_count=len(analogs),
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
        )

    @staticmethod
    def _daily_returns(closes: list[float]) -> list[float]:
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

        if (
            close <= 0
            or five_day_base <= 0
            or twenty_day_base <= 0
            or current_rsi is None
            or current_ema8 is None
            or current_ema21 is None
            or current_avg_volume20 is None
            or current_avg_volume20 <= 0
            or current_vol20 is None
        ):
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
        if strategy_type in {
            "long_call",
            "covered_call",
            "cash_secured_put",
            "bull_call_debit_spread",
            "bull_put_credit_spread",
            "wheel_strategy",
        }:
            direction_hint = "bullish"
        elif strategy_type in {"long_put", "bear_put_debit_spread", "bear_call_credit_spread"}:
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

    @staticmethod
    def _calendar_to_trading_days(calendar_days: int) -> int:
        if calendar_days <= 0:
            return 0
        return max(1, int(calendar_days * 5 / 7))

    @staticmethod
    def _to_decimal(value: float) -> Decimal:
        return Decimal(str(round(value, 4)))
