"""Regime classifier for the nightly scan pipeline.

Takes daily bars for a symbol and produces a set of regime labels that
describe the current market conditions.  No backtesting — just indicator
evaluation on the most recent bar window.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from backtestforecast.indicators.calculations import (
    ema,
    rolling_stddev,
    rsi,
    sma,
)
from backtestforecast.market_data.types import DailyBar


class Regime(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    HIGH_IV = "high_iv"
    LOW_IV = "low_iv"
    TRENDING = "trending"
    RANGE_BOUND = "range_bound"
    EARNINGS_IMMINENT = "earnings_imminent"
    HIGH_VOLUME = "high_volume"


@dataclass(frozen=True, slots=True)
class RegimeSnapshot:
    """Regime classification result for a single symbol."""

    symbol: str
    regimes: frozenset[Regime]
    rsi_14: float | None = None
    ema_8: float | None = None
    ema_21: float | None = None
    sma_50: float | None = None
    sma_200: float | None = None
    realized_vol_20: float | None = None
    iv_rank_proxy: float | None = None
    volume_ratio: float | None = None
    close_price: float = 0.0


# Minimum bars required for classification
MIN_BARS = 210


def classify_regime(
    symbol: str,
    bars: list[DailyBar],
    *,
    earnings_dates: set | None = None,
) -> RegimeSnapshot | None:
    """Classify the current regime for a symbol from daily bars.

    Returns None if there isn't enough data (< 210 bars).
    """
    if len(bars) < MIN_BARS:
        return None

    sorted_bars = sorted(bars, key=lambda b: b.trade_date)
    closes = [b.close_price for b in sorted_bars]
    volumes = [b.volume for b in sorted_bars]
    current_close = closes[-1]
    current_volume = volumes[-1]

    # Compute indicators
    rsi_values = rsi(closes, 14)
    ema_8_values = ema(closes, 8)
    ema_21_values = ema(closes, 21)
    sma_50_values = sma(closes, 50)
    sma_200_values = sma(closes, 200)
    vol_20_values = rolling_stddev(
        _daily_returns(closes),
        20,
        ddof=1,
    )
    _ANNUALIZATION_FACTOR = math.sqrt(252)
    vol_20_values = [v * _ANNUALIZATION_FACTOR if v is not None else None for v in vol_20_values]
    avg_volume_20 = sma(volumes, 20)

    # Extract latest values
    rsi_14 = rsi_values[-1]
    ema_8 = ema_8_values[-1]
    ema_21 = ema_21_values[-1]
    sma_50 = sma_50_values[-1]
    sma_200 = sma_200_values[-1]
    vol_20 = vol_20_values[-1]
    avg_vol = avg_volume_20[-1]

    if any(v is None for v in [rsi_14, ema_8, ema_21, sma_50, sma_200]):
        return None

    # IV rank proxy: where current 20-day realized vol sits in its 252-day range
    vol_values = [v for v in vol_20_values[-252:] if v is not None]
    if len(vol_values) >= 60 and vol_20 is not None:
        vol_min = min(vol_values)
        vol_max = max(vol_values)
        iv_rank_proxy = ((vol_20 - vol_min) / (vol_max - vol_min) * 100.0) if vol_max > vol_min else 50.0
    else:
        iv_rank_proxy = 50.0

    volume_ratio = (current_volume / avg_vol) if avg_vol and avg_vol > 0 else 1.0

    # Classify
    regimes: set[Regime] = set()

    # Directional
    if ema_8 > ema_21 and current_close > sma_50 and rsi_14 > 50:  # type: ignore[operator]
        regimes.add(Regime.BULLISH)
    elif ema_8 < ema_21 and current_close < sma_50 and rsi_14 < 50:  # type: ignore[operator]
        regimes.add(Regime.BEARISH)
    else:
        regimes.add(Regime.NEUTRAL)

    # Volatility
    if iv_rank_proxy > 60:
        regimes.add(Regime.HIGH_IV)
    elif iv_rank_proxy < 30:
        regimes.add(Regime.LOW_IV)

    # Trend vs range
    if sma_50 is not None and sma_200 is not None:
        if current_close > 0:
            sma_spread_pct = abs(sma_50 - sma_200) / current_close * 100.0  # type: ignore[operator]
            if sma_spread_pct > 5.0:
                regimes.add(Regime.TRENDING)
            else:
                regimes.add(Regime.RANGE_BOUND)
        else:
            regimes.add(Regime.RANGE_BOUND)

    # Volume
    if volume_ratio > 1.5:
        regimes.add(Regime.HIGH_VOLUME)

    # Earnings — flag if earnings are within 10 days ahead or 2 days behind
    if earnings_dates:
        last_date = sorted_bars[-1].trade_date
        nearby = [d for d in earnings_dates if -2 <= (d - last_date).days <= 10]
        if nearby:
            regimes.add(Regime.EARNINGS_IMMINENT)

    return RegimeSnapshot(
        symbol=symbol,
        regimes=frozenset(regimes),
        rsi_14=rsi_14,
        ema_8=ema_8,
        ema_21=ema_21,
        sma_50=sma_50,
        sma_200=sma_200,
        realized_vol_20=vol_20,
        iv_rank_proxy=iv_rank_proxy,
        volume_ratio=volume_ratio,
        close_price=current_close,
    )


def _daily_returns(closes: list[float]) -> list[float]:
    """Compute daily returns as raw decimals (0.02 = 2% gain).

    Note: forecasts/analog.py uses percentage format (2.0 = 2% gain).
    These conventions are independent and should not be mixed.
    """
    returns = [0.0]
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            returns.append((closes[i] - closes[i - 1]) / closes[i - 1])
        else:
            returns.append(0.0)
    return returns
