"""Regime classifier for the nightly scan pipeline.

Takes daily bars for a symbol and produces a set of regime labels that
describe the current market conditions. No backtesting, just indicator
evaluation on the most recent bar window.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from backtestforecast.indicators.calculations import (
    adx,
    ema,
    rolling_stddev,
    rsi,
    sma,
)
from backtestforecast.market_data.types import DailyBar


class Regime(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    # These labels use realized-vol rank as a proxy for implied vol. The proxy
    # tracks actual IV closely for liquid underlyings but can diverge during
    # vol risk-premium compression or expansion. See the iv_rank_proxy and
    # vol_ratio_20_over_60 computation below for details.
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


# Minimum bars required for classification.
MIN_BARS = 210
_ANNUALIZATION_FACTOR = math.sqrt(252.0)

# Directional regime thresholds use multiple confirming signals so one noisy
# crossover does not flip the label by itself.
_DIRECTION_EMA_SPREAD_MIN = 0.002
_DIRECTION_PRICE_SMA50_MIN = 0.005
_DIRECTION_SMA_SPREAD_MIN = 0.01
_DIRECTION_MOMENTUM_20_MIN = 0.03
_DIRECTION_RSI_BULLISH = 55.0
_DIRECTION_RSI_BEARISH = 45.0
_DIRECTION_SCORE_THRESHOLD = 2.5

# Trend-vs-range is anchored by ADX, but price structure and recent momentum
# can still confirm a persistent trend when ADX is only middling.
_TREND_ADX_HIGH = 23.0
_TREND_ADX_LOW = 18.0
_TREND_EMA_SPREAD_MIN = 0.004
_TREND_EMA_SPREAD_RANGE_MAX = 0.0015
_TREND_SMA_SPREAD_MIN = 0.02
_TREND_SMA_SPREAD_RANGE_MAX = 0.006
_TREND_MOMENTUM_20_MIN = 0.03
_TREND_MOMENTUM_20_RANGE_MAX = 0.01
_TREND_SCORE_THRESHOLD = 1.5

# Volatility still uses a realized-vol proxy for IV, but the 20d/60d vol ratio
# suppresses false positives when the trailing rank is stale and current vol
# has already normalized.
_VOL_HIGH_PROXY_THRESHOLD = 60.0
_VOL_HIGH_ABSOLUTE_PROXY_THRESHOLD = 75.0
_VOL_HIGH_RATIO_THRESHOLD = 1.10
_VOL_LOW_PROXY_THRESHOLD = 30.0
_VOL_LOW_ABSOLUTE_PROXY_THRESHOLD = 20.0
_VOL_LOW_RATIO_THRESHOLD = 0.90


def _pct_diff(left: float | None, right: float | None) -> float:
    if left is None or right is None or not math.isfinite(left) or not math.isfinite(right) or abs(right) < 1e-12:
        return 0.0
    return (left - right) / right


def _annualized_rolling_vol(closes: list[float], window: int) -> list[float | None]:
    values = rolling_stddev(
        _daily_returns(closes),
        window,
        ddof=1,
    )
    return [value * _ANNUALIZATION_FACTOR if value is not None else None for value in values]


def _classify_directional_regime(
    *,
    current_close: float,
    ema_8: float,
    ema_21: float,
    sma_50: float,
    sma_200: float,
    rsi_14: float,
    momentum_20: float,
) -> Regime:
    score = 0.0

    ema_spread = _pct_diff(ema_8, ema_21)
    if ema_spread >= _DIRECTION_EMA_SPREAD_MIN:
        score += 1.25
    elif ema_spread <= -_DIRECTION_EMA_SPREAD_MIN:
        score -= 1.25

    price_vs_sma50 = _pct_diff(current_close, sma_50)
    if price_vs_sma50 >= _DIRECTION_PRICE_SMA50_MIN:
        score += 1.0
    elif price_vs_sma50 <= -_DIRECTION_PRICE_SMA50_MIN:
        score -= 1.0

    sma_trend = _pct_diff(sma_50, sma_200)
    if sma_trend >= _DIRECTION_SMA_SPREAD_MIN:
        score += 1.0
    elif sma_trend <= -_DIRECTION_SMA_SPREAD_MIN:
        score -= 1.0

    if rsi_14 >= _DIRECTION_RSI_BULLISH:
        score += 0.75
    elif rsi_14 <= _DIRECTION_RSI_BEARISH:
        score -= 0.75

    if momentum_20 >= _DIRECTION_MOMENTUM_20_MIN:
        score += 0.75
    elif momentum_20 <= -_DIRECTION_MOMENTUM_20_MIN:
        score -= 0.75

    if score >= _DIRECTION_SCORE_THRESHOLD:
        return Regime.BULLISH
    if score <= -_DIRECTION_SCORE_THRESHOLD:
        return Regime.BEARISH
    return Regime.NEUTRAL


def _classify_volatility_regime(
    *,
    iv_rank_proxy: float,
    vol_ratio_20_over_60: float,
) -> Regime | None:
    if (
        iv_rank_proxy >= _VOL_HIGH_ABSOLUTE_PROXY_THRESHOLD
        or (iv_rank_proxy >= _VOL_HIGH_PROXY_THRESHOLD and vol_ratio_20_over_60 >= _VOL_HIGH_RATIO_THRESHOLD)
    ):
        return Regime.HIGH_IV
    if (
        iv_rank_proxy <= _VOL_LOW_ABSOLUTE_PROXY_THRESHOLD
        or (iv_rank_proxy <= _VOL_LOW_PROXY_THRESHOLD and vol_ratio_20_over_60 <= _VOL_LOW_RATIO_THRESHOLD)
    ):
        return Regime.LOW_IV
    return None


def _classify_structure_regime(
    *,
    adx_14: float | None,
    ema_spread: float,
    sma_trend: float,
    momentum_20: float,
) -> Regime:
    trend_score = 0.0

    if adx_14 is not None and math.isfinite(adx_14):
        if adx_14 >= _TREND_ADX_HIGH:
            trend_score += 1.25
        elif adx_14 <= _TREND_ADX_LOW:
            trend_score -= 0.75

    abs_ema_spread = abs(ema_spread)
    if abs_ema_spread >= _TREND_EMA_SPREAD_MIN:
        trend_score += 0.75
    elif abs_ema_spread <= _TREND_EMA_SPREAD_RANGE_MAX:
        trend_score -= 0.25

    abs_sma_trend = abs(sma_trend)
    if abs_sma_trend >= _TREND_SMA_SPREAD_MIN:
        trend_score += 0.75
    elif abs_sma_trend <= _TREND_SMA_SPREAD_RANGE_MAX:
        trend_score -= 0.25

    abs_momentum_20 = abs(momentum_20)
    if abs_momentum_20 >= _TREND_MOMENTUM_20_MIN:
        trend_score += 0.5
    elif abs_momentum_20 <= _TREND_MOMENTUM_20_RANGE_MAX:
        trend_score -= 0.25

    if trend_score >= _TREND_SCORE_THRESHOLD:
        return Regime.TRENDING
    return Regime.RANGE_BOUND


def _compute_regime_indicator_series(
    sorted_bars: list[DailyBar],
) -> dict[str, list[float | None] | list[float]]:
    closes = [bar.close_price for bar in sorted_bars]
    volumes = [bar.volume for bar in sorted_bars]
    return {
        "closes": closes,
        "volumes": volumes,
        "rsi_values": rsi(closes, 14),
        "ema_8_values": ema(closes, 8),
        "ema_21_values": ema(closes, 21),
        "sma_50_values": sma(closes, 50),
        "sma_200_values": sma(closes, 200),
        "adx_14_values": adx(
            [bar.high_price for bar in sorted_bars],
            [bar.low_price for bar in sorted_bars],
            closes,
            14,
        ),
        "vol_20_values": _annualized_rolling_vol(closes, 20),
        "vol_60_values": _annualized_rolling_vol(closes, 60),
        "avg_volume_20": sma(volumes, 20),
    }


def _build_regime_snapshot_at_index(
    *,
    symbol: str,
    sorted_bars: list[DailyBar],
    series: dict[str, list[float | None] | list[float]],
    index: int,
    earnings_dates: set[date] | None = None,
) -> RegimeSnapshot | None:
    if index + 1 < MIN_BARS:
        return None

    closes = series["closes"]
    volumes = series["volumes"]
    rsi_values = series["rsi_values"]
    ema_8_values = series["ema_8_values"]
    ema_21_values = series["ema_21_values"]
    sma_50_values = series["sma_50_values"]
    sma_200_values = series["sma_200_values"]
    adx_14_values = series["adx_14_values"]
    vol_20_values = series["vol_20_values"]
    vol_60_values = series["vol_60_values"]
    avg_volume_20 = series["avg_volume_20"]

    current_close = closes[index]
    current_volume = volumes[index]
    rsi_14 = rsi_values[index]
    ema_8 = ema_8_values[index]
    ema_21 = ema_21_values[index]
    sma_50 = sma_50_values[index]
    sma_200 = sma_200_values[index]
    adx_14 = adx_14_values[index]
    vol_20 = vol_20_values[index]
    vol_60 = vol_60_values[index]
    avg_vol = avg_volume_20[index]

    if any(value is None for value in [rsi_14, ema_8, ema_21, sma_50, sma_200]):
        return None
    if not all(math.isfinite(value) for value in [rsi_14, ema_8, ema_21, sma_50, sma_200]):  # type: ignore[arg-type]
        return None
    if vol_20 is not None and not math.isfinite(vol_20):
        vol_20 = None
    if vol_60 is not None and not math.isfinite(vol_60):
        vol_60 = None

    vol_window_start = max(0, index - 251)
    vol_values = [value for value in vol_20_values[vol_window_start : index + 1] if value is not None]
    if len(vol_values) >= 60 and vol_20 is not None:
        vol_min = min(vol_values)
        vol_max = max(vol_values)
        iv_rank_proxy = ((vol_20 - vol_min) / (vol_max - vol_min) * 100.0) if vol_max > vol_min else 50.0
    else:
        iv_rank_proxy = 50.0

    vol_ratio_20_over_60 = (vol_20 / vol_60) if vol_20 is not None and vol_60 not in (None, 0.0) else 1.0
    volume_ratio = (current_volume / avg_vol) if avg_vol and avg_vol > 0 else 1.0
    ema_spread = _pct_diff(ema_8, ema_21)
    sma_trend = _pct_diff(sma_50, sma_200)
    momentum_20 = _pct_diff(current_close, closes[index - 20]) if index >= 20 else 0.0

    regimes: set[Regime] = set()
    regimes.add(
        _classify_directional_regime(
            current_close=current_close,
            ema_8=ema_8,
            ema_21=ema_21,
            sma_50=sma_50,
            sma_200=sma_200,
            rsi_14=rsi_14,
            momentum_20=momentum_20,
        )
    )

    volatility_regime = _classify_volatility_regime(
        iv_rank_proxy=iv_rank_proxy,
        vol_ratio_20_over_60=vol_ratio_20_over_60,
    )
    if volatility_regime is not None:
        regimes.add(volatility_regime)

    regimes.add(
        _classify_structure_regime(
            adx_14=adx_14,
            ema_spread=ema_spread,
            sma_trend=sma_trend,
            momentum_20=momentum_20,
        )
    )

    if volume_ratio > 1.5:
        regimes.add(Regime.HIGH_VOLUME)

    if earnings_dates:
        current_date = sorted_bars[index].trade_date
        nearby = [value for value in earnings_dates if -2 <= (value - current_date).days <= 10]
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


def build_regime_snapshots(
    symbol: str,
    bars: list[DailyBar],
    *,
    earnings_dates: set[date] | None = None,
) -> list[RegimeSnapshot | None]:
    """Build point-in-time regime snapshots for each bar in a sorted bar window."""
    if not bars:
        return []

    sorted_bars = sorted(bars, key=lambda b: b.trade_date)
    series = _compute_regime_indicator_series(sorted_bars)
    return [
        _build_regime_snapshot_at_index(
            symbol=symbol,
            sorted_bars=sorted_bars,
            series=series,
            index=index,
            earnings_dates=earnings_dates,
        )
        for index in range(len(sorted_bars))
    ]


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
    return _build_regime_snapshot_at_index(
        symbol=symbol,
        sorted_bars=sorted_bars,
        series=_compute_regime_indicator_series(sorted_bars),
        index=len(sorted_bars) - 1,
        earnings_dates=earnings_dates,
    )


def _daily_returns(closes: list[float]) -> list[float]:
    """Compute daily returns as raw decimals (0.02 = 2% gain).

    Returns a list of length ``len(closes)`` with a leading 0.0 for the
    first close (no prior bar to compute a return from). This keeps the
    output aligned index-for-index with other indicator series derived
    from ``closes``.
    """
    returns: list[float] = [0.0]
    for index in range(1, len(closes)):
        if closes[index - 1] > 0:
            returns.append((closes[index] - closes[index - 1]) / closes[index - 1])
        else:
            returns.append(0.0)
    return returns
