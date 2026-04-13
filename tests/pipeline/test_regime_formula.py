from __future__ import annotations

from datetime import date, timedelta

from backtestforecast.market_data.types import DailyBar
from backtestforecast.pipeline.regime import (
    Regime,
    build_regime_snapshots,
    classify_regime,
    _classify_directional_regime,
    _classify_structure_regime,
    _classify_volatility_regime,
)


def test_directional_regime_rejects_short_term_bounce_against_long_term_downtrend() -> None:
    result = _classify_directional_regime(
        current_close=101.0,
        ema_8=101.5,
        ema_21=101.0,
        sma_50=100.0,
        sma_200=108.0,
        rsi_14=53.0,
        momentum_20=0.02,
    )

    assert result is Regime.NEUTRAL


def test_directional_regime_marks_bullish_when_timeframes_align() -> None:
    result = _classify_directional_regime(
        current_close=112.0,
        ema_8=111.5,
        ema_21=109.5,
        sma_50=106.0,
        sma_200=101.0,
        rsi_14=61.0,
        momentum_20=0.055,
    )

    assert result is Regime.BULLISH


def test_structure_regime_can_be_trending_with_persistent_structure_and_momentum() -> None:
    result = _classify_structure_regime(
        adx_14=21.0,
        ema_spread=0.006,
        sma_trend=0.035,
        momentum_20=0.05,
    )

    assert result is Regime.TRENDING


def test_structure_regime_marks_range_when_signals_are_muted() -> None:
    result = _classify_structure_regime(
        adx_14=17.5,
        ema_spread=0.0008,
        sma_trend=0.003,
        momentum_20=0.004,
    )

    assert result is Regime.RANGE_BOUND


def test_volatility_regime_uses_rank_and_term_ratio_for_high_vol() -> None:
    result = _classify_volatility_regime(
        iv_rank_proxy=63.0,
        vol_ratio_20_over_60=1.14,
    )

    assert result is Regime.HIGH_IV


def test_volatility_regime_avoids_high_vol_label_when_rank_is_stale() -> None:
    result = _classify_volatility_regime(
        iv_rank_proxy=63.0,
        vol_ratio_20_over_60=0.96,
    )

    assert result is None


def test_volatility_regime_uses_rank_and_term_ratio_for_low_vol() -> None:
    result = _classify_volatility_regime(
        iv_rank_proxy=28.0,
        vol_ratio_20_over_60=0.86,
    )

    assert result is Regime.LOW_IV


def test_build_regime_snapshots_matches_latest_classification() -> None:
    bars = [
        DailyBar(
            trade_date=date(2024, 1, 2) + timedelta(days=index),
            open_price=100.0 + (index * 0.05),
            high_price=100.5 + (index * 0.05),
            low_price=99.5 + (index * 0.05),
            close_price=100.0 + (index * 0.05),
            volume=1_000_000 + (index * 100),
        )
        for index in range(240)
    ]

    snapshots = build_regime_snapshots("TEST", bars)
    latest = classify_regime("TEST", bars)

    assert len(snapshots) == len(bars)
    assert all(snapshot is None for snapshot in snapshots[:209])
    assert snapshots[-1] == latest
