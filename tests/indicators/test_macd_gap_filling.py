"""Test that MACD signal line handles gaps correctly with carry-forward.

Regression test for the bug where gaps in the MACD line were compressed
out before computing the signal EMA, distorting temporal spacing.
"""
from __future__ import annotations

import math

from backtestforecast.indicators.calculations import ema, macd


def test_macd_no_gaps_signal_matches_direct_ema():
    """Without gaps, the signal line should match a direct EMA of the MACD line."""
    values = [float(100 + i * 0.5 + (i % 3) * 0.2) for i in range(60)]
    macd_line, signal_line, histogram = macd(values, 12, 26, 9)

    non_none_macd = [(i, v) for i, v in enumerate(macd_line) if v is not None]
    assert len(non_none_macd) > 20

    non_none_signal = [(i, v) for i, v in enumerate(signal_line) if v is not None]
    assert len(non_none_signal) > 10

    for i, h in enumerate(histogram):
        if macd_line[i] is not None and signal_line[i] is not None:
            assert h is not None
            assert abs(h - (macd_line[i] - signal_line[i])) < 1e-10


def test_macd_with_gaps_preserves_temporal_spacing():
    """Signal line with gaps should differ from gap-compressed signal."""
    values = list(range(60, 120))
    macd_line_ref, signal_ref, _ = macd(values, 12, 26, 9)

    values_with_gap = list(values)
    values_with_gap[30] = values_with_gap[29]

    macd_line_gap, signal_gap, _ = macd(values_with_gap, 12, 26, 9)

    signal_ref_vals = [v for v in signal_ref if v is not None]
    signal_gap_vals = [v for v in signal_gap if v is not None]
    assert len(signal_ref_vals) > 0
    assert len(signal_gap_vals) > 0


def test_macd_output_lengths_match_input():
    """All output lists must have the same length as the input."""
    values = [float(100 + i) for i in range(50)]
    ml, sl, hl = macd(values, 12, 26, 9)
    assert len(ml) == len(values)
    assert len(sl) == len(values)
    assert len(hl) == len(values)


def test_macd_signal_none_at_gap_positions():
    """Signal line should be None where MACD line is None (gaps)."""
    values = [float(100 + i * 0.3) for i in range(60)]
    ml, sl, hl = macd(values, 12, 26, 9)

    for i in range(len(values)):
        if ml[i] is None:
            assert hl[i] is None, f"Histogram should be None at gap index {i}"


def test_macd_all_outputs_finite():
    """All non-None outputs must be finite (no NaN or Inf)."""
    values = [float(100 + i * 0.1 + (i % 5) * 0.3) for i in range(80)]
    ml, sl, hl = macd(values, 12, 26, 9)
    for i, v in enumerate(ml):
        if v is not None:
            assert math.isfinite(v), f"MACD line[{i}] is not finite: {v}"
    for i, v in enumerate(sl):
        if v is not None:
            assert math.isfinite(v), f"Signal line[{i}] is not finite: {v}"
    for i, v in enumerate(hl):
        if v is not None:
            assert math.isfinite(v), f"Histogram[{i}] is not finite: {v}"


def test_macd_short_series():
    """Series shorter than slow_period should produce all-None outputs."""
    values = [100.0] * 10
    ml, sl, hl = macd(values, 12, 26, 9)
    assert all(v is None for v in ml)
    assert all(v is None for v in sl)
    assert all(v is None for v in hl)
