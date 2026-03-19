"""Unit tests for indicator calculations with known-good expected outputs."""
from __future__ import annotations

import math

import pytest

from backtestforecast.indicators.calculations import (
    bollinger_bands,
    ema,
    macd,
    rolling_max,
    rolling_mean,
    rolling_min,
    rolling_stddev,
    rsi,
    sma,
)


class TestSma:
    def test_basic_period_3(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = sma(values, 3)
        assert result[:2] == [None, None]
        assert result[2] == pytest.approx(2.0)
        assert result[3] == pytest.approx(3.0)
        assert result[4] == pytest.approx(4.0)

    def test_period_1(self):
        values = [10.0, 20.0, 30.0]
        result = sma(values, 1)
        assert result == [pytest.approx(10.0), pytest.approx(20.0), pytest.approx(30.0)]

    def test_period_equals_length(self):
        values = [2.0, 4.0, 6.0]
        result = sma(values, 3)
        assert result == [None, None, pytest.approx(4.0)]

    def test_period_zero(self):
        values = [1.0, 2.0]
        result = sma(values, 0)
        assert result == [None, None]

    def test_empty(self):
        assert sma([], 5) == []


class TestEma:
    def test_seed_is_sma(self):
        values = [2.0, 4.0, 6.0, 8.0, 10.0]
        result = ema(values, 3)
        assert result[:2] == [None, None]
        assert result[2] == pytest.approx(4.0)  # SMA seed

    def test_subsequent_values(self):
        values = [2.0, 4.0, 6.0, 8.0, 10.0]
        result = ema(values, 3)
        k = 2.0 / 4.0  # 2/(3+1)
        expected_3 = (8.0 - 4.0) * k + 4.0
        expected_4 = (10.0 - expected_3) * k + expected_3
        assert result[3] == pytest.approx(expected_3)
        assert result[4] == pytest.approx(expected_4)

    def test_insufficient_data(self):
        result = ema([1.0, 2.0], 5)
        assert result == [None, None]


class TestRsi:
    def test_constant_gains(self):
        values = [float(i) for i in range(20)]
        result = rsi(values, 14)
        assert result[14] == pytest.approx(100.0)

    def test_constant_losses(self):
        values = [float(20 - i) for i in range(20)]
        result = rsi(values, 14)
        assert result[14] == pytest.approx(0.0)

    def test_equal_gains_losses(self):
        values = [100.0, 101.0, 100.0] * 10
        result = rsi(values, 14)
        for v in result[14:]:
            if v is not None:
                assert 40.0 < v < 60.0

    def test_short_input(self):
        result = rsi([1.0, 2.0], 14)
        assert all(v is None for v in result)


class TestMacd:
    def test_output_lengths(self):
        values = [float(i) for i in range(50)]
        macd_line, signal_line, histogram = macd(values, 12, 26, 9)
        assert len(macd_line) == 50
        assert len(signal_line) == 50
        assert len(histogram) == 50

    def test_macd_line_values(self):
        values = [100.0 + i * 0.5 for i in range(50)]
        macd_line, _, _ = macd(values, 12, 26, 9)
        assert macd_line[24] is None  # slow EMA not yet ready
        assert macd_line[25] is not None  # both EMAs ready
        assert macd_line[25] > 0  # uptrend → fast > slow

    def test_no_none_zero_contamination(self):
        values = [100.0 + i * 0.1 for i in range(50)]
        macd_line, signal_line, histogram = macd(values, 12, 26, 9)
        for i in range(25, 50):
            assert macd_line[i] is not None


class TestBollingerBands:
    def test_constant_series(self):
        values = [100.0] * 30
        lower, middle, upper = bollinger_bands(values, 20, 2.0)
        for i in range(19, 30):
            assert middle[i] == pytest.approx(100.0)
            assert lower[i] == pytest.approx(100.0)
            assert upper[i] == pytest.approx(100.0)

    def test_bands_widen_with_volatility(self):
        values = [100.0] * 20 + [110.0, 90.0, 110.0, 90.0, 110.0]
        lower, middle, upper = bollinger_bands(values, 20, 2.0)
        band_width_start = upper[19] - lower[19]
        band_width_end = upper[24] - lower[24]
        assert band_width_end > band_width_start


class TestRollingStddev:
    def test_constant_series(self):
        values = [5.0] * 10
        result = rolling_stddev(values, 5)
        for v in result[4:]:
            assert v == pytest.approx(0.0)

    def test_known_values(self):
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        result = rolling_stddev(values, 5, ddof=0)
        window = values[0:5]  # [2, 4, 4, 4, 5]
        mean = sum(window) / 5
        variance = sum((x - mean) ** 2 for x in window) / 5
        assert result[4] == pytest.approx(math.sqrt(variance))


class TestRollingMin:
    def test_basic_period_3(self):
        values = [5.0, 3.0, 8.0, 1.0, 7.0]
        result = rolling_min(values, 3)
        assert result[:2] == [None, None]
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(1.0)
        assert result[4] == pytest.approx(1.0)

    def test_period_1(self):
        values = [10.0, 5.0, 15.0]
        result = rolling_min(values, 1)
        assert result == [pytest.approx(10.0), pytest.approx(5.0), pytest.approx(15.0)]

    def test_constant_series(self):
        values = [7.0] * 6
        result = rolling_min(values, 3)
        for v in result[2:]:
            assert v == pytest.approx(7.0)

    def test_descending(self):
        values = [5.0, 4.0, 3.0, 2.0, 1.0]
        result = rolling_min(values, 3)
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(2.0)
        assert result[4] == pytest.approx(1.0)

    def test_ascending(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = rolling_min(values, 3)
        assert result[2] == pytest.approx(1.0)
        assert result[3] == pytest.approx(2.0)
        assert result[4] == pytest.approx(3.0)

    def test_period_0_returns_all_none(self):
        values = [1.0, 2.0, 3.0]
        result = rolling_min(values, 0)
        assert result == [None, None, None]


class TestRollingMax:
    def test_basic_period_3(self):
        values = [5.0, 8.0, 3.0, 9.0, 2.0]
        result = rolling_max(values, 3)
        assert result[:2] == [None, None]
        assert result[2] == pytest.approx(8.0)
        assert result[3] == pytest.approx(9.0)
        assert result[4] == pytest.approx(9.0)

    def test_period_1(self):
        values = [10.0, 5.0, 15.0]
        result = rolling_max(values, 1)
        assert result == [pytest.approx(10.0), pytest.approx(5.0), pytest.approx(15.0)]

    def test_constant_series(self):
        values = [7.0] * 6
        result = rolling_max(values, 3)
        for v in result[2:]:
            assert v == pytest.approx(7.0)

    def test_ascending(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = rolling_max(values, 3)
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(4.0)
        assert result[4] == pytest.approx(5.0)

    def test_descending(self):
        values = [5.0, 4.0, 3.0, 2.0, 1.0]
        result = rolling_max(values, 3)
        assert result[2] == pytest.approx(5.0)
        assert result[3] == pytest.approx(4.0)
        assert result[4] == pytest.approx(3.0)

    def test_period_0_returns_all_none(self):
        values = [1.0, 2.0, 3.0]
        result = rolling_max(values, 0)
        assert result == [None, None, None]


class TestRollingMean:
    def test_identical_to_sma(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert rolling_mean(values, 3) == sma(values, 3)
