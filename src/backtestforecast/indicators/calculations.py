"""Technical indicator calculations (SMA, EMA, RSI, MACD, Bollinger Bands, etc.).

Callers should validate indicator outputs for finiteness before using them
in downstream computations. Indicator functions may produce non-finite values
(NaN, inf) when fed degenerate input (e.g. zero-valued price series).  The
functions intentionally do not clamp or filter outputs to avoid masking data
quality issues - that responsibility belongs to the caller.
"""

from __future__ import annotations

import logging
import math
from collections import deque

_logger = logging.getLogger(__name__)


def sma(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    running_sum = 0.0
    for index, value in enumerate(values):
        running_sum += value
        if index >= period:
            running_sum -= values[index - period]
        if index >= period - 1:
            result[index] = running_sum / period
    return result


def _sma_optional(values: list[float | None], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    window: deque[float | None] = deque()
    running_sum = 0.0
    valid_count = 0
    for index, value in enumerate(values):
        window.append(value)
        if value is not None:
            running_sum += value
            valid_count += 1
        if len(window) > period:
            evicted = window.popleft()
            if evicted is not None:
                running_sum -= evicted
                valid_count -= 1
        if len(window) == period and valid_count == period:
            result[index] = running_sum / period
    return result


def rolling_mean(values: list[float], period: int) -> list[float | None]:
    return sma(values, period)


def rolling_stddev(values: list[float], period: int, *, ddof: int = 0) -> list[float | None]:
    """O(n) rolling standard deviation using Welford's online algorithm.

    Numerically stable for large-valued instruments where the naive
    ``E[X^2] - E[X]^2`` formula suffers from catastrophic cancellation.

    Args:
        ddof: Delta degrees of freedom. Use 0 for population stddev (Bollinger Bands),
              1 for sample stddev (realized volatility).
    """
    result: list[float | None] = [None] * len(values)
    if period <= 1 or period <= ddof:
        return result
    buf: deque[float] = deque()
    mean = 0.0
    m2 = 0.0
    for index in range(len(values)):
        x = values[index]
        buf.append(x)
        n = len(buf)
        if n <= period:
            delta = x - mean
            mean += delta / n
            m2 += delta * (x - mean)
        else:
            old = buf.popleft()
            old_mean = mean
            mean += (x - old) / period
            m2 += (x - old) * ((x - mean) + (old - old_mean))
        if index >= period - 1:
            variance = m2 / (period - ddof)
            result[index] = math.sqrt(max(variance, 0.0))
    return result


def rolling_min(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    dq: deque[int] = deque()
    for index in range(len(values)):
        while dq and dq[0] <= index - period:
            dq.popleft()
        while dq and values[dq[-1]] >= values[index]:
            dq.pop()
        dq.append(index)
        if index >= period - 1:
            result[index] = values[dq[0]]
    return result


def rolling_max(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    dq: deque[int] = deque()
    for index in range(len(values)):
        while dq and dq[0] <= index - period:
            dq.popleft()
        while dq and values[dq[-1]] <= values[index]:
            dq.pop()
        dq.append(index)
        if index >= period - 1:
            result[index] = values[dq[0]]
    return result


def ema(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return result

    seed = sum(values[:period]) / period
    multiplier = 2.0 / (period + 1)
    result[period - 1] = seed
    previous = seed

    for index in range(period, len(values)):
        current = (values[index] - previous) * multiplier + previous
        result[index] = current
        previous = current

    return result


def rsi(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0 or len(values) <= period:
        return result

    gains = [0.0] * len(values)
    losses = [0.0] * len(values)
    for index in range(1, len(values)):
        change = values[index] - values[index - 1]
        gains[index] = max(change, 0.0)
        losses[index] = max(-change, 0.0)

    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    if avg_loss == 0:
        result[period] = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - (100.0 / (1.0 + rs))

    for index in range(period + 1, len(values)):
        avg_gain = ((avg_gain * (period - 1)) + gains[index]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[index]) / period

        if avg_loss == 0:
            result[index] = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            result[index] = 100.0 - (100.0 / (1.0 + rs))

    return result


def macd(
    values: list[float], fast_period: int, slow_period: int, signal_period: int
) -> tuple[
    list[float | None],
    list[float | None],
    list[float | None],
]:
    fast = ema(values, fast_period)
    slow = ema(values, slow_period)
    macd_line: list[float | None] = [None] * len(values)
    for index in range(len(values)):
        if fast[index] is None or slow[index] is None:
            continue
        macd_line[index] = fast[index] - slow[index]  # type: ignore[operator]

    first_valid = next(
        (i for i, v in enumerate(macd_line) if v is not None),
        len(values),
    )

    if first_valid < len(values):
        macd_filled: list[float] = []
        last_good: float = 0.0
        gap_count = 0
        for i in range(first_valid, len(values)):
            if macd_line[i] is not None:
                last_good = macd_line[i]
            else:
                gap_count += 1
            macd_filled.append(last_good)
        if gap_count > 0:
            _logger.warning(
                "MACD line contains %d None gap(s) in range [%d..%d]; "
                "gaps filled with carry-forward for signal line accuracy",
                gap_count,
                first_valid,
                len(values) - 1,
            )
        raw_signal = ema(macd_filled, signal_period)
    else:
        raw_signal = []

    signal_line: list[float | None] = [None] * len(values)
    for i, sig_val in enumerate(raw_signal):
        if sig_val is not None:
            signal_line[first_valid + i] = sig_val
    histogram: list[float | None] = [None] * len(values)
    for index in range(len(values)):
        if macd_line[index] is None or signal_line[index] is None:
            continue
        histogram[index] = macd_line[index] - signal_line[index]  # type: ignore[operator]

    return macd_line, signal_line, histogram


def bollinger_bands(
    values: list[float],
    period: int,
    standard_deviations: float,
) -> tuple[list[float | None], list[float | None], list[float | None]]:
    if standard_deviations <= 0:
        raise ValueError(f"standard_deviations must be positive, got {standard_deviations}")
    middle = sma(values, period)
    stddev = rolling_stddev(values, period)
    upper: list[float | None] = [None] * len(values)
    lower: list[float | None] = [None] * len(values)

    for index in range(len(values)):
        if middle[index] is None or stddev[index] is None:
            continue
        upper[index] = middle[index] + (standard_deviations * stddev[index])  # type: ignore[operator]
        lower[index] = middle[index] - (standard_deviations * stddev[index])  # type: ignore[operator]

    return lower, middle, upper


def roc(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    for index in range(period, len(values)):
        baseline = values[index - period]
        if baseline == 0:
            continue
        result[index] = ((values[index] - baseline) / baseline) * 100.0
    return result


def cci(highs: list[float], lows: list[float], closes: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if period <= 0:
        return result
    typical_prices = [(high + low + close) / 3.0 for high, low, close in zip(highs, lows, closes, strict=False)]
    typical_sma = sma(typical_prices, period)
    for index in range(period - 1, len(typical_prices)):
        mean = typical_sma[index]
        if mean is None:
            continue
        window = typical_prices[index - period + 1 : index + 1]
        mean_deviation = sum(abs(value - mean) for value in window) / period
        if math.isclose(mean_deviation, 0.0):
            result[index] = 0.0
            continue
        result[index] = (typical_prices[index] - mean) / (0.015 * mean_deviation)
    return result


def mfi(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    period: int,
) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if period <= 0 or len(closes) <= period:
        return result
    typical_prices = [(high + low + close) / 3.0 for high, low, close in zip(highs, lows, closes, strict=False)]
    positive_flow = [0.0] * len(closes)
    negative_flow = [0.0] * len(closes)
    for index in range(1, len(closes)):
        money_flow = typical_prices[index] * volumes[index]
        if typical_prices[index] > typical_prices[index - 1]:
            positive_flow[index] = money_flow
        elif typical_prices[index] < typical_prices[index - 1]:
            negative_flow[index] = money_flow
    for index in range(period, len(closes)):
        pos_sum = sum(positive_flow[index - period + 1 : index + 1])
        neg_sum = sum(negative_flow[index - period + 1 : index + 1])
        if math.isclose(neg_sum, 0.0):
            result[index] = 100.0 if pos_sum > 0 else 50.0
            continue
        money_ratio = pos_sum / neg_sum
        result[index] = 100.0 - (100.0 / (1.0 + money_ratio))
    return result


def stochastic_oscillator(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    *,
    k_period: int,
    d_period: int = 3,
    smooth_k: int = 3,
) -> tuple[list[float | None], list[float | None]]:
    raw_k: list[float | None] = [None] * len(closes)
    if k_period <= 0 or d_period <= 0 or smooth_k <= 0:
        return raw_k, [None] * len(closes)
    rolling_low = rolling_min(lows, k_period)
    rolling_high = rolling_max(highs, k_period)
    for index in range(k_period - 1, len(closes)):
        lowest = rolling_low[index]
        highest = rolling_high[index]
        if lowest is None or highest is None:
            continue
        denominator = highest - lowest
        if math.isclose(denominator, 0.0):
            raw_k[index] = 50.0
            continue
        raw_k[index] = ((closes[index] - lowest) / denominator) * 100.0
    percent_k = _sma_optional(raw_k, smooth_k) if smooth_k > 1 else raw_k
    percent_d = _sma_optional(percent_k, d_period)
    return percent_k, percent_d


def _wilder_sum(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0 or len(values) <= period:
        return result
    initial = sum(values[1 : period + 1])
    result[period] = initial
    previous = initial
    for index in range(period + 1, len(values)):
        previous = previous - (previous / period) + values[index]
        result[index] = previous
    return result


def adx(highs: list[float], lows: list[float], closes: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if period <= 0 or len(closes) <= (period * 2):
        return result

    true_range = [0.0] * len(closes)
    plus_dm = [0.0] * len(closes)
    minus_dm = [0.0] * len(closes)
    for index in range(1, len(closes)):
        high_move = highs[index] - highs[index - 1]
        low_move = lows[index - 1] - lows[index]
        plus_dm[index] = high_move if high_move > low_move and high_move > 0 else 0.0
        minus_dm[index] = low_move if low_move > high_move and low_move > 0 else 0.0
        true_range[index] = max(
            highs[index] - lows[index],
            abs(highs[index] - closes[index - 1]),
            abs(lows[index] - closes[index - 1]),
        )

    smoothed_tr = _wilder_sum(true_range, period)
    smoothed_plus_dm = _wilder_sum(plus_dm, period)
    smoothed_minus_dm = _wilder_sum(minus_dm, period)
    dx: list[float | None] = [None] * len(closes)

    for index in range(period, len(closes)):
        current_tr = smoothed_tr[index]
        current_plus_dm = smoothed_plus_dm[index]
        current_minus_dm = smoothed_minus_dm[index]
        if current_tr is None or current_plus_dm is None or current_minus_dm is None or math.isclose(current_tr, 0.0):
            continue
        plus_di = 100.0 * (current_plus_dm / current_tr)
        minus_di = 100.0 * (current_minus_dm / current_tr)
        denominator = plus_di + minus_di
        dx[index] = 0.0 if math.isclose(denominator, 0.0) else 100.0 * abs(plus_di - minus_di) / denominator

    first_adx_index = (period * 2) - 1
    initial_dx = [value for value in dx[period : first_adx_index + 1] if value is not None]
    if len(initial_dx) < period:
        return result
    result[first_adx_index] = sum(initial_dx) / period
    previous_adx = result[first_adx_index]
    for index in range(first_adx_index + 1, len(closes)):
        if dx[index] is None or previous_adx is None:
            continue
        previous_adx = ((previous_adx * (period - 1)) + dx[index]) / period
        result[index] = previous_adx
    return result


def williams_r(highs: list[float], lows: list[float], closes: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if period <= 0:
        return result
    rolling_low = rolling_min(lows, period)
    rolling_high = rolling_max(highs, period)
    for index in range(period - 1, len(closes)):
        lowest = rolling_low[index]
        highest = rolling_high[index]
        if lowest is None or highest is None:
            continue
        denominator = highest - lowest
        if math.isclose(denominator, 0.0):
            result[index] = -50.0
            continue
        result[index] = -100.0 * ((highest - closes[index]) / denominator)
    return result
