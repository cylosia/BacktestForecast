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
