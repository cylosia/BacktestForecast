from __future__ import annotations

import math
from collections import deque


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
    """O(n) rolling standard deviation using an online sliding window.

    Args:
        ddof: Delta degrees of freedom. Use 0 for population stddev (Bollinger Bands),
              1 for sample stddev (realized volatility).
    """
    result: list[float | None] = [None] * len(values)
    if period <= 1 or period <= ddof:
        return result
    sum_x = 0.0
    sum_x2 = 0.0
    for index in range(len(values)):
        sum_x += values[index]
        sum_x2 += values[index] * values[index]
        if index >= period:
            old = values[index - period]
            sum_x -= old
            sum_x2 -= old * old
        if index >= period - 1:
            mean = sum_x / period
            variance = (sum_x2 - period * mean * mean) / (period - ddof)
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
    macd_seed = [v if v is not None else 0.0 for v in macd_line[first_valid:]]
    raw_signal = ema(macd_seed, signal_period)
    signal_line: list[float | None] = [None] * first_valid + raw_signal
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
