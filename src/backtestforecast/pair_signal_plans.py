from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date

from backtestforecast.underlying_rotation import UnderlyingRotationDataset, _PriceHistory


@dataclass(frozen=True, slots=True)
class PairSignalPlan:
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]]


def build_xlf_regime_signal_plan(
    dataset: UnderlyingRotationDataset,
    *,
    pair_symbols: tuple[str, str],
    signal_symbol: str,
    start_date: date,
    end_date: date,
    lookback_days: int,
    rebalance_frequency_days: int,
    neutral_threshold_pct: float,
    positive_signal_short_symbol: str,
    negative_signal_short_symbol: str,
) -> PairSignalPlan:
    signal_history = dataset.histories.get(signal_symbol.upper())
    if signal_history is None:
        raise ValueError(f"Signal history unavailable for {signal_symbol}")
    return _build_directional_signal_plan(
        dataset,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
        rebalance_frequency_days=rebalance_frequency_days,
        neutral_threshold_pct=neutral_threshold_pct,
        positive_score_short_symbol=positive_signal_short_symbol.upper(),
        negative_score_short_symbol=negative_signal_short_symbol.upper(),
        score_getter=lambda signal_date: _lookback_return(signal_history, signal_date, lookback_days),
        pair_symbols=tuple(symbol.upper() for symbol in pair_symbols),
    )


def build_pair_return_spread_signal_plan(
    dataset: UnderlyingRotationDataset,
    *,
    pair_symbols: tuple[str, str],
    start_date: date,
    end_date: date,
    lookback_days: int,
    rebalance_frequency_days: int,
    neutral_threshold_pct: float,
    positive_spread_short_symbol: str,
    negative_spread_short_symbol: str,
) -> PairSignalPlan:
    normalized_pair_symbols = tuple(symbol.upper() for symbol in pair_symbols)
    first_history = dataset.histories.get(normalized_pair_symbols[0])
    second_history = dataset.histories.get(normalized_pair_symbols[1])
    if first_history is None or second_history is None:
        raise ValueError("Pair histories unavailable for spread selector")

    def _score(signal_date: date) -> float | None:
        first_return = _lookback_return(first_history, signal_date, lookback_days)
        second_return = _lookback_return(second_history, signal_date, lookback_days)
        if first_return is None or second_return is None:
            return None
        return first_return - second_return

    return _build_directional_signal_plan(
        dataset,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
        rebalance_frequency_days=rebalance_frequency_days,
        neutral_threshold_pct=neutral_threshold_pct,
        positive_score_short_symbol=positive_spread_short_symbol.upper(),
        negative_score_short_symbol=negative_spread_short_symbol.upper(),
        score_getter=_score,
        pair_symbols=normalized_pair_symbols,
    )


def _build_directional_signal_plan(
    dataset: UnderlyingRotationDataset,
    *,
    start_date: date,
    end_date: date,
    lookback_days: int,
    rebalance_frequency_days: int,
    neutral_threshold_pct: float,
    positive_score_short_symbol: str,
    negative_score_short_symbol: str,
    score_getter,
    pair_symbols: tuple[str, str],
) -> PairSignalPlan:
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    if rebalance_frequency_days < 1:
        raise ValueError("rebalance_frequency_days must be >= 1")
    if neutral_threshold_pct < 0:
        raise ValueError("neutral_threshold_pct must be >= 0")
    if positive_score_short_symbol not in pair_symbols:
        raise ValueError("positive_score_short_symbol must be in pair_symbols")
    if negative_score_short_symbol not in pair_symbols:
        raise ValueError("negative_score_short_symbol must be in pair_symbols")

    trade_dates = [item for item in dataset.trade_dates if start_date <= item <= end_date]
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]] = {}
    if len(trade_dates) < 2:
        return PairSignalPlan(ranked_symbols_by_execution_date)

    for signal_index in range(0, len(trade_dates) - 1, rebalance_frequency_days):
        signal_date = trade_dates[signal_index]
        execution_date = trade_dates[signal_index + 1]
        score = score_getter(signal_date)
        if score is None:
            continue
        if score > neutral_threshold_pct:
            ranked_symbols_by_execution_date[execution_date] = (positive_score_short_symbol,)
        elif score < -neutral_threshold_pct:
            ranked_symbols_by_execution_date[execution_date] = (negative_score_short_symbol,)
        else:
            ranked_symbols_by_execution_date[execution_date] = ()
    return PairSignalPlan(ranked_symbols_by_execution_date)


def _lookback_return(history: _PriceHistory, signal_date: date, lookback_days: int) -> float | None:
    signal_index = bisect_right(history.dates, signal_date) - 1
    if signal_index < 0 or history.dates[signal_index] != signal_date:
        return None
    base_index = signal_index - lookback_days
    if base_index < 0:
        return None
    base_close = history.closes[base_index]
    current_close = history.closes[signal_index]
    if base_close <= 0 or current_close <= 0:
        return None
    return (current_close / base_close) - 1.0
