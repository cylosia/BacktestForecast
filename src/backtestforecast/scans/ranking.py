from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import structlog

from backtestforecast.backtests.strategies.registry import (
    BEARISH_STRATEGIES,
    BULLISH_STRATEGIES,
    NEUTRAL_STRATEGIES,
)
from backtestforecast.backtests.types import BacktestExecutionResult
from backtestforecast.schemas.backtests import EntryRule, rule_bias
from backtestforecast.schemas.scans import (
    HistoricalAnalogForecastResponse,
    HistoricalPerformanceResponse,
    RankingBreakdownResponse,
)

# Current-backtest scoring weights. Net P&L is intentionally omitted because,
# for a fixed account size, it is just ROI in dollar form and would double-
# count the same return signal.
CURRENT_ROI_WEIGHT = 0.40
CURRENT_WIN_RATE_WEIGHT = 0.20
CURRENT_TRADE_COUNT_WEIGHT = 0.15
CURRENT_DRAWDOWN_WEIGHT = 0.25

# Scaling denominators - normalise each metric into roughly [-1, 1] before
# weighting.  Chosen empirically so that "typical" backtest values map to the
# middle of the range.
CURRENT_ROI_SCALE = 30.0          # +/-30% ROI -> +/-1
CURRENT_WIN_RATE_SCALE = 50.0     # Win rate deviation from 50% -> +/-1
CURRENT_NET_PNL_SCALE = 0.20      # Net PnL as fraction of account -> +/-1
CURRENT_TRADE_COUNT_SCALE = 12.0  # 12 trades -> 1 (capped)
CURRENT_DRAWDOWN_SCALE = 30.0     # 30% drawdown -> 1 (capped)


@dataclass(frozen=True, slots=True)
class HistoricalObservation:
    completed_at: datetime
    win_rate: float
    total_roi_pct: float
    max_drawdown_pct: float
    total_net_pnl: float = 0.0


def hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def rule_set_hash(entry_rules: Sequence[EntryRule]) -> str:
    payload = [rule.model_dump(mode="json") for rule in entry_rules]
    return hash_payload({"entry_rules": payload})


def detect_rule_set_bias(entry_rules: Sequence[EntryRule]) -> str | None:
    biases = {bias for bias in (rule_bias(rule) for rule in entry_rules) if bias is not None}
    if len(biases) == 1:
        return next(iter(biases))
    return None


def is_strategy_rule_set_compatible(strategy_type: str, entry_rules: Sequence[EntryRule]) -> bool:
    bias = detect_rule_set_bias(entry_rules)
    if bias is None:
        return True
    if strategy_type in BULLISH_STRATEGIES:
        return bias == "bullish"
    if strategy_type in BEARISH_STRATEGIES:
        return bias == "bearish"
    if strategy_type in NEUTRAL_STRATEGIES:
        structlog.get_logger("scans.ranking").debug(
            "ranking.neutral_with_directional_rules",
            strategy_type=strategy_type,
            rule_set_bias=bias,
        )
        return True
    return True


def aggregate_historical_performance(
    observations: Iterable[HistoricalObservation],
    *,
    reference_time: datetime,
    recency_half_life_days: int = 180,
) -> HistoricalPerformanceResponse:
    recency_half_life_days = max(recency_half_life_days, 1)
    weighted_total = 0.0
    weighted_win_rate = 0.0
    weighted_roi = 0.0
    weighted_drawdown = 0.0
    sample_count = 0
    last_observed_at: datetime | None = None

    for observation in observations:
        if not all(
            math.isfinite(value)
            for value in (
                observation.win_rate,
                observation.total_roi_pct,
                observation.max_drawdown_pct,
            )
        ):
            continue
        age_days = max((reference_time - observation.completed_at).total_seconds() / 86400.0, 0.0)
        weight = math.exp((-math.log(2.0) * age_days) / recency_half_life_days)
        weighted_total += weight
        weighted_win_rate += observation.win_rate * weight
        weighted_roi += observation.total_roi_pct * weight
        weighted_drawdown += observation.max_drawdown_pct * weight
        sample_count += 1
        if last_observed_at is None or observation.completed_at > last_observed_at:
            last_observed_at = observation.completed_at

    if weighted_total <= 0:
        return HistoricalPerformanceResponse(
            sample_count=0,
            effective_sample_size=Decimal("0"),
            recency_half_life_days=recency_half_life_days,
        )

    return HistoricalPerformanceResponse(
        sample_count=sample_count,
        effective_sample_size=_to_decimal(weighted_total),
        weighted_win_rate=_to_decimal(weighted_win_rate / weighted_total),
        weighted_total_roi_pct=_to_decimal(weighted_roi / weighted_total),
        weighted_max_drawdown_pct=_to_decimal(weighted_drawdown / weighted_total),
        recency_half_life_days=recency_half_life_days,
        last_observed_at=last_observed_at,
    )


def build_ranking_breakdown(
    *,
    execution_result: BacktestExecutionResult,
    historical_performance: HistoricalPerformanceResponse,
    forecast: HistoricalAnalogForecastResponse,
    strategy_type: str,
    account_size: float,
) -> RankingBreakdownResponse:
    summary = execution_result.summary
    reasons: list[str] = []

    current_score = (
        (_clamp(summary.total_roi_pct / CURRENT_ROI_SCALE, -1.0, 1.0) * CURRENT_ROI_WEIGHT)
        + (_clamp((summary.win_rate - 50.0) / CURRENT_WIN_RATE_SCALE, -1.0, 1.0) * CURRENT_WIN_RATE_WEIGHT)
        + (_clamp(summary.decided_trades / CURRENT_TRADE_COUNT_SCALE, 0.0, 1.0) * CURRENT_TRADE_COUNT_WEIGHT)
        - (_clamp(summary.max_drawdown_pct / CURRENT_DRAWDOWN_SCALE, 0.0, 1.0) * CURRENT_DRAWDOWN_WEIGHT)
    )
    if summary.decided_trades >= 3:
        reasons.append("Current backtest generated multiple decided trades rather than a single isolated outcome.")
    if summary.max_drawdown_pct <= 15:
        reasons.append("Current backtest stayed within a moderate drawdown band.")

    hist_score = 0.0
    if historical_performance.sample_count > 0:
        effective_sample_size = float(historical_performance.effective_sample_size)
        if effective_sample_size <= 0:
            effective_sample_size = float(historical_performance.sample_count)
        confidence = min(1.0, effective_sample_size / 12.0)
        hist_score = confidence * (
            (_clamp(float(historical_performance.weighted_total_roi_pct) / 25.0, -1.0, 1.0) * 0.40)
            + (_clamp((float(historical_performance.weighted_win_rate) - 50.0) / 50.0, -1.0, 1.0) * 0.25)
            - (_clamp(float(historical_performance.weighted_max_drawdown_pct) / 30.0, 0.0, 1.0) * 0.20)
        )
        reasons.append(
            f"Historical weighting included {historical_performance.sample_count} "
            f"prior comparable scanner outcomes with recency decay."
        )

    forecast_score = _forecast_alignment_score(strategy_type, forecast, reasons)
    final_score = ((current_score * 0.55) + (hist_score * 0.35) + (forecast_score * 0.10)) * 100.0

    return RankingBreakdownResponse(
        current_performance_score=_to_decimal(current_score * 100.0),
        historical_performance_score=_to_decimal(hist_score * 100.0),
        forecast_alignment_score=_to_decimal(forecast_score * 100.0),
        final_score=_to_decimal(final_score),
        reasoning=reasons,
    )


def recommendation_sort_key(item: tuple[str, str, str, RankingBreakdownResponse]) -> tuple[float, str, str, str]:
    symbol, strategy_type, rule_set_name, ranking = item
    return (-float(ranking.final_score), symbol, strategy_type, rule_set_name)


def _forecast_alignment_score(
    strategy_type: str,
    forecast: HistoricalAnalogForecastResponse,
    reasons: list[str],
) -> float:
    if forecast.analog_count == 0:
        reasons.append("No analog forecast data available; forecast alignment excluded from scoring.")
        return 0.0

    median_return = float(forecast.expected_return_median_pct)
    positive_rate = (
        float(forecast.positive_outcome_rate_pct)
        if forecast.positive_outcome_rate_pct is not None
        else None
    )
    dispersion = abs(float(forecast.expected_return_high_pct) - float(forecast.expected_return_low_pct))

    if strategy_type in BULLISH_STRATEGIES:
        alignment = _clamp(median_return / 8.0, -1.0, 1.0)
        if median_return > 0:
            reasons.append("Historical analog forecast leaned upward for this bullish structure.")
    elif strategy_type in BEARISH_STRATEGIES:
        alignment = _clamp((-median_return) / 8.0, -1.0, 1.0)
        if median_return < 0:
            reasons.append("Historical analog forecast leaned downward for this bearish structure.")
    else:
        neutral_bonus = 1.0 - _clamp(abs(median_return) / 6.0, 0.0, 1.0)
        dispersion_penalty = _clamp(dispersion / 25.0, 0.0, 1.0)
        alignment = (neutral_bonus * 0.8) - (dispersion_penalty * 0.4)
        reasons.append("Neutral/volatility structure score favored contained median direction and bounded dispersion.")

    probability_bonus = 0.0
    if positive_rate is not None and strategy_type not in NEUTRAL_STRATEGIES:
        probability_bonus = _clamp((positive_rate - 50.0) / 50.0, -1.0, 1.0)
    return (alignment * 0.7) + (probability_bonus * 0.3)


def _clamp(value: float, lower: float, upper: float) -> float:
    if not math.isfinite(value):
        structlog.get_logger("scans.ranking").warning(
            "ranking.non_finite_value_clamped", value=str(value),
        )
        return 0.0
    return max(lower, min(upper, value))


_QUANT = Decimal("0.0001")


def _to_decimal(value: float) -> Decimal:
    if not math.isfinite(value):
        return Decimal("0")
    return Decimal(str(value)).quantize(_QUANT, rounding=ROUND_HALF_UP)
