from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterable, Sequence

from backtestforecast.backtests.types import BacktestExecutionResult
from backtestforecast.schemas.backtests import EntryRule, rule_bias
from backtestforecast.schemas.scans import (
    HistoricalAnalogForecastResponse,
    HistoricalPerformanceResponse,
    RankingBreakdownResponse,
)

BULLISH_STRATEGIES = {
    "long_call",
    "covered_call",
    "cash_secured_put",
    "bull_call_debit_spread",
    "bull_put_credit_spread",
    "wheel_strategy",
    "poor_mans_covered_call",
    "collar",
    "diagonal_spread",
}
BEARISH_STRATEGIES = {
    "long_put",
    "bear_put_debit_spread",
    "bear_call_credit_spread",
    "synthetic_put",
    "ratio_put_backspread",
}
NEUTRAL_STRATEGIES = {
    "iron_condor",
    "iron_butterfly",
    "short_straddle",
    "short_strangle",
    "long_straddle",
    "long_strangle",
    "butterfly",
    "calendar_spread",
    "double_diagonal",
    "covered_strangle",
    "jade_lizard",
}


@dataclass(frozen=True, slots=True)
class HistoricalObservation:
    completed_at: datetime
    win_rate: float
    total_roi_pct: float
    total_net_pnl: float
    max_drawdown_pct: float


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
        return False
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
    weighted_net_pnl = 0.0
    weighted_drawdown = 0.0
    sample_count = 0
    last_observed_at: datetime | None = None

    for observation in observations:
        age_days = max((reference_time - observation.completed_at).total_seconds() / 86400.0, 0.0)
        weight = math.exp((-math.log(2.0) * age_days) / recency_half_life_days)
        weighted_total += weight
        weighted_win_rate += observation.win_rate * weight
        weighted_roi += observation.total_roi_pct * weight
        weighted_net_pnl += observation.total_net_pnl * weight
        weighted_drawdown += observation.max_drawdown_pct * weight
        sample_count += 1
        if last_observed_at is None or observation.completed_at > last_observed_at:
            last_observed_at = observation.completed_at

    if weighted_total <= 0:
        return HistoricalPerformanceResponse(sample_count=0, recency_half_life_days=recency_half_life_days)

    return HistoricalPerformanceResponse(
        sample_count=sample_count,
        weighted_win_rate=_to_decimal(weighted_win_rate / weighted_total),
        weighted_total_roi_pct=_to_decimal(weighted_roi / weighted_total),
        weighted_total_net_pnl=_to_decimal(weighted_net_pnl / weighted_total),
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
        (_clamp(summary.total_roi_pct / 30.0, -1.0, 1.0) * 0.35)
        + (_clamp((summary.win_rate - 50.0) / 50.0, -1.0, 1.0) * 0.15)
        + (_clamp((summary.total_net_pnl / max(account_size, 1.0)) / 0.20, -1.0, 1.0) * 0.20)
        + (_clamp(summary.trade_count / 12.0, 0.0, 1.0) * 0.10)
        - (_clamp(summary.max_drawdown_pct / 30.0, 0.0, 1.0) * 0.20)
    )
    if summary.trade_count >= 3:
        reasons.append("Current backtest generated multiple trades rather than a single isolated outcome.")
    if summary.max_drawdown_pct <= 15:
        reasons.append("Current backtest stayed within a moderate drawdown band.")

    hist_score = 0.0
    if historical_performance.sample_count > 0:
        confidence = min(1.0, historical_performance.sample_count / 12.0)
        hist_score = confidence * (
            (_clamp(float(historical_performance.weighted_total_roi_pct) / 25.0, -1.0, 1.0) * 0.40)
            + (_clamp((float(historical_performance.weighted_win_rate) - 50.0) / 50.0, -1.0, 1.0) * 0.25)
            + (
                _clamp(float(historical_performance.weighted_total_net_pnl) / max(account_size * 0.15, 1.0), -1.0, 1.0)
                * 0.15
            )
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
    median_return = float(forecast.expected_return_median_pct)
    positive_rate = float(forecast.positive_outcome_rate_pct)
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

    if strategy_type in BEARISH_STRATEGIES:
        probability_bonus = _clamp(((100.0 - positive_rate) - 50.0) / 50.0, -1.0, 1.0)
    else:
        probability_bonus = _clamp((positive_rate - 50.0) / 50.0, -1.0, 1.0)
    return (alignment * 0.7) + (probability_bonus * 0.3)


def _clamp(value: float, lower: float, upper: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(lower, min(upper, value))


_QUANT = Decimal("0.0001")


def _to_decimal(value: float) -> Decimal:
    if not math.isfinite(value):
        return Decimal("0")
    return Decimal(str(value)).quantize(_QUANT, rounding=ROUND_HALF_UP)
