from __future__ import annotations

import argparse
import heapq
import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy import func

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import ema, rolling_stddev  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
import grid_search_weekly_calendar_policy_two_stage as two_stage  # noqa: E402


DEFAULT_MIN_START_DATE = date(2015, 1, 1)
DEFAULT_REQUESTED_END_DATE = date(2026, 4, 2)
DEFAULT_TOP_K = 10
DEFAULT_OBJECTIVE = "macro_f1"
DEFAULT_EMA_GAP_THRESHOLD_PCTS = "none,0.5"
DEFAULT_HEAVY_VOL_THRESHOLD_PCTS = "none,25"
DEFAULT_MIN_PREDICTED_REGIME_COUNT = 10
DEFAULT_REQUIRE_MONOTONIC_FORWARD_RETURNS = False
DEFAULT_CONFIDENCE_THRESHOLDS = "0,0.25,0.5,0.75,1.0,1.25,1.5,2.0"
REGIME_LABELS = ("heavy_bullish", "bullish", "neutral", "bearish", "heavy_bearish")
DIRECTIONAL_REGIME_LABELS = ("heavy_bullish", "bullish", "bearish", "heavy_bearish")
MONOTONIC_REGIME_ORDER = ("heavy_bearish", "bearish", "neutral", "bullish", "heavy_bullish")
OBJECTIVE_FIELD_MAP = {
    "exact_accuracy": "exact_accuracy_pct",
    "directional_accuracy": "directional_accuracy_pct",
    "balanced_accuracy": "balanced_accuracy_pct",
    "macro_f1": "macro_f1_pct",
    "precision_first": "macro_precision_pct",
}
_DIRECTION_BUCKETS = {
    "heavy_bullish": 1,
    "bullish": 1,
    "neutral": 0,
    "bearish": -1,
    "heavy_bearish": -1,
}


@dataclass(frozen=True, slots=True)
class LabelThresholdConfig:
    neutral_move_pct: float
    heavy_move_pct: float

    @property
    def label(self) -> str:
        return f"n{self.neutral_move_pct:g}_h{self.heavy_move_pct:g}"


@dataclass(frozen=True, slots=True)
class FeatureGateConfig:
    ema_gap_threshold_pct: float | None = None
    heavy_vol_threshold_pct: float | None = None

    @property
    def label(self) -> str:
        ema_part = "none" if self.ema_gap_threshold_pct is None else f"{self.ema_gap_threshold_pct:g}"
        vol_part = "none" if self.heavy_vol_threshold_pct is None else f"{self.heavy_vol_threshold_pct:g}"
        return f"ema{ema_part}_hvol{vol_part}"


@dataclass(frozen=True, slots=True)
class PredictionCandidate:
    indicator_periods: two_stage.IndicatorPeriodConfig
    bull_filter: object
    bear_filter: object
    threshold_config: LabelThresholdConfig
    feature_gate: FeatureGateConfig
    metrics: dict[str, object]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate the five-regime weekly-calendar classifier as a forward price-direction predictor "
            "for one or more symbols."
        )
    )
    parser.add_argument("--symbols", nargs="*", help="Optional explicit symbol list.")
    parser.add_argument("--symbols-file", type=Path, help="Optional newline/comma separated symbol file.")
    parser.add_argument(
        "--min-start-date",
        type=date.fromisoformat,
        default=DEFAULT_MIN_START_DATE,
        help="Earliest start date to use for each symbol. Defaults to 2015-01-01.",
    )
    parser.add_argument(
        "--requested-end-date",
        type=date.fromisoformat,
        default=DEFAULT_REQUESTED_END_DATE,
        help="Requested end date. Defaults to 2026-04-02.",
    )
    parser.add_argument(
        "--forward-weeks",
        type=int,
        default=1,
        help="How many Friday-to-Friday steps ahead to score. Defaults to 1.",
    )
    parser.add_argument(
        "--neutral-move-pct",
        type=float,
        default=1.0,
        help="Fallback neutral threshold when --neutral-move-pcts is not supplied. Defaults to 1.0.",
    )
    parser.add_argument(
        "--heavy-move-pct",
        type=float,
        default=3.0,
        help="Fallback heavy threshold when --heavy-move-pcts is not supplied. Defaults to 3.0.",
    )
    parser.add_argument(
        "--neutral-move-pcts",
        help="Optional comma-separated neutral thresholds to sweep. Example: 0.75,1.0,1.25",
    )
    parser.add_argument(
        "--heavy-move-pcts",
        help="Optional comma-separated heavy thresholds to sweep. Example: 2.5,3.0,3.5",
    )
    parser.add_argument(
        "--ema-gap-threshold-pcts",
        default=DEFAULT_EMA_GAP_THRESHOLD_PCTS,
        help=(
            "Comma-separated EMA8/EMA21 gap thresholds for bull/bear gating. "
            "Use 'none' to include an ungated variant. Defaults to none,0.5."
        ),
    )
    parser.add_argument(
        "--heavy-vol-threshold-pcts",
        default=DEFAULT_HEAVY_VOL_THRESHOLD_PCTS,
        help=(
            "Comma-separated realized-vol thresholds for heavy-regime gating. "
            "Use 'none' to include an ungated variant. Defaults to none,25."
        ),
    )
    parser.add_argument(
        "--objective",
        choices=tuple(OBJECTIVE_FIELD_MAP),
        default=DEFAULT_OBJECTIVE,
        help="Primary ranking metric. Defaults to macro_f1.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help="How many top rows per symbol to keep in the output. Defaults to 10.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=52,
        help="Minimum number of scored Fridays required per symbol. Defaults to 52.",
    )
    parser.add_argument(
        "--min-predicted-regime-count",
        type=int,
        default=DEFAULT_MIN_PREDICTED_REGIME_COUNT,
        help=(
            "Minimum predicted count required for each regime before a candidate is considered "
            "constraint-passing. Defaults to 10."
        ),
    )
    parser.add_argument(
        "--abstention-thresholds",
        default=DEFAULT_CONFIDENCE_THRESHOLDS,
        help=(
            "Comma-separated confidence thresholds to evaluate on the selected best config. "
            "Defaults to 0,0.25,0.5,0.75,1.0,1.25,1.5,2.0."
        ),
    )
    parser.add_argument(
        "--abstention-min-trades",
        type=int,
        default=30,
        help=(
            "Minimum directional predictions required for a confidence threshold to be considered "
            "a recommended abstention setting. Defaults to 30."
        ),
    )
    parser.set_defaults(require_monotonic_forward_returns=DEFAULT_REQUIRE_MONOTONIC_FORWARD_RETURNS)
    monotonic_group = parser.add_mutually_exclusive_group()
    monotonic_group.add_argument(
        "--allow-non-monotonic-forward-returns",
        dest="require_monotonic_forward_returns",
        action="store_false",
        help=(
            "Allow non-monotonic average forward returns across predicted regimes. "
            "This is now the default."
        ),
    )
    monotonic_group.add_argument(
        "--require-monotonic-forward-returns",
        dest="require_monotonic_forward_returns",
        action="store_true",
        help=(
            "Require candidates whose average forward returns increase "
            "from heavy_bearish through heavy_bullish."
        ),
    )
    parser.add_argument(
        "--indicator-workers",
        type=int,
        default=two_stage.DEFAULT_INDICATOR_WORKERS,
        help="Thread count for uncached indicator loading. Defaults to the weekly-calendar setting.",
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable indicator-cache reuse.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional JSON output path. Defaults to logs/five_regime_price_predictions_*.json.",
    )
    return parser.parse_args()


def _load_symbols(args: argparse.Namespace) -> list[str]:
    raw_symbols: list[str] = []
    if args.symbols:
        raw_symbols.extend(args.symbols)
    if args.symbols_file:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        for chunk in raw_text.replace("\n", ",").split(","):
            item = chunk.strip().upper()
            if item:
                raw_symbols.append(item)
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    if not ordered:
        raise SystemExit("No symbols supplied.")
    return ordered


def _discover_symbols(
    *,
    min_start_date: date,
    requested_end_date: date,
) -> list[str]:
    with create_readonly_session() as session:
        rows = (
            session.query(HistoricalUnderlyingDayBar.symbol)
            .filter(
                HistoricalUnderlyingDayBar.trade_date >= min_start_date,
                HistoricalUnderlyingDayBar.trade_date <= requested_end_date,
            )
            .distinct()
            .order_by(HistoricalUnderlyingDayBar.symbol)
            .all()
        )
    return [str(symbol) for (symbol,) in rows if symbol]


def _resolve_symbol_start_dates(
    *,
    symbols: list[str],
    min_start_date: date,
    requested_end_date: date,
) -> dict[str, date]:
    with create_readonly_session() as session:
        rows = (
            session.query(
                HistoricalUnderlyingDayBar.symbol,
                func.min(HistoricalUnderlyingDayBar.trade_date),
            )
            .filter(
                HistoricalUnderlyingDayBar.symbol.in_(symbols),
                HistoricalUnderlyingDayBar.trade_date >= min_start_date,
                HistoricalUnderlyingDayBar.trade_date <= requested_end_date,
            )
            .group_by(HistoricalUnderlyingDayBar.symbol)
            .all()
        )
    return {
        str(symbol): max(min_start_date, earliest_trade_date)
        for symbol, earliest_trade_date in rows
        if earliest_trade_date is not None
    }


def _default_output_path(*, min_start_date: date, requested_end_date: date, forward_weeks: int, objective: str) -> Path:
    return (
        ROOT
        / "logs"
        / (
            "five_regime_price_predictions_"
            f"w{forward_weeks}_{min_start_date.isoformat()}_{requested_end_date.isoformat()}_{objective}.json"
        )
    )


def _dedupe_preserve_order[T](values: tuple[T, ...]) -> tuple[T, ...]:
    ordered: list[T] = []
    seen: set[T] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _parse_float_csv(raw_value: str) -> tuple[float, ...]:
    values: list[float] = []
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(float(item))
    if not values:
        raise argparse.ArgumentTypeError("At least one numeric value is required.")
    return _dedupe_preserve_order(tuple(values))


def _parse_optional_float_csv(raw_value: str) -> tuple[float | None, ...]:
    values: list[float | None] = []
    for chunk in raw_value.split(","):
        item = chunk.strip().lower()
        if not item:
            continue
        if item == "none":
            values.append(None)
        else:
            values.append(float(item))
    if not values:
        raise argparse.ArgumentTypeError("At least one value is required.")
    return _dedupe_preserve_order(tuple(values))


def _build_label_threshold_configs(args: argparse.Namespace) -> tuple[LabelThresholdConfig, ...]:
    neutral_values = (
        _parse_float_csv(args.neutral_move_pcts)
        if args.neutral_move_pcts
        else (float(args.neutral_move_pct),)
    )
    heavy_values = (
        _parse_float_csv(args.heavy_move_pcts)
        if args.heavy_move_pcts
        else (float(args.heavy_move_pct),)
    )
    configs = tuple(
        LabelThresholdConfig(neutral_move_pct=neutral_value, heavy_move_pct=heavy_value)
        for neutral_value in neutral_values
        for heavy_value in heavy_values
        if neutral_value >= 0 and heavy_value > neutral_value
    )
    if not configs:
        raise SystemExit("No valid threshold pairs were supplied. Each heavy threshold must be greater than neutral.")
    return _dedupe_preserve_order(configs)


def _build_feature_gate_configs(args: argparse.Namespace) -> tuple[FeatureGateConfig, ...]:
    ema_gap_thresholds = _parse_optional_float_csv(args.ema_gap_threshold_pcts)
    heavy_vol_thresholds = _parse_optional_float_csv(args.heavy_vol_threshold_pcts)
    configs = tuple(
        FeatureGateConfig(
            ema_gap_threshold_pct=ema_gap_threshold_pct,
            heavy_vol_threshold_pct=heavy_vol_threshold_pct,
        )
        for ema_gap_threshold_pct in ema_gap_thresholds
        for heavy_vol_threshold_pct in heavy_vol_thresholds
    )
    return _dedupe_preserve_order(configs)


def _float_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _float_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100.0, 4)


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _positive_margin_score(value: float | None, threshold: float, scale: float) -> float:
    if value is None:
        return 0.0
    if scale <= 0:
        raise ValueError("scale must be positive.")
    return max(0.0, (float(value) - threshold) / scale)


def _negative_margin_score(value: float | None, threshold: float, scale: float) -> float:
    if value is None:
        return 0.0
    if scale <= 0:
        raise ValueError("scale must be positive.")
    return max(0.0, (threshold - float(value)) / scale)


def _bull_confidence_score(
    *,
    filter_config: object,
    indicator_triplet: tuple[float | None, float | None, float | None],
    heavy: bool = False,
) -> float:
    roc_value, adx_value, rsi_value = indicator_triplet
    roc_threshold = float(filter_config.roc_threshold) + (two_stage.HEAVY_ROC_BUFFER if heavy else 0.0)
    adx_threshold = float(filter_config.adx_threshold) + (two_stage.HEAVY_ADX_BUFFER if heavy else 0.0)
    rsi_threshold = (
        None
        if getattr(filter_config, "rsi_threshold", None) is None
        else float(filter_config.rsi_threshold) + (two_stage.HEAVY_RSI_BUFFER if heavy else 0.0)
    )
    roc_score = _positive_margin_score(roc_value, roc_threshold, two_stage.HEAVY_ROC_BUFFER)
    adx_score = _positive_margin_score(adx_value, adx_threshold, two_stage.HEAVY_ADX_BUFFER)
    if rsi_threshold is None:
        qualifier_score = adx_score
    else:
        qualifier_score = max(
            adx_score,
            _positive_margin_score(rsi_value, rsi_threshold, two_stage.HEAVY_RSI_BUFFER),
        )
    if roc_score <= 0.0 or qualifier_score <= 0.0:
        return 0.0
    return min(roc_score, qualifier_score)


def _bear_confidence_score(
    *,
    filter_config: object,
    indicator_triplet: tuple[float | None, float | None, float | None],
    heavy: bool = False,
) -> float:
    roc_value, adx_value, rsi_value = indicator_triplet
    roc_threshold = float(filter_config.roc_threshold) - (two_stage.HEAVY_ROC_BUFFER if heavy else 0.0)
    adx_threshold = float(filter_config.adx_threshold) + (two_stage.HEAVY_ADX_BUFFER if heavy else 0.0)
    rsi_threshold = (
        None
        if getattr(filter_config, "rsi_threshold", None) is None
        else float(filter_config.rsi_threshold) - (two_stage.HEAVY_RSI_BUFFER if heavy else 0.0)
    )
    roc_score = _negative_margin_score(roc_value, roc_threshold, two_stage.HEAVY_ROC_BUFFER)
    adx_score = _positive_margin_score(adx_value, adx_threshold, two_stage.HEAVY_ADX_BUFFER)
    if rsi_threshold is None:
        qualifier_score = adx_score
    else:
        qualifier_score = max(
            adx_score,
            _negative_margin_score(rsi_value, rsi_threshold, two_stage.HEAVY_RSI_BUFFER),
        )
    if roc_score <= 0.0 or qualifier_score <= 0.0:
        return 0.0
    return min(roc_score, qualifier_score)


def _feature_gate_confidence_scores(
    *,
    feature_gate: FeatureGateConfig,
    context_row: dict[str, float | None] | None,
) -> dict[str, float]:
    context_row = context_row or {}
    ema_gap_pct = context_row.get("ema_gap_pct")
    realized_vol_pct = context_row.get("realized_vol_pct")
    ema_scale = (
        max(abs(float(feature_gate.ema_gap_threshold_pct)), 0.5)
        if feature_gate.ema_gap_threshold_pct is not None
        else 0.5
    )
    heavy_vol_scale = (
        max(abs(float(feature_gate.heavy_vol_threshold_pct)), 25.0)
        if feature_gate.heavy_vol_threshold_pct is not None
        else 25.0
    )
    return {
        "bull_ema": (
            math.inf
            if feature_gate.ema_gap_threshold_pct is None
            else _positive_margin_score(ema_gap_pct, float(feature_gate.ema_gap_threshold_pct), ema_scale)
        ),
        "bear_ema": (
            math.inf
            if feature_gate.ema_gap_threshold_pct is None
            else _negative_margin_score(ema_gap_pct, -float(feature_gate.ema_gap_threshold_pct), ema_scale)
        ),
        "heavy_vol": (
            math.inf
            if feature_gate.heavy_vol_threshold_pct is None
            else _positive_margin_score(realized_vol_pct, float(feature_gate.heavy_vol_threshold_pct), heavy_vol_scale)
        ),
    }


def _classify_regime_with_confidence(
    *,
    indicator_triplet: tuple[float | None, float | None, float | None],
    bull_filter: object,
    bear_filter: object,
    feature_gate: FeatureGateConfig,
    context_row: dict[str, float | None] | None,
) -> tuple[str, float]:
    gate_scores = _feature_gate_confidence_scores(feature_gate=feature_gate, context_row=context_row)
    bull_score = min(
        _bull_confidence_score(filter_config=bull_filter, indicator_triplet=indicator_triplet),
        gate_scores["bull_ema"],
    )
    bear_score = min(
        _bear_confidence_score(filter_config=bear_filter, indicator_triplet=indicator_triplet),
        gate_scores["bear_ema"],
    )
    heavy_bull_score = min(
        _bull_confidence_score(filter_config=bull_filter, indicator_triplet=indicator_triplet, heavy=True),
        gate_scores["bull_ema"],
        gate_scores["heavy_vol"],
    )
    heavy_bear_score = min(
        _bear_confidence_score(filter_config=bear_filter, indicator_triplet=indicator_triplet, heavy=True),
        gate_scores["bear_ema"],
        gate_scores["heavy_vol"],
    )
    if bull_score > 0.0 and bear_score <= 0.0:
        if heavy_bull_score > 0.0:
            return ("heavy_bullish", heavy_bull_score)
        return ("bullish", bull_score)
    if bear_score > 0.0 and bull_score <= 0.0:
        if heavy_bear_score > 0.0:
            return ("heavy_bearish", heavy_bear_score)
        return ("bearish", bear_score)
    return ("neutral", 0.0)


def _prediction_series_with_confidence(
    *,
    evaluation_dates: list[date],
    indicator_triplets: list[tuple[float | None, float | None, float | None]],
    context_by_date: dict[date, dict[str, float | None]],
    bull_filter: object,
    bear_filter: object,
    feature_gate: FeatureGateConfig,
) -> dict[str, object]:
    predicted_regimes: list[str] = []
    confidence_scores: list[float] = []
    for trade_date, indicator_triplet in zip(evaluation_dates, indicator_triplets, strict=True):
        regime, confidence = _classify_regime_with_confidence(
            indicator_triplet=indicator_triplet,
            bull_filter=bull_filter,
            bear_filter=bear_filter,
            feature_gate=feature_gate,
            context_row=context_by_date.get(trade_date),
        )
        predicted_regimes.append(regime)
        confidence_scores.append(round(float(confidence), 6))
    return {
        "evaluation_dates": evaluation_dates,
        "predicted_regimes": predicted_regimes,
        "confidence_scores": confidence_scores,
    }


def _directional_subset_metrics(
    *,
    predicted_regimes: list[str],
    actual_regimes: list[str],
    confidence_scores: list[float],
    threshold: float,
) -> dict[str, object]:
    if len(predicted_regimes) != len(actual_regimes) or len(predicted_regimes) != len(confidence_scores):
        raise ValueError("prediction, actual, and confidence series must have the same length.")
    retained_indices = [
        index
        for index, (predicted_regime, confidence_score) in enumerate(zip(predicted_regimes, confidence_scores, strict=True))
        if predicted_regime != "neutral" and confidence_score >= threshold
    ]
    trade_count = len(retained_indices)
    observation_count = len(predicted_regimes)
    coverage_pct = _pct(trade_count, observation_count)
    if trade_count == 0:
        return {
            "confidence_threshold": round(threshold, 4),
            "trade_count": 0,
            "coverage_pct": coverage_pct,
            "exact_accuracy_pct": 0.0,
            "directional_accuracy_pct": 0.0,
            "macro_precision_pct": 0.0,
            "average_confidence": None,
            "median_confidence": None,
            "predicted_counts": {label: 0 for label in DIRECTIONAL_REGIME_LABELS},
            "precision_by_label_pct": {label: 0.0 for label in DIRECTIONAL_REGIME_LABELS},
            "best_bucket": None,
            "best_bucket_precision_pct": None,
            "best_bucket_count": 0,
        }

    predicted_subset = [predicted_regimes[index] for index in retained_indices]
    actual_subset = [actual_regimes[index] for index in retained_indices]
    confidence_subset = [confidence_scores[index] for index in retained_indices]
    predicted_counts = {
        label: sum(1 for predicted_regime in predicted_subset if predicted_regime == label)
        for label in DIRECTIONAL_REGIME_LABELS
    }
    precision_by_label_pct: dict[str, float] = {}
    exact_hit_count = 0
    directional_hit_count = 0
    for predicted_regime, actual_regime in zip(predicted_subset, actual_subset, strict=True):
        if predicted_regime == actual_regime:
            exact_hit_count += 1
        if _DIRECTION_BUCKETS[predicted_regime] == _DIRECTION_BUCKETS[actual_regime]:
            directional_hit_count += 1
    for label in DIRECTIONAL_REGIME_LABELS:
        predicted_count = predicted_counts[label]
        exact_count = sum(
            1
            for predicted_regime, actual_regime in zip(predicted_subset, actual_subset, strict=True)
            if predicted_regime == label and actual_regime == label
        )
        precision_by_label_pct[label] = round(_safe_ratio(exact_count, predicted_count) * 100.0, 4)
    active_labels = [label for label in DIRECTIONAL_REGIME_LABELS if predicted_counts[label] > 0]
    macro_precision_pct = round(
        (
            sum(precision_by_label_pct[label] for label in active_labels) / len(active_labels)
            if active_labels
            else 0.0
        ),
        4,
    )
    best_bucket = None
    best_bucket_precision_pct = None
    best_bucket_count = 0
    if active_labels:
        best_bucket = max(
            active_labels,
            key=lambda label: (precision_by_label_pct[label], predicted_counts[label], label),
        )
        best_bucket_precision_pct = precision_by_label_pct[best_bucket]
        best_bucket_count = predicted_counts[best_bucket]
    return {
        "confidence_threshold": round(threshold, 4),
        "trade_count": trade_count,
        "coverage_pct": coverage_pct,
        "exact_accuracy_pct": round(exact_hit_count / trade_count * 100.0, 4),
        "directional_accuracy_pct": round(directional_hit_count / trade_count * 100.0, 4),
        "macro_precision_pct": macro_precision_pct,
        "average_confidence": round(sum(confidence_subset) / len(confidence_subset), 4),
        "median_confidence": round(float(_float_median(confidence_subset) or 0.0), 4),
        "predicted_counts": predicted_counts,
        "precision_by_label_pct": precision_by_label_pct,
        "best_bucket": best_bucket,
        "best_bucket_precision_pct": best_bucket_precision_pct,
        "best_bucket_count": best_bucket_count,
    }


def _best_abstention_row(
    *,
    rows: list[dict[str, object]],
    min_trades: int,
    metric: str,
) -> dict[str, object] | None:
    eligible_rows = [row for row in rows if int(row["trade_count"]) >= min_trades]
    if not eligible_rows:
        return None
    if metric == "macro_precision":
        return max(
            eligible_rows,
            key=lambda row: (
                float(row["macro_precision_pct"]),
                float(row["exact_accuracy_pct"]),
                float(row["trade_count"]),
                -float(row["confidence_threshold"]),
            ),
        )
    return max(
        eligible_rows,
        key=lambda row: (
            float(row["exact_accuracy_pct"]),
            float(row["directional_accuracy_pct"]),
            float(row["trade_count"]),
            -float(row["confidence_threshold"]),
        ),
    )


def _realized_regime_label(
    *,
    forward_return_pct: float,
    neutral_move_pct: float,
    heavy_move_pct: float,
) -> str:
    if forward_return_pct >= heavy_move_pct:
        return "heavy_bullish"
    if forward_return_pct > neutral_move_pct:
        return "bullish"
    if forward_return_pct <= -heavy_move_pct:
        return "heavy_bearish"
    if forward_return_pct < -neutral_move_pct:
        return "bearish"
    return "neutral"


def _predicted_regimes_from_masks(
    *,
    observation_count: int,
    bull_mask: int,
    bear_mask: int,
    heavy_bull_mask: int,
    heavy_bear_mask: int,
) -> list[str]:
    bull_only_mask = bull_mask & ~bear_mask
    bear_only_mask = bear_mask & ~bull_mask
    heavy_bull_only_mask = heavy_bull_mask & bull_only_mask
    heavy_bear_only_mask = heavy_bear_mask & bear_only_mask
    regular_bull_mask = bull_only_mask & ~heavy_bull_only_mask
    regular_bear_mask = bear_only_mask & ~heavy_bear_only_mask
    all_dates_mask = (1 << observation_count) - 1
    neutral_mask = all_dates_mask & ~(heavy_bull_only_mask | regular_bull_mask | regular_bear_mask | heavy_bear_only_mask)

    predicted = ["neutral"] * observation_count
    for trade_index in range(observation_count):
        bit_mask = 1 << trade_index
        if heavy_bull_only_mask & bit_mask:
            predicted[trade_index] = "heavy_bullish"
        elif regular_bull_mask & bit_mask:
            predicted[trade_index] = "bullish"
        elif regular_bear_mask & bit_mask:
            predicted[trade_index] = "bearish"
        elif heavy_bear_only_mask & bit_mask:
            predicted[trade_index] = "heavy_bearish"
        elif neutral_mask & bit_mask:
            predicted[trade_index] = "neutral"
    return predicted


def _score_predictions(
    *,
    predicted_regimes: list[str],
    actual_regimes: list[str],
    forward_returns_pct: list[float],
) -> dict[str, object]:
    if not predicted_regimes or len(predicted_regimes) != len(actual_regimes):
        raise ValueError("Predicted and actual regime lists must have the same non-zero length.")

    predicted_counts = {label: 0 for label in REGIME_LABELS}
    actual_counts = {label: 0 for label in REGIME_LABELS}
    true_positives = {label: 0 for label in REGIME_LABELS}
    forward_returns_by_prediction = {label: [] for label in REGIME_LABELS}

    exact_hit_count = 0
    directional_hit_count = 0
    for predicted_label, actual_label, forward_return_pct in zip(
        predicted_regimes,
        actual_regimes,
        forward_returns_pct,
        strict=True,
    ):
        predicted_counts[predicted_label] += 1
        actual_counts[actual_label] += 1
        forward_returns_by_prediction[predicted_label].append(forward_return_pct)
        if predicted_label == actual_label:
            exact_hit_count += 1
            true_positives[predicted_label] += 1
        if _DIRECTION_BUCKETS[predicted_label] == _DIRECTION_BUCKETS[actual_label]:
            directional_hit_count += 1

    active_labels = [label for label in REGIME_LABELS if predicted_counts[label] > 0 or actual_counts[label] > 0]
    labels_with_actuals = [label for label in REGIME_LABELS if actual_counts[label] > 0]

    precision_by_label: dict[str, float] = {}
    recall_by_label: dict[str, float] = {}
    f1_by_label: dict[str, float] = {}
    for label in active_labels:
        precision = _safe_ratio(true_positives[label], predicted_counts[label])
        recall = _safe_ratio(true_positives[label], actual_counts[label])
        f1 = 0.0 if precision + recall == 0.0 else (2.0 * precision * recall) / (precision + recall)
        precision_by_label[label] = round(precision * 100.0, 4)
        recall_by_label[label] = round(recall * 100.0, 4)
        f1_by_label[label] = round(f1 * 100.0, 4)

    macro_precision_pct = round(
        (
            sum(precision_by_label[label] for label in active_labels) / len(active_labels)
            if active_labels
            else 0.0
        ),
        4,
    )
    macro_recall_pct = round(
        (
            sum(recall_by_label[label] for label in active_labels) / len(active_labels)
            if active_labels
            else 0.0
        ),
        4,
    )
    macro_f1_pct = round(
        (
            sum(f1_by_label[label] for label in active_labels) / len(active_labels)
            if active_labels
            else 0.0
        ),
        4,
    )
    balanced_accuracy_pct = round(
        (
            sum(recall_by_label[label] for label in labels_with_actuals) / len(labels_with_actuals)
            if labels_with_actuals
            else 0.0
        ),
        4,
    )

    observation_count = len(actual_regimes)
    return {
        "observation_count": observation_count,
        "exact_hit_count": exact_hit_count,
        "exact_accuracy_pct": _pct(exact_hit_count, observation_count),
        "directional_hit_count": directional_hit_count,
        "directional_accuracy_pct": _pct(directional_hit_count, observation_count),
        "macro_precision_pct": macro_precision_pct,
        "macro_recall_pct": macro_recall_pct,
        "macro_f1_pct": macro_f1_pct,
        "balanced_accuracy_pct": balanced_accuracy_pct,
        "predicted_counts": predicted_counts,
        "actual_counts": actual_counts,
        "precision_by_label_pct": precision_by_label,
        "recall_by_label_pct": recall_by_label,
        "f1_by_label_pct": f1_by_label,
        "average_forward_return_by_predicted_regime_pct": {
            label: (
                None
                if (mean_value := _float_mean(forward_returns_by_prediction[label])) is None
                else round(mean_value, 4)
            )
            for label in REGIME_LABELS
        },
    }


def _is_monotonic_forward_return_profile(average_returns_by_regime: dict[str, float | None]) -> bool:
    prior_value: float | None = None
    for regime in MONOTONIC_REGIME_ORDER:
        value = average_returns_by_regime.get(regime)
        if value is None:
            return False
        numeric_value = float(value)
        if prior_value is not None and numeric_value <= prior_value:
            return False
        prior_value = numeric_value
    return True


def _candidate_constraint_summary(
    *,
    metrics: dict[str, object],
    min_predicted_regime_count: int,
    require_monotonic_forward_returns: bool,
) -> dict[str, object]:
    predicted_counts = metrics["predicted_counts"]
    assert isinstance(predicted_counts, dict)
    min_predicted_regime_count_passed = (
        all(int(predicted_counts[label]) >= min_predicted_regime_count for label in REGIME_LABELS)
        if min_predicted_regime_count > 0
        else True
    )
    monotonic_forward_return_passed = (
        _is_monotonic_forward_return_profile(metrics["average_forward_return_by_predicted_regime_pct"])
        if require_monotonic_forward_returns
        else True
    )

    fail_reasons: list[str] = []
    if not min_predicted_regime_count_passed:
        fail_reasons.append("min_predicted_regime_count")
    if not monotonic_forward_return_passed:
        fail_reasons.append("monotonic_forward_returns")

    return {
        "min_predicted_regime_count_required": min_predicted_regime_count,
        "min_predicted_regime_count_passed": min_predicted_regime_count_passed,
        "monotonic_forward_return_required": require_monotonic_forward_returns,
        "monotonic_forward_return_passed": monotonic_forward_return_passed,
        "constraint_passed": not fail_reasons,
        "constraint_fail_reasons": fail_reasons,
    }


def _daily_returns_pct(closes: list[float]) -> list[float]:
    returns: list[float] = [0.0]
    for index in range(1, len(closes)):
        prior = closes[index - 1]
        current = closes[index]
        returns.append(0.0 if prior <= 0 else ((current - prior) / prior) * 100.0)
    return returns


def _load_context_feature_series(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> dict[date, dict[str, float | None]]:
    dates, _, _, closes = two_stage._load_adjusted_indicator_source(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        period_configs=(two_stage.IndicatorPeriodConfig(roc_period=63, adx_period=14, rsi_period=14),),
    )
    ema8 = ema(closes, 8)
    ema21 = ema(closes, 21)
    vol20 = rolling_stddev(_daily_returns_pct(closes), 20, ddof=1)

    context_by_date: dict[date, dict[str, float | None]] = {}
    for index, trade_date in enumerate(dates):
        close_price = closes[index]
        ema_gap_pct: float | None = None
        realized_vol_pct: float | None = None
        if close_price > 0 and ema8[index] is not None and ema21[index] is not None:
            ema_gap_pct = ((float(ema8[index]) - float(ema21[index])) / close_price) * 100.0
        if vol20[index] is not None and math.isfinite(float(vol20[index])):
            realized_vol_pct = float(vol20[index]) * math.sqrt(252.0)
        context_by_date[trade_date] = {
            "ema_gap_pct": None if ema_gap_pct is None or not math.isfinite(ema_gap_pct) else ema_gap_pct,
            "realized_vol_pct": (
                None
                if realized_vol_pct is None or not math.isfinite(realized_vol_pct)
                else realized_vol_pct
            ),
        }
    return context_by_date


def _build_positive_feature_mask(values: list[float | None], threshold: float | None) -> int:
    if threshold is None:
        return (1 << len(values)) - 1
    mask = 0
    for index, value in enumerate(values):
        if value is not None and value > threshold:
            mask |= 1 << index
    return mask


def _build_negative_feature_mask(values: list[float | None], threshold: float | None) -> int:
    if threshold is None:
        return (1 << len(values)) - 1
    mask = 0
    for index, value in enumerate(values):
        if value is not None and value < -threshold:
            mask |= 1 << index
    return mask


def _build_high_feature_mask(values: list[float | None], threshold: float | None) -> int:
    if threshold is None:
        return (1 << len(values)) - 1
    mask = 0
    for index, value in enumerate(values):
        if value is not None and value > threshold:
            mask |= 1 << index
    return mask


def _build_feature_gate_masks(
    *,
    evaluation_dates: list[date],
    context_by_date: dict[date, dict[str, float | None]],
    feature_gate_configs: tuple[FeatureGateConfig, ...],
) -> dict[FeatureGateConfig, dict[str, int]]:
    ema_gap_values = [context_by_date.get(trade_date, {}).get("ema_gap_pct") for trade_date in evaluation_dates]
    realized_vol_values = [context_by_date.get(trade_date, {}).get("realized_vol_pct") for trade_date in evaluation_dates]
    return {
        feature_gate: {
            "bull_ema_mask": _build_positive_feature_mask(ema_gap_values, feature_gate.ema_gap_threshold_pct),
            "bear_ema_mask": _build_negative_feature_mask(ema_gap_values, feature_gate.ema_gap_threshold_pct),
            "heavy_vol_mask": _build_high_feature_mask(realized_vol_values, feature_gate.heavy_vol_threshold_pct),
        }
        for feature_gate in feature_gate_configs
    }


def _build_actual_regimes_by_threshold(
    *,
    forward_returns_pct: list[float],
    threshold_configs: tuple[LabelThresholdConfig, ...],
) -> dict[LabelThresholdConfig, list[str]]:
    return {
        threshold_config: [
            _realized_regime_label(
                forward_return_pct=forward_return_pct,
                neutral_move_pct=threshold_config.neutral_move_pct,
                heavy_move_pct=threshold_config.heavy_move_pct,
            )
            for forward_return_pct in forward_returns_pct
        ]
        for threshold_config in threshold_configs
    }


def _candidate_to_row(candidate: PredictionCandidate) -> dict[str, object]:
    metrics = dict(candidate.metrics)
    return {
        "indicator_periods": candidate.indicator_periods.label,
        "roc_period": candidate.indicator_periods.roc_period,
        "adx_period": candidate.indicator_periods.adx_period,
        "rsi_period": candidate.indicator_periods.rsi_period,
        "bull_filter": candidate.bull_filter.label,
        "bear_filter": candidate.bear_filter.label,
        "threshold_config": candidate.threshold_config.label,
        "neutral_move_pct": candidate.threshold_config.neutral_move_pct,
        "heavy_move_pct": candidate.threshold_config.heavy_move_pct,
        "feature_gate": candidate.feature_gate.label,
        "ema_gap_threshold_pct": candidate.feature_gate.ema_gap_threshold_pct,
        "heavy_vol_threshold_pct": candidate.feature_gate.heavy_vol_threshold_pct,
        **metrics,
    }


def _metric_ranking_key(
    metrics: dict[str, object],
    *,
    objective: str,
) -> tuple[float, float, float, float, float, int]:
    if objective == "precision_first":
        return (
            float(metrics["macro_precision_pct"]),
            float(metrics["exact_accuracy_pct"]),
            float(metrics["directional_accuracy_pct"]),
            float(metrics["balanced_accuracy_pct"]),
            float(metrics["macro_f1_pct"]),
            int(metrics["observation_count"]),
        )
    return (
        float(metrics[OBJECTIVE_FIELD_MAP[objective]]),
        float(metrics["balanced_accuracy_pct"]),
        float(metrics["macro_f1_pct"]),
        float(metrics["exact_accuracy_pct"]),
        float(metrics["macro_precision_pct"]),
        int(metrics["observation_count"]),
    )


def _candidate_ranking_key(candidate: PredictionCandidate, *, objective: str) -> tuple[float, float, float, float, float, int]:
    return _metric_ranking_key(candidate.metrics, objective=objective)


def _push_top_candidate(
    *,
    heap: list[tuple[tuple[float, float, float, float, float, int], int, PredictionCandidate]],
    candidate: PredictionCandidate,
    counter: int,
    limit: int,
    objective: str,
) -> None:
    entry = (_candidate_ranking_key(candidate, objective=objective), counter, candidate)
    if len(heap) < limit:
        heapq.heappush(heap, entry)
        return
    if entry[0] > heap[0][0]:
        heapq.heapreplace(heap, entry)


def _evaluate_symbol(
    *,
    symbol: str,
    start_date: date,
    requested_end_date: date,
    threshold_configs: tuple[LabelThresholdConfig, ...],
    feature_gate_configs: tuple[FeatureGateConfig, ...],
    objective: str,
    top_k: int,
    min_predicted_regime_count: int,
    require_monotonic_forward_returns: bool,
    abstention_thresholds: tuple[float, ...],
    abstention_min_trades: int,
    use_cache: bool,
    indicator_workers: int,
    forward_weeks: int,
) -> dict[str, object]:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = two_stage._build_bundle(store, symbol=symbol, start_date=start_date, end_date=requested_end_date)
    latest_available_date = two_stage._resolve_latest_available_date_from_bundle(bundle, requested_end_date)
    close_by_date = {
        bar.trade_date: float(bar.close_price)
        for bar in bundle.bars
        if start_date <= bar.trade_date <= latest_available_date
    }
    trading_fridays = [
        bar.trade_date
        for bar in bundle.bars
        if start_date <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]
    if len(trading_fridays) <= forward_weeks:
        raise SystemExit(f"Not enough Friday observations for {symbol} to score forward_weeks={forward_weeks}.")

    evaluation_dates = trading_fridays[:-forward_weeks]
    forward_returns_pct: list[float] = []
    for trade_index, trade_date in enumerate(evaluation_dates):
        current_close = close_by_date.get(trade_date)
        future_date = trading_fridays[trade_index + forward_weeks]
        future_close = close_by_date.get(future_date)
        if current_close is None or future_close is None or current_close <= 0:
            raise SystemExit(f"Missing close prices required to score {symbol} on {trade_date.isoformat()}.")
        forward_returns_pct.append(((future_close - current_close) / current_close) * 100.0)

    actual_regimes_by_threshold = _build_actual_regimes_by_threshold(
        forward_returns_pct=forward_returns_pct,
        threshold_configs=threshold_configs,
    )
    context_by_date = _load_context_feature_series(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
    )
    feature_gate_masks = _build_feature_gate_masks(
        evaluation_dates=evaluation_dates,
        context_by_date=context_by_date,
        feature_gate_configs=feature_gate_configs,
    )

    period_configs = tuple(
        two_stage.IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in two_stage.BROAD_ROC_PERIODS
        for adx_period in two_stage.BROAD_ADX_PERIODS
        for rsi_period in two_stage.BROAD_RSI_PERIODS
    )
    bull_filters = two_stage._build_default_bull_filters()
    bear_filters = two_stage._build_default_bear_filters()
    indicators_by_period = two_stage._build_period_cache(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
        period_configs=period_configs,
        use_cache=use_cache,
        worker_count=indicator_workers,
    )

    total_config_count = len(period_configs) * len(bull_filters) * len(bear_filters) * len(threshold_configs) * len(feature_gate_configs)
    print(
        f"[{symbol}] scoring {total_config_count} combinations across {len(evaluation_dates)} Friday observations "
        f"({len(period_configs)} periods x {len(bull_filters)} bull x {len(bear_filters)} bear "
        f"x {len(feature_gate_configs)} feature gates x {len(threshold_configs)} threshold pairs)"
    )

    constrained_heap: list[tuple[tuple[float, float, float, float, float, int], int, PredictionCandidate]] = []
    unconstrained_heap: list[tuple[tuple[float, float, float, float, float, int], int, PredictionCandidate]] = []
    counter = 0
    constrained_best_candidate: PredictionCandidate | None = None
    constrained_best_key: tuple[float, float, float, float, float, int] | None = None
    unconstrained_best_candidate: PredictionCandidate | None = None
    unconstrained_best_key: tuple[float, float, float, float, float, int] | None = None
    constrained_candidate_count = 0

    for period_config in period_configs:
        indicator_triplets = two_stage._indicator_triplets_for_trading_fridays(
            indicators=indicators_by_period[period_config.label],
            trading_fridays=evaluation_dates,
        )
        bull_masks = {
            bull_filter: two_stage._build_bull_filter_mask(
                filter_config=bull_filter,
                indicator_triplets=indicator_triplets,
            )
            for bull_filter in bull_filters
        }
        heavy_bull_masks = {
            bull_filter: two_stage._build_heavy_bull_filter_mask(
                filter_config=bull_filter,
                indicator_triplets=indicator_triplets,
            )
            for bull_filter in bull_filters
        }
        bear_masks = {
            bear_filter: two_stage._build_bear_filter_mask(
                filter_config=bear_filter,
                indicator_triplets=indicator_triplets,
            )
            for bear_filter in bear_filters
        }
        heavy_bear_masks = {
            bear_filter: two_stage._build_heavy_bear_filter_mask(
                filter_config=bear_filter,
                indicator_triplets=indicator_triplets,
            )
            for bear_filter in bear_filters
        }

        for feature_gate in feature_gate_configs:
            gate_masks = feature_gate_masks[feature_gate]
            bull_ema_mask = gate_masks["bull_ema_mask"]
            bear_ema_mask = gate_masks["bear_ema_mask"]
            heavy_vol_mask = gate_masks["heavy_vol_mask"]
            for bull_filter in bull_filters:
                bull_mask = bull_masks[bull_filter] & bull_ema_mask
                heavy_bull_mask = heavy_bull_masks[bull_filter] & bull_ema_mask & heavy_vol_mask
                for bear_filter in bear_filters:
                    predicted_regimes = _predicted_regimes_from_masks(
                        observation_count=len(evaluation_dates),
                        bull_mask=bull_mask,
                        bear_mask=bear_masks[bear_filter] & bear_ema_mask,
                        heavy_bull_mask=heavy_bull_mask,
                        heavy_bear_mask=heavy_bear_masks[bear_filter] & bear_ema_mask & heavy_vol_mask,
                    )
                    for threshold_config in threshold_configs:
                        metrics = _score_predictions(
                            predicted_regimes=predicted_regimes,
                            actual_regimes=actual_regimes_by_threshold[threshold_config],
                            forward_returns_pct=forward_returns_pct,
                        )
                        metrics.update(
                            _candidate_constraint_summary(
                                metrics=metrics,
                                min_predicted_regime_count=min_predicted_regime_count,
                                require_monotonic_forward_returns=require_monotonic_forward_returns,
                            )
                        )
                        candidate = PredictionCandidate(
                            indicator_periods=period_config,
                            bull_filter=bull_filter,
                            bear_filter=bear_filter,
                            threshold_config=threshold_config,
                            feature_gate=feature_gate,
                            metrics=metrics,
                        )
                        ranking_key = _candidate_ranking_key(candidate, objective=objective)
                        if unconstrained_best_key is None or ranking_key > unconstrained_best_key:
                            unconstrained_best_key = ranking_key
                            unconstrained_best_candidate = candidate
                        _push_top_candidate(
                            heap=unconstrained_heap,
                            candidate=candidate,
                            counter=counter,
                            limit=top_k,
                            objective=objective,
                        )

                        if bool(metrics["constraint_passed"]):
                            constrained_candidate_count += 1
                            if constrained_best_key is None or ranking_key > constrained_best_key:
                                constrained_best_key = ranking_key
                                constrained_best_candidate = candidate
                            _push_top_candidate(
                                heap=constrained_heap,
                                candidate=candidate,
                                counter=counter,
                                limit=top_k,
                                objective=objective,
                            )
                        counter += 1

    selected_mode = "constraint_passed" if constrained_best_candidate is not None else "fallback_unfiltered"
    best_candidate = constrained_best_candidate or unconstrained_best_candidate
    top_candidates = [
        item[2]
        for item in sorted(
            constrained_heap if constrained_heap else unconstrained_heap,
            key=lambda item: item[0],
            reverse=True,
        )
    ]
    if best_candidate is None:
        raise SystemExit(f"No candidates were evaluated for {symbol}.")

    best_row = _candidate_to_row(best_candidate)
    best_indicator_triplets = two_stage._indicator_triplets_for_trading_fridays(
        indicators=indicators_by_period[best_candidate.indicator_periods.label],
        trading_fridays=evaluation_dates,
    )
    best_prediction_series = _prediction_series_with_confidence(
        evaluation_dates=evaluation_dates,
        indicator_triplets=best_indicator_triplets,
        context_by_date=context_by_date,
        bull_filter=best_candidate.bull_filter,
        bear_filter=best_candidate.bear_filter,
        feature_gate=best_candidate.feature_gate,
    )
    abstention_rows = [
        _directional_subset_metrics(
            predicted_regimes=best_prediction_series["predicted_regimes"],
            actual_regimes=actual_regimes_by_threshold[best_candidate.threshold_config],
            confidence_scores=best_prediction_series["confidence_scores"],
            threshold=threshold,
        )
        for threshold in abstention_thresholds
    ]
    baseline_abstention_row = next(
        row for row in abstention_rows if math.isclose(float(row["confidence_threshold"]), 0.0, abs_tol=1e-9)
    )
    best_exact_abstention_row = _best_abstention_row(
        rows=abstention_rows,
        min_trades=abstention_min_trades,
        metric="exact_accuracy",
    )
    best_macro_precision_abstention_row = _best_abstention_row(
        rows=abstention_rows,
        min_trades=abstention_min_trades,
        metric="macro_precision",
    )
    latest_signal_date = evaluation_dates[-1]
    latest_signal_regime = str(best_prediction_series["predicted_regimes"][-1])
    latest_signal_confidence = float(best_prediction_series["confidence_scores"][-1])
    abstain_exact_threshold_label = (
        "none"
        if best_exact_abstention_row is None
        else f"{float(best_exact_abstention_row['confidence_threshold']):.2f}"
    )
    best_row.update(
        {
            "latest_signal_date": latest_signal_date.isoformat(),
            "latest_signal_regime": latest_signal_regime,
            "latest_signal_confidence": round(latest_signal_confidence, 6),
            "recommended_abstention_exact_threshold": (
                None
                if best_exact_abstention_row is None
                else best_exact_abstention_row["confidence_threshold"]
            ),
            "recommended_abstention_exact_trade_count": (
                0 if best_exact_abstention_row is None else best_exact_abstention_row["trade_count"]
            ),
            "recommended_abstention_exact_coverage_pct": (
                0.0 if best_exact_abstention_row is None else best_exact_abstention_row["coverage_pct"]
            ),
            "recommended_abstention_exact_accuracy_pct": (
                0.0 if best_exact_abstention_row is None else best_exact_abstention_row["exact_accuracy_pct"]
            ),
            "recommended_abstention_exact_accuracy_delta_pct": (
                None
                if best_exact_abstention_row is None
                else round(
                    float(best_exact_abstention_row["exact_accuracy_pct"])
                    - float(baseline_abstention_row["exact_accuracy_pct"]),
                    4,
                )
            ),
            "recommended_abstention_exact_should_trade_latest": (
                False
                if best_exact_abstention_row is None
                else (
                    latest_signal_regime != "neutral"
                    and latest_signal_confidence >= float(best_exact_abstention_row["confidence_threshold"])
                )
            ),
            "recommended_abstention_macro_precision_threshold": (
                None
                if best_macro_precision_abstention_row is None
                else best_macro_precision_abstention_row["confidence_threshold"]
            ),
            "recommended_abstention_macro_precision_trade_count": (
                0
                if best_macro_precision_abstention_row is None
                else best_macro_precision_abstention_row["trade_count"]
            ),
            "recommended_abstention_macro_precision_pct": (
                0.0
                if best_macro_precision_abstention_row is None
                else best_macro_precision_abstention_row["macro_precision_pct"]
            ),
            "recommended_abstention_macro_precision_delta_pct": (
                None
                if best_macro_precision_abstention_row is None
                else round(
                    float(best_macro_precision_abstention_row["macro_precision_pct"])
                    - float(baseline_abstention_row["macro_precision_pct"]),
                    4,
                )
            ),
        }
    )
    print(
        f"[{symbol}] best {objective}={best_row[OBJECTIVE_FIELD_MAP[objective]]:.4f}% "
        f"exact={best_row['exact_accuracy_pct']:.4f}% "
        f"balanced={best_row['balanced_accuracy_pct']:.4f}% "
        f"macro_f1={best_row['macro_f1_pct']:.4f}% "
        f"macro_precision={best_row['macro_precision_pct']:.4f}% "
        f"abstain_exact={abstain_exact_threshold_label} "
        f"mode={selected_mode} "
        f"thresholds={best_row['threshold_config']} "
        f"gates={best_row['feature_gate']}"
    )

    return {
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "latest_available_date": latest_available_date.isoformat(),
        "requested_end_date": requested_end_date.isoformat(),
        "forward_weeks": forward_weeks,
        "observation_count": len(evaluation_dates),
        "scored_config_count": total_config_count,
        "constraint_passing_config_count": constrained_candidate_count,
        "best_result_selection": selected_mode,
        "best_result": best_row,
        "abstention_analysis": {
            "min_trades": abstention_min_trades,
            "thresholds": list(abstention_thresholds),
            "baseline": baseline_abstention_row,
            "best_exact": best_exact_abstention_row,
            "best_macro_precision": best_macro_precision_abstention_row,
            "rows": abstention_rows,
        },
        "top_results": [_candidate_to_row(candidate) for candidate in top_candidates],
    }


def main() -> int:
    args = _parse_args()
    if args.forward_weeks < 1:
        raise SystemExit("--forward-weeks must be >= 1.")
    if args.top_k < 1:
        raise SystemExit("--top-k must be >= 1.")
    if args.min_observations < 1:
        raise SystemExit("--min-observations must be >= 1.")
    if args.min_predicted_regime_count < 0:
        raise SystemExit("--min-predicted-regime-count must be >= 0.")
    if args.abstention_min_trades < 0:
        raise SystemExit("--abstention-min-trades must be >= 0.")

    threshold_configs = _build_label_threshold_configs(args)
    feature_gate_configs = _build_feature_gate_configs(args)
    abstention_thresholds = _dedupe_preserve_order((0.0, *_parse_float_csv(args.abstention_thresholds)))

    symbols = _load_symbols(args)
    start_dates = _resolve_symbol_start_dates(
        symbols=symbols,
        min_start_date=args.min_start_date,
        requested_end_date=args.requested_end_date,
    )
    missing_symbols = [symbol for symbol in symbols if symbol not in start_dates]
    if missing_symbols:
        print(f"Skipping missing symbols: {', '.join(missing_symbols)}")

    symbol_results: list[dict[str, object]] = []
    use_cache = not args.disable_cache
    for symbol in symbols:
        start_date = start_dates.get(symbol)
        if start_date is None:
            continue
        result = _evaluate_symbol(
            symbol=symbol,
            start_date=start_date,
            requested_end_date=args.requested_end_date,
            threshold_configs=threshold_configs,
            feature_gate_configs=feature_gate_configs,
            objective=args.objective,
            top_k=args.top_k,
            min_predicted_regime_count=args.min_predicted_regime_count,
            require_monotonic_forward_returns=args.require_monotonic_forward_returns,
            abstention_thresholds=abstention_thresholds,
            abstention_min_trades=args.abstention_min_trades,
            use_cache=use_cache,
            indicator_workers=args.indicator_workers,
            forward_weeks=args.forward_weeks,
        )
        if int(result["observation_count"]) < args.min_observations:
            print(
                f"[{symbol}] skipping output because observation_count={result['observation_count']} "
                f"is below min_observations={args.min_observations}"
            )
            continue
        symbol_results.append(result)

    payload = {
        "objective": args.objective,
        "threshold_configs": [
            {
                "label": threshold_config.label,
                "neutral_move_pct": threshold_config.neutral_move_pct,
                "heavy_move_pct": threshold_config.heavy_move_pct,
            }
            for threshold_config in threshold_configs
        ],
        "feature_gate_configs": [
            {
                "label": feature_gate.label,
                "ema_gap_threshold_pct": feature_gate.ema_gap_threshold_pct,
                "heavy_vol_threshold_pct": feature_gate.heavy_vol_threshold_pct,
            }
            for feature_gate in feature_gate_configs
        ],
        "forward_weeks": int(args.forward_weeks),
        "min_start_date": args.min_start_date.isoformat(),
        "requested_end_date": args.requested_end_date.isoformat(),
        "search_space": {
            "period_config_count": len(two_stage.BROAD_ROC_PERIODS) * len(two_stage.BROAD_ADX_PERIODS) * len(two_stage.BROAD_RSI_PERIODS),
            "bull_filter_count": len(two_stage._build_default_bull_filters()),
            "bear_filter_count": len(two_stage._build_default_bear_filters()),
            "threshold_config_count": len(threshold_configs),
            "feature_gate_count": len(feature_gate_configs),
            "constraints": {
                "min_predicted_regime_count": args.min_predicted_regime_count,
                "require_monotonic_forward_returns": args.require_monotonic_forward_returns,
            },
            "abstention": {
                "thresholds": list(abstention_thresholds),
                "min_trades": args.abstention_min_trades,
            },
            "heavy_buffers": {
                "roc": two_stage.HEAVY_ROC_BUFFER,
                "adx": two_stage.HEAVY_ADX_BUFFER,
                "rsi": two_stage.HEAVY_RSI_BUFFER,
            },
        },
        "symbols": symbol_results,
    }

    output_json = args.output_json or _default_output_path(
        min_start_date=args.min_start_date,
        requested_end_date=args.requested_end_date,
        forward_weeks=args.forward_weeks,
        objective=args.objective,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
