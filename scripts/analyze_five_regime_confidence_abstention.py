from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from statistics import median

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
import evaluate_five_regime_price_predictions as evaluator  # noqa: E402
import grid_search_weekly_calendar_policy_two_stage as two_stage  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep confidence thresholds over a selected five-regime result and measure the "
            "trade-off between abstaining and keeping only higher-confidence directional calls."
        )
    )
    parser.add_argument(
        "--result-json",
        type=Path,
        required=True,
        help="Path to a single-symbol evaluate_five_regime_price_predictions result JSON.",
    )
    parser.add_argument(
        "--confidence-thresholds",
        default=evaluator.DEFAULT_CONFIDENCE_THRESHOLDS,
        help=(
            "Comma-separated normalized confidence thresholds. "
            "Defaults to 0,0.25,0.5,0.75,1.0,1.25,1.5,2.0."
        ),
    )
    parser.add_argument(
        "--min-trades-for-best",
        type=int,
        default=30,
        help="Minimum directional predictions required before a threshold is considered for best-of summary.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional JSON output path for the sweep summary.",
    )
    return parser.parse_args()


def _load_result_payload(path: Path) -> tuple[dict[str, object], dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbol_rows = payload.get("symbols")
    if not isinstance(symbol_rows, list) or len(symbol_rows) != 1 or not isinstance(symbol_rows[0], dict):
        raise SystemExit("Expected a single-symbol evaluation payload.")
    symbol_row = dict(symbol_rows[0])
    best_result = symbol_row.get("best_result")
    if not isinstance(best_result, dict):
        raise SystemExit("The supplied payload did not contain a best_result row.")
    return symbol_row, dict(best_result)


def _build_prediction_series(
    *,
    symbol_row: dict[str, object],
    best_result: dict[str, object],
) -> dict[str, object]:
    bull_filters = {config.label: config for config in two_stage._build_default_bull_filters()}
    bear_filters = {config.label: config for config in two_stage._build_default_bear_filters()}
    symbol = str(symbol_row["symbol"])
    start_date = date.fromisoformat(str(symbol_row["start_date"]))
    latest_available_date = date.fromisoformat(str(symbol_row["latest_available_date"]))
    forward_weeks = int(symbol_row["forward_weeks"])
    period_config = two_stage.IndicatorPeriodConfig(
        roc_period=int(best_result["roc_period"]),
        adx_period=int(best_result["adx_period"]),
        rsi_period=int(best_result["rsi_period"]),
    )
    bull_filter = bull_filters[str(best_result["bull_filter"])]
    bear_filter = bear_filters[str(best_result["bear_filter"])]
    feature_gate = evaluator.FeatureGateConfig(
        ema_gap_threshold_pct=(
            None if best_result.get("ema_gap_threshold_pct") is None else float(best_result["ema_gap_threshold_pct"])
        ),
        heavy_vol_threshold_pct=(
            None
            if best_result.get("heavy_vol_threshold_pct") is None
            else float(best_result["heavy_vol_threshold_pct"])
        ),
    )
    threshold_config = evaluator.LabelThresholdConfig(
        neutral_move_pct=float(best_result["neutral_move_pct"]),
        heavy_move_pct=float(best_result["heavy_move_pct"]),
    )

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = two_stage._build_bundle(store, symbol=symbol, start_date=start_date, end_date=latest_available_date)
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
    evaluation_dates = trading_fridays[:-forward_weeks]
    forward_returns_pct: list[float] = []
    for trade_index, trade_date in enumerate(evaluation_dates):
        current_close = close_by_date.get(trade_date)
        future_close = close_by_date.get(trading_fridays[trade_index + forward_weeks])
        if current_close is None or future_close is None or current_close <= 0:
            raise SystemExit(f"Missing close prices required to evaluate {symbol} on {trade_date.isoformat()}.")
        forward_returns_pct.append(((future_close - current_close) / current_close) * 100.0)

    actual_regimes = [
        evaluator._realized_regime_label(
            forward_return_pct=forward_return_pct,
            neutral_move_pct=threshold_config.neutral_move_pct,
            heavy_move_pct=threshold_config.heavy_move_pct,
        )
        for forward_return_pct in forward_returns_pct
    ]
    context_by_date = evaluator._load_context_feature_series(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
    )
    indicators_by_period = two_stage._build_period_cache(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicator_triplets = two_stage._indicator_triplets_for_trading_fridays(
        indicators=indicators_by_period[period_config.label],
        trading_fridays=evaluation_dates,
    )

    predicted_regimes: list[str] = []
    confidence_scores: list[float] = []
    for trade_date, indicator_triplet in zip(evaluation_dates, indicator_triplets, strict=True):
        regime, confidence = evaluator._classify_regime_with_confidence(
            indicator_triplet=indicator_triplet,
            bull_filter=bull_filter,
            bear_filter=bear_filter,
            feature_gate=feature_gate,
            context_row=context_by_date.get(trade_date),
        )
        predicted_regimes.append(regime)
        confidence_scores.append(round(float(confidence), 6))

    return {
        "symbol": symbol,
        "evaluation_dates": [trade_date.isoformat() for trade_date in evaluation_dates],
        "predicted_regimes": predicted_regimes,
        "confidence_scores": confidence_scores,
        "actual_regimes": actual_regimes,
    }


def _directional_subset_metrics(
    *,
    predicted_regimes: list[str],
    actual_regimes: list[str],
    confidence_scores: list[float],
    threshold: float,
) -> dict[str, object]:
    if len(predicted_regimes) != len(actual_regimes) or len(predicted_regimes) != len(confidence_scores):
        raise ValueError("prediction, actual, and confidence series must be the same length.")
    retained_indices = [
        index
        for index, (predicted_regime, confidence_score) in enumerate(zip(predicted_regimes, confidence_scores, strict=True))
        if predicted_regime != "neutral" and confidence_score >= threshold
    ]
    trade_count = len(retained_indices)
    available_count = len(predicted_regimes)
    coverage_pct = 0.0 if available_count <= 0 else round(trade_count / available_count * 100.0, 4)
    if trade_count == 0:
        return {
            "confidence_threshold": round(threshold, 4),
            "available_observation_count": available_count,
            "trade_count": 0,
            "coverage_pct": coverage_pct,
            "exact_accuracy_pct": 0.0,
            "directional_accuracy_pct": 0.0,
            "macro_precision_pct": 0.0,
            "average_confidence": None,
            "median_confidence": None,
            "predicted_counts": {label: 0 for label in evaluator.DIRECTIONAL_REGIME_LABELS},
            "precision_by_label_pct": {label: 0.0 for label in evaluator.DIRECTIONAL_REGIME_LABELS},
            "best_bucket": None,
            "best_bucket_precision_pct": None,
            "best_bucket_count": 0,
        }

    predicted_subset = [predicted_regimes[index] for index in retained_indices]
    actual_subset = [actual_regimes[index] for index in retained_indices]
    confidence_subset = [confidence_scores[index] for index in retained_indices]
    predicted_counts = {
        label: sum(1 for predicted_regime in predicted_subset if predicted_regime == label)
        for label in evaluator.DIRECTIONAL_REGIME_LABELS
    }
    precision_by_label_pct: dict[str, float] = {}
    exact_hit_count = 0
    directional_hit_count = 0
    for predicted_regime, actual_regime in zip(predicted_subset, actual_subset, strict=True):
        if predicted_regime == actual_regime:
            exact_hit_count += 1
        if evaluator._DIRECTION_BUCKETS[predicted_regime] == evaluator._DIRECTION_BUCKETS[actual_regime]:
            directional_hit_count += 1
    for label in evaluator.DIRECTIONAL_REGIME_LABELS:
        predicted_count = predicted_counts[label]
        exact_count = sum(
            1
            for predicted_regime, actual_regime in zip(predicted_subset, actual_subset, strict=True)
            if predicted_regime == label and actual_regime == label
        )
        precision_by_label_pct[label] = round(
            evaluator._safe_ratio(exact_count, predicted_count) * 100.0,
            4,
        )
    active_labels = [label for label in evaluator.DIRECTIONAL_REGIME_LABELS if predicted_counts[label] > 0]
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
        "available_observation_count": available_count,
        "trade_count": trade_count,
        "coverage_pct": coverage_pct,
        "exact_accuracy_pct": round(exact_hit_count / trade_count * 100.0, 4),
        "directional_accuracy_pct": round(directional_hit_count / trade_count * 100.0, 4),
        "macro_precision_pct": macro_precision_pct,
        "average_confidence": round(sum(confidence_subset) / len(confidence_subset), 4),
        "median_confidence": round(float(median(confidence_subset)), 4),
        "predicted_counts": predicted_counts,
        "precision_by_label_pct": precision_by_label_pct,
        "best_bucket": best_bucket,
        "best_bucket_precision_pct": best_bucket_precision_pct,
        "best_bucket_count": best_bucket_count,
    }


def _print_summary(*, symbol: str, result_path: Path, rows: list[dict[str, object]], min_trades_for_best: int) -> None:
    print(f"symbol={symbol}")
    print(f"result_json={result_path}")
    print("threshold\ttrades\tcoverage_pct\texact_pct\tdirectional_pct\tmacro_precision_pct\tbest_bucket\tbest_bucket_precision_pct\tbest_bucket_count")
    for row in rows:
        best_bucket = row["best_bucket"] or "-"
        best_bucket_precision_pct = "-" if row["best_bucket_precision_pct"] is None else f"{row['best_bucket_precision_pct']:.4f}"
        print(
            f"{row['confidence_threshold']:.2f}\t"
            f"{row['trade_count']}\t"
            f"{row['coverage_pct']:.4f}\t"
            f"{row['exact_accuracy_pct']:.4f}\t"
            f"{row['directional_accuracy_pct']:.4f}\t"
            f"{row['macro_precision_pct']:.4f}\t"
            f"{best_bucket}\t"
            f"{best_bucket_precision_pct}\t"
            f"{row['best_bucket_count']}"
        )

    eligible_rows = [row for row in rows if int(row["trade_count"]) >= min_trades_for_best]
    if not eligible_rows:
        print(f"best_exact_threshold(min_trades>={min_trades_for_best})=none")
        print(f"best_macro_precision_threshold(min_trades>={min_trades_for_best})=none")
        return

    best_exact = max(
        eligible_rows,
        key=lambda row: (
            float(row["exact_accuracy_pct"]),
            float(row["directional_accuracy_pct"]),
            float(row["trade_count"]),
            -float(row["confidence_threshold"]),
        ),
    )
    best_macro_precision = max(
        eligible_rows,
        key=lambda row: (
            float(row["macro_precision_pct"]),
            float(row["exact_accuracy_pct"]),
            float(row["trade_count"]),
            -float(row["confidence_threshold"]),
        ),
    )
    print(
        "best_exact_threshold"
        f"(min_trades>={min_trades_for_best})="
        f"{best_exact['confidence_threshold']:.2f}"
        f" exact_pct={best_exact['exact_accuracy_pct']:.4f}"
        f" trades={best_exact['trade_count']}"
        f" coverage_pct={best_exact['coverage_pct']:.4f}"
    )
    print(
        "best_macro_precision_threshold"
        f"(min_trades>={min_trades_for_best})="
        f"{best_macro_precision['confidence_threshold']:.2f}"
        f" macro_precision_pct={best_macro_precision['macro_precision_pct']:.4f}"
        f" trades={best_macro_precision['trade_count']}"
        f" coverage_pct={best_macro_precision['coverage_pct']:.4f}"
    )


def main() -> int:
    args = _parse_args()
    symbol_row, best_result = _load_result_payload(args.result_json)
    prediction_series = _build_prediction_series(symbol_row=symbol_row, best_result=best_result)
    confidence_thresholds = evaluator._parse_float_csv(args.confidence_thresholds)
    rows = [
        _directional_subset_metrics(
            predicted_regimes=prediction_series["predicted_regimes"],
            actual_regimes=prediction_series["actual_regimes"],
            confidence_scores=prediction_series["confidence_scores"],
            threshold=threshold,
        )
        for threshold in confidence_thresholds
    ]
    _print_summary(
        symbol=str(prediction_series["symbol"]),
        result_path=args.result_json,
        rows=rows,
        min_trades_for_best=args.min_trades_for_best,
    )

    output_json = args.output_json
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(
                {
                    "symbol": prediction_series["symbol"],
                    "result_json": str(args.result_json),
                    "confidence_thresholds": list(confidence_thresholds),
                    "rows": rows,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
