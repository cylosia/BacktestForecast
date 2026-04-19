from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
import json
from pathlib import Path
from statistics import fmean, median

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
LOGS_DIR = ROOT / "logs"

import evaluate_five_regime_price_predictions as evaluator  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.forecasts.analog import HistoricalAnalogForecaster  # noqa: E402
from backtestforecast.indicators.calculations import ema, rolling_stddev, rsi, sma  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402


DEFAULT_BASELINE_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "five_regime_price_predictions"
    / "filtered112_precision_first_abstain_w1_5y_20260417"
    / "summary.csv"
)
DEFAULT_MAX_ANALOGS = 20
DEFAULT_MIN_CANDIDATE_COUNT = 15
DEFAULT_RUN_LABEL_PREFIX = "weekly_regime_model_benchmark"

ANALOG_FORECASTER = HistoricalAnalogForecaster()
BULL_FILTERS_BY_LABEL = {
    filter_config.label: filter_config for filter_config in evaluator.two_stage._build_default_bull_filters()
}
BEAR_FILTERS_BY_LABEL = {
    filter_config.label: filter_config for filter_config in evaluator.two_stage._build_default_bear_filters()
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the current best per-symbol five-regime predictor against a "
            "strictly historical weekly analog predictor on the same Friday dates."
        )
    )
    parser.add_argument(
        "--baseline-summary-csv",
        type=Path,
        default=DEFAULT_BASELINE_SUMMARY_CSV,
        help="Path to the five-regime batch summary CSV. Defaults to the latest 5y precision-first batch.",
    )
    parser.add_argument("--symbols", nargs="*", help="Optional explicit symbol list.")
    parser.add_argument("--symbols-file", type=Path, help="Optional newline/comma separated symbol file.")
    parser.add_argument(
        "--max-analogs",
        type=int,
        default=DEFAULT_MAX_ANALOGS,
        help="How many nearest analog Fridays to use. Defaults to 20.",
    )
    parser.add_argument(
        "--min-candidate-count",
        type=int,
        default=DEFAULT_MIN_CANDIDATE_COUNT,
        help="Minimum historical Friday analog candidates required before scoring a date. Defaults to 15.",
    )
    parser.add_argument(
        "--indicator-workers",
        type=int,
        default=evaluator.two_stage.DEFAULT_INDICATOR_WORKERS,
        help="Thread count for uncached indicator loading when rebuilding the baseline series.",
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable indicator-cache reuse while rebuilding the baseline series.",
    )
    parser.add_argument(
        "--run-label",
        help="Optional output directory label. Defaults to a timestamped label.",
    )
    return parser.parse_args()


def _normalize_symbols(raw_symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _load_requested_symbols(args: argparse.Namespace) -> list[str] | None:
    raw_symbols: list[str] = []
    if args.symbols:
        raw_symbols.extend(args.symbols)
    if args.symbols_file:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        raw_symbols.extend(raw_text.replace("\n", ",").split(","))
    ordered = _normalize_symbols(raw_symbols)
    return ordered or None


def _read_summary_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _safe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(fmean(values)), 4)


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 4)


def _resolve_output_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else ROOT / path


def _load_baseline_payloads(
    *,
    summary_rows: list[dict[str, str]],
    requested_symbols: list[str] | None,
) -> list[dict[str, object]]:
    requested_set = None if requested_symbols is None else set(requested_symbols)
    payload_rows: list[dict[str, object]] = []
    for row in summary_rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        if requested_set is not None and symbol not in requested_set:
            continue
        if str(row.get("status") or "").lower() not in {"completed", "skipped_existing"}:
            continue
        output_path_raw = row.get("output_path")
        if not output_path_raw:
            continue
        output_path = _resolve_output_path(output_path_raw)
        if not output_path.exists():
            raise SystemExit(f"Missing baseline output JSON: {output_path}")
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        symbols = payload.get("symbols")
        if not isinstance(symbols, list) or len(symbols) != 1 or not isinstance(symbols[0], dict):
            raise SystemExit(f"Expected exactly one symbol payload in {output_path}")
        payload_rows.append(
            {
                "symbol": symbol,
                "summary_row": row,
                "payload": payload,
                "symbol_payload": symbols[0],
                "output_path": output_path,
            }
        )
    if requested_symbols is not None:
        found = {str(item["symbol"]) for item in payload_rows}
        missing = [symbol for symbol in requested_symbols if symbol not in found]
        if missing:
            raise SystemExit(f"Requested symbols missing from baseline summary: {', '.join(missing)}")
        payload_rows.sort(key=lambda item: requested_symbols.index(str(item["symbol"])))
    else:
        payload_rows.sort(key=lambda item: str(item["symbol"]))
    if not payload_rows:
        raise SystemExit("No matching baseline payloads were found.")
    return payload_rows


def _build_weekly_observations(
    *,
    symbol: str,
    start_date: date,
    requested_end_date: date,
) -> dict[str, object]:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = evaluator.two_stage._build_bundle(store, symbol=symbol, start_date=start_date, end_date=requested_end_date)
    latest_available_date = evaluator.two_stage._resolve_latest_available_date_from_bundle(bundle, requested_end_date)
    bars = sorted(
        [bar for bar in bundle.bars if start_date <= bar.trade_date <= latest_available_date],
        key=lambda bar: bar.trade_date,
    )
    if not bars:
        raise SystemExit(f"No bars available for {symbol} in {start_date.isoformat()}..{requested_end_date.isoformat()}.")
    close_by_date = {bar.trade_date: float(bar.close_price) for bar in bars}
    bar_index_by_date = {bar.trade_date: index for index, bar in enumerate(bars)}
    trading_fridays = [bar.trade_date for bar in bars if bar.trade_date.weekday() == 4]
    return {
        "bars": bars,
        "latest_available_date": latest_available_date,
        "close_by_date": close_by_date,
        "bar_index_by_date": bar_index_by_date,
        "trading_fridays": trading_fridays,
    }


def _build_forward_series(
    *,
    close_by_date: dict[date, float],
    trading_fridays: list[date],
    forward_weeks: int,
) -> dict[str, object]:
    if len(trading_fridays) <= forward_weeks:
        raise SystemExit(f"Not enough Friday observations to score forward_weeks={forward_weeks}.")
    evaluation_dates = trading_fridays[:-forward_weeks]
    future_dates: list[date] = []
    forward_returns_pct: list[float] = []
    for index, trade_date in enumerate(evaluation_dates):
        future_date = trading_fridays[index + forward_weeks]
        current_close = close_by_date.get(trade_date)
        future_close = close_by_date.get(future_date)
        if current_close is None or future_close is None or current_close <= 0:
            raise SystemExit(f"Missing close prices required to score {trade_date.isoformat()}.")
        future_dates.append(future_date)
        forward_returns_pct.append(((future_close - current_close) / current_close) * 100.0)
    return {
        "evaluation_dates": evaluation_dates,
        "future_dates": future_dates,
        "forward_returns_pct": forward_returns_pct,
        "friday_position_by_date": {trade_date: index for index, trade_date in enumerate(trading_fridays)},
    }


def _build_actual_regimes(
    *,
    forward_returns_pct: list[float],
    neutral_move_pct: float,
    heavy_move_pct: float,
) -> list[str]:
    return [
        evaluator._realized_regime_label(
            forward_return_pct=forward_return_pct,
            neutral_move_pct=neutral_move_pct,
            heavy_move_pct=heavy_move_pct,
        )
        for forward_return_pct in forward_returns_pct
    ]


def _rebuild_baseline_prediction_series(
    *,
    symbol: str,
    start_date: date,
    latest_available_date: date,
    evaluation_dates: list[date],
    best_result: dict[str, object],
    indicator_workers: int,
    use_cache: bool,
) -> dict[str, object]:
    bull_label = str(best_result["bull_filter"])
    bear_label = str(best_result["bear_filter"])
    if bull_label not in BULL_FILTERS_BY_LABEL:
        raise SystemExit(f"Unsupported bull filter label for {symbol}: {bull_label}")
    if bear_label not in BEAR_FILTERS_BY_LABEL:
        raise SystemExit(f"Unsupported bear filter label for {symbol}: {bear_label}")
    period_config = evaluator.two_stage.IndicatorPeriodConfig(
        roc_period=int(best_result["roc_period"]),
        adx_period=int(best_result["adx_period"]),
        rsi_period=int(best_result["rsi_period"]),
    )
    indicators_by_period = evaluator.two_stage._build_period_cache(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=use_cache,
        worker_count=indicator_workers,
    )
    indicator_triplets = evaluator.two_stage._indicator_triplets_for_trading_fridays(
        indicators=indicators_by_period[period_config.label],
        trading_fridays=evaluation_dates,
    )
    context_by_date = evaluator._load_context_feature_series(
        symbol=symbol,
        start_date=start_date,
        end_date=latest_available_date,
    )
    return evaluator._prediction_series_with_confidence(
        evaluation_dates=evaluation_dates,
        indicator_triplets=indicator_triplets,
        context_by_date=context_by_date,
        bull_filter=BULL_FILTERS_BY_LABEL[bull_label],
        bear_filter=BEAR_FILTERS_BY_LABEL[bear_label],
        feature_gate=evaluator.FeatureGateConfig(
            ema_gap_threshold_pct=_safe_float(best_result.get("ema_gap_threshold_pct")),
            heavy_vol_threshold_pct=_safe_float(best_result.get("heavy_vol_threshold_pct")),
        ),
    )


def _build_analog_feature_vectors(bars: list[object]) -> list[tuple[float, ...] | None]:
    closes = [float(bar.close_price) for bar in bars]
    volumes = [float(bar.volume) for bar in bars]
    daily_returns = ANALOG_FORECASTER._daily_returns(closes)
    rsi14 = rsi(closes, 14)
    ema8 = ema(closes, 8)
    ema21 = ema(closes, 21)
    avg_volume20 = sma(volumes, 20)
    vol20 = rolling_stddev(daily_returns, 20, ddof=1)
    return [
        ANALOG_FORECASTER._features_for_index(
            index=index,
            closes=closes,
            volumes=volumes,
            returns=daily_returns,
            rsi14=rsi14,
            ema8=ema8,
            ema21=ema21,
            avg_volume20=avg_volume20,
            vol20=vol20,
        )
        for index in range(len(bars))
    ]


def _analog_regime_margin_pct(
    *,
    predicted_return_pct: float,
    neutral_move_pct: float,
    heavy_move_pct: float,
) -> float:
    if predicted_return_pct >= heavy_move_pct:
        return round(predicted_return_pct - heavy_move_pct, 4)
    if predicted_return_pct > neutral_move_pct:
        return round(predicted_return_pct - neutral_move_pct, 4)
    if predicted_return_pct <= -heavy_move_pct:
        return round(abs(predicted_return_pct) - heavy_move_pct, 4)
    if predicted_return_pct < -neutral_move_pct:
        return round(abs(predicted_return_pct) - neutral_move_pct, 4)
    return round(max(neutral_move_pct - abs(predicted_return_pct), 0.0), 4)


def _build_weekly_analog_prediction_series(
    *,
    bars: list[object],
    bar_index_by_date: dict[date, int],
    trading_fridays: list[date],
    evaluation_dates: list[date],
    forward_returns_pct: list[float],
    friday_position_by_date: dict[date, int],
    forward_weeks: int,
    neutral_move_pct: float,
    heavy_move_pct: float,
    max_analogs: int,
    min_candidate_count: int,
) -> dict[str, object]:
    feature_vectors = _build_analog_feature_vectors(bars)
    predicted_regimes: list[str | None] = []
    rows: list[dict[str, object]] = []
    for trade_date in evaluation_dates:
        friday_position = friday_position_by_date[trade_date]
        latest_candidate_position = friday_position - forward_weeks
        current_bar_index = bar_index_by_date[trade_date]
        current_features = feature_vectors[current_bar_index]
        if current_features is None or latest_candidate_position < 0:
            predicted_regimes.append(None)
            rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "predicted_regime": None,
                    "predicted_return_median_pct": None,
                    "regime_margin_pct": None,
                    "candidate_pool_count": 0,
                    "analogs_used": 0,
                }
            )
            continue
        candidate_rows: list[tuple[float, date, float]] = []
        for candidate_position in range(latest_candidate_position + 1):
            candidate_date = trading_fridays[candidate_position]
            candidate_bar_index = bar_index_by_date[candidate_date]
            candidate_features = feature_vectors[candidate_bar_index]
            if candidate_features is None:
                continue
            candidate_rows.append(
                (
                    ANALOG_FORECASTER._distance(current_features, candidate_features),
                    candidate_date,
                    float(forward_returns_pct[candidate_position]),
                )
            )
        if len(candidate_rows) < min_candidate_count:
            predicted_regimes.append(None)
            rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "predicted_regime": None,
                    "predicted_return_median_pct": None,
                    "regime_margin_pct": None,
                    "candidate_pool_count": len(candidate_rows),
                    "analogs_used": 0,
                }
            )
            continue
        candidate_rows.sort(key=lambda item: (item[0], item[1]))
        selected_rows = candidate_rows[:max_analogs]
        selected_returns = [item[2] for item in selected_rows]
        predicted_return_median_pct = float(median(selected_returns))
        predicted_regime = evaluator._realized_regime_label(
            forward_return_pct=predicted_return_median_pct,
            neutral_move_pct=neutral_move_pct,
            heavy_move_pct=heavy_move_pct,
        )
        predicted_regimes.append(predicted_regime)
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "predicted_regime": predicted_regime,
                "predicted_return_median_pct": round(predicted_return_median_pct, 4),
                "regime_margin_pct": _analog_regime_margin_pct(
                    predicted_return_pct=predicted_return_median_pct,
                    neutral_move_pct=neutral_move_pct,
                    heavy_move_pct=heavy_move_pct,
                ),
                "candidate_pool_count": len(candidate_rows),
                "analogs_used": len(selected_rows),
            }
        )
    available_count = sum(1 for item in predicted_regimes if item is not None)
    return {
        "predicted_regimes": predicted_regimes,
        "rows": rows,
        "available_count": available_count,
        "availability_pct": _pct(available_count, len(evaluation_dates)),
    }


def _score_overlap(
    *,
    baseline_predicted_regimes: list[str],
    analog_predicted_regimes: list[str | None],
    actual_regimes: list[str],
    forward_returns_pct: list[float],
) -> dict[str, object]:
    overlap_indices = [index for index, regime in enumerate(analog_predicted_regimes) if regime is not None]
    if not overlap_indices:
        raise SystemExit("Analog predictor did not produce any scorable overlap observations.")
    baseline_overlap = [baseline_predicted_regimes[index] for index in overlap_indices]
    analog_overlap = [str(analog_predicted_regimes[index]) for index in overlap_indices]
    actual_overlap = [actual_regimes[index] for index in overlap_indices]
    forward_return_overlap = [forward_returns_pct[index] for index in overlap_indices]
    return {
        "overlap_indices": overlap_indices,
        "baseline_metrics": evaluator._score_predictions(
            predicted_regimes=baseline_overlap,
            actual_regimes=actual_overlap,
            forward_returns_pct=forward_return_overlap,
        ),
        "analog_metrics": evaluator._score_predictions(
            predicted_regimes=analog_overlap,
            actual_regimes=actual_overlap,
            forward_returns_pct=forward_return_overlap,
        ),
    }


def _latest_available_analog_row(rows: list[dict[str, object]]) -> dict[str, object] | None:
    for row in reversed(rows):
        if row.get("predicted_regime") is not None:
            return row
    return None


def _evaluate_symbol(
    *,
    item: dict[str, object],
    indicator_workers: int,
    use_cache: bool,
    max_analogs: int,
    min_candidate_count: int,
) -> dict[str, object]:
    symbol = str(item["symbol"])
    payload = dict(item["payload"])
    symbol_payload = dict(item["symbol_payload"])
    best_result = dict(symbol_payload["best_result"])  # type: ignore[index]
    start_date = date.fromisoformat(str(symbol_payload["start_date"]))
    requested_end_date = date.fromisoformat(str(symbol_payload["requested_end_date"]))
    forward_weeks = int(payload["forward_weeks"])
    neutral_move_pct = float(best_result["neutral_move_pct"])
    heavy_move_pct = float(best_result["heavy_move_pct"])

    weekly_observations = _build_weekly_observations(
        symbol=symbol,
        start_date=start_date,
        requested_end_date=requested_end_date,
    )
    forward_series = _build_forward_series(
        close_by_date=weekly_observations["close_by_date"],  # type: ignore[arg-type]
        trading_fridays=weekly_observations["trading_fridays"],  # type: ignore[arg-type]
        forward_weeks=forward_weeks,
    )
    actual_regimes = _build_actual_regimes(
        forward_returns_pct=forward_series["forward_returns_pct"],  # type: ignore[arg-type]
        neutral_move_pct=neutral_move_pct,
        heavy_move_pct=heavy_move_pct,
    )
    baseline_series = _rebuild_baseline_prediction_series(
        symbol=symbol,
        start_date=start_date,
        latest_available_date=weekly_observations["latest_available_date"],  # type: ignore[arg-type]
        evaluation_dates=forward_series["evaluation_dates"],  # type: ignore[arg-type]
        best_result=best_result,
        indicator_workers=indicator_workers,
        use_cache=use_cache,
    )
    analog_series = _build_weekly_analog_prediction_series(
        bars=weekly_observations["bars"],  # type: ignore[arg-type]
        bar_index_by_date=weekly_observations["bar_index_by_date"],  # type: ignore[arg-type]
        trading_fridays=weekly_observations["trading_fridays"],  # type: ignore[arg-type]
        evaluation_dates=forward_series["evaluation_dates"],  # type: ignore[arg-type]
        forward_returns_pct=forward_series["forward_returns_pct"],  # type: ignore[arg-type]
        friday_position_by_date=forward_series["friday_position_by_date"],  # type: ignore[arg-type]
        forward_weeks=forward_weeks,
        neutral_move_pct=neutral_move_pct,
        heavy_move_pct=heavy_move_pct,
        max_analogs=max_analogs,
        min_candidate_count=min_candidate_count,
    )
    overlap_scores = _score_overlap(
        baseline_predicted_regimes=baseline_series["predicted_regimes"],  # type: ignore[arg-type]
        analog_predicted_regimes=analog_series["predicted_regimes"],  # type: ignore[arg-type]
        actual_regimes=actual_regimes,
        forward_returns_pct=forward_series["forward_returns_pct"],  # type: ignore[arg-type]
    )
    latest_analog_row = _latest_available_analog_row(analog_series["rows"])  # type: ignore[arg-type]
    baseline_metrics = overlap_scores["baseline_metrics"]
    analog_metrics = overlap_scores["analog_metrics"]
    overlap_observation_count = int(baseline_metrics["observation_count"])
    return {
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "latest_available_date": weekly_observations["latest_available_date"].isoformat(),  # type: ignore[union-attr]
        "requested_end_date": requested_end_date.isoformat(),
        "forward_weeks": forward_weeks,
        "neutral_move_pct": neutral_move_pct,
        "heavy_move_pct": heavy_move_pct,
        "baseline_full_observation_count": int(best_result["observation_count"]),
        "baseline_full_exact_accuracy_pct": float(best_result["exact_accuracy_pct"]),
        "baseline_full_directional_accuracy_pct": float(best_result["directional_accuracy_pct"]),
        "baseline_full_macro_precision_pct": float(best_result["macro_precision_pct"]),
        "baseline_full_macro_f1_pct": float(best_result["macro_f1_pct"]),
        "analog_available_count": int(analog_series["available_count"]),
        "analog_availability_pct": float(analog_series["availability_pct"]),
        "overlap_observation_count": overlap_observation_count,
        "baseline_overlap_exact_hit_count": int(baseline_metrics["exact_hit_count"]),
        "baseline_overlap_directional_hit_count": int(baseline_metrics["directional_hit_count"]),
        "baseline_overlap_exact_accuracy_pct": float(baseline_metrics["exact_accuracy_pct"]),
        "baseline_overlap_directional_accuracy_pct": float(baseline_metrics["directional_accuracy_pct"]),
        "baseline_overlap_macro_precision_pct": float(baseline_metrics["macro_precision_pct"]),
        "baseline_overlap_macro_f1_pct": float(baseline_metrics["macro_f1_pct"]),
        "analog_exact_hit_count": int(analog_metrics["exact_hit_count"]),
        "analog_directional_hit_count": int(analog_metrics["directional_hit_count"]),
        "analog_exact_accuracy_pct": float(analog_metrics["exact_accuracy_pct"]),
        "analog_directional_accuracy_pct": float(analog_metrics["directional_accuracy_pct"]),
        "analog_macro_precision_pct": float(analog_metrics["macro_precision_pct"]),
        "analog_macro_f1_pct": float(analog_metrics["macro_f1_pct"]),
        "exact_accuracy_delta_pct": round(
            float(analog_metrics["exact_accuracy_pct"]) - float(baseline_metrics["exact_accuracy_pct"]),
            4,
        ),
        "directional_accuracy_delta_pct": round(
            float(analog_metrics["directional_accuracy_pct"]) - float(baseline_metrics["directional_accuracy_pct"]),
            4,
        ),
        "macro_precision_delta_pct": round(
            float(analog_metrics["macro_precision_pct"]) - float(baseline_metrics["macro_precision_pct"]),
            4,
        ),
        "macro_f1_delta_pct": round(
            float(analog_metrics["macro_f1_pct"]) - float(baseline_metrics["macro_f1_pct"]),
            4,
        ),
        "baseline_latest_signal_date": str(baseline_series["evaluation_dates"][-1]),
        "baseline_latest_signal_regime": str(baseline_series["predicted_regimes"][-1]),
        "baseline_latest_signal_confidence": float(baseline_series["confidence_scores"][-1]),
        "analog_latest_signal_date": None if latest_analog_row is None else latest_analog_row["trade_date"],
        "analog_latest_signal_regime": None if latest_analog_row is None else latest_analog_row["predicted_regime"],
        "analog_latest_predicted_return_median_pct": (
            None if latest_analog_row is None else latest_analog_row["predicted_return_median_pct"]
        ),
        "analog_latest_regime_margin_pct": None if latest_analog_row is None else latest_analog_row["regime_margin_pct"],
        "analog_latest_candidate_pool_count": (
            None if latest_analog_row is None else latest_analog_row["candidate_pool_count"]
        ),
        "analog_latest_analogs_used": None if latest_analog_row is None else latest_analog_row["analogs_used"],
    }


def _aggregate_results(rows: list[dict[str, object]]) -> dict[str, object]:
    overlap_observations = sum(int(row["overlap_observation_count"]) for row in rows)
    baseline_exact_hits = sum(int(row["baseline_overlap_exact_hit_count"]) for row in rows)
    baseline_directional_hits = sum(int(row["baseline_overlap_directional_hit_count"]) for row in rows)
    analog_exact_hits = sum(int(row["analog_exact_hit_count"]) for row in rows)
    analog_directional_hits = sum(int(row["analog_directional_hit_count"]) for row in rows)

    exact_deltas = [float(row["exact_accuracy_delta_pct"]) for row in rows]
    directional_deltas = [float(row["directional_accuracy_delta_pct"]) for row in rows]
    precision_deltas = [float(row["macro_precision_delta_pct"]) for row in rows]
    f1_deltas = [float(row["macro_f1_delta_pct"]) for row in rows]

    baseline_exacts = [float(row["baseline_overlap_exact_accuracy_pct"]) for row in rows]
    analog_exacts = [float(row["analog_exact_accuracy_pct"]) for row in rows]
    baseline_directionals = [float(row["baseline_overlap_directional_accuracy_pct"]) for row in rows]
    analog_directionals = [float(row["analog_directional_accuracy_pct"]) for row in rows]
    baseline_precisions = [float(row["baseline_overlap_macro_precision_pct"]) for row in rows]
    analog_precisions = [float(row["analog_macro_precision_pct"]) for row in rows]
    baseline_f1s = [float(row["baseline_overlap_macro_f1_pct"]) for row in rows]
    analog_f1s = [float(row["analog_macro_f1_pct"]) for row in rows]
    availability_pcts = [float(row["analog_availability_pct"]) for row in rows]

    top_exact_improvements = [
        {
            "symbol": row["symbol"],
            "exact_accuracy_delta_pct": row["exact_accuracy_delta_pct"],
            "baseline_overlap_exact_accuracy_pct": row["baseline_overlap_exact_accuracy_pct"],
            "analog_exact_accuracy_pct": row["analog_exact_accuracy_pct"],
            "overlap_observation_count": row["overlap_observation_count"],
        }
        for row in sorted(rows, key=lambda item: (float(item["exact_accuracy_delta_pct"]), str(item["symbol"])), reverse=True)[:20]
    ]
    top_macro_precision_improvements = [
        {
            "symbol": row["symbol"],
            "macro_precision_delta_pct": row["macro_precision_delta_pct"],
            "baseline_overlap_macro_precision_pct": row["baseline_overlap_macro_precision_pct"],
            "analog_macro_precision_pct": row["analog_macro_precision_pct"],
            "overlap_observation_count": row["overlap_observation_count"],
        }
        for row in sorted(rows, key=lambda item: (float(item["macro_precision_delta_pct"]), str(item["symbol"])), reverse=True)[:20]
    ]

    return {
        "symbol_count": len(rows),
        "overlap_observation_count": overlap_observations,
        "baseline_weighted_exact_accuracy_pct": _pct(baseline_exact_hits, overlap_observations),
        "analog_weighted_exact_accuracy_pct": _pct(analog_exact_hits, overlap_observations),
        "baseline_weighted_directional_accuracy_pct": _pct(baseline_directional_hits, overlap_observations),
        "analog_weighted_directional_accuracy_pct": _pct(analog_directional_hits, overlap_observations),
        "mean_analog_availability_pct": _mean_or_none(availability_pcts),
        "median_analog_availability_pct": _median_or_none(availability_pcts),
        "baseline_mean_exact_accuracy_pct": _mean_or_none(baseline_exacts),
        "analog_mean_exact_accuracy_pct": _mean_or_none(analog_exacts),
        "baseline_median_exact_accuracy_pct": _median_or_none(baseline_exacts),
        "analog_median_exact_accuracy_pct": _median_or_none(analog_exacts),
        "baseline_mean_directional_accuracy_pct": _mean_or_none(baseline_directionals),
        "analog_mean_directional_accuracy_pct": _mean_or_none(analog_directionals),
        "baseline_median_directional_accuracy_pct": _median_or_none(baseline_directionals),
        "analog_median_directional_accuracy_pct": _median_or_none(analog_directionals),
        "baseline_mean_macro_precision_pct": _mean_or_none(baseline_precisions),
        "analog_mean_macro_precision_pct": _mean_or_none(analog_precisions),
        "baseline_median_macro_precision_pct": _median_or_none(baseline_precisions),
        "analog_median_macro_precision_pct": _median_or_none(analog_precisions),
        "baseline_mean_macro_f1_pct": _mean_or_none(baseline_f1s),
        "analog_mean_macro_f1_pct": _mean_or_none(analog_f1s),
        "baseline_median_macro_f1_pct": _median_or_none(baseline_f1s),
        "analog_median_macro_f1_pct": _median_or_none(analog_f1s),
        "mean_exact_accuracy_delta_pct": _mean_or_none(exact_deltas),
        "median_exact_accuracy_delta_pct": _median_or_none(exact_deltas),
        "mean_directional_accuracy_delta_pct": _mean_or_none(directional_deltas),
        "median_directional_accuracy_delta_pct": _median_or_none(directional_deltas),
        "mean_macro_precision_delta_pct": _mean_or_none(precision_deltas),
        "median_macro_precision_delta_pct": _median_or_none(precision_deltas),
        "mean_macro_f1_delta_pct": _mean_or_none(f1_deltas),
        "median_macro_f1_delta_pct": _median_or_none(f1_deltas),
        "symbols_improved_exact_accuracy": sum(1 for value in exact_deltas if value > 0.0),
        "symbols_improved_directional_accuracy": sum(1 for value in directional_deltas if value > 0.0),
        "symbols_improved_macro_precision": sum(1 for value in precision_deltas if value > 0.0),
        "symbols_improved_macro_f1": sum(1 for value in f1_deltas if value > 0.0),
        "top_exact_improvements": top_exact_improvements,
        "top_macro_precision_improvements": top_macro_precision_improvements,
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _parse_args()
    if args.max_analogs < 1:
        raise SystemExit("--max-analogs must be >= 1.")
    if args.min_candidate_count < 1:
        raise SystemExit("--min-candidate-count must be >= 1.")
    if args.indicator_workers < 1:
        raise SystemExit("--indicator-workers must be >= 1.")
    if not args.baseline_summary_csv.exists():
        raise SystemExit(f"Baseline summary CSV not found: {args.baseline_summary_csv}")

    requested_symbols = _load_requested_symbols(args)
    summary_rows = _read_summary_rows(args.baseline_summary_csv)
    baseline_payloads = _load_baseline_payloads(
        summary_rows=summary_rows,
        requested_symbols=requested_symbols,
    )
    run_label = args.run_label or (
        f"{DEFAULT_RUN_LABEL_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir = LOGS_DIR / "batch" / "weekly_regime_model_benchmark" / run_label
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    for index, item in enumerate(baseline_payloads, start=1):
        symbol = str(item["symbol"])
        print(f"[{index}/{len(baseline_payloads)}] benchmarking {symbol}")
        row = _evaluate_symbol(
            item=item,
            indicator_workers=args.indicator_workers,
            use_cache=not args.disable_cache,
            max_analogs=args.max_analogs,
            min_candidate_count=args.min_candidate_count,
        )
        results.append(row)
        print(
            f"[{symbol}] overlap={row['overlap_observation_count']} "
            f"baseline_exact={row['baseline_overlap_exact_accuracy_pct']:.4f}% "
            f"analog_exact={row['analog_exact_accuracy_pct']:.4f}% "
            f"delta={row['exact_accuracy_delta_pct']:.4f}% "
            f"baseline_precision={row['baseline_overlap_macro_precision_pct']:.4f}% "
            f"analog_precision={row['analog_macro_precision_pct']:.4f}%"
        )

    results.sort(key=lambda row: (float(row["exact_accuracy_delta_pct"]), str(row["symbol"])), reverse=True)
    aggregate = _aggregate_results(results)
    metadata = {
        "baseline_summary_csv": str(args.baseline_summary_csv),
        "symbols_requested": requested_symbols,
        "max_analogs": args.max_analogs,
        "min_candidate_count": args.min_candidate_count,
        "indicator_workers": args.indicator_workers,
        "use_cache": not args.disable_cache,
        "generated_at": datetime.now().isoformat(),
    }

    _write_csv(output_dir / "comparison.csv", results)
    (output_dir / "aggregate.json").write_text(
        json.dumps(
            {
                "metadata": metadata,
                "aggregate": aggregate,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    print(f"Wrote comparison to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
