from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from decimal import Decimal
from math import ceil, floor
from pathlib import Path
from statistics import median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from grid_search_weekly_calendar_policy_two_stage import (  # noqa: E402
    IndicatorPeriodConfig,
    _build_bundle,
    _build_default_bear_filters,
    _build_default_bull_filters,
    _build_period_cache,
    _build_strategy_sets,
    _label_maps,
    _ranking_key,
    _resolve_latest_available_date_from_bundle,
    _strategy_with_profit_target,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)
from portfolio_weighting import BASE_SCHEMES, _build_weight_scheme, _weighted_median  # noqa: E402

import grid_search_weekly_calendar_policy_two_stage as two_stage  # noqa: E402


DEFAULT_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "weekly_calendar_policy_two_stage"
    / "combined_103_median_train_20251231_20260411"
    / "summary.csv"
)
DEFAULT_ENTRY_START_DATE = date(2026, 1, 1)
DEFAULT_ENTRY_END_DATE = date(2026, 3, 31)
DEFAULT_MIN_TRADE_COUNT = 70
DEFAULT_TOP_K = 20
DEFAULT_MAX_WORKERS = 2
DEFAULT_WEIGHTING_SCHEME = "total_roi_shrunk"
DEFAULT_MAX_SYMBOL_WEIGHT_PCT = 8.0
DEFAULT_STABILITY_TOP_POOL = 60
DEFAULT_STABILITY_MIN_POSITIVE_MONTHS = 21
DEFAULT_STABILITY_MIN_P25_MONTHLY_MEDIAN_ROI_PCT = 25.0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a weekly calendar policy walk-forward using frozen training results. "
            "By default this ranks symbols by training median ROI, applies the monthly stability filter "
            "to the top 60 candidates, and then selects the top 20 survivors."
        )
    )
    parser.add_argument("--summary-csv", type=Path, default=DEFAULT_SUMMARY_CSV)
    parser.add_argument("--entry-start-date", type=date.fromisoformat, default=DEFAULT_ENTRY_START_DATE)
    parser.add_argument("--entry-end-date", type=date.fromisoformat, default=DEFAULT_ENTRY_END_DATE)
    parser.add_argument(
        "--replay-data-end",
        type=date.fromisoformat,
        help="Optional data cutoff used to allow post-quarter exits. Defaults to entry end date + 14 days.",
    )
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--min-trade-count", type=int, default=DEFAULT_MIN_TRADE_COUNT)
    parser.add_argument(
        "--train-objective",
        choices=("average", "median"),
        default="median",
        help="Training objective the summary rows must match. Defaults to median.",
    )
    parser.add_argument(
        "--min-median-roi",
        type=float,
        help="Optional minimum training median ROI per trade filter before ranking.",
    )
    parser.add_argument(
        "--max-training-assignment-count",
        type=int,
        help="Optional maximum allowed training early-assignment count for a candidate.",
    )
    parser.add_argument(
        "--max-training-assignment-rate-pct",
        type=float,
        help="Optional maximum allowed training early-assignment rate, as a percent of entered trades.",
    )
    parser.add_argument(
        "--max-training-put-assignment-count",
        type=int,
        help="Optional maximum allowed training deep-ITM put-assignment count for a candidate.",
    )
    parser.add_argument(
        "--max-training-put-assignment-rate-pct",
        type=float,
        help="Optional maximum allowed training deep-ITM put-assignment rate, as a percent of entered trades.",
    )
    parser.add_argument(
        "--stability-top-pool",
        type=int,
        default=DEFAULT_STABILITY_TOP_POOL,
        help=(
            "Apply the monthly training stability filter to the top-N ranked candidates before final selection. "
            f"Defaults to {DEFAULT_STABILITY_TOP_POOL}. Set to 0 to disable."
        ),
    )
    parser.add_argument(
        "--stability-min-positive-months",
        type=int,
        default=DEFAULT_STABILITY_MIN_POSITIVE_MONTHS,
        help=(
            "Minimum number of positive training months required to pass the stability filter. "
            f"Defaults to {DEFAULT_STABILITY_MIN_POSITIVE_MONTHS}."
        ),
    )
    parser.add_argument(
        "--stability-min-p25-monthly-median-roi-pct",
        type=float,
        default=DEFAULT_STABILITY_MIN_P25_MONTHLY_MEDIAN_ROI_PCT,
        help=(
            "Minimum 25th-percentile monthly median ROI on margin required to pass the stability filter. "
            f"Defaults to {DEFAULT_STABILITY_MIN_P25_MONTHLY_MEDIAN_ROI_PCT}."
        ),
    )
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument(
        "--weighting-scheme",
        choices=BASE_SCHEMES,
        default=DEFAULT_WEIGHTING_SCHEME,
        help=(
            "Portfolio weighting scheme applied across the selected symbols. "
            f"Defaults to {DEFAULT_WEIGHTING_SCHEME}."
        ),
    )
    parser.add_argument(
        "--max-symbol-weight-pct",
        type=float,
        default=DEFAULT_MAX_SYMBOL_WEIGHT_PCT,
        help=(
            "Optional max symbol weight cap, in percent, applied after building the base weighting scheme. "
            f"Defaults to {DEFAULT_MAX_SYMBOL_WEIGHT_PCT}."
        ),
    )
    parser.add_argument(
        "--weight-trade-count-cap",
        type=float,
        default=100.0,
        help="Trade-count shrink cap used by median-shrunk and total-roi-shrunk weighting schemes. Defaults to 100.",
    )
    parser.add_argument(
        "--output-prefix",
        type=Path,
        help="Optional output prefix. Defaults to logs/weekly_calendar_policy_walk_forward_topK_trainYYYYMMDD_q1_2026",
    )
    return parser.parse_args()


def _default_output_prefix(*, top_k: int) -> Path:
    return ROOT / "logs" / f"weekly_calendar_policy_walk_forward_top{top_k}_train20251231_q1_2026"


def _load_candidates(
    *,
    summary_csv: Path,
    train_objective: str,
    min_trade_count: int,
    min_median_roi: float | None,
    max_training_assignment_count: int | None,
    max_training_assignment_rate_pct: float | None,
    max_training_put_assignment_count: int | None,
    max_training_put_assignment_rate_pct: float | None,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    rows = list(csv.DictReader(summary_csv.open(newline="", encoding="utf-8")))
    candidates: list[dict[str, object]] = []
    base_candidate_count = 0
    assignment_filtered_out_count = 0
    assignment_filter_active = _assignment_filter_requested(
        max_training_assignment_count=max_training_assignment_count,
        max_training_assignment_rate_pct=max_training_assignment_rate_pct,
        max_training_put_assignment_count=max_training_put_assignment_count,
        max_training_put_assignment_rate_pct=max_training_put_assignment_rate_pct,
    )
    for row in rows:
        status = str(row.get("status") or "")
        if status not in {"completed", "skipped_existing"}:
            continue
        if row.get("objective") != train_objective:
            continue
        trade_count = int(float(row.get("trade_count") or 0))
        if trade_count <= min_trade_count:
            continue
        output_path = ROOT / str(row["output_path"])
        if not output_path.exists():
            continue
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        best = dict(payload["combined_best_result"])
        median_roi = float(best["median_roi_on_margin_pct"])
        if min_median_roi is not None and median_roi < min_median_roi:
            continue
        base_candidate_count += 1
        candidate = {
            "symbol": row["symbol"],
            "start_date": row["start_date"],
            "requested_end_date": row["requested_end_date"],
            "output_path": output_path,
            "payload": payload,
            "best": best,
        }
        if assignment_filter_active:
            metrics = _load_candidate_training_assignment_metrics(candidate)
            candidate["training_assignment_metrics"] = metrics
            if not _passes_assignment_filters(
                metrics=metrics,
                max_training_assignment_count=max_training_assignment_count,
                max_training_assignment_rate_pct=max_training_assignment_rate_pct,
                max_training_put_assignment_count=max_training_put_assignment_count,
                max_training_put_assignment_rate_pct=max_training_put_assignment_rate_pct,
            ):
                assignment_filtered_out_count += 1
                continue
        candidates.append(candidate)
    candidates.sort(key=lambda item: _ranking_key(dict(item["best"]), objective=train_objective), reverse=True)
    return candidates, {
        "base_candidate_count": base_candidate_count,
        "assignment_filtered_out_count": assignment_filtered_out_count,
    }


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _round_or_blank(value: object, *, digits: int = 4) -> float | str:
    parsed = _safe_float(value)
    if parsed is None:
        return ""
    return round(parsed, digits)


def _int_or_blank(value: object) -> int | str:
    parsed = _safe_int(value)
    if parsed is None:
        return ""
    return parsed


def _is_assignment_exit_reason(exit_reason: object) -> bool:
    return str(exit_reason or "").startswith("early_assignment_")


def _is_put_assignment_exit_reason(exit_reason: object) -> bool:
    return str(exit_reason or "") == "early_assignment_put_deep_itm"


def _assignment_filter_requested(
    *,
    max_training_assignment_count: int | None,
    max_training_assignment_rate_pct: float | None,
    max_training_put_assignment_count: int | None,
    max_training_put_assignment_rate_pct: float | None,
) -> bool:
    return any(
        value is not None
        for value in (
            max_training_assignment_count,
            max_training_assignment_rate_pct,
            max_training_put_assignment_count,
            max_training_put_assignment_rate_pct,
        )
    )


def _stability_filter_requested(*, stability_top_pool: int) -> bool:
    return stability_top_pool > 0


def _passes_assignment_filters(
    *,
    metrics: dict[str, object],
    max_training_assignment_count: int | None,
    max_training_assignment_rate_pct: float | None,
    max_training_put_assignment_count: int | None,
    max_training_put_assignment_rate_pct: float | None,
) -> bool:
    assignment_count = _safe_int(metrics.get("training_assignment_count")) or 0
    assignment_rate_pct = _safe_float(metrics.get("training_assignment_rate_pct")) or 0.0
    put_assignment_count = _safe_int(metrics.get("training_put_assignment_count")) or 0
    put_assignment_rate_pct = _safe_float(metrics.get("training_put_assignment_rate_pct")) or 0.0
    if max_training_assignment_count is not None and assignment_count > max_training_assignment_count:
        return False
    if max_training_assignment_rate_pct is not None and assignment_rate_pct > max_training_assignment_rate_pct:
        return False
    if max_training_put_assignment_count is not None and put_assignment_count > max_training_put_assignment_count:
        return False
    if max_training_put_assignment_rate_pct is not None and put_assignment_rate_pct > max_training_put_assignment_rate_pct:
        return False
    return True


def _interpolated_percentile(*, sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    bounded_quantile = min(max(quantile, 0.0), 1.0)
    index = (len(sorted_values) - 1) * bounded_quantile
    low_index = floor(index)
    high_index = ceil(index)
    if low_index == high_index:
        return sorted_values[low_index]
    low_value = sorted_values[low_index]
    high_value = sorted_values[high_index]
    return low_value * (high_index - index) + high_value * (index - low_index)


def _resolve_candidate_components(candidate: dict[str, object]) -> dict[str, object]:
    payload = dict(candidate["payload"])
    best = dict(candidate["best"])
    symbol = str(candidate["symbol"])
    train_start_date = date.fromisoformat(payload["period"]["start"])
    latest_available_date = date.fromisoformat(payload["period"].get("latest_available_date") or payload["period"]["requested_end"])

    bullish_strategies, bearish_strategies, neutral_strategies = _build_strategy_sets(symbol)
    all_strategies = _expand_candidate_strategy_variants(
        bullish_strategies + bearish_strategies + neutral_strategies,
        (
            str(best["bull_strategy"]),
            str(best["bear_strategy"]),
            str(best["neutral_strategy"]),
        ),
    )
    bull_filters = _build_default_bull_filters()
    bear_filters = _build_default_bear_filters()
    bull_filter_lookup, bear_filter_lookup, strategy_lookup = _label_maps(
        bull_filters=bull_filters,
        bear_filters=bear_filters,
        strategies=all_strategies,
    )

    period_config = IndicatorPeriodConfig(
        roc_period=int(best["roc_period"]),
        adx_period=int(best["adx_period"]),
        rsi_period=int(best["rsi_period"]),
    )
    return {
        "payload": payload,
        "best": best,
        "symbol": symbol,
        "train_start_date": train_start_date,
        "latest_available_date": latest_available_date,
        "period_config": period_config,
        "bull_filter": bull_filter_lookup[str(best["bull_filter"])],
        "bear_filter": bear_filter_lookup[str(best["bear_filter"])],
        "bull_strategy": strategy_lookup[str(best["bull_strategy"])],
        "bear_strategy": strategy_lookup[str(best["bear_strategy"])],
        "neutral_strategy": strategy_lookup[str(best["neutral_strategy"])],
    }


def _expand_candidate_strategy_variants(
    base_strategies: tuple[object, ...],
    required_labels: tuple[str, ...],
) -> tuple[object, ...]:
    strategies = list(base_strategies)
    strategy_by_label = {strategy.label: strategy for strategy in strategies}
    strategy_by_prefix = {str(strategy.label).rpartition("_pt")[0]: strategy for strategy in strategies}
    for label in required_labels:
        if label in strategy_by_label:
            continue
        label_prefix, separator, profit_target_suffix = label.rpartition("_pt")
        if not separator:
            raise KeyError(label)
        template = strategy_by_prefix.get(label_prefix)
        if template is None:
            raise KeyError(label)
        strategy = _strategy_with_profit_target(template, int(profit_target_suffix))
        strategies.append(strategy)
        strategy_by_label[label] = strategy
    return tuple(strategies)


def _trade_map_cache_path(*, symbol: str, start_date: date, latest_available_date: date, strategy_label: str) -> Path:
    return (
        two_stage.CACHE_ROOT
        / symbol.lower()
        / f"{start_date.isoformat()}_{latest_available_date.isoformat()}"
        / "trade_maps"
        / f"{strategy_label}.json"
    )


def _load_cached_trade_map_rows(cache_path: Path) -> dict[date, dict[str, object]]:
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    return {
        date.fromisoformat(trade_date): dict(trade_row)
        for trade_date, trade_row in payload.get("trade_map", {}).items()
    }


def _load_candidate_training_trade_rows(candidate: dict[str, object]) -> list[dict[str, object]]:
    cached_rows = candidate.get("training_trade_rows")
    if isinstance(cached_rows, list):
        return cached_rows

    components = _resolve_candidate_components(candidate)
    symbol = str(components["symbol"])
    train_start_date = components["train_start_date"]
    latest_available_date = components["latest_available_date"]
    bull_strategy = components["bull_strategy"]
    bear_strategy = components["bear_strategy"]
    neutral_strategy = components["neutral_strategy"]
    period_config = components["period_config"]
    bull_filter = components["bull_filter"]
    bear_filter = components["bear_filter"]

    indicator_cache = _build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = indicator_cache[period_config.label]
    trading_fridays = [
        trade_date
        for trade_date in sorted(indicators)
        if train_start_date <= trade_date <= latest_available_date and trade_date.weekday() == 4
    ]

    trade_maps: dict[str, dict[date, dict[str, object]]] = {}
    strategies = (bull_strategy, bear_strategy, neutral_strategy)
    missing_cache = False
    for strategy in strategies:
        cache_path = _trade_map_cache_path(
            symbol=symbol,
            start_date=train_start_date,
            latest_available_date=latest_available_date,
            strategy_label=strategy.label,
        )
        if not cache_path.exists():
            missing_cache = True
            break
        trade_maps[strategy.label] = _load_cached_trade_map_rows(cache_path)

    if missing_cache:
        store = HistoricalMarketDataStore(create_session, create_readonly_session)
        bundle = _build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=latest_available_date)
        curve = two_stage._load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
        trade_maps = two_stage._precompute_trade_maps(
            strategies=strategies,
            bundle=bundle,
            trading_fridays=trading_fridays,
            latest_available_date=latest_available_date,
            curve=curve,
            start_date=train_start_date,
            use_cache=True,
            worker_count=1,
        )

    training_trade_rows: list[dict[str, object]] = []
    for entry_date in trading_fridays:
        indicator_row = indicators.get(entry_date)
        bull = bull_filter.matches(indicator_row)
        bear = bear_filter.matches(indicator_row)
        if bull and not bear:
            strategy = bull_strategy
        elif bear and not bull:
            strategy = bear_strategy
        else:
            strategy = neutral_strategy
        trade_row = trade_maps.get(strategy.label, {}).get(entry_date)
        if trade_row is None:
            continue
        training_trade_rows.append(
            {
                **dict(trade_row),
                "_entry_date": entry_date,
            }
        )

    candidate["training_trade_rows"] = training_trade_rows
    return training_trade_rows


def _load_candidate_training_assignment_metrics(candidate: dict[str, object]) -> dict[str, object]:
    best = dict(candidate["best"])
    embedded_keys = (
        "assignment_count",
        "assignment_rate_pct",
        "put_assignment_count",
        "put_assignment_rate_pct",
    )
    if all(key in best for key in embedded_keys):
        return {
            "training_assignment_count": int(best["assignment_count"]),
            "training_assignment_rate_pct": round(float(best["assignment_rate_pct"]), 4),
            "training_put_assignment_count": int(best["put_assignment_count"]),
            "training_put_assignment_rate_pct": round(float(best["put_assignment_rate_pct"]), 4),
        }

    training_trade_rows = _load_candidate_training_trade_rows(candidate)
    trade_count = 0
    assignment_count = 0
    put_assignment_count = 0
    for trade_row in training_trade_rows:
        trade_count += 1
        exit_reason = trade_row.get("exit_reason")
        if _is_assignment_exit_reason(exit_reason):
            assignment_count += 1
        if _is_put_assignment_exit_reason(exit_reason):
            put_assignment_count += 1

    return {
        "training_assignment_count": assignment_count,
        "training_assignment_rate_pct": round((assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
        "training_put_assignment_count": put_assignment_count,
        "training_put_assignment_rate_pct": round((put_assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
    }


def _load_candidate_training_stability_metrics(candidate: dict[str, object]) -> dict[str, object]:
    cached_metrics = candidate.get("training_stability_metrics")
    if isinstance(cached_metrics, dict):
        return cached_metrics

    training_trade_rows = _load_candidate_training_trade_rows(candidate)
    roi_by_month: dict[str, list[float]] = defaultdict(list)
    for trade_row in training_trade_rows:
        entry_date = trade_row.get("_entry_date")
        if not isinstance(entry_date, date):
            continue
        roi_on_margin_pct = _safe_float(trade_row.get("roi_on_margin_pct"))
        if roi_on_margin_pct is None:
            continue
        roi_by_month[entry_date.strftime("%Y-%m")].append(roi_on_margin_pct)

    monthly_medians = [
        median(month_values)
        for _, month_values in sorted(roi_by_month.items())
        if month_values
    ]
    sorted_monthly_medians = sorted(monthly_medians)
    metrics = {
        "training_months_with_trades": len(monthly_medians),
        "training_positive_month_count": sum(1 for value in monthly_medians if value > 0.0),
        "training_negative_month_count": sum(1 for value in monthly_medians if value < 0.0),
        "training_worst_month_median_roi_pct": min(sorted_monthly_medians) if sorted_monthly_medians else None,
        "training_p25_monthly_median_roi_pct": (
            _interpolated_percentile(sorted_values=sorted_monthly_medians, quantile=0.25)
            if sorted_monthly_medians
            else None
        ),
        "training_median_monthly_median_roi_pct": (
            _interpolated_percentile(sorted_values=sorted_monthly_medians, quantile=0.5)
            if sorted_monthly_medians
            else None
        ),
        "training_best_month_median_roi_pct": max(sorted_monthly_medians) if sorted_monthly_medians else None,
    }
    candidate["training_stability_metrics"] = metrics
    return metrics


def _stability_ranking_key(
    candidate: dict[str, object],
    *,
    objective: str,
) -> tuple[float, float, tuple[float, float, float, float, int]]:
    metrics = dict(candidate.get("training_stability_metrics") or {})
    return (
        _safe_float(metrics.get("training_p25_monthly_median_roi_pct")) or float("-inf"),
        _safe_float(metrics.get("training_median_monthly_median_roi_pct")) or float("-inf"),
        _ranking_key(dict(candidate["best"]), objective=objective),
    )


def _apply_stability_filter(
    *,
    candidates: list[dict[str, object]],
    stability_top_pool: int,
    stability_min_positive_months: int,
    stability_min_p25_monthly_median_roi_pct: float,
    train_objective: str,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    if not _stability_filter_requested(stability_top_pool=stability_top_pool):
        return candidates, {
            "stability_filter_enabled": False,
            "stability_top_pool_count": 0,
            "stability_filtered_out_count": 0,
            "stability_survivor_count": len(candidates),
        }

    top_pool_candidates = list(candidates[: min(stability_top_pool, len(candidates))])
    survivors: list[dict[str, object]] = []
    for candidate in top_pool_candidates:
        metrics = _load_candidate_training_stability_metrics(candidate)
        candidate["training_stability_metrics"] = metrics
        positive_month_count = _safe_int(metrics.get("training_positive_month_count")) or 0
        p25_monthly_median_roi_pct = _safe_float(metrics.get("training_p25_monthly_median_roi_pct")) or float("-inf")
        if (
            positive_month_count >= stability_min_positive_months
            and p25_monthly_median_roi_pct >= stability_min_p25_monthly_median_roi_pct
        ):
            survivors.append(candidate)

    survivors.sort(key=lambda item: _stability_ranking_key(item, objective=train_objective), reverse=True)
    return survivors, {
        "stability_filter_enabled": True,
        "stability_top_pool_count": len(top_pool_candidates),
        "stability_filtered_out_count": len(top_pool_candidates) - len(survivors),
        "stability_survivor_count": len(survivors),
    }


def _replay_symbol(
    *,
    candidate: dict[str, object],
    entry_start_date: date,
    entry_end_date: date,
    replay_data_end: date,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    components = _resolve_candidate_components(candidate)
    payload = dict(components["payload"])
    best = dict(components["best"])
    symbol = str(components["symbol"])
    train_start_date = components["train_start_date"]
    period_config = components["period_config"]
    bull_filter = components["bull_filter"]
    bear_filter = components["bear_filter"]
    bull_strategy = components["bull_strategy"]
    bear_strategy = components["bear_strategy"]
    neutral_strategy = components["neutral_strategy"]
    training_assignment_metrics = dict(candidate.get("training_assignment_metrics") or {})

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = _build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=replay_data_end)
    latest_available_date = _resolve_latest_available_date_from_bundle(bundle, replay_data_end)
    curve = two_stage._load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
    indicator_cache = _build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = indicator_cache[period_config.label]
    entry_dates = [
        bar.trade_date
        for bar in bundle.bars
        if entry_start_date <= bar.trade_date <= entry_end_date and bar.trade_date.weekday() == 4
    ]

    strategies_to_run = (bull_strategy, bear_strategy, neutral_strategy)
    engine = OptionsBacktestEngine()
    trade_map: dict[str, dict[date, object]] = {}
    for strategy in strategies_to_run:
        local_entry_rule_cache = bundle.entry_rule_cache.__class__()
        per_date: dict[date, object] = {}
        for entry_date in entry_dates:
            config = two_stage._build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                latest_available_date=latest_available_date,
                risk_free_curve=curve,
            )
            result = engine.run(
                config=config,
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                ex_dividend_dates=bundle.ex_dividend_dates,
                option_gateway=bundle.option_gateway,
                shared_entry_rule_cache=local_entry_rule_cache,
            )
            trade = next((item for item in result.trades if item.entry_date == entry_date), None)
            if trade is not None:
                per_date[entry_date] = trade
        trade_map[strategy.label] = per_date

    ledger_rows: list[dict[str, object]] = []
    for entry_date in entry_dates:
        indicator_row = indicators.get(entry_date)
        bull = bull_filter.matches(indicator_row)
        bear = bear_filter.matches(indicator_row)
        if bull and not bear:
            regime = "bullish"
            strategy = bull_strategy
        elif bear and not bull:
            regime = "bearish"
            strategy = bear_strategy
        else:
            regime = "neutral"
            strategy = neutral_strategy
        trade = trade_map[strategy.label].get(entry_date)
        if trade is None:
            continue

        quantity = _safe_float(getattr(trade, "quantity", 1.0)) or 1.0
        detail_json = getattr(trade, "detail_json", {}) or {}
        entry_debit = _safe_float(detail_json.get("entry_package_market_value"))
        capital_required = _safe_float(detail_json.get("capital_required_per_unit"))
        total_capital_required = None if capital_required is None else capital_required * quantity
        net_pnl = float(trade.net_pnl)
        roi_capital = two_stage._trade_roi_on_margin_pct(trade)
        roi_debit = None
        if entry_debit is not None and entry_debit > 0:
            roi_debit = net_pnl / entry_debit * 100.0

        ledger_rows.append(
            {
                "symbol": symbol,
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "regime": regime,
                "strategy": strategy.label,
                "option_ticker": getattr(trade, "option_ticker", ""),
                "quantity": round(quantity, 4),
                "entry_debit": None if entry_debit is None else round(entry_debit, 4),
                "capital_required": None if total_capital_required is None else round(total_capital_required, 4),
                "net_pnl": round(net_pnl, 4),
                "roi_on_debit_pct": None if roi_debit is None else round(roi_debit, 4),
                "roi_on_capital_required_pct": None if roi_capital is None else round(roi_capital, 4),
                "exit_reason": getattr(trade, "exit_reason", ""),
                "entry_underlying_close": round(float(getattr(trade, "entry_underlying_close", 0.0)), 4),
                "exit_underlying_close": round(float(getattr(trade, "exit_underlying_close", 0.0)), 4),
                "training_trade_count": int(best["trade_count"]),
                "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
                "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
                "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
                "training_assignment_count": int(training_assignment_metrics.get("training_assignment_count") or 0),
                "training_assignment_rate_pct": round(float(training_assignment_metrics.get("training_assignment_rate_pct") or 0.0), 4),
                "training_put_assignment_count": int(training_assignment_metrics.get("training_put_assignment_count") or 0),
                "training_put_assignment_rate_pct": round(float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0), 4),
            }
        )

    capital_values = [float(item["capital_required"]) for item in ledger_rows if item["capital_required"] is not None]
    roi_values = [float(item["roi_on_capital_required_pct"]) for item in ledger_rows if item["roi_on_capital_required_pct"] is not None]
    total_capital = sum(capital_values)
    total_pnl = sum(float(item["net_pnl"]) for item in ledger_rows)
    result_row = {
        "symbol": symbol,
        "train_start_date": train_start_date.isoformat(),
        "train_end_date": payload["period"]["requested_end"],
        "entry_window_start": entry_start_date.isoformat(),
        "entry_window_end": entry_end_date.isoformat(),
        "replay_data_end": latest_available_date.isoformat(),
        "training_stage": best.get("stage", ""),
        "training_trade_count": int(best["trade_count"]),
        "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
        "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
        "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
        "training_assignment_count": int(training_assignment_metrics.get("training_assignment_count") or 0),
        "training_assignment_rate_pct": round(float(training_assignment_metrics.get("training_assignment_rate_pct") or 0.0), 4),
        "training_put_assignment_count": int(training_assignment_metrics.get("training_put_assignment_count") or 0),
        "training_put_assignment_rate_pct": round(float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0), 4),
        "trade_count": len(ledger_rows),
        "total_capital_required": round(total_capital, 4),
        "total_net_pnl": round(total_pnl, 4),
        "roi_on_capital_required_pct": round(total_pnl / total_capital * 100.0, 4) if total_capital else 0.0,
        "average_roi_on_capital_required_pct": round(sum(roi_values) / len(roi_values), 4) if roi_values else 0.0,
        "median_roi_on_capital_required_pct": round(median(roi_values), 4) if roi_values else 0.0,
    }
    return result_row, ledger_rows


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _apply_portfolio_weights(
    *,
    selection_rows: list[dict[str, object]],
    result_rows: list[dict[str, object]],
    ledger_rows: list[dict[str, object]],
    weighting_scheme: str,
    max_symbol_weight_pct: float | None,
    weight_trade_count_cap: float,
) -> tuple[str, dict[str, float]]:
    applied_scheme, weights = _build_weight_scheme(
        selection_rows,
        scheme=weighting_scheme,
        trade_count_cap=weight_trade_count_cap,
        max_symbol_weight_pct=max_symbol_weight_pct,
    )
    symbol_count = len(selection_rows)
    multipliers = {symbol: weight * symbol_count for symbol, weight in weights.items()}

    for row in selection_rows:
        symbol = str(row["symbol"])
        weight = weights.get(symbol, 0.0)
        multiplier = multipliers.get(symbol, 0.0)
        row["weighting_scheme"] = applied_scheme
        row["weight_pct"] = round(weight * 100.0, 4)
        row["position_multiplier"] = round(multiplier, 6)

    for row in result_rows:
        symbol = str(row["symbol"])
        weight = weights.get(symbol, 0.0)
        multiplier = multipliers.get(symbol, 0.0)
        total_capital = float(row["total_capital_required"])
        total_pnl = float(row["total_net_pnl"])
        row["weighting_scheme"] = applied_scheme
        row["weight_pct"] = round(weight * 100.0, 4)
        row["position_multiplier"] = round(multiplier, 6)
        row["weighted_total_capital_required"] = round(total_capital * multiplier, 4)
        row["weighted_total_net_pnl"] = round(total_pnl * multiplier, 4)

    for row in ledger_rows:
        symbol = str(row["symbol"])
        weight = weights.get(symbol, 0.0)
        multiplier = multipliers.get(symbol, 0.0)
        row["weighting_scheme"] = applied_scheme
        row["weight_pct"] = round(weight * 100.0, 4)
        row["position_multiplier"] = round(multiplier, 6)
        entry_debit = row.get("entry_debit")
        capital_required = row.get("capital_required")
        row["weighted_entry_debit"] = None if entry_debit is None else round(float(entry_debit) * multiplier, 4)
        row["weighted_capital_required"] = (
            None if capital_required is None else round(float(capital_required) * multiplier, 4)
        )
        row["weighted_net_pnl"] = round(float(row["net_pnl"]) * multiplier, 4)

    return applied_scheme, weights


def main() -> int:
    args = _parse_args()
    replay_data_end = args.replay_data_end or (args.entry_end_date + timedelta(days=14))
    output_prefix = args.output_prefix or _default_output_prefix(top_k=args.top_k)
    if not output_prefix.is_absolute():
        output_prefix = ROOT / output_prefix

    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    candidates, candidate_stats = _load_candidates(
        summary_csv=args.summary_csv,
        train_objective=args.train_objective,
        min_trade_count=args.min_trade_count,
        min_median_roi=args.min_median_roi,
        max_training_assignment_count=args.max_training_assignment_count,
        max_training_assignment_rate_pct=args.max_training_assignment_rate_pct,
        max_training_put_assignment_count=args.max_training_put_assignment_count,
        max_training_put_assignment_rate_pct=args.max_training_put_assignment_rate_pct,
    )
    candidates, stability_stats = _apply_stability_filter(
        candidates=candidates,
        stability_top_pool=args.stability_top_pool,
        stability_min_positive_months=args.stability_min_positive_months,
        stability_min_p25_monthly_median_roi_pct=args.stability_min_p25_monthly_median_roi_pct,
        train_objective=args.train_objective,
    )
    candidate_stats.update(stability_stats)
    if len(candidates) < args.top_k:
        if candidate_stats.get("stability_filter_enabled"):
            raise SystemExit(
                "Stability filter left only "
                f"{len(candidates)} candidates for top-{args.top_k} selection. "
                "Reduce the stability thresholds or set --stability-top-pool 0 to disable it."
            )
        raise SystemExit(f"Only {len(candidates)} candidates passed the requested filters, below top-k={args.top_k}.")
    selected = candidates[: args.top_k]
    for item in selected:
        if "training_assignment_metrics" not in item:
            item["training_assignment_metrics"] = _load_candidate_training_assignment_metrics(item)
        if "training_stability_metrics" not in item and candidate_stats.get("stability_filter_enabled"):
            item["training_stability_metrics"] = _load_candidate_training_stability_metrics(item)

    selection_rows: list[dict[str, object]] = []
    for rank, item in enumerate(selected, start=1):
        best = dict(item["best"])
        assignment_metrics = dict(item.get("training_assignment_metrics") or {})
        stability_metrics = dict(item.get("training_stability_metrics") or {})
        selection_rows.append(
            {
                "rank": rank,
                "symbol": item["symbol"],
                "train_start_date": item["payload"]["period"]["start"],
                "train_end_date": item["payload"]["period"]["requested_end"],
                "training_stage": best.get("stage", ""),
                "training_trade_count": int(best["trade_count"]),
                "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
                "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
                "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
                "training_total_roi_pct": round(float(best["total_roi_pct"]), 4),
                "training_win_rate_pct": round(float(best["win_rate_pct"]), 4),
                "training_assignment_count": int(assignment_metrics.get("training_assignment_count") or 0),
                "training_assignment_rate_pct": round(float(assignment_metrics.get("training_assignment_rate_pct") or 0.0), 4),
                "training_put_assignment_count": int(assignment_metrics.get("training_put_assignment_count") or 0),
                "training_put_assignment_rate_pct": round(float(assignment_metrics.get("training_put_assignment_rate_pct") or 0.0), 4),
                "training_months_with_trades": _int_or_blank(stability_metrics.get("training_months_with_trades")),
                "training_positive_month_count": _int_or_blank(stability_metrics.get("training_positive_month_count")),
                "training_negative_month_count": _int_or_blank(stability_metrics.get("training_negative_month_count")),
                "training_worst_month_median_roi_pct": _round_or_blank(
                    stability_metrics.get("training_worst_month_median_roi_pct")
                ),
                "training_p25_monthly_median_roi_pct": _round_or_blank(
                    stability_metrics.get("training_p25_monthly_median_roi_pct")
                ),
                "training_median_monthly_median_roi_pct": _round_or_blank(
                    stability_metrics.get("training_median_monthly_median_roi_pct")
                ),
                "training_best_month_median_roi_pct": _round_or_blank(
                    stability_metrics.get("training_best_month_median_roi_pct")
                ),
                "output_path": str(Path(item["output_path"]).relative_to(ROOT)).replace("\\", "/"),
            }
        )

    result_rows: list[dict[str, object]] = []
    ledger_rows: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = {
            executor.submit(
                _replay_symbol,
                candidate=item,
                entry_start_date=args.entry_start_date,
                entry_end_date=args.entry_end_date,
                replay_data_end=replay_data_end,
            ): str(item["symbol"])
            for item in selected
        }
        for future in as_completed(futures):
            symbol = futures[future]
            result_row, symbol_ledger = future.result()
            result_rows.append(result_row)
            ledger_rows.extend(symbol_ledger)
            print(
                json.dumps(
                    {
                        "symbol": symbol,
                        "trade_count": result_row["trade_count"],
                        "total_net_pnl": result_row["total_net_pnl"],
                        "roi_on_capital_required_pct": result_row["roi_on_capital_required_pct"],
                    },
                    sort_keys=True,
                )
            )

    result_rows.sort(key=lambda item: str(item["symbol"]))
    ledger_rows.sort(key=lambda item: (str(item["entry_date"]), str(item["symbol"])))
    applied_weighting_scheme, weights = _apply_portfolio_weights(
        selection_rows=selection_rows,
        result_rows=result_rows,
        ledger_rows=ledger_rows,
        weighting_scheme=args.weighting_scheme,
        max_symbol_weight_pct=args.max_symbol_weight_pct,
        weight_trade_count_cap=args.weight_trade_count_cap,
    )

    weekly: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {
            "trade_count": 0,
            "total_entry_debit": 0.0,
            "total_capital_required": 0.0,
            "total_net_pnl": 0.0,
            "roi_values": [],
            "roi_weights": [],
        }
    )
    for row in ledger_rows:
        week = str(row["entry_date"])
        weekly[week]["trade_count"] += 1
        if row["weighted_entry_debit"] is not None:
            weekly[week]["total_entry_debit"] += float(row["weighted_entry_debit"])
        if row["weighted_capital_required"] is not None:
            weekly[week]["total_capital_required"] += float(row["weighted_capital_required"])
        weekly[week]["total_net_pnl"] += float(row["weighted_net_pnl"])
        if row["roi_on_capital_required_pct"] is not None:
            weekly[week]["roi_values"].append(float(row["roi_on_capital_required_pct"]))
            weekly[week]["roi_weights"].append(float(row["position_multiplier"]))

    weekly_rows: list[dict[str, object]] = []
    for week in sorted(weekly):
        agg = weekly[week]
        debit = float(agg["total_entry_debit"])
        capital = float(agg["total_capital_required"])
        pnl = float(agg["total_net_pnl"])
        median_trade_roi = _weighted_median(
            list(agg["roi_values"]),
            list(agg["roi_weights"]),
        )
        weekly_rows.append(
            {
                "weighting_scheme": applied_weighting_scheme,
                "entry_week": week,
                "trade_count": int(agg["trade_count"]),
                "total_entry_debit": round(debit, 4),
                "total_capital_required": round(capital, 4),
                "total_net_pnl": round(pnl, 4),
                "roi_on_debit_pct": round(pnl / debit * 100.0, 4) if debit > 0 else "",
                "roi_on_capital_required_pct": round(pnl / capital * 100.0, 4) if capital > 0 else "",
                "median_roi_per_trade_pct": round(median_trade_roi, 4),
            }
        )

    selection_csv = Path(f"{output_prefix}_selection.csv")
    results_csv = Path(f"{output_prefix}_results.csv")
    ledger_csv = Path(f"{output_prefix}_trade_ledger.csv")
    weekly_csv = Path(f"{output_prefix}_weekly_aggregate.csv")

    _write_csv(selection_csv, selection_rows, list(selection_rows[0].keys()) if selection_rows else ["rank", "symbol"])
    _write_csv(results_csv, result_rows, list(result_rows[0].keys()) if result_rows else ["symbol"])
    _write_csv(ledger_csv, ledger_rows, list(ledger_rows[0].keys()) if ledger_rows else ["symbol"])
    _write_csv(weekly_csv, weekly_rows, list(weekly_rows[0].keys()) if weekly_rows else ["entry_week"])

    quarter_total_capital = sum(float(item["weighted_total_capital_required"]) for item in result_rows)
    quarter_total_pnl = sum(float(item["weighted_total_net_pnl"]) for item in result_rows)
    quarter_unweighted_capital = sum(float(item["total_capital_required"]) for item in result_rows)
    quarter_unweighted_pnl = sum(float(item["total_net_pnl"]) for item in result_rows)
    trade_roi_values = [float(item["roi_on_capital_required_pct"]) for item in ledger_rows if item["roi_on_capital_required_pct"] is not None]
    trade_roi_weights = [float(item["position_multiplier"]) for item in ledger_rows if item["roi_on_capital_required_pct"] is not None]
    average_trade_roi = (
        sum(value * weight for value, weight in zip(trade_roi_values, trade_roi_weights)) / sum(trade_roi_weights)
        if trade_roi_weights
        else 0.0
    )
    median_trade_roi = _weighted_median(trade_roi_values, trade_roi_weights)
    summary = {
        "candidate_pool_count": len(candidates),
        "base_candidate_count": candidate_stats["base_candidate_count"],
        "assignment_filtered_out_count": candidate_stats["assignment_filtered_out_count"],
        "selection_count": len(selection_rows),
        "entry_window_start": args.entry_start_date.isoformat(),
        "entry_window_end": args.entry_end_date.isoformat(),
        "replay_data_end": replay_data_end.isoformat(),
        "weighting_scheme": applied_weighting_scheme,
        "requested_weighting_scheme": args.weighting_scheme,
        "max_symbol_weight_pct": args.max_symbol_weight_pct,
        "weight_trade_count_cap": args.weight_trade_count_cap,
        "max_training_assignment_count": args.max_training_assignment_count,
        "max_training_assignment_rate_pct": args.max_training_assignment_rate_pct,
        "max_training_put_assignment_count": args.max_training_put_assignment_count,
        "max_training_put_assignment_rate_pct": args.max_training_put_assignment_rate_pct,
        "stability_filter_enabled": bool(candidate_stats.get("stability_filter_enabled")),
        "stability_top_pool": args.stability_top_pool,
        "stability_min_positive_months": (
            args.stability_min_positive_months if candidate_stats.get("stability_filter_enabled") else None
        ),
        "stability_min_p25_monthly_median_roi_pct": (
            args.stability_min_p25_monthly_median_roi_pct if candidate_stats.get("stability_filter_enabled") else None
        ),
        "stability_top_pool_count": int(candidate_stats.get("stability_top_pool_count") or 0),
        "stability_filtered_out_count": int(candidate_stats.get("stability_filtered_out_count") or 0),
        "stability_survivor_count": int(candidate_stats.get("stability_survivor_count") or len(candidates)),
        "trade_count": len(ledger_rows),
        "total_capital_required": round(quarter_total_capital, 4),
        "total_net_pnl": round(quarter_total_pnl, 4),
        "roi_on_capital_required_pct": round(quarter_total_pnl / quarter_total_capital * 100.0, 4) if quarter_total_capital > 0 else 0.0,
        "unweighted_total_capital_required": round(quarter_unweighted_capital, 4),
        "unweighted_total_net_pnl": round(quarter_unweighted_pnl, 4),
        "unweighted_roi_on_capital_required_pct": (
            round(quarter_unweighted_pnl / quarter_unweighted_capital * 100.0, 4) if quarter_unweighted_capital > 0 else 0.0
        ),
        "average_roi_per_trade_pct": round(average_trade_roi, 4),
        "median_roi_per_trade_pct": round(median_trade_roi, 4),
        "average_weekly_median_roi_per_trade_pct": (
            round(sum(float(item["median_roi_per_trade_pct"]) for item in weekly_rows) / len(weekly_rows), 4) if weekly_rows else 0.0
        ),
        "max_applied_symbol_weight_pct": round(max(weights.values()) * 100.0, 4) if weights else 0.0,
        "top3_applied_symbol_weight_pct": round(sum(sorted(weights.values(), reverse=True)[:3]) * 100.0, 4) if weights else 0.0,
        "selection_csv": str(selection_csv.relative_to(ROOT)).replace("\\", "/"),
        "results_csv": str(results_csv.relative_to(ROOT)).replace("\\", "/"),
        "ledger_csv": str(ledger_csv.relative_to(ROOT)).replace("\\", "/"),
        "weekly_csv": str(weekly_csv.relative_to(ROOT)).replace("\\", "/"),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
