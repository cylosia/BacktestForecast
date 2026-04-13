from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import heapq
from itertools import chain
import json
import math
from statistics import median
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
LOGS_DIR = ROOT / "logs"

from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
import grid_search_weekly_calendar_policy_two_stage as two_stage  # noqa: E402
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)


SYMBOLS = [
    "AGQ",
    "AMDL",
    "BITX",
    "BOIL",
    "KOLD",
    "CONL",
    "DPST",
    "ETHU",
    "FAS",
    "LABD",
    "LABU",
    "METU",
    "MSTU",
    "MSTX",
    "MSTZ",
    "NAIL",
    "NUGT",
    "NVDL",
    "NVDX",
    "SCO",
    "SOXL",
    "SOXS",
    "SPXL",
    "SPXS",
    "SPXU",
    "SQQQ",
    "SSO",
    "TMF",
    "TNA",
    "TQQQ",
    "TSLL",
    "TZA",
    "UPRO",
    "UVIX",
    "UVXY",
    "YINN",
    "ZSL",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rescore cached weekly calendar two-stage searches using a configurable ranking objective."
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=SYMBOLS,
        help="Optional symbol subset. Defaults to the 37-symbol weekly calendar universe.",
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help="Optional newline/comma separated symbol file. When provided, overrides the default symbol universe.",
    )
    parser.add_argument(
        "--objective",
        choices=("median", "blended"),
        default="blended",
        help="Ranking objective. 'blended' requires positive expectancy and then ranks by median ROI first.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional CSV output path. Defaults to logs/weekly_calendar_policy_<objective>_rescore_37_symbols.csv",
    )
    parser.add_argument(
        "--recompute-symbols",
        nargs="*",
        default=[],
        help="Optional symbol subset that should bypass cached trade maps and recompute live with the current engine.",
    )
    return parser.parse_args()


def _load_symbols(args: argparse.Namespace) -> list[str]:
    raw_symbols: list[str] = []
    if args.symbols_file:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        raw_symbols.extend(chunk.strip().upper() for chunk in raw_text.replace("\n", ",").split(","))
    elif args.symbols:
        raw_symbols.extend(symbol.strip().upper() for symbol in args.symbols)
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered


@dataclass(frozen=True, slots=True)
class RescoreStrategySeries:
    trade_mask: int
    net_pnls: tuple[float | None, ...]
    rois: tuple[float | None, ...]
    inferred_capitals: tuple[float | None, ...]


@dataclass(frozen=True, slots=True)
class RescoreMaskSummary:
    trade_count: int
    total_net_pnl: float
    roi_count: int
    roi_sum: float
    roi_values: tuple[float, ...]
    win_count: int
    win_sum: float
    loss_count: int
    loss_sum: float
    inferred_trade_count: int
    inferred_capital_sum: float
    inferred_net_pnl_sum: float


def _find_any_result_file(symbol: str) -> Path | None:
    lower = symbol.lower()
    candidates = [
        *sorted(LOGS_DIR.glob(f"{lower}_weekly_calendar_policy_two_stage_*.json")),
        *sorted(LOGS_DIR.glob(f"{lower}_weekly_calendar_policy_refine_periods_*.json")),
        *sorted(LOGS_DIR.glob(f"{lower}_weekly_calendar_policy_grid_indicator_periods_*.json")),
        *sorted(LOGS_DIR.glob(f"{lower}_weekly_calendar_policy_grid_*.json")),
    ]
    if not candidates:
        return None
    return candidates[0]


def _infer_trade_margin_capital(net_pnl: float | None, roi_on_margin_pct: float | None) -> float | None:
    if net_pnl is None or roi_on_margin_pct is None:
        return None
    if abs(roi_on_margin_pct) < 1e-12:
        return None
    inferred_capital = net_pnl / (roi_on_margin_pct / 100.0)
    if not math.isfinite(inferred_capital) or inferred_capital <= 0:
        return None
    return float(inferred_capital)


def _build_rescore_strategy_series(
    *,
    strategies: tuple[two_stage.StrategyConfig, ...],
    precomputed: dict[str, dict[date, dict[str, object]]],
    trading_fridays: list[date],
) -> dict[str, RescoreStrategySeries]:
    date_to_index = {trade_date: index for index, trade_date in enumerate(trading_fridays)}
    trade_series: dict[str, RescoreStrategySeries] = {}
    for strategy in strategies:
        net_pnls: list[float | None] = [None] * len(trading_fridays)
        rois: list[float | None] = [None] * len(trading_fridays)
        inferred_capitals: list[float | None] = [None] * len(trading_fridays)
        trade_mask = 0
        for trade_date, trade_row in precomputed[strategy.label].items():
            trade_index = date_to_index.get(trade_date)
            if trade_index is None:
                continue
            trade_mask |= 1 << trade_index
            net_pnl = float(trade_row["net_pnl"])
            roi_value = trade_row["roi_on_margin_pct"]
            roi = None if roi_value is None else float(roi_value)
            net_pnls[trade_index] = net_pnl
            rois[trade_index] = roi
            inferred_capitals[trade_index] = _infer_trade_margin_capital(net_pnl, roi)
        trade_series[strategy.label] = RescoreStrategySeries(
            trade_mask=trade_mask,
            net_pnls=tuple(net_pnls),
            rois=tuple(rois),
            inferred_capitals=tuple(inferred_capitals),
        )
    return trade_series


def _zero_rescore_summary() -> RescoreMaskSummary:
    return RescoreMaskSummary(
        trade_count=0,
        total_net_pnl=0.0,
        roi_count=0,
        roi_sum=0.0,
        roi_values=(),
        win_count=0,
        win_sum=0.0,
        loss_count=0,
        loss_sum=0.0,
        inferred_trade_count=0,
        inferred_capital_sum=0.0,
        inferred_net_pnl_sum=0.0,
    )


def _summarize_rescore_series_for_mask(
    *,
    series: RescoreStrategySeries,
    selection_mask: int,
) -> RescoreMaskSummary:
    active_trade_mask = series.trade_mask & selection_mask
    if active_trade_mask == 0:
        return _zero_rescore_summary()

    total_net_pnl = 0.0
    roi_sum = 0.0
    roi_count = 0
    roi_values: list[float] = []
    win_count = 0
    win_sum = 0.0
    loss_count = 0
    loss_sum = 0.0
    inferred_trade_count = 0
    inferred_capital_sum = 0.0
    inferred_net_pnl_sum = 0.0

    for trade_index in two_stage._iter_set_bit_indexes(active_trade_mask):
        net_pnl = series.net_pnls[trade_index]
        if net_pnl is None:
            continue
        total_net_pnl += net_pnl
        if net_pnl > 0:
            win_count += 1
            win_sum += net_pnl
        elif net_pnl < 0:
            loss_count += 1
            loss_sum += net_pnl

        roi_value = series.rois[trade_index]
        if roi_value is not None:
            roi_count += 1
            roi_sum += roi_value
            roi_values.append(roi_value)

        inferred_capital = series.inferred_capitals[trade_index]
        if inferred_capital is not None:
            inferred_trade_count += 1
            inferred_capital_sum += inferred_capital
            inferred_net_pnl_sum += net_pnl

    roi_values.sort()
    return RescoreMaskSummary(
        trade_count=active_trade_mask.bit_count(),
        total_net_pnl=total_net_pnl,
        roi_count=roi_count,
        roi_sum=roi_sum,
        roi_values=tuple(roi_values),
        win_count=win_count,
        win_sum=win_sum,
        loss_count=loss_count,
        loss_sum=loss_sum,
        inferred_trade_count=inferred_trade_count,
        inferred_capital_sum=inferred_capital_sum,
        inferred_net_pnl_sum=inferred_net_pnl_sum,
    )


def _combine_rescore_median_roi(
    first: RescoreMaskSummary,
    second: RescoreMaskSummary,
    third: RescoreMaskSummary,
) -> float:
    roi_count = first.roi_count + second.roi_count + third.roi_count
    if roi_count == 0:
        return 0.0
    merged_rois = sorted(chain(first.roi_values, second.roi_values, third.roi_values))
    return round(float(median(merged_rois)), 4)


def _blended_roi_score_from_values(*, total_net_pnl: float, median_roi: float, weighted_roi: float) -> float:
    if total_net_pnl <= 0.0 or median_roi <= 0.0 or weighted_roi <= 0.0:
        return 0.0
    return round((2.0 * median_roi * weighted_roi) / (median_roi + weighted_roi), 4)


def _summarize_rescore_row(
    *,
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
    indicator_periods: two_stage.IndicatorPeriodConfig,
    bull_filter,
    bear_filter,
    bull_strategy: two_stage.StrategyConfig,
    bear_strategy: two_stage.StrategyConfig,
    neutral_strategy: two_stage.StrategyConfig,
    bull_summary: RescoreMaskSummary,
    bear_summary: RescoreMaskSummary,
    neutral_summary: RescoreMaskSummary,
) -> dict[str, object]:
    trade_count = bull_summary.trade_count + bear_summary.trade_count + neutral_summary.trade_count
    total_net_pnl = bull_summary.total_net_pnl + bear_summary.total_net_pnl + neutral_summary.total_net_pnl
    total_roi_count = bull_summary.roi_count + bear_summary.roi_count + neutral_summary.roi_count
    total_roi_sum = bull_summary.roi_sum + bear_summary.roi_sum + neutral_summary.roi_sum
    total_win_count = bull_summary.win_count + bear_summary.win_count + neutral_summary.win_count
    total_loss_count = bull_summary.loss_count + bear_summary.loss_count + neutral_summary.loss_count
    total_win_sum = bull_summary.win_sum + bear_summary.win_sum + neutral_summary.win_sum
    total_loss_sum = bull_summary.loss_sum + bear_summary.loss_sum + neutral_summary.loss_sum
    inferred_trade_count = (
        bull_summary.inferred_trade_count + bear_summary.inferred_trade_count + neutral_summary.inferred_trade_count
    )
    inferred_capital_sum = (
        bull_summary.inferred_capital_sum + bear_summary.inferred_capital_sum + neutral_summary.inferred_capital_sum
    )
    inferred_net_pnl_sum = (
        bull_summary.inferred_net_pnl_sum + bear_summary.inferred_net_pnl_sum + neutral_summary.inferred_net_pnl_sum
    )
    weighted_roi = 0.0 if inferred_capital_sum <= 0 else round(inferred_net_pnl_sum / inferred_capital_sum * 100.0, 4)
    median_roi = _combine_rescore_median_roi(bull_summary, bear_summary, neutral_summary)
    blended_score = _blended_roi_score_from_values(
        total_net_pnl=total_net_pnl,
        median_roi=median_roi,
        weighted_roi=weighted_roi,
    )
    positive_expectancy = weighted_roi > 0.0 and float(total_net_pnl) > 0.0
    return {
        "indicator_periods": indicator_periods.label,
        "roc_period": indicator_periods.roc_period,
        "adx_period": indicator_periods.adx_period,
        "rsi_period": indicator_periods.rsi_period,
        "bull_filter": bull_filter.label,
        "bear_filter": bear_filter.label,
        "bull_strategy": bull_strategy.label,
        "bear_strategy": bear_strategy.label,
        "neutral_strategy": neutral_strategy.label,
        "trade_count": trade_count,
        "selection_counts": selection_counts,
        "entered_counts": entered_counts,
        "overlap_signal_count": overlap_signal_count,
        "total_net_pnl": round(total_net_pnl, 4),
        "total_roi_pct": round(total_net_pnl / two_stage.STARTING_EQUITY * 100.0, 4),
        "average_roi_on_margin_pct": round(total_roi_sum / total_roi_count, 4) if total_roi_count else 0.0,
        "median_roi_on_margin_pct": median_roi,
        "weighted_roi_on_margin_pct_inferred": weighted_roi,
        "blended_roi_score": blended_score,
        "weighted_inferred_capital_sum": round(inferred_capital_sum, 4),
        "weighted_inferred_trade_count": inferred_trade_count,
        "positive_expectancy_pass": positive_expectancy,
        "win_rate_pct": round(total_win_count / trade_count * 100.0, 4) if trade_count else 0.0,
        "average_win": round(total_win_sum / total_win_count, 4) if total_win_count else 0.0,
        "average_loss": round(total_loss_sum / total_loss_count, 4) if total_loss_count else 0.0,
    }


def _ranking_key_for_objective(item: dict[str, object], objective: str) -> tuple:
    if objective == "median":
        return (
            float(item["median_roi_on_margin_pct"]),
            float(item["average_roi_on_margin_pct"]),
            float(item["weighted_roi_on_margin_pct_inferred"]),
            float(item["total_roi_pct"]),
            float(item["win_rate_pct"]),
            int(item["trade_count"]),
        )
    return (
        int(float(item["total_net_pnl"]) > 0.0),
        float(item["blended_roi_score"]),
        float(item["weighted_roi_on_margin_pct_inferred"]),
        float(item["median_roi_on_margin_pct"]),
        float(item["total_roi_pct"]),
        float(item["average_roi_on_margin_pct"]),
        float(item["win_rate_pct"]),
        int(item["trade_count"]),
    )


def _evaluate_stage_for_objective(
    *,
    stage_name: str,
    objective: str,
    search_config: two_stage.StageSearchConfig,
    trading_fridays: list[date],
    strategy_series: dict[str, RescoreStrategySeries],
    indicators_by_period: dict[str, dict[date, dict[str, float | None]]],
) -> dict[str, object]:
    branch_strategy_labels = {
        "bullish": tuple(sorted({triplet[0].label for triplet in search_config.strategy_triplets})),
        "bearish": tuple(sorted({triplet[1].label for triplet in search_config.strategy_triplets})),
        "neutral": tuple(sorted({triplet[2].label for triplet in search_config.strategy_triplets})),
    }
    summary_cache: dict[tuple[str, int], RescoreMaskSummary] = {}
    best_result: dict[str, object] | None = None
    combo_count = 0
    total_combos = (
        len(search_config.period_configs)
        * len(search_config.bull_filters)
        * len(search_config.bear_filters)
        * len(search_config.strategy_triplets)
    )

    for period_config in search_config.period_configs:
        indicators = indicators_by_period[period_config.label]
        for bull_filter in search_config.bull_filters:
            for bear_filter in search_config.bear_filters:
                bull_mask = 0
                bear_mask = 0
                for trade_index, trade_date in enumerate(trading_fridays):
                    indicator_row = indicators.get(trade_date)
                    if bull_filter.matches(indicator_row):
                        bull_mask |= 1 << trade_index
                    if bear_filter.matches(indicator_row):
                        bear_mask |= 1 << trade_index

                bull_only_mask = bull_mask & ~bear_mask
                bear_only_mask = bear_mask & ~bull_mask
                all_dates_mask = (1 << len(trading_fridays)) - 1
                neutral_mask = all_dates_mask & ~(bull_only_mask | bear_only_mask)
                overlap_signal_count = (bull_mask & bear_mask).bit_count()
                selection_counts = {
                    "bullish": bull_only_mask.bit_count(),
                    "bearish": bear_only_mask.bit_count(),
                    "neutral": neutral_mask.bit_count(),
                }

                branch_masks = {
                    "bullish": bull_only_mask,
                    "bearish": bear_only_mask,
                    "neutral": neutral_mask,
                }
                branch_summaries: dict[str, dict[str, RescoreMaskSummary]] = {
                    "bullish": {},
                    "bearish": {},
                    "neutral": {},
                }
                for branch_name, branch_mask in branch_masks.items():
                    for strategy_label in branch_strategy_labels[branch_name]:
                        cache_key = (strategy_label, branch_mask)
                        cached_summary = summary_cache.get(cache_key)
                        if cached_summary is None:
                            cached_summary = _summarize_rescore_series_for_mask(
                                series=strategy_series[strategy_label],
                                selection_mask=branch_mask,
                            )
                            summary_cache[cache_key] = cached_summary
                        branch_summaries[branch_name][strategy_label] = cached_summary

                for bull_strategy, bear_strategy, neutral_strategy in search_config.strategy_triplets:
                    combo_count += 1
                    bull_summary = branch_summaries["bullish"][bull_strategy.label]
                    bear_summary = branch_summaries["bearish"][bear_strategy.label]
                    neutral_summary = branch_summaries["neutral"][neutral_strategy.label]
                    entered_counts = {
                        "bullish": bull_summary.trade_count,
                        "bearish": bear_summary.trade_count,
                        "neutral": neutral_summary.trade_count,
                    }
                    row = _summarize_rescore_row(
                        selection_counts=selection_counts,
                        entered_counts=entered_counts,
                        overlap_signal_count=overlap_signal_count,
                        indicator_periods=period_config,
                        bull_filter=bull_filter,
                        bear_filter=bear_filter,
                        bull_strategy=bull_strategy,
                        bear_strategy=bear_strategy,
                        neutral_strategy=neutral_strategy,
                        bull_summary=bull_summary,
                        bear_summary=bear_summary,
                        neutral_summary=neutral_summary,
                    )
                    if best_result is None or _ranking_key_for_objective(row, objective) > _ranking_key_for_objective(
                        best_result, objective
                    ):
                        best_result = row

    print(f"[{stage_name} {total_combos}/{total_combos}] objective={objective} completed")
    return {
        "stage_name": stage_name,
        "evaluated_combo_count": combo_count,
        "best_result": best_result,
    }


def _parse_period_configs(search_space: dict[str, object]) -> tuple[two_stage.IndicatorPeriodConfig, ...]:
    indicator_search = dict(search_space["indicator_period_search"])
    return tuple(
        two_stage.IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in indicator_search["roc_periods"]
        for adx_period in indicator_search["adx_periods"]
        for rsi_period in indicator_search["rsi_periods"]
    )


def _dedupe_period_configs(
    *collections: tuple[two_stage.IndicatorPeriodConfig, ...],
) -> tuple[two_stage.IndicatorPeriodConfig, ...]:
    ordered: dict[str, two_stage.IndicatorPeriodConfig] = {}
    for configs in collections:
        for config in configs:
            ordered.setdefault(config.label, config)
    return tuple(ordered.values())


def _build_stage_config(
    stage_payload: dict[str, object],
    *,
    bull_filter_lookup: dict[str, object],
    bear_filter_lookup: dict[str, object],
    strategy_lookup: dict[str, two_stage.StrategyConfig],
) -> two_stage.StageSearchConfig:
    search_space = dict(stage_payload["search_space"])
    return two_stage.StageSearchConfig(
        period_configs=_parse_period_configs(search_space),
        bull_filters=tuple(bull_filter_lookup[label] for label in search_space["bull_filters"]),
        bear_filters=tuple(bear_filter_lookup[label] for label in search_space["bear_filters"]),
        strategy_triplets=tuple(
            (
                strategy_lookup[item["bull_strategy"]],
                strategy_lookup[item["bear_strategy"]],
                strategy_lookup[item["neutral_strategy"]],
            )
            for item in search_space["strategy_triplets"]
        ),
    )


def _load_cached_trade_maps(
    *,
    symbol: str,
    start_date: date,
    latest_available_date: date,
    strategies: tuple[two_stage.StrategyConfig, ...],
) -> tuple[dict[str, dict[date, dict[str, object]]], list[str]]:
    precomputed: dict[str, dict[date, dict[str, object]]] = {}
    missing_labels: list[str] = []
    for strategy in strategies:
        cache_path = two_stage._strategy_trade_cache_path(
            symbol=symbol,
            start_date=start_date,
            latest_available_date=latest_available_date,
            strategy=strategy,
        )
        if not cache_path.exists():
            missing_labels.append(strategy.label)
            continue
        cached_payload = json.loads(cache_path.read_text())
        precomputed[strategy.label] = {
            date.fromisoformat(trade_date): trade_row
            for trade_date, trade_row in cached_payload["trade_map"].items()
        }
    return precomputed, missing_labels


def _load_cached_indicator_maps(
    *,
    symbol: str,
    start_date: date,
    latest_available_date: date,
    period_configs: tuple[two_stage.IndicatorPeriodConfig, ...],
) -> tuple[dict[str, dict[date, dict[str, float | None]]], list[str]]:
    indicator_cache: dict[str, dict[date, dict[str, float | None]]] = {}
    missing_labels: list[str] = []
    for period_config in period_configs:
        cache_path = two_stage._indicator_cache_path(
            symbol=symbol,
            start_date=start_date,
            latest_available_date=latest_available_date,
            period_config=period_config,
        )
        if not cache_path.exists():
            missing_labels.append(period_config.label)
            continue
        cached_payload = json.loads(cache_path.read_text())
        indicator_cache[period_config.label] = {
            date.fromisoformat(trade_date): indicator_row
            for trade_date, indicator_row in cached_payload["indicators"].items()
        }
    return indicator_cache, missing_labels


def _load_trading_fridays(
    *,
    store: HistoricalMarketDataStore,
    symbol: str,
    start_date: date,
    latest_available_date: date,
) -> list[date]:
    bars = store.get_underlying_day_bars(symbol, start_date, latest_available_date)
    return [
        bar.trade_date
        for bar in bars
        if start_date <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]


def _row_identity(row: dict[str, object]) -> tuple[object, ...]:
    return (
        row.get("stage"),
        row.get("indicator_periods"),
        row.get("bull_filter"),
        row.get("bear_filter"),
        row.get("bull_strategy"),
        row.get("bear_strategy"),
        row.get("neutral_strategy"),
    )


def _rescore_two_stage_symbol(
    *,
    symbol: str,
    store: HistoricalMarketDataStore,
    recompute_trade_maps: bool,
    objective: str,
) -> dict[str, object]:
    result_file = _find_any_result_file(symbol)
    if result_file is None:
        return {"symbol": symbol, "status": "missing_result_file"}

    payload = json.loads(result_file.read_text())
    if "combined_best_result" not in payload:
        return {
            "symbol": symbol,
            "status": "legacy_result_needs_rerun",
            "source_file": str(result_file.relative_to(ROOT)).replace("\\", "/"),
        }

    period = dict(payload["period"])
    start_date = date.fromisoformat(period["start"])
    latest_available_date = date.fromisoformat(period["latest_available_date"])
    bullish_strategies, bearish_strategies, neutral_strategies = two_stage._build_strategy_sets(symbol)
    all_strategies = bullish_strategies + bearish_strategies + neutral_strategies
    bull_filters = two_stage._build_default_bull_filters()
    bear_filters = two_stage._build_default_bear_filters()
    bull_filter_lookup, bear_filter_lookup, strategy_lookup = two_stage._label_maps(
        bull_filters=bull_filters,
        bear_filters=bear_filters,
        strategies=all_strategies,
    )

    broad_config = _build_stage_config(
        dict(payload["stage_1_broad"]),
        bull_filter_lookup=bull_filter_lookup,
        bear_filter_lookup=bear_filter_lookup,
        strategy_lookup=strategy_lookup,
    )
    refine_config = _build_stage_config(
        dict(payload["stage_2_refine"]),
        bull_filter_lookup=bull_filter_lookup,
        bear_filter_lookup=bear_filter_lookup,
        strategy_lookup=strategy_lookup,
    )

    trading_fridays = _load_trading_fridays(
        store=store,
        symbol=symbol,
        start_date=start_date,
        latest_available_date=latest_available_date,
    )
    if recompute_trade_maps:
        bundle = two_stage._build_bundle(
            store,
            symbol=symbol,
            start_date=start_date,
            end_date=latest_available_date,
        )
        curve = two_stage._load_risk_free_curve(
            store,
            start_date=start_date,
            end_date=latest_available_date,
        )
        precomputed = two_stage._precompute_trade_maps(
            strategies=all_strategies,
            bundle=bundle,
            trading_fridays=trading_fridays,
            latest_available_date=latest_available_date,
            curve=curve,
            start_date=start_date,
            use_cache=False,
            worker_count=2,
        )
    else:
        precomputed, missing_trade_maps = _load_cached_trade_maps(
            symbol=symbol,
            start_date=start_date,
            latest_available_date=latest_available_date,
            strategies=all_strategies,
        )
        if missing_trade_maps:
            return {
                "symbol": symbol,
                "status": "missing_trade_maps",
                "source_file": str(result_file.relative_to(ROOT)).replace("\\", "/"),
                "missing_trade_map_count": len(missing_trade_maps),
                "missing_trade_maps": ",".join(missing_trade_maps),
            }

    strategy_series = _build_rescore_strategy_series(
        strategies=all_strategies,
        precomputed=precomputed,
        trading_fridays=trading_fridays,
    )

    all_period_configs = _dedupe_period_configs(broad_config.period_configs, refine_config.period_configs)
    indicator_cache, missing_indicator_caches = _load_cached_indicator_maps(
        symbol=symbol,
        start_date=start_date,
        latest_available_date=latest_available_date,
        period_configs=all_period_configs,
    )
    if missing_indicator_caches:
        indicator_cache = two_stage._build_period_cache(
            symbol=symbol,
            start_date=start_date,
            end_date=latest_available_date,
            period_configs=all_period_configs,
            cache=indicator_cache,
            use_cache=True,
            worker_count=1,
        )

    broad_stage = _evaluate_stage_for_objective(
        stage_name="broad",
        objective=objective,
        search_config=broad_config,
        trading_fridays=trading_fridays,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
    )
    refine_stage = _evaluate_stage_for_objective(
        stage_name="refine",
        objective=objective,
        search_config=refine_config,
        trading_fridays=trading_fridays,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
    )

    rescored_best = two_stage._with_stage(dict(broad_stage["best_result"]), "broad")
    if refine_stage["best_result"] is not None and _ranking_key_for_objective(
        refine_stage["best_result"], objective
    ) > _ranking_key_for_objective(broad_stage["best_result"], objective):
        rescored_best = two_stage._with_stage(dict(refine_stage["best_result"]), "refine")

    current_best = dict(payload["combined_best_result"])
    same_as_current = _row_identity(rescored_best) == _row_identity(current_best)
    return {
        "symbol": symbol,
        "status": "rescored",
        "objective": objective,
        "source_file": str(result_file.relative_to(ROOT)).replace("\\", "/"),
        "recomputed_trade_maps": recompute_trade_maps,
        "period_start": period["start"],
        "period_latest_available_date": period["latest_available_date"],
        "rescored_stage": rescored_best["stage"],
        "rescored_indicator_periods": rescored_best["indicator_periods"],
        "rescored_roc_period": rescored_best["roc_period"],
        "rescored_adx_period": rescored_best["adx_period"],
        "rescored_rsi_period": rescored_best["rsi_period"],
        "rescored_bull_filter": rescored_best["bull_filter"],
        "rescored_bear_filter": rescored_best["bear_filter"],
        "rescored_bull_strategy": rescored_best["bull_strategy"],
        "rescored_bear_strategy": rescored_best["bear_strategy"],
        "rescored_neutral_strategy": rescored_best["neutral_strategy"],
        "rescored_trade_count": rescored_best["trade_count"],
        "rescored_total_net_pnl": rescored_best["total_net_pnl"],
        "rescored_total_roi_pct": rescored_best["total_roi_pct"],
        "rescored_average_roi_on_margin_pct": rescored_best["average_roi_on_margin_pct"],
        "rescored_median_roi_on_margin_pct": rescored_best["median_roi_on_margin_pct"],
        "rescored_weighted_roi_on_margin_pct_inferred": rescored_best["weighted_roi_on_margin_pct_inferred"],
        "rescored_blended_roi_score": rescored_best["blended_roi_score"],
        "rescored_weighted_inferred_capital_sum": rescored_best["weighted_inferred_capital_sum"],
        "rescored_weighted_inferred_trade_count": rescored_best["weighted_inferred_trade_count"],
        "rescored_positive_expectancy_pass": rescored_best["positive_expectancy_pass"],
        "rescored_win_rate_pct": rescored_best["win_rate_pct"],
        "current_best_avg_stage": current_best.get("stage", ""),
        "current_best_avg_indicator_periods": current_best["indicator_periods"],
        "current_best_avg_bull_filter": current_best["bull_filter"],
        "current_best_avg_bear_filter": current_best["bear_filter"],
        "current_best_avg_bull_strategy": current_best["bull_strategy"],
        "current_best_avg_bear_strategy": current_best["bear_strategy"],
        "current_best_avg_neutral_strategy": current_best["neutral_strategy"],
        "current_best_avg_trade_count": current_best["trade_count"],
        "current_best_avg_total_net_pnl": current_best["total_net_pnl"],
        "current_best_avg_total_roi_pct": current_best["total_roi_pct"],
        "current_best_avg_average_roi_on_margin_pct": current_best["average_roi_on_margin_pct"],
        "current_best_avg_median_roi_on_margin_pct": current_best["median_roi_on_margin_pct"],
        "current_best_avg_win_rate_pct": current_best["win_rate_pct"],
        "median_roi_delta_vs_current_best_avg": round(
            float(rescored_best["median_roi_on_margin_pct"]) - float(current_best["median_roi_on_margin_pct"]),
            4,
        ),
        "average_roi_delta_vs_current_best_avg": round(
            float(rescored_best["average_roi_on_margin_pct"]) - float(current_best["average_roi_on_margin_pct"]),
            4,
        ),
        "total_net_pnl_delta_vs_current_best_avg": round(
            float(rescored_best["total_net_pnl"]) - float(current_best["total_net_pnl"]),
            4,
        ),
        "same_as_current_best_avg": same_as_current,
    }


def _write_csv(*, rows: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _parse_args()
    symbols = _load_symbols(args)
    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    recompute_symbols = {symbol.upper() for symbol in args.recompute_symbols}
    output_path = args.output or LOGS_DIR / f"weekly_calendar_policy_{args.objective}_rescore_37_symbols.csv"
    rows: list[dict[str, object]] = []
    for raw_symbol in symbols:
        symbol = raw_symbol.upper()
        print(f"[rescore] {symbol} objective={args.objective}")
        rows.append(
            _rescore_two_stage_symbol(
                symbol=symbol,
                store=store,
                recompute_trade_maps=symbol in recompute_symbols,
                objective=args.objective,
            )
        )
    _write_csv(rows=rows, output_path=output_path)
    rescored_count = sum(1 for row in rows if row.get("status") == "rescored")
    changed_count = sum(
        1
        for row in rows
        if row.get("status") == "rescored" and str(row.get("same_as_current_best_avg", "")).lower() in {"false", "0"}
    )
    legacy_count = sum(1 for row in rows if row.get("status") == "legacy_result_needs_rerun")
    print(
        json.dumps(
            {
                "output_path": str(output_path),
                "objective": args.objective,
                "rescored_count": rescored_count,
                "changed_count": changed_count,
                "legacy_count": legacy_count,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
