from __future__ import annotations

import argparse
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
import heapq
import json
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import adx, roc, rsi  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.schemas.backtests import StrategyType  # noqa: E402
from grid_search_agq_weekly_calendar_policy import (  # noqa: E402
    IndicatorPeriodConfig,
    NegativeFilterConfig,
)
from grid_search_fas_faz_weekly_calendar_policy import (  # noqa: E402
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    FilterConfig,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

BROAD_ROC_PERIODS = (21, 42, 63, 126)
BROAD_ADX_PERIODS = (7, 14, 21)
BROAD_RSI_PERIODS = (7, 14, 21)
TOP_RESULT_LIMIT = 100
DEFAULT_REFINE_TOP_ROWS = 12
DEFAULT_REFINE_TOP_PERIOD_SEEDS = 3
DEFAULT_REFINE_TOP_STRATEGY_TRIPLETS = 4
DEFAULT_REFINE_TOP_BULL_FILTERS = 6
DEFAULT_REFINE_TOP_BEAR_FILTERS = 6
DEFAULT_REFINE_DELTA_STEP = 5
DEFAULT_REFINE_PROFIT_TARGET_PCTS = (50, 60, 70, 75, 80)
DEFAULT_PRECOMPUTE_WORKERS = 4
DEFAULT_INDICATOR_WORKERS = 4
DEFAULT_PROGRESS_INTERVAL = 10_000
CACHE_ROOT = ROOT / "logs" / "search_cache" / "weekly_calendar_policy_two_stage"
STARTING_EQUITY_PCT_MULTIPLIER = 100.0 / STARTING_EQUITY
_STRATEGY_LABEL_PATTERN = re.compile(r"^(?P<prefix>.+)_d(?P<delta>\d+)_pt(?P<profit_target>\d+)$")
HEAVY_ROC_BUFFER = 5.0
HEAVY_ADX_BUFFER = 4.0
HEAVY_RSI_BUFFER = 5.0
REGIME_MODE_ALL = "all"
REGIME_MODE_BEST_REGIME_ONLY = "best_regime_only"


def _is_assignment_exit_reason(exit_reason: object) -> bool:
    return str(exit_reason or "").startswith("early_assignment_")


def _is_put_assignment_exit_reason(exit_reason: object) -> bool:
    return str(exit_reason or "") == "early_assignment_put_deep_itm"


@dataclass(frozen=True, slots=True)
class StageSearchConfig:
    period_configs: tuple[IndicatorPeriodConfig, ...]
    bull_filters: tuple[FilterConfig, ...]
    bear_filters: tuple[NegativeFilterConfig, ...]
    strategy_triplets: tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...]
    heavy_bull_strategies: tuple[StrategyConfig, ...] = ()
    heavy_bear_strategies: tuple[StrategyConfig, ...] = ()


@dataclass(frozen=True, slots=True)
class StrategyTradeSeries:
    trade_mask: int
    assignment_mask: int
    put_assignment_mask: int
    net_pnls: tuple[float | None, ...]
    rois: tuple[float | None, ...]


@dataclass(frozen=True, slots=True)
class StrategyMaskSummary:
    trade_count: int
    assignment_count: int
    put_assignment_count: int
    total_net_pnl: float
    roi_count: int
    roi_sum: float
    roi_values: tuple[float, ...]
    win_count: int
    win_sum: float
    loss_count: int
    loss_sum: float


@dataclass(frozen=True, slots=True)
class StageCandidate:
    regime_mode: str
    active_regime: str | None
    indicator_periods: IndicatorPeriodConfig
    bull_filter: FilterConfig
    bear_filter: NegativeFilterConfig
    heavy_bull_strategy: StrategyConfig | None
    bull_strategy: StrategyConfig
    bear_strategy: StrategyConfig
    heavy_bear_strategy: StrategyConfig | None
    neutral_strategy: StrategyConfig
    selection_counts: dict[str, int]
    entered_counts: dict[str, int]
    overlap_signal_count: int
    trade_count: int
    assignment_count: int
    assignment_rate_pct: float
    put_assignment_count: int
    put_assignment_rate_pct: float
    total_net_pnl: float
    total_roi_pct: float
    average_roi_on_margin_pct: float
    median_roi_on_margin_pct: float
    win_rate_pct: float
    average_win: float
    average_loss: float


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a broad weekly calendar policy grid search and an automatic focused refinement "
            "around the best broad signal family."
        )
    )
    parser.add_argument("--symbol", required=True, help="Underlying symbol, for example AGQ or CONL.")
    parser.add_argument(
        "--start-date",
        required=True,
        type=date.fromisoformat,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--requested-end-date",
        default=REQUESTED_END_DATE.isoformat(),
        type=date.fromisoformat,
        help="Requested end date in YYYY-MM-DD format. Defaults to the repo standard end date.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON output path. Defaults to logs/<symbol>_weekly_calendar_policy_two_stage_*.json.",
    )
    parser.add_argument(
        "--refine-top-rows",
        type=int,
        default=DEFAULT_REFINE_TOP_ROWS,
        help="How many top broad rows to mine for refinement seeds.",
    )
    parser.add_argument(
        "--refine-top-period-seeds",
        type=int,
        default=DEFAULT_REFINE_TOP_PERIOD_SEEDS,
        help="How many unique broad period configs to seed into the refinement neighborhood.",
    )
    parser.add_argument(
        "--refine-top-strategy-triplets",
        type=int,
        default=DEFAULT_REFINE_TOP_STRATEGY_TRIPLETS,
        help="How many unique broad strategy triplets to keep for refinement.",
    )
    parser.add_argument(
        "--refine-top-bull-filters",
        type=int,
        default=DEFAULT_REFINE_TOP_BULL_FILTERS,
        help="How many unique broad bullish filters to keep for refinement.",
    )
    parser.add_argument(
        "--refine-top-bear-filters",
        type=int,
        default=DEFAULT_REFINE_TOP_BEAR_FILTERS,
        help="How many unique broad bearish filters to keep for refinement.",
    )
    parser.add_argument(
        "--refine-delta-step",
        type=int,
        default=DEFAULT_REFINE_DELTA_STEP,
        help=(
            "Delta step used by refine-stage one-branch-at-a-time neighborhood expansion. "
            "Use 0 to disable delta expansion. Defaults to 5."
        ),
    )
    parser.add_argument(
        "--refine-profit-target-pcts",
        type=_parse_profit_target_pcts,
        default=DEFAULT_REFINE_PROFIT_TARGET_PCTS,
        help=(
            "Comma-separated profit-target percentages to probe during refine-stage TP expansion. "
            "Defaults to 50,60,70,75,80."
        ),
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable disk caching for precomputed trade maps and indicator series.",
    )
    parser.add_argument(
        "--precompute-workers",
        type=int,
        default=DEFAULT_PRECOMPUTE_WORKERS,
        help="Thread count for uncached strategy precompute work. Defaults to 4.",
    )
    parser.add_argument(
        "--indicator-workers",
        type=int,
        default=DEFAULT_INDICATOR_WORKERS,
        help="Thread count for uncached indicator period loading. Defaults to 4.",
    )
    parser.add_argument(
        "--objective",
        choices=("average", "median"),
        default="average",
        help="Ranking objective for the primary best-result selection. Defaults to average ROI on margin.",
    )
    parser.add_argument(
        "--regime-mode",
        choices=(REGIME_MODE_ALL, REGIME_MODE_BEST_REGIME_ONLY),
        default=REGIME_MODE_BEST_REGIME_ONLY,
        help=(
            "Whether to optimize across all regimes together or select a single best regime and "
            "optimize only that branch. Defaults to best_regime_only."
        ),
    )
    return parser.parse_args()


def _parse_profit_target_pcts(raw_value: str) -> tuple[int, ...]:
    values: list[int] = []
    seen: set[int] = set()
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        value = int(item)
        if value <= 0:
            raise argparse.ArgumentTypeError("Profit-target percentages must be positive integers.")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("At least one refine-stage profit-target percentage is required.")
    return tuple(values)


def _parse_strategy_label(label: str) -> tuple[str, int, int]:
    match = _STRATEGY_LABEL_PATTERN.fullmatch(label)
    if match is None:
        raise ValueError(f"Unsupported strategy label format: {label}")
    return (
        match.group("prefix"),
        int(match.group("delta")),
        int(match.group("profit_target")),
    )


def _build_default_bull_filters() -> tuple[FilterConfig, ...]:
    return (
        FilterConfig(0.0, 10.0, None),
        FilterConfig(0.0, 14.0, None),
        FilterConfig(0.0, 18.0, None),
        FilterConfig(0.0, 14.0, 55.0),
        FilterConfig(0.0, 14.0, 60.0),
        FilterConfig(5.0, 14.0, None),
        FilterConfig(5.0, 18.0, None),
        FilterConfig(5.0, 14.0, 55.0),
        FilterConfig(5.0, 18.0, 60.0),
        FilterConfig(10.0, 14.0, None),
        FilterConfig(10.0, 18.0, None),
        FilterConfig(10.0, 18.0, 60.0),
    )


def _build_default_bear_filters() -> tuple[NegativeFilterConfig, ...]:
    return (
        NegativeFilterConfig(0.0, 14.0, None),
        NegativeFilterConfig(0.0, 18.0, None),
        NegativeFilterConfig(0.0, 22.0, None),
        NegativeFilterConfig(0.0, 18.0, 45.0),
        NegativeFilterConfig(0.0, 18.0, 40.0),
        NegativeFilterConfig(-5.0, 18.0, None),
        NegativeFilterConfig(-5.0, 22.0, None),
        NegativeFilterConfig(-5.0, 18.0, 45.0),
        NegativeFilterConfig(-5.0, 22.0, 40.0),
        NegativeFilterConfig(-10.0, 18.0, None),
        NegativeFilterConfig(-10.0, 22.0, None),
        NegativeFilterConfig(-10.0, 18.0, 40.0),
    )


def _build_strategy_sets(symbol: str) -> tuple[tuple[StrategyConfig, ...], tuple[StrategyConfig, ...], tuple[StrategyConfig, ...]]:
    lower = symbol.lower()
    bullish = (
        StrategyConfig(f"{lower}_call_d40_pt50", symbol, StrategyType.CALENDAR_SPREAD, 40, 50),
        StrategyConfig(f"{lower}_call_d40_pt75", symbol, StrategyType.CALENDAR_SPREAD, 40, 75),
        StrategyConfig(f"{lower}_call_d50_pt50", symbol, StrategyType.CALENDAR_SPREAD, 50, 50),
        StrategyConfig(f"{lower}_call_d50_pt75", symbol, StrategyType.CALENDAR_SPREAD, 50, 75),
    )
    bearish = (
        StrategyConfig(f"bear_{lower}_call_d40_pt50", symbol, StrategyType.CALENDAR_SPREAD, 40, 50),
        StrategyConfig(f"bear_{lower}_call_d40_pt75", symbol, StrategyType.CALENDAR_SPREAD, 40, 75),
        StrategyConfig(f"bear_{lower}_call_d50_pt50", symbol, StrategyType.CALENDAR_SPREAD, 50, 50),
        StrategyConfig(f"bear_{lower}_call_d50_pt75", symbol, StrategyType.CALENDAR_SPREAD, 50, 75),
        StrategyConfig(f"bear_{lower}_put_d40_pt50", symbol, StrategyType.PUT_CALENDAR_SPREAD, 40, 50),
        StrategyConfig(f"bear_{lower}_put_d40_pt75", symbol, StrategyType.PUT_CALENDAR_SPREAD, 40, 75),
        StrategyConfig(f"bear_{lower}_put_d50_pt50", symbol, StrategyType.PUT_CALENDAR_SPREAD, 50, 50),
        StrategyConfig(f"bear_{lower}_put_d50_pt75", symbol, StrategyType.PUT_CALENDAR_SPREAD, 50, 75),
    )
    neutral = (
        StrategyConfig(f"neutral_{lower}_call_d40_pt50", symbol, StrategyType.CALENDAR_SPREAD, 40, 50),
        StrategyConfig(f"neutral_{lower}_call_d40_pt75", symbol, StrategyType.CALENDAR_SPREAD, 40, 75),
        StrategyConfig(f"neutral_{lower}_call_d50_pt50", symbol, StrategyType.CALENDAR_SPREAD, 50, 50),
        StrategyConfig(f"neutral_{lower}_call_d50_pt75", symbol, StrategyType.CALENDAR_SPREAD, 50, 75),
    )
    return bullish, bearish, neutral


def _build_heavy_strategy_sets(symbol: str) -> tuple[tuple[StrategyConfig, ...], tuple[StrategyConfig, ...]]:
    lower = symbol.lower()
    heavy_bullish = (
        StrategyConfig(f"{lower}_call_d30_pt50", symbol, StrategyType.CALENDAR_SPREAD, 30, 50),
        StrategyConfig(f"{lower}_call_d30_pt75", symbol, StrategyType.CALENDAR_SPREAD, 30, 75),
    )
    heavy_bearish = (
        StrategyConfig(f"bear_{lower}_call_d30_pt50", symbol, StrategyType.CALENDAR_SPREAD, 30, 50),
        StrategyConfig(f"bear_{lower}_call_d30_pt75", symbol, StrategyType.CALENDAR_SPREAD, 30, 75),
        StrategyConfig(f"bear_{lower}_put_d30_pt50", symbol, StrategyType.PUT_CALENDAR_SPREAD, 30, 50),
        StrategyConfig(f"bear_{lower}_put_d30_pt75", symbol, StrategyType.PUT_CALENDAR_SPREAD, 30, 75),
    )
    return heavy_bullish, heavy_bearish


def _resolve_latest_available_date_from_bundle(bundle, requested_end: date) -> date:
    if not bundle.bars:
        raise SystemExit("Missing underlying bars.")
    return min(max(bar.trade_date for bar in bundle.bars), requested_end)


def _symbol_cache_dir(*, symbol: str, start_date: date, latest_available_date: date) -> Path:
    return CACHE_ROOT / symbol.lower() / f"{start_date.isoformat()}_{latest_available_date.isoformat()}"


def _strategy_trade_cache_path(
    *,
    symbol: str,
    start_date: date,
    latest_available_date: date,
    strategy: StrategyConfig,
) -> Path:
    return _symbol_cache_dir(symbol=symbol, start_date=start_date, latest_available_date=latest_available_date) / "trade_maps" / f"{strategy.label}.json"


def _indicator_cache_path(
    *,
    symbol: str,
    start_date: date,
    latest_available_date: date,
    period_config: IndicatorPeriodConfig,
) -> Path:
    return _symbol_cache_dir(symbol=symbol, start_date=start_date, latest_available_date=latest_available_date) / "indicators" / f"{period_config.label}.json"


def _indicator_search_payload(period_configs: tuple[IndicatorPeriodConfig, ...]) -> dict[str, list[int]]:
    return {
        "roc_periods": sorted({item.roc_period for item in period_configs}),
        "adx_periods": sorted({item.adx_period for item in period_configs}),
        "rsi_periods": sorted({item.rsi_period for item in period_configs}),
    }


def _iter_set_bit_indexes(mask: int):
    while mask:
        lowest_set_bit = mask & -mask
        yield lowest_set_bit.bit_length() - 1
        mask ^= lowest_set_bit


def _build_strategy_trade_series(
    *,
    strategies: tuple[StrategyConfig, ...],
    precomputed: dict[str, dict[date, dict[str, object]]],
    trading_fridays: list[date],
) -> dict[str, StrategyTradeSeries]:
    date_to_index = {trade_date: index for index, trade_date in enumerate(trading_fridays)}
    trade_series: dict[str, StrategyTradeSeries] = {}
    for strategy in strategies:
        net_pnls: list[float | None] = [None] * len(trading_fridays)
        rois: list[float | None] = [None] * len(trading_fridays)
        trade_mask = 0
        assignment_mask = 0
        put_assignment_mask = 0
        for trade_date, trade_row in precomputed[strategy.label].items():
            trade_index = date_to_index.get(trade_date)
            if trade_index is None:
                continue
            trade_mask |= 1 << trade_index
            exit_reason = trade_row.get("exit_reason")
            if _is_assignment_exit_reason(exit_reason):
                assignment_mask |= 1 << trade_index
            if _is_put_assignment_exit_reason(exit_reason):
                put_assignment_mask |= 1 << trade_index
            net_pnls[trade_index] = float(trade_row["net_pnl"])
            roi_value = trade_row["roi_on_margin_pct"]
            rois[trade_index] = None if roi_value is None else float(roi_value)
        trade_series[strategy.label] = StrategyTradeSeries(
            trade_mask=trade_mask,
            assignment_mask=assignment_mask,
            put_assignment_mask=put_assignment_mask,
            net_pnls=tuple(net_pnls),
            rois=tuple(rois),
        )
    return trade_series


def _zero_summary() -> StrategyMaskSummary:
    return StrategyMaskSummary(
        trade_count=0,
        assignment_count=0,
        put_assignment_count=0,
        total_net_pnl=0.0,
        roi_count=0,
        roi_sum=0.0,
        roi_values=(),
        win_count=0,
        win_sum=0.0,
        loss_count=0,
        loss_sum=0.0,
    )


def _summarize_series_for_mask(
    *,
    series: StrategyTradeSeries,
    selection_mask: int,
) -> StrategyMaskSummary:
    active_trade_mask = series.trade_mask & selection_mask
    if active_trade_mask == 0:
        return _zero_summary()

    total_net_pnl = 0.0
    roi_sum = 0.0
    roi_count = 0
    roi_values: list[float] = []
    win_count = 0
    win_sum = 0.0
    loss_count = 0
    loss_sum = 0.0

    for trade_index in _iter_set_bit_indexes(active_trade_mask):
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

    roi_values.sort()
    return StrategyMaskSummary(
        trade_count=active_trade_mask.bit_count(),
        assignment_count=(series.assignment_mask & active_trade_mask).bit_count(),
        put_assignment_count=(series.put_assignment_mask & active_trade_mask).bit_count(),
        total_net_pnl=total_net_pnl,
        roi_count=roi_count,
        roi_sum=roi_sum,
        roi_values=tuple(roi_values),
        win_count=win_count,
        win_sum=win_sum,
        loss_count=loss_count,
        loss_sum=loss_sum,
    )


def _combine_median_value(*summaries: StrategyMaskSummary) -> float:
    roi_count = sum(summary.roi_count for summary in summaries)
    if roi_count == 0:
        return 0.0
    median_low_index = (roi_count - 1) // 2
    median_high_index = roi_count // 2
    merged_rois = sorted(roi for summary in summaries for roi in summary.roi_values)
    return (merged_rois[median_low_index] + merged_rois[median_high_index]) / 2.0


def _metric_ranking_key(
    *,
    average_roi_on_margin_pct: float,
    median_roi_on_margin_pct: float,
    total_roi_pct: float,
    win_rate_pct: float,
    trade_count: int,
    objective: str,
) -> tuple[float, float, float, float, int]:
    if objective == "median":
        return (
            median_roi_on_margin_pct,
            average_roi_on_margin_pct,
            total_roi_pct,
            win_rate_pct,
            trade_count,
        )
    return (
        average_roi_on_margin_pct,
        median_roi_on_margin_pct,
        total_roi_pct,
        win_rate_pct,
        trade_count,
    )


def _build_stage_candidate(
    *,
    regime_mode: str,
    active_regime: str | None,
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
    indicator_periods: IndicatorPeriodConfig,
    bull_filter: FilterConfig,
    bear_filter: NegativeFilterConfig,
    heavy_bull_strategy: StrategyConfig | None,
    bull_strategy: StrategyConfig,
    bear_strategy: StrategyConfig,
    heavy_bear_strategy: StrategyConfig | None,
    neutral_strategy: StrategyConfig,
    trade_count: int,
    assignment_count: int,
    put_assignment_count: int,
    total_net_pnl: float,
    total_roi_pct: float,
    average_roi_on_margin_pct: float,
    median_roi_on_margin_pct: float,
    win_rate_pct: float,
    average_win: float,
    average_loss: float,
) -> StageCandidate:
    return StageCandidate(
        regime_mode=regime_mode,
        active_regime=active_regime,
        indicator_periods=indicator_periods,
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        heavy_bull_strategy=heavy_bull_strategy,
        bull_strategy=bull_strategy,
        bear_strategy=bear_strategy,
        heavy_bear_strategy=heavy_bear_strategy,
        neutral_strategy=neutral_strategy,
        selection_counts=selection_counts,
        entered_counts=entered_counts,
        overlap_signal_count=overlap_signal_count,
        trade_count=trade_count,
        assignment_count=assignment_count,
        assignment_rate_pct=round((assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
        put_assignment_count=put_assignment_count,
        put_assignment_rate_pct=round((put_assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
        total_net_pnl=round(total_net_pnl, 4),
        total_roi_pct=round(total_roi_pct, 4),
        average_roi_on_margin_pct=round(average_roi_on_margin_pct, 4),
        median_roi_on_margin_pct=round(median_roi_on_margin_pct, 4),
        win_rate_pct=round(win_rate_pct, 4),
        average_win=round(average_win, 4),
        average_loss=round(average_loss, 4),
    )


def _candidate_to_row(candidate: StageCandidate | None) -> dict[str, object] | None:
    if candidate is None:
        return None
    return {
        "regime_mode": candidate.regime_mode,
        **({"active_regime": candidate.active_regime} if candidate.active_regime is not None else {}),
        "indicator_periods": candidate.indicator_periods.label,
        "roc_period": candidate.indicator_periods.roc_period,
        "adx_period": candidate.indicator_periods.adx_period,
        "rsi_period": candidate.indicator_periods.rsi_period,
        "bull_filter": candidate.bull_filter.label,
        "bear_filter": candidate.bear_filter.label,
        **(
            {"heavy_bull_strategy": candidate.heavy_bull_strategy.label}
            if candidate.heavy_bull_strategy is not None
            else {}
        ),
        "bull_strategy": candidate.bull_strategy.label,
        "bear_strategy": candidate.bear_strategy.label,
        **(
            {"heavy_bear_strategy": candidate.heavy_bear_strategy.label}
            if candidate.heavy_bear_strategy is not None
            else {}
        ),
        "neutral_strategy": candidate.neutral_strategy.label,
        "trade_count": candidate.trade_count,
        "assignment_count": candidate.assignment_count,
        "assignment_rate_pct": candidate.assignment_rate_pct,
        "put_assignment_count": candidate.put_assignment_count,
        "put_assignment_rate_pct": candidate.put_assignment_rate_pct,
        "selection_counts": dict(candidate.selection_counts),
        "entered_counts": dict(candidate.entered_counts),
        "overlap_signal_count": candidate.overlap_signal_count,
        "total_net_pnl": candidate.total_net_pnl,
        "total_roi_pct": candidate.total_roi_pct,
        "average_roi_on_margin_pct": candidate.average_roi_on_margin_pct,
        "median_roi_on_margin_pct": candidate.median_roi_on_margin_pct,
        "win_rate_pct": candidate.win_rate_pct,
        "average_win": candidate.average_win,
        "average_loss": candidate.average_loss,
    }


def _candidate_ranking_key(candidate: StageCandidate, *, objective: str) -> tuple[float, float, float, float, int]:
    return _metric_ranking_key(
        average_roi_on_margin_pct=candidate.average_roi_on_margin_pct,
        median_roi_on_margin_pct=candidate.median_roi_on_margin_pct,
        total_roi_pct=candidate.total_roi_pct,
        win_rate_pct=candidate.win_rate_pct,
        trade_count=candidate.trade_count,
        objective=objective,
    )


def _ranking_key(item: dict[str, object], *, objective: str) -> tuple[float, float, float, float, int]:
    return _metric_ranking_key(
        average_roi_on_margin_pct=float(item["average_roi_on_margin_pct"]),
        median_roi_on_margin_pct=float(item["median_roi_on_margin_pct"]),
        total_roi_pct=float(item["total_roi_pct"]),
        win_rate_pct=float(item["win_rate_pct"]),
        trade_count=int(item["trade_count"]),
        objective=objective,
    )


def _push_top_candidate(
    *,
    heap: list[tuple[tuple[float, float, float, float, int], int, StageCandidate]],
    candidate: StageCandidate,
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


def _available_regime_labels(*, heavy_bull_enabled: bool, heavy_bear_enabled: bool) -> tuple[str, ...]:
    labels: list[str] = []
    if heavy_bull_enabled:
        labels.append("heavy_bullish")
    labels.extend(("bullish", "neutral", "bearish"))
    if heavy_bear_enabled:
        labels.append("heavy_bearish")
    return tuple(labels)


def _indicator_triplet_from_row(
    indicator_row: dict[str, float | None] | None,
) -> tuple[float | None, float | None, float | None]:
    if indicator_row is None:
        return (None, None, None)
    return (
        indicator_row.get("roc63"),
        indicator_row.get("adx14"),
        indicator_row.get("rsi14"),
    )


def _bull_filter_matches_triplet(
    *,
    filter_config: FilterConfig,
    indicator_triplet: tuple[float | None, float | None, float | None],
) -> bool:
    roc_threshold = filter_config.roc_threshold
    adx_threshold = filter_config.adx_threshold
    rsi_threshold = filter_config.rsi_threshold
    roc_value, adx_value, rsi_value = indicator_triplet
    if roc_value is None or roc_value <= roc_threshold:
        return False
    adx_ok = adx_value is not None and adx_value > adx_threshold
    if rsi_threshold is None:
        return adx_ok
    return adx_ok or (rsi_value is not None and rsi_value > rsi_threshold)


def _bear_filter_matches_triplet(
    *,
    filter_config: NegativeFilterConfig,
    indicator_triplet: tuple[float | None, float | None, float | None],
) -> bool:
    roc_threshold = filter_config.roc_threshold
    adx_threshold = filter_config.adx_threshold
    rsi_threshold = filter_config.rsi_threshold
    roc_value, adx_value, rsi_value = indicator_triplet
    if roc_value is None or roc_value >= roc_threshold:
        return False
    adx_ok = adx_value is not None and adx_value > adx_threshold
    if rsi_threshold is None:
        return adx_ok
    return adx_ok or (rsi_value is not None and rsi_value < rsi_threshold)


def _heavy_bull_filter_matches_triplet(
    *,
    filter_config: FilterConfig,
    indicator_triplet: tuple[float | None, float | None, float | None],
) -> bool:
    roc_threshold = filter_config.roc_threshold + HEAVY_ROC_BUFFER
    adx_threshold = filter_config.adx_threshold + HEAVY_ADX_BUFFER
    rsi_threshold = None if filter_config.rsi_threshold is None else filter_config.rsi_threshold + HEAVY_RSI_BUFFER
    roc_value, adx_value, rsi_value = indicator_triplet
    if roc_value is None or roc_value <= roc_threshold:
        return False
    adx_ok = adx_value is not None and adx_value > adx_threshold
    if rsi_threshold is None:
        return adx_ok
    return adx_ok or (rsi_value is not None and rsi_value > rsi_threshold)


def _heavy_bear_filter_matches_triplet(
    *,
    filter_config: NegativeFilterConfig,
    indicator_triplet: tuple[float | None, float | None, float | None],
) -> bool:
    roc_threshold = filter_config.roc_threshold - HEAVY_ROC_BUFFER
    adx_threshold = filter_config.adx_threshold + HEAVY_ADX_BUFFER
    rsi_threshold = None if filter_config.rsi_threshold is None else filter_config.rsi_threshold - HEAVY_RSI_BUFFER
    roc_value, adx_value, rsi_value = indicator_triplet
    if roc_value is None or roc_value >= roc_threshold:
        return False
    adx_ok = adx_value is not None and adx_value > adx_threshold
    if rsi_threshold is None:
        return adx_ok
    return adx_ok or (rsi_value is not None and rsi_value < rsi_threshold)


def _classify_trade_regime(
    *,
    indicator_row: dict[str, float | None] | None,
    bull_filter: FilterConfig,
    bear_filter: NegativeFilterConfig,
    enable_heavy_bull: bool,
    enable_heavy_bear: bool,
) -> str:
    indicator_triplet = _indicator_triplet_from_row(indicator_row)
    bull = _bull_filter_matches_triplet(filter_config=bull_filter, indicator_triplet=indicator_triplet)
    bear = _bear_filter_matches_triplet(filter_config=bear_filter, indicator_triplet=indicator_triplet)
    if bull and not bear:
        if enable_heavy_bull and _heavy_bull_filter_matches_triplet(
            filter_config=bull_filter,
            indicator_triplet=indicator_triplet,
        ):
            return "heavy_bullish"
        return "bullish"
    if bear and not bull:
        if enable_heavy_bear and _heavy_bear_filter_matches_triplet(
            filter_config=bear_filter,
            indicator_triplet=indicator_triplet,
        ):
            return "heavy_bearish"
        return "bearish"
    return "neutral"


def _indicator_triplets_for_trading_fridays(
    *,
    indicators: dict[date, dict[str, float | None]],
    trading_fridays: list[date],
) -> list[tuple[float | None, float | None, float | None]]:
    indicator_triplets: list[tuple[float | None, float | None, float | None]] = []
    for trade_date in trading_fridays:
        indicator_triplets.append(_indicator_triplet_from_row(indicators.get(trade_date)))
    return indicator_triplets


def _build_bull_filter_mask(
    *,
    filter_config: FilterConfig,
    indicator_triplets: list[tuple[float | None, float | None, float | None]],
) -> int:
    mask = 0
    for trade_index, indicator_triplet in enumerate(indicator_triplets):
        if _bull_filter_matches_triplet(
            filter_config=filter_config,
            indicator_triplet=indicator_triplet,
        ):
            mask |= 1 << trade_index
    return mask


def _build_bear_filter_mask(
    *,
    filter_config: NegativeFilterConfig,
    indicator_triplets: list[tuple[float | None, float | None, float | None]],
) -> int:
    mask = 0
    for trade_index, indicator_triplet in enumerate(indicator_triplets):
        if _bear_filter_matches_triplet(
            filter_config=filter_config,
            indicator_triplet=indicator_triplet,
        ):
            mask |= 1 << trade_index
    return mask


def _build_heavy_bull_filter_mask(
    *,
    filter_config: FilterConfig,
    indicator_triplets: list[tuple[float | None, float | None, float | None]],
) -> int:
    mask = 0
    for trade_index, indicator_triplet in enumerate(indicator_triplets):
        if _heavy_bull_filter_matches_triplet(
            filter_config=filter_config,
            indicator_triplet=indicator_triplet,
        ):
            mask |= 1 << trade_index
    return mask


def _build_heavy_bear_filter_mask(
    *,
    filter_config: NegativeFilterConfig,
    indicator_triplets: list[tuple[float | None, float | None, float | None]],
) -> int:
    mask = 0
    for trade_index, indicator_triplet in enumerate(indicator_triplets):
        if _heavy_bear_filter_matches_triplet(
            filter_config=filter_config,
            indicator_triplet=indicator_triplet,
        ):
            mask |= 1 << trade_index
    return mask


def _summarize_from_branch_summaries(
    *,
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
    indicator_periods: IndicatorPeriodConfig,
    bull_filter: FilterConfig,
    bear_filter: NegativeFilterConfig,
    bull_strategy: StrategyConfig,
    bear_strategy: StrategyConfig,
    neutral_strategy: StrategyConfig,
    bull_summary: StrategyMaskSummary,
    bear_summary: StrategyMaskSummary,
    neutral_summary: StrategyMaskSummary,
) -> dict[str, object]:
    trade_count = bull_summary.trade_count + bear_summary.trade_count + neutral_summary.trade_count
    total_net_pnl = bull_summary.total_net_pnl + bear_summary.total_net_pnl + neutral_summary.total_net_pnl
    assignment_count = bull_summary.assignment_count + bear_summary.assignment_count + neutral_summary.assignment_count
    put_assignment_count = (
        bull_summary.put_assignment_count + bear_summary.put_assignment_count + neutral_summary.put_assignment_count
    )
    total_roi_count = bull_summary.roi_count + bear_summary.roi_count + neutral_summary.roi_count
    total_roi_sum = bull_summary.roi_sum + bear_summary.roi_sum + neutral_summary.roi_sum
    total_win_count = bull_summary.win_count + bear_summary.win_count + neutral_summary.win_count
    total_loss_count = bull_summary.loss_count + bear_summary.loss_count + neutral_summary.loss_count
    total_win_sum = bull_summary.win_sum + bear_summary.win_sum + neutral_summary.win_sum
    total_loss_sum = bull_summary.loss_sum + bear_summary.loss_sum + neutral_summary.loss_sum
    return _candidate_to_row(
        _build_stage_candidate(
            regime_mode=REGIME_MODE_ALL,
            active_regime=None,
            selection_counts=selection_counts,
            entered_counts=entered_counts,
            overlap_signal_count=overlap_signal_count,
            indicator_periods=indicator_periods,
            bull_filter=bull_filter,
            bear_filter=bear_filter,
            heavy_bull_strategy=None,
            bull_strategy=bull_strategy,
            bear_strategy=bear_strategy,
            heavy_bear_strategy=None,
            neutral_strategy=neutral_strategy,
            trade_count=trade_count,
            assignment_count=assignment_count,
            put_assignment_count=put_assignment_count,
            total_net_pnl=total_net_pnl,
            total_roi_pct=total_net_pnl * STARTING_EQUITY_PCT_MULTIPLIER,
            average_roi_on_margin_pct=(total_roi_sum / total_roi_count) if total_roi_count else 0.0,
            median_roi_on_margin_pct=_combine_median_value(bull_summary, bear_summary, neutral_summary),
            win_rate_pct=(total_win_count / trade_count * 100.0) if trade_count else 0.0,
            average_win=(total_win_sum / total_win_count) if total_win_count else 0.0,
            average_loss=(total_loss_sum / total_loss_count) if total_loss_count else 0.0,
        )
    ) or {}


def _precompute_trade_maps(
    *,
    strategies: tuple[StrategyConfig, ...],
    bundle,
    trading_fridays: list[date],
    latest_available_date: date,
    curve,
    start_date: date,
    use_cache: bool,
    worker_count: int,
) -> dict[str, dict[date, dict[str, object]]]:
    precomputed: dict[str, dict[date, dict[str, object]]] = {}
    uncached_work: list[tuple[int, StrategyConfig, Path]] = []
    for index, strategy in enumerate(strategies, start=1):
        cache_path = _strategy_trade_cache_path(
            symbol=strategy.symbol,
            start_date=start_date,
            latest_available_date=latest_available_date,
            strategy=strategy,
        )
        if use_cache and cache_path.exists():
            cached_payload = json.loads(cache_path.read_text())
            precomputed[strategy.label] = {
                date.fromisoformat(trade_date): trade_row
                for trade_date, trade_row in cached_payload["trade_map"].items()
            }
            print(f"[precompute {index}/{len(strategies)}] {strategy.label}: {len(precomputed[strategy.label])} tradable Fridays (cache)")
            continue
        uncached_work.append((index, strategy, cache_path))

    bars = sorted(bundle.bars, key=lambda bar: bar.trade_date)
    bar_dates = [bar.trade_date for bar in bars]
    bar_date_to_index = {bar_date: index for index, bar_date in enumerate(bar_dates)}
    earnings_dates = tuple(sorted(bundle.earnings_dates))
    ex_dividend_dates = tuple(sorted(bundle.ex_dividend_dates))
    entry_windows: dict[date, tuple[list[object], set[date], set[date]]] = {}
    for entry_date in trading_fridays:
        start_index = bar_date_to_index[entry_date]
        end_date = min(latest_available_date, entry_date + timedelta(days=35))
        end_index = bisect_right(bar_dates, end_date)
        window_bars = bars[start_index:end_index]
        entry_windows[entry_date] = (
            window_bars,
            {event_date for event_date in earnings_dates if entry_date <= event_date <= end_date},
            {event_date for event_date in ex_dividend_dates if entry_date <= event_date <= end_date},
        )

    grouped_uncached_work: list[list[tuple[int, StrategyConfig, Path]]] = []
    grouped_lookup: dict[tuple[str, str, int], list[tuple[int, StrategyConfig, Path]]] = {}
    for item in uncached_work:
        _, strategy, _ = item
        group_key = (strategy.symbol, strategy.strategy_type.value, strategy.delta_target)
        group = grouped_lookup.get(group_key)
        if group is None:
            group = []
            grouped_lookup[group_key] = group
            grouped_uncached_work.append(group)
        group.append(item)

    def _trade_map_row(trade) -> dict[str, object]:
        roi_on_margin_pct = _trade_roi_on_margin_pct(trade)
        return {
            "entry_date": trade.entry_date.isoformat(),
            "exit_date": trade.exit_date.isoformat(),
            "option_ticker": trade.option_ticker,
            "net_pnl": round(float(trade.net_pnl), 4),
            "roi_on_margin_pct": None if roi_on_margin_pct is None else round(roi_on_margin_pct, 4),
            "exit_reason": trade.exit_reason,
        }

    def _compute_strategy_group(
        group_items: list[tuple[int, StrategyConfig, Path]],
    ) -> list[tuple[int, StrategyConfig, Path, dict[date, dict[str, object]]]]:
        engine = OptionsBacktestEngine()
        ordered_group_items = sorted(group_items, key=lambda item: (item[1].profit_target_pct, item[1].label))
        trade_maps = {strategy.label: {} for _, strategy, _ in ordered_group_items}
        unique_profit_groups: list[tuple[int, list[tuple[int, StrategyConfig, Path]]]] = []
        unique_profit_lookup: dict[int, list[tuple[int, StrategyConfig, Path]]] = {}
        for item in ordered_group_items:
            profit_target_pct = item[1].profit_target_pct
            profit_group = unique_profit_lookup.get(profit_target_pct)
            if profit_group is None:
                profit_group = []
                unique_profit_lookup[profit_target_pct] = profit_group
                unique_profit_groups.append((profit_target_pct, profit_group))
            profit_group.append(item)
        for entry_date in trading_fridays:
            window_bars, window_earnings_dates, window_ex_dividend_dates = entry_windows[entry_date]
            configs = [
                _build_calendar_config(
                    strategy=profit_group[0][1],
                    entry_date=entry_date,
                    latest_available_date=latest_available_date,
                    risk_free_curve=curve,
                )
                for _, profit_group in unique_profit_groups
            ]
            if len(configs) == 1:
                results = [
                    engine.run(
                        config=configs[0],
                        bars=window_bars,
                        earnings_dates=window_earnings_dates,
                        ex_dividend_dates=window_ex_dividend_dates,
                        option_gateway=bundle.option_gateway,
                        shared_entry_rule_cache=None,
                    )
                ]
            else:
                results = engine.run_exit_policy_variants(
                    configs=configs,
                    bars=window_bars,
                    earnings_dates=window_earnings_dates,
                    ex_dividend_dates=window_ex_dividend_dates,
                    option_gateway=bundle.option_gateway,
                    shared_entry_rule_cache=None,
                )
            for (_, profit_group), result in zip(unique_profit_groups, results):
                trade = next((item for item in result.trades if item.entry_date == entry_date), None)
                if trade is None:
                    continue
                trade_row = _trade_map_row(trade)
                for _, strategy, _ in profit_group:
                    trade_maps[strategy.label][entry_date] = trade_row
        return [
            (index, strategy, cache_path, trade_maps[strategy.label])
            for index, strategy, cache_path in ordered_group_items
        ]

    if uncached_work:
        resolved_worker_count = max(1, min(worker_count, len(grouped_uncached_work), os.cpu_count() or worker_count))
        if resolved_worker_count == 1:
            computed_results = [
                result
                for group_items in grouped_uncached_work
                for result in _compute_strategy_group(group_items)
            ]
        else:
            computed_results = []
            with ThreadPoolExecutor(max_workers=resolved_worker_count) as executor:
                futures = {executor.submit(_compute_strategy_group, group_items): group_items for group_items in grouped_uncached_work}
                for future in as_completed(futures):
                    computed_results.extend(future.result())
        for index, strategy, cache_path, trade_map in sorted(computed_results, key=lambda item: item[0]):
            precomputed[strategy.label] = trade_map
            if use_cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    json.dumps(
                        {
                            "symbol": strategy.symbol,
                            "start_date": start_date.isoformat(),
                            "latest_available_date": latest_available_date.isoformat(),
                            "strategy_label": strategy.label,
                            "trade_map": {trade_date.isoformat(): trade_row for trade_date, trade_row in trade_map.items()},
                        },
                        separators=(",", ":"),
                    )
                )
            print(f"[precompute {index}/{len(strategies)}] {strategy.label}: {len(trade_map)} tradable Fridays")
    return precomputed


def _evaluate_stage(
    *,
    stage_name: str,
    objective: str,
    search_config: StageSearchConfig,
    trading_fridays: list[date],
    strategy_series: dict[str, StrategyTradeSeries],
    indicators_by_period: dict[str, dict[date, dict[str, float | None]]],
    regime_mode: str = REGIME_MODE_ALL,
    forced_active_regime: str | None = None,
) -> dict[str, object]:
    metric_ranking_key = _metric_ranking_key
    build_stage_candidate = _build_stage_candidate
    combine_median_value = _combine_median_value
    build_bull_filter_mask = _build_bull_filter_mask
    build_bear_filter_mask = _build_bear_filter_mask
    if regime_mode not in {REGIME_MODE_ALL, REGIME_MODE_BEST_REGIME_ONLY}:
        raise ValueError(f"Unsupported regime mode: {regime_mode}")
    branch_strategy_labels = {
        "bullish": tuple(sorted({triplet[0].label for triplet in search_config.strategy_triplets})),
        "bearish": tuple(sorted({triplet[1].label for triplet in search_config.strategy_triplets})),
        "neutral": tuple(sorted({triplet[2].label for triplet in search_config.strategy_triplets})),
    }
    if search_config.heavy_bull_strategies:
        branch_strategy_labels["heavy_bullish"] = tuple(
            sorted(strategy.label for strategy in search_config.heavy_bull_strategies)
        )
    if search_config.heavy_bear_strategies:
        branch_strategy_labels["heavy_bearish"] = tuple(
            sorted(strategy.label for strategy in search_config.heavy_bear_strategies)
        )
    summary_cache: dict[tuple[str, int], StrategyMaskSummary] = {}
    top_ranked_heap: list[tuple[tuple[float, float, float, float, int], int, StageCandidate]] = []
    best_result: StageCandidate | None = None
    best_result_key: tuple[float, float, float, float, int] | None = None
    best_total_roi_result: StageCandidate | None = None
    best_total_roi_pct: float | None = None
    combo_count = 0
    heavy_bull_combo_count = max(1, len(search_config.heavy_bull_strategies))
    heavy_bear_combo_count = max(1, len(search_config.heavy_bear_strategies))
    total_combos = (
        len(search_config.period_configs)
        * len(search_config.bull_filters)
        * len(search_config.bear_filters)
        * len(search_config.strategy_triplets)
        * heavy_bull_combo_count
        * heavy_bear_combo_count
    )
    row_counter = 0
    all_dates_mask = (1 << len(trading_fridays)) - 1
    zero_summary = _zero_summary()
    allowed_active_regimes = (
        (forced_active_regime,)
        if forced_active_regime is not None
        else _available_regime_labels(
            heavy_bull_enabled=bool(search_config.heavy_bull_strategies),
            heavy_bear_enabled=bool(search_config.heavy_bear_strategies),
        )
    )

    for period_config in search_config.period_configs:
        indicators = indicators_by_period[period_config.label]
        indicator_triplets = _indicator_triplets_for_trading_fridays(
            indicators=indicators,
            trading_fridays=trading_fridays,
        )
        bull_masks = {
            bull_filter: build_bull_filter_mask(
                filter_config=bull_filter,
                indicator_triplets=indicator_triplets,
            )
            for bull_filter in search_config.bull_filters
        }
        bear_masks = {
            bear_filter: build_bear_filter_mask(
                filter_config=bear_filter,
                indicator_triplets=indicator_triplets,
            )
            for bear_filter in search_config.bear_filters
        }
        for bull_filter in search_config.bull_filters:
            bull_mask = bull_masks[bull_filter]
            heavy_bull_mask = 0
            if search_config.heavy_bull_strategies:
                heavy_bull_mask = _build_heavy_bull_filter_mask(
                    filter_config=bull_filter,
                    indicator_triplets=indicator_triplets,
                )
            for bear_filter in search_config.bear_filters:
                bear_mask = bear_masks[bear_filter]
                heavy_bear_mask = 0
                if search_config.heavy_bear_strategies:
                    heavy_bear_mask = _build_heavy_bear_filter_mask(
                        filter_config=bear_filter,
                        indicator_triplets=indicator_triplets,
                    )
                bull_only_mask = bull_mask & ~bear_mask
                bear_only_mask = bear_mask & ~bull_mask
                heavy_bull_only_mask = heavy_bull_mask & bull_only_mask
                heavy_bear_only_mask = heavy_bear_mask & bear_only_mask
                regular_bull_mask = bull_only_mask & ~heavy_bull_only_mask
                regular_bear_mask = bear_only_mask & ~heavy_bear_only_mask
                neutral_mask = all_dates_mask & ~(
                    heavy_bull_only_mask | regular_bull_mask | regular_bear_mask | heavy_bear_only_mask
                )
                overlap_signal_count = (bull_mask & bear_mask).bit_count()
                selection_counts = {
                    "bullish": regular_bull_mask.bit_count(),
                    "bearish": regular_bear_mask.bit_count(),
                    "neutral": neutral_mask.bit_count(),
                }
                if search_config.heavy_bull_strategies:
                    selection_counts["heavy_bullish"] = heavy_bull_only_mask.bit_count()
                if search_config.heavy_bear_strategies:
                    selection_counts["heavy_bearish"] = heavy_bear_only_mask.bit_count()

                branch_masks = {
                    "bullish": regular_bull_mask,
                    "bearish": regular_bear_mask,
                    "neutral": neutral_mask,
                }
                if search_config.heavy_bull_strategies:
                    branch_masks["heavy_bullish"] = heavy_bull_only_mask
                if search_config.heavy_bear_strategies:
                    branch_masks["heavy_bearish"] = heavy_bear_only_mask
                branch_summaries: dict[str, dict[str, StrategyMaskSummary]] = {
                    "bullish": {},
                    "bearish": {},
                    "neutral": {},
                }
                if search_config.heavy_bull_strategies:
                    branch_summaries["heavy_bullish"] = {}
                if search_config.heavy_bear_strategies:
                    branch_summaries["heavy_bearish"] = {}
                for branch_name, branch_mask in branch_masks.items():
                    for strategy_label in branch_strategy_labels[branch_name]:
                        cache_key = (strategy_label, branch_mask)
                        cached_summary = summary_cache.get(cache_key)
                        if cached_summary is None:
                            cached_summary = _summarize_series_for_mask(
                                series=strategy_series[strategy_label],
                                selection_mask=branch_mask,
                            )
                            summary_cache[cache_key] = cached_summary
                        branch_summaries[branch_name][strategy_label] = cached_summary

                heavy_bull_strategies = search_config.heavy_bull_strategies or (None,)
                heavy_bear_strategies = search_config.heavy_bear_strategies or (None,)
                for bull_strategy, bear_strategy, neutral_strategy in search_config.strategy_triplets:
                    bull_summary = branch_summaries["bullish"][bull_strategy.label]
                    bear_summary = branch_summaries["bearish"][bear_strategy.label]
                    neutral_summary = branch_summaries["neutral"][neutral_strategy.label]
                    for heavy_bull_strategy in heavy_bull_strategies:
                        heavy_bull_summary = (
                            branch_summaries["heavy_bullish"][heavy_bull_strategy.label]
                            if heavy_bull_strategy is not None
                            else zero_summary
                        )
                        for heavy_bear_strategy in heavy_bear_strategies:
                            combo_count += 1
                            heavy_bear_summary = (
                                branch_summaries["heavy_bearish"][heavy_bear_strategy.label]
                                if heavy_bear_strategy is not None
                                else zero_summary
                            )
                            summaries_by_regime: dict[str, StrategyMaskSummary] = {
                                "bullish": bull_summary,
                                "bearish": bear_summary,
                                "neutral": neutral_summary,
                            }
                            if heavy_bull_strategy is not None:
                                summaries_by_regime["heavy_bullish"] = heavy_bull_summary
                            if heavy_bear_strategy is not None:
                                summaries_by_regime["heavy_bearish"] = heavy_bear_summary
                            entered_counts = {
                                regime_label: summary.trade_count
                                for regime_label, summary in summaries_by_regime.items()
                            }

                            candidate_specs: list[tuple[str | None, tuple[StrategyMaskSummary, ...]]] = []
                            if regime_mode == REGIME_MODE_ALL:
                                candidate_specs.append(
                                    (
                                        None,
                                        tuple(
                                            summaries_by_regime[regime_label]
                                            for regime_label in _available_regime_labels(
                                                heavy_bull_enabled=heavy_bull_strategy is not None,
                                                heavy_bear_enabled=heavy_bear_strategy is not None,
                                            )
                                            if regime_label in summaries_by_regime
                                        ),
                                    )
                                )
                            else:
                                for active_regime in allowed_active_regimes:
                                    active_summary = summaries_by_regime.get(active_regime)
                                    if active_summary is None or active_summary.trade_count == 0:
                                        continue
                                    candidate_specs.append((active_regime, (active_summary,)))

                            for active_regime, candidate_summaries in candidate_specs:
                                trade_count = sum(summary.trade_count for summary in candidate_summaries)
                                assignment_count = sum(summary.assignment_count for summary in candidate_summaries)
                                put_assignment_count = sum(summary.put_assignment_count for summary in candidate_summaries)
                                total_net_pnl = sum(summary.total_net_pnl for summary in candidate_summaries)
                                total_roi_count = sum(summary.roi_count for summary in candidate_summaries)
                                total_roi_sum = sum(summary.roi_sum for summary in candidate_summaries)
                                total_win_count = sum(summary.win_count for summary in candidate_summaries)
                                total_loss_count = sum(summary.loss_count for summary in candidate_summaries)
                                total_win_sum = sum(summary.win_sum for summary in candidate_summaries)
                                total_loss_sum = sum(summary.loss_sum for summary in candidate_summaries)
                                total_roi_pct = total_net_pnl * STARTING_EQUITY_PCT_MULTIPLIER
                                average_roi_on_margin_pct = (total_roi_sum / total_roi_count) if total_roi_count else 0.0
                                median_roi_on_margin_pct = combine_median_value(*candidate_summaries)
                                win_rate_pct = (total_win_count / trade_count * 100.0) if trade_count else 0.0
                                average_win = (total_win_sum / total_win_count) if total_win_count else 0.0
                                average_loss = (total_loss_sum / total_loss_count) if total_loss_count else 0.0
                                ranking_key = metric_ranking_key(
                                    average_roi_on_margin_pct=average_roi_on_margin_pct,
                                    median_roi_on_margin_pct=median_roi_on_margin_pct,
                                    total_roi_pct=total_roi_pct,
                                    win_rate_pct=win_rate_pct,
                                    trade_count=trade_count,
                                    objective=objective,
                                )
                                row_counter += 1
                                needs_candidate = False
                                if best_result_key is None or ranking_key > best_result_key:
                                    needs_candidate = True
                                if best_total_roi_pct is None or total_roi_pct > best_total_roi_pct:
                                    needs_candidate = True
                                if len(top_ranked_heap) < TOP_RESULT_LIMIT or ranking_key > top_ranked_heap[0][0]:
                                    needs_candidate = True
                                if needs_candidate:
                                    candidate = build_stage_candidate(
                                        regime_mode=regime_mode,
                                        active_regime=active_regime,
                                        selection_counts=selection_counts,
                                        entered_counts=entered_counts,
                                        overlap_signal_count=overlap_signal_count,
                                        indicator_periods=period_config,
                                        bull_filter=bull_filter,
                                        bear_filter=bear_filter,
                                        heavy_bull_strategy=heavy_bull_strategy,
                                        bull_strategy=bull_strategy,
                                        bear_strategy=bear_strategy,
                                        heavy_bear_strategy=heavy_bear_strategy,
                                        neutral_strategy=neutral_strategy,
                                        trade_count=trade_count,
                                        assignment_count=assignment_count,
                                        put_assignment_count=put_assignment_count,
                                        total_net_pnl=total_net_pnl,
                                        total_roi_pct=total_roi_pct,
                                        average_roi_on_margin_pct=average_roi_on_margin_pct,
                                        median_roi_on_margin_pct=median_roi_on_margin_pct,
                                        win_rate_pct=win_rate_pct,
                                        average_win=average_win,
                                        average_loss=average_loss,
                                    )
                                    if best_result_key is None or ranking_key > best_result_key:
                                        best_result = candidate
                                        best_result_key = ranking_key
                                    if best_total_roi_pct is None or total_roi_pct > best_total_roi_pct:
                                        best_total_roi_result = candidate
                                        best_total_roi_pct = total_roi_pct
                                    if len(top_ranked_heap) < TOP_RESULT_LIMIT or ranking_key > top_ranked_heap[0][0]:
                                        _push_top_candidate(
                                            heap=top_ranked_heap,
                                            candidate=candidate,
                                            counter=row_counter,
                                            limit=TOP_RESULT_LIMIT,
                                            objective=objective,
                                        )
                            if combo_count % DEFAULT_PROGRESS_INTERVAL == 0 or combo_count == total_combos:
                                best_metric = 0.0
                                if best_result is not None:
                                    best_metric = (
                                        best_result.median_roi_on_margin_pct
                                        if objective == "median"
                                        else best_result.average_roi_on_margin_pct
                                    )
                                print(
                                    f"[{stage_name} {combo_count}/{total_combos}] "
                                    f"objective={objective} best-so-far={best_metric:.4f}"
                                )

    ranked = [
        _candidate_to_row(item[2])
        for item in sorted(
            top_ranked_heap,
            key=lambda entry: (entry[0], -entry[1]),
            reverse=True,
        )
    ]
    return {
        "stage_name": stage_name,
        "objective": objective,
        "regime_mode": regime_mode,
        "forced_active_regime": forced_active_regime,
        "evaluated_combo_count": combo_count,
        "search_space": {
            "indicator_period_search": _indicator_search_payload(search_config.period_configs),
            "bull_filters": [item.label for item in search_config.bull_filters],
            "bear_filters": [item.label for item in search_config.bear_filters],
            "heavy_bull_strategies": [item.label for item in search_config.heavy_bull_strategies],
            "heavy_bear_strategies": [item.label for item in search_config.heavy_bear_strategies],
            "strategy_triplets": [
                {
                    "bull_strategy": bull_strategy.label,
                    "bear_strategy": bear_strategy.label,
                    "neutral_strategy": neutral_strategy.label,
                }
                for bull_strategy, bear_strategy, neutral_strategy in search_config.strategy_triplets
            ],
        },
        "best_result": _candidate_to_row(best_result),
        "best_result_by_total_roi_pct": _candidate_to_row(best_total_roi_result),
        "top_100_ranked_results": ranked,
        "top_100_ranked_by_average_roi_on_margin_pct": ranked,
    }


def _label_maps(
    *,
    bull_filters: tuple[FilterConfig, ...],
    bear_filters: tuple[NegativeFilterConfig, ...],
    strategies: tuple[StrategyConfig, ...],
) -> tuple[dict[str, FilterConfig], dict[str, NegativeFilterConfig], dict[str, StrategyConfig]]:
    return (
        {item.label: item for item in bull_filters},
        {item.label: item for item in bear_filters},
        {item.label: item for item in strategies},
    )


def _unique_period_seeds(rows: list[dict[str, object]], limit: int) -> tuple[IndicatorPeriodConfig, ...]:
    seen: set[tuple[int, int, int]] = set()
    seeds: list[IndicatorPeriodConfig] = []
    for row in rows:
        key = (int(row["roc_period"]), int(row["adx_period"]), int(row["rsi_period"]))
        if key in seen:
            continue
        seen.add(key)
        seeds.append(IndicatorPeriodConfig(*key))
        if len(seeds) >= limit:
            break
    return tuple(seeds)


def _unique_bull_filters(rows: list[dict[str, object]], lookup: dict[str, FilterConfig], limit: int) -> tuple[FilterConfig, ...]:
    seen: set[str] = set()
    selected: list[FilterConfig] = []
    for row in rows:
        label = str(row["bull_filter"])
        if label in seen:
            continue
        seen.add(label)
        selected.append(lookup[label])
        if len(selected) >= limit:
            break
    return tuple(selected)


def _unique_bear_filters(rows: list[dict[str, object]], lookup: dict[str, NegativeFilterConfig], limit: int) -> tuple[NegativeFilterConfig, ...]:
    seen: set[str] = set()
    selected: list[NegativeFilterConfig] = []
    for row in rows:
        label = str(row["bear_filter"])
        if label in seen:
            continue
        seen.add(label)
        selected.append(lookup[label])
        if len(selected) >= limit:
            break
    return tuple(selected)


def _unique_strategy_triplets(
    rows: list[dict[str, object]],
    strategy_lookup: dict[str, StrategyConfig],
    limit: int,
) -> tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...]:
    seen: set[tuple[tuple[str, int], tuple[str, int], tuple[str, int]]] = set()
    selected: list[tuple[StrategyConfig, StrategyConfig, StrategyConfig]] = []
    for row in rows:
        bull_strategy = strategy_lookup[str(row["bull_strategy"])]
        bear_strategy = strategy_lookup[str(row["bear_strategy"])]
        neutral_strategy = strategy_lookup[str(row["neutral_strategy"])]
        key = (
            (bull_strategy.strategy_type.value, bull_strategy.delta_target),
            (bear_strategy.strategy_type.value, bear_strategy.delta_target),
            (neutral_strategy.strategy_type.value, neutral_strategy.delta_target),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append((bull_strategy, bear_strategy, neutral_strategy))
        if len(selected) >= limit:
            break
    return tuple(selected)


def _strategy_with_delta_and_profit_target(
    strategy: StrategyConfig,
    *,
    delta_target: int | None = None,
    profit_target_pct: int | None = None,
) -> StrategyConfig:
    target_delta = strategy.delta_target if delta_target is None else int(delta_target)
    target_profit_target = strategy.profit_target_pct if profit_target_pct is None else int(profit_target_pct)
    if strategy.delta_target == target_delta and strategy.profit_target_pct == target_profit_target:
        return strategy
    label_prefix, _, _ = _parse_strategy_label(strategy.label)
    return StrategyConfig(
        label=f"{label_prefix}_d{target_delta}_pt{target_profit_target}",
        symbol=strategy.symbol,
        strategy_type=strategy.strategy_type,
        delta_target=target_delta,
        profit_target_pct=target_profit_target,
    )


def _strategy_with_delta_target(strategy: StrategyConfig, delta_target: int) -> StrategyConfig:
    return _strategy_with_delta_and_profit_target(strategy, delta_target=delta_target)


def _strategy_with_profit_target(strategy: StrategyConfig, profit_target_pct: int) -> StrategyConfig:
    return _strategy_with_delta_and_profit_target(strategy, profit_target_pct=profit_target_pct)


def _refine_delta_values(seed: int, step: int) -> tuple[int, ...]:
    values = {seed, 30}
    if step > 0:
        values.update({seed - step, seed + step})
    return tuple(
        value
        for value in sorted(values)
        if 1 <= value <= 99
    )


def _expand_strategy_triplet_delta_neighborhood(
    strategy_triplets: tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...],
    delta_step: int,
) -> tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...]:
    expanded: list[tuple[StrategyConfig, StrategyConfig, StrategyConfig]] = []
    seen: set[tuple[str, str, str]] = set()

    def _append(
        bull_strategy: StrategyConfig,
        bear_strategy: StrategyConfig,
        neutral_strategy: StrategyConfig,
    ) -> None:
        key = (bull_strategy.label, bear_strategy.label, neutral_strategy.label)
        if key in seen:
            return
        seen.add(key)
        expanded.append((bull_strategy, bear_strategy, neutral_strategy))

    for bull_strategy, bear_strategy, neutral_strategy in strategy_triplets:
        _append(bull_strategy, bear_strategy, neutral_strategy)
        for delta_target in _refine_delta_values(bull_strategy.delta_target, delta_step):
            _append(_strategy_with_delta_target(bull_strategy, delta_target), bear_strategy, neutral_strategy)
        for delta_target in _refine_delta_values(bear_strategy.delta_target, delta_step):
            _append(bull_strategy, _strategy_with_delta_target(bear_strategy, delta_target), neutral_strategy)
        for delta_target in _refine_delta_values(neutral_strategy.delta_target, delta_step):
            _append(bull_strategy, bear_strategy, _strategy_with_delta_target(neutral_strategy, delta_target))

    return tuple(expanded)


def _expand_strategy_triplet_profit_target_neighborhood(
    strategy_triplets: tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...],
    profit_target_pcts: tuple[int, ...],
) -> tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...]:
    expanded: list[tuple[StrategyConfig, StrategyConfig, StrategyConfig]] = []
    seen: set[tuple[str, str, str]] = set()

    def _append(
        bull_strategy: StrategyConfig,
        bear_strategy: StrategyConfig,
        neutral_strategy: StrategyConfig,
    ) -> None:
        key = (bull_strategy.label, bear_strategy.label, neutral_strategy.label)
        if key in seen:
            return
        seen.add(key)
        expanded.append((bull_strategy, bear_strategy, neutral_strategy))

    for bull_strategy, bear_strategy, neutral_strategy in strategy_triplets:
        _append(bull_strategy, bear_strategy, neutral_strategy)
        for profit_target_pct in profit_target_pcts:
            _append(_strategy_with_profit_target(bull_strategy, profit_target_pct), bear_strategy, neutral_strategy)
            _append(bull_strategy, _strategy_with_profit_target(bear_strategy, profit_target_pct), neutral_strategy)
            _append(bull_strategy, bear_strategy, _strategy_with_profit_target(neutral_strategy, profit_target_pct))

    return tuple(expanded)


def _unique_strategies_from_triplets(
    strategy_triplets: tuple[tuple[StrategyConfig, StrategyConfig, StrategyConfig], ...],
) -> tuple[StrategyConfig, ...]:
    ordered: list[StrategyConfig] = []
    seen: set[str] = set()
    for bull_strategy, bear_strategy, neutral_strategy in strategy_triplets:
        for strategy in (bull_strategy, bear_strategy, neutral_strategy):
            if strategy.label in seen:
                continue
            seen.add(strategy.label)
            ordered.append(strategy)
    return tuple(ordered)


def _unique_strategy_seeds(
    rows: list[dict[str, object]],
    strategy_lookup: dict[str, StrategyConfig],
    field_name: str,
    limit: int,
) -> tuple[StrategyConfig, ...]:
    seen: set[tuple[str, int]] = set()
    selected: list[StrategyConfig] = []
    for row in rows:
        label = row.get(field_name)
        if not label:
            continue
        strategy = strategy_lookup[str(label)]
        key = (strategy.strategy_type.value, strategy.delta_target)
        if key in seen:
            continue
        seen.add(key)
        selected.append(strategy)
        if len(selected) >= limit:
            break
    return tuple(selected)


def _expand_strategy_neighborhood(
    strategies: tuple[StrategyConfig, ...],
    *,
    delta_step: int,
    profit_target_pcts: tuple[int, ...],
) -> tuple[StrategyConfig, ...]:
    if not strategies:
        return ()

    delta_expanded: list[StrategyConfig] = []
    seen_delta_labels: set[str] = set()
    for strategy in strategies:
        for delta_target in _refine_delta_values(strategy.delta_target, delta_step):
            candidate = _strategy_with_delta_target(strategy, delta_target)
            if candidate.label in seen_delta_labels:
                continue
            seen_delta_labels.add(candidate.label)
            delta_expanded.append(candidate)

    expanded: list[StrategyConfig] = []
    seen_labels: set[str] = set()
    for strategy in delta_expanded:
        for profit_target_pct in profit_target_pcts:
            candidate = _strategy_with_profit_target(strategy, profit_target_pct)
            if candidate.label in seen_labels:
                continue
            seen_labels.add(candidate.label)
            expanded.append(candidate)
    return tuple(expanded)


def _roc_refine_values(seed: int) -> list[int]:
    step = 21 if seed >= 84 else 7
    values = list(range(max(step, seed - step * 3), seed + step * 3 + 1, step))
    return sorted(set(value for value in values if value > 0))


def _contiguous_period_values(seed: int) -> list[int]:
    return list(range(max(5, seed - 4), seed + 4 + 1))


def _build_refine_period_configs(seeds: tuple[IndicatorPeriodConfig, ...]) -> tuple[IndicatorPeriodConfig, ...]:
    roc_values: set[int] = set()
    adx_values: set[int] = set()
    rsi_values: set[int] = set()
    for seed in seeds:
        roc_values.update(_roc_refine_values(seed.roc_period))
        adx_values.update(_contiguous_period_values(seed.adx_period))
        rsi_values.update(_contiguous_period_values(seed.rsi_period))
    return tuple(
        IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in sorted(roc_values)
        for adx_period in sorted(adx_values)
        for rsi_period in sorted(rsi_values)
    )


def _load_adjusted_indicator_source(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    period_configs: tuple[IndicatorPeriodConfig, ...],
) -> tuple[list[date], list[float], list[float], list[float]]:
    if not period_configs:
        return [], [], [], []
    warmup_days = max(450, max(period.roc_period for period in period_configs) * 4)
    warmup_start = start_date - timedelta(days=warmup_days)
    with create_readonly_session() as session:
        rows = session.query(
            HistoricalUnderlyingDayBar.trade_date,
            HistoricalUnderlyingDayBar.high_price,
            HistoricalUnderlyingDayBar.low_price,
            HistoricalUnderlyingDayBar.close_price,
        ).filter(
            HistoricalUnderlyingDayBar.symbol == symbol,
            HistoricalUnderlyingDayBar.trade_date >= warmup_start,
            HistoricalUnderlyingDayBar.trade_date <= end_date,
        ).order_by(HistoricalUnderlyingDayBar.trade_date).all()
    if not rows:
        raise SystemExit(f"Missing adjusted bars for {symbol}.")
    return (
        [row.trade_date for row in rows],
        [float(row.high_price) for row in rows],
        [float(row.low_price) for row in rows],
        [float(row.close_price) for row in rows],
    )


def _load_adjusted_indicator_batch(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    period_configs: tuple[IndicatorPeriodConfig, ...],
) -> dict[str, dict[date, dict[str, float | None]]]:
    dates, highs, lows, closes = _load_adjusted_indicator_source(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        period_configs=period_configs,
    )
    roc_by_period = {period: roc(closes, period) for period in sorted({item.roc_period for item in period_configs})}
    adx_by_period = {period: adx(highs, lows, closes, period) for period in sorted({item.adx_period for item in period_configs})}
    rsi_by_period = {period: rsi(closes, period) for period in sorted({item.rsi_period for item in period_configs})}

    loaded_periods: dict[str, dict[date, dict[str, float | None]]] = {}
    for period_config in period_configs:
        roc_values = roc_by_period[period_config.roc_period]
        adx_values = adx_by_period[period_config.adx_period]
        rsi_values = rsi_by_period[period_config.rsi_period]
        loaded_periods[period_config.label] = {
            trade_date: {
                "roc63": roc_values[index],
                "adx14": adx_values[index],
                "rsi14": rsi_values[index],
            }
            for index, trade_date in enumerate(dates)
        }
    return loaded_periods


def _build_period_cache(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    period_configs: tuple[IndicatorPeriodConfig, ...],
    cache: dict[str, dict[date, dict[str, float | None]]] | None = None,
    use_cache: bool = True,
    worker_count: int = DEFAULT_INDICATOR_WORKERS,
) -> dict[str, dict[date, dict[str, float | None]]]:
    indicator_cache = {} if cache is None else dict(cache)
    uncached_periods: list[tuple[IndicatorPeriodConfig, Path]] = []
    cached_count = 0
    for period_config in period_configs:
        if period_config.label in indicator_cache:
            continue
        cache_path = _indicator_cache_path(
            symbol=symbol,
            start_date=start_date,
            latest_available_date=end_date,
            period_config=period_config,
        )
        if use_cache and cache_path.exists():
            cached_payload = json.loads(cache_path.read_text())
            indicator_cache[period_config.label] = {
                date.fromisoformat(trade_date): indicator_row
                for trade_date, indicator_row in cached_payload["indicators"].items()
            }
            cached_count += 1
            continue
        uncached_periods.append((period_config, cache_path))

    if cached_count:
        print(f"[indicators] loaded {cached_count} config(s) from cache")

    if uncached_periods:
        uncached_configs = tuple(item[0] for item in uncached_periods)
        unique_roc_count = len({item.roc_period for item in uncached_configs})
        unique_adx_count = len({item.adx_period for item in uncached_configs})
        unique_rsi_count = len({item.rsi_period for item in uncached_configs})
        print(
            f"[indicators] computing {len(uncached_configs)} config(s) "
            f"from {unique_roc_count} roc x {unique_adx_count} adx x {unique_rsi_count} rsi periods"
        )
        loaded_periods = _load_adjusted_indicator_batch(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period_configs=uncached_configs,
        )
        for period_config, cache_path in sorted(uncached_periods, key=lambda item: item[0].label):
            loaded = loaded_periods[period_config.label]
            indicator_cache[period_config.label] = loaded
            if use_cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    json.dumps(
                        {
                            "symbol": symbol,
                            "start_date": start_date.isoformat(),
                            "latest_available_date": end_date.isoformat(),
                            "period_config": period_config.label,
                            "indicators": {trade_date.isoformat(): indicator_row for trade_date, indicator_row in loaded.items()},
                        },
                        separators=(",", ":"),
                    )
                )
        print(f"[indicators] wrote {len(uncached_periods)} config(s)")
    return indicator_cache


def _with_stage(row: dict[str, object] | None, stage_name: str) -> dict[str, object] | None:
    if row is None:
        return None
    return {"stage": stage_name, **row}


def main() -> int:
    args = _parse_args()
    if args.refine_delta_step < 0:
        raise SystemExit("--refine-delta-step must be >= 0.")
    symbol = args.symbol.upper()
    output_json = args.output or ROOT / "logs" / f"{symbol.lower()}_weekly_calendar_policy_two_stage_{args.start_date.isoformat()}_{args.requested_end_date.isoformat()}.json"
    use_cache = not args.disable_cache

    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    bullish_strategies, bearish_strategies, neutral_strategies = _build_strategy_sets(symbol)
    heavy_bullish_strategies, heavy_bearish_strategies = _build_heavy_strategy_sets(symbol)
    all_strategies = (
        bullish_strategies
        + bearish_strategies
        + neutral_strategies
        + heavy_bullish_strategies
        + heavy_bearish_strategies
    )
    bull_filters = _build_default_bull_filters()
    bear_filters = _build_default_bear_filters()
    bull_filter_lookup, bear_filter_lookup, strategy_lookup = _label_maps(
        bull_filters=bull_filters,
        bear_filters=bear_filters,
        strategies=all_strategies,
    )

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = _build_bundle(store, symbol=symbol, start_date=args.start_date, end_date=args.requested_end_date)
    latest_available_date = _resolve_latest_available_date_from_bundle(bundle, args.requested_end_date)
    curve = _load_risk_free_curve(store, start_date=args.start_date, end_date=latest_available_date)
    trading_fridays = [
        bar.trade_date
        for bar in bundle.bars
        if args.start_date <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]

    precomputed = _precompute_trade_maps(
        strategies=all_strategies,
        bundle=bundle,
        trading_fridays=trading_fridays,
        latest_available_date=latest_available_date,
        curve=curve,
        start_date=args.start_date,
        use_cache=use_cache,
        worker_count=args.precompute_workers,
    )
    strategy_series = _build_strategy_trade_series(
        strategies=all_strategies,
        precomputed=precomputed,
        trading_fridays=trading_fridays,
    )

    broad_period_configs = tuple(
        IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in BROAD_ROC_PERIODS
        for adx_period in BROAD_ADX_PERIODS
        for rsi_period in BROAD_RSI_PERIODS
    )
    indicator_cache = _build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=broad_period_configs,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
    )

    broad_strategy_triplets = tuple(
        (bull_strategy, bear_strategy, neutral_strategy)
        for bull_strategy in bullish_strategies
        for bear_strategy in bearish_strategies
        for neutral_strategy in neutral_strategies
    )
    broad_stage = _evaluate_stage(
        stage_name="broad",
        objective=args.objective,
        search_config=StageSearchConfig(
            period_configs=broad_period_configs,
            bull_filters=bull_filters,
            bear_filters=bear_filters,
            strategy_triplets=broad_strategy_triplets,
            heavy_bull_strategies=heavy_bullish_strategies,
            heavy_bear_strategies=heavy_bearish_strategies,
        ),
        trading_fridays=trading_fridays,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
        regime_mode=args.regime_mode,
    )

    broad_ranked = list(broad_stage["top_100_ranked_results"])
    refine_active_regime: str | None = None
    if args.regime_mode == REGIME_MODE_BEST_REGIME_ONLY and broad_stage["best_result"] is not None:
        active_regime = broad_stage["best_result"].get("active_regime")
        if active_regime:
            refine_active_regime = str(active_regime)
            broad_ranked = [
                row
                for row in broad_ranked
                if str(row.get("active_regime") or "") == refine_active_regime
            ]
    broad_seeds_source = broad_ranked[: args.refine_top_rows]
    if broad_stage["best_result_by_total_roi_pct"] is not None and (
        args.regime_mode != REGIME_MODE_BEST_REGIME_ONLY
        or str(broad_stage["best_result_by_total_roi_pct"].get("active_regime") or "") == refine_active_regime
    ):
        broad_seeds_source.append(broad_stage["best_result_by_total_roi_pct"])

    refine_period_seeds = _unique_period_seeds(broad_seeds_source, args.refine_top_period_seeds)
    refine_period_configs = _build_refine_period_configs(refine_period_seeds)
    indicator_cache = _build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=refine_period_configs,
        cache=indicator_cache,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
    )

    refine_bull_filters = _unique_bull_filters(broad_seeds_source, bull_filter_lookup, args.refine_top_bull_filters)
    refine_bear_filters = _unique_bear_filters(broad_seeds_source, bear_filter_lookup, args.refine_top_bear_filters)
    refine_strategy_triplet_seeds = _unique_strategy_triplets(
        broad_seeds_source,
        strategy_lookup,
        args.refine_top_strategy_triplets,
    )
    refine_delta_expanded_triplets = _expand_strategy_triplet_delta_neighborhood(
        refine_strategy_triplet_seeds,
        args.refine_delta_step,
    )
    refine_strategy_triplets = _expand_strategy_triplet_profit_target_neighborhood(
        refine_delta_expanded_triplets,
        args.refine_profit_target_pcts,
    )
    refine_strategies = _unique_strategies_from_triplets(refine_strategy_triplets)
    refine_heavy_bull_seeds = _unique_strategy_seeds(
        broad_seeds_source,
        strategy_lookup,
        "heavy_bull_strategy",
        args.refine_top_strategy_triplets,
    )
    refine_heavy_bear_seeds = _unique_strategy_seeds(
        broad_seeds_source,
        strategy_lookup,
        "heavy_bear_strategy",
        args.refine_top_strategy_triplets,
    )
    refine_heavy_bull_strategies = _expand_strategy_neighborhood(
        refine_heavy_bull_seeds,
        delta_step=args.refine_delta_step,
        profit_target_pcts=args.refine_profit_target_pcts,
    )
    refine_heavy_bear_strategies = _expand_strategy_neighborhood(
        refine_heavy_bear_seeds,
        delta_step=args.refine_delta_step,
        profit_target_pcts=args.refine_profit_target_pcts,
    )
    missing_refine_strategies = tuple(
        strategy
        for strategy in refine_strategies + refine_heavy_bull_strategies + refine_heavy_bear_strategies
        if strategy.label not in strategy_series
    )
    if missing_refine_strategies:
        extra_precomputed = _precompute_trade_maps(
            strategies=missing_refine_strategies,
            bundle=bundle,
            trading_fridays=trading_fridays,
            latest_available_date=latest_available_date,
            curve=curve,
            start_date=args.start_date,
            use_cache=use_cache,
            worker_count=args.precompute_workers,
        )
        strategy_series.update(
            _build_strategy_trade_series(
                strategies=missing_refine_strategies,
                precomputed=extra_precomputed,
                trading_fridays=trading_fridays,
            )
        )
    refine_stage = _evaluate_stage(
        stage_name="refine",
        objective=args.objective,
        search_config=StageSearchConfig(
            period_configs=refine_period_configs,
            bull_filters=refine_bull_filters,
            bear_filters=refine_bear_filters,
            strategy_triplets=refine_strategy_triplets,
            heavy_bull_strategies=refine_heavy_bull_strategies or heavy_bullish_strategies,
            heavy_bear_strategies=refine_heavy_bear_strategies or heavy_bearish_strategies,
        ),
        trading_fridays=trading_fridays,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
        regime_mode=args.regime_mode,
        forced_active_regime=refine_active_regime,
    )

    best_primary = refine_stage["best_result"] or broad_stage["best_result"]
    best_primary_stage = "refine" if refine_stage["best_result"] is not None else "broad"

    best_total = refine_stage["best_result_by_total_roi_pct"] or broad_stage["best_result_by_total_roi_pct"]
    best_total_stage = "refine" if refine_stage["best_result_by_total_roi_pct"] is not None else "broad"

    payload = {
        "symbol": symbol,
        "selection_objective": args.objective,
        "selection_regime_mode": args.regime_mode,
        "period": {
            "start": args.start_date.isoformat(),
            "requested_end": args.requested_end_date.isoformat(),
            "latest_available_date": latest_available_date.isoformat(),
        },
        "starting_equity": STARTING_EQUITY,
        "cache_enabled": use_cache,
        "cache_root": str(_symbol_cache_dir(symbol=symbol, start_date=args.start_date, latest_available_date=latest_available_date)),
        "stage_1_broad": broad_stage,
        "stage_2_refine": {
            **refine_stage,
            "seeded_from_top_rows": args.refine_top_rows,
            "seeded_period_configs": [item.label for item in refine_period_seeds],
            "seeded_strategy_triplets": [
                {
                    "bull_strategy": bull_strategy.label,
                    "bear_strategy": bear_strategy.label,
                    "neutral_strategy": neutral_strategy.label,
                }
                for bull_strategy, bear_strategy, neutral_strategy in refine_strategy_triplet_seeds
            ],
            "refine_delta_step": args.refine_delta_step,
            "refine_profit_target_pcts": list(args.refine_profit_target_pcts),
            "forced_active_regime": refine_active_regime,
            "delta_expanded_strategy_triplets": [
                {
                    "bull_strategy": bull_strategy.label,
                    "bear_strategy": bear_strategy.label,
                    "neutral_strategy": neutral_strategy.label,
                }
                for bull_strategy, bear_strategy, neutral_strategy in refine_delta_expanded_triplets
            ],
            "expanded_strategy_triplets": [
                {
                    "bull_strategy": bull_strategy.label,
                    "bear_strategy": bear_strategy.label,
                    "neutral_strategy": neutral_strategy.label,
                }
                for bull_strategy, bear_strategy, neutral_strategy in refine_strategy_triplets
            ],
            "expanded_heavy_bull_strategies": [strategy.label for strategy in refine_heavy_bull_strategies],
            "expanded_heavy_bear_strategies": [strategy.label for strategy in refine_heavy_bear_strategies],
        },
        "combined_best_result": _with_stage(best_primary, best_primary_stage),
        "combined_best_result_by_total_roi_pct": _with_stage(best_total, best_total_stage),
    }
    output_json.write_text(json.dumps(payload, indent=2))
    print(
        json.dumps(
            {
                "combined_best_result": payload["combined_best_result"],
                "combined_best_result_by_total_roi_pct": payload["combined_best_result_by_total_roi_pct"],
                "output": str(output_json),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
