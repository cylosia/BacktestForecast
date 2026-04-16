from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_right
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.calendar_adjustments import (  # noqa: E402
    CalendarAdjustmentPolicy,
    default_calendar_adjustment_policies,
    run_adjusted_calendar_backtest,
)
from backtestforecast.backtests.types import BacktestConfig  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
    StrategyType,
)
from grid_search_fas_faz_weekly_calendar_policy import (  # noqa: E402
    STARTING_EQUITY,
    _build_bundle,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
)
from grid_search_weekly_calendar_policy_two_stage import (  # noqa: E402
    _build_period_cache,
    _resolve_latest_available_date_from_bundle,
)
from portfolio_weighting import _weighted_median  # noqa: E402
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)
import run_weekly_calendar_policy_walk_forward as walk_forward  # noqa: E402


DEFAULT_SELECTION_CSV = (
    ROOT
    / "logs"
    / "weekly_calendar_policy_walk_forward_combined683_top22_train2y_20251231_q1_2026_20260413_selection.csv"
)
DEFAULT_ENTRY_START_DATE = date(2026, 1, 1)
DEFAULT_ENTRY_END_DATE = date(2026, 3, 31)
DEFAULT_SHORT_DTES = (5, 7, 9)
DEFAULT_FAR_DTES = (12, 14, 21)
DEFAULT_MAX_HOLDING_DAYS = (5, 7, 10)
DEFAULT_DTE_TOLERANCE_DAYS = 3
DEFAULT_MAX_WORKERS = 2
DEFAULT_WEIGHT_SOURCE = "selection"
DEFAULT_ADJUSTMENT_POLICY = ""
DEFAULT_ENTRY_CADENCE = "weekly"
DEFAULT_PROFIT_TARGETS = ""


@dataclass(frozen=True, slots=True)
class StructuralVariant:
    short_dte: int
    far_leg_target_dte: int
    max_holding_days: int
    profit_target_pct: int | None = None

    @property
    def label(self) -> str:
        label = f"sdte{self.short_dte}_fdte{self.far_leg_target_dte}_hold{self.max_holding_days}"
        if self.profit_target_pct is not None:
            label = f"{label}_pt{self.profit_target_pct}"
        return label


def _parse_int_grid(raw_value: str) -> tuple[int, ...]:
    values: list[int] = []
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(int(item))
    if not values:
        raise ValueError("Grid argument must contain at least one integer value.")
    return tuple(values)


def _parse_optional_int_grid(raw_value: str) -> tuple[int, ...]:
    value = raw_value.strip()
    if not value:
        return ()
    return _parse_int_grid(value)


def _default_output_prefix() -> Path:
    return ROOT / "logs" / "weekly_calendar_policy_structural_grid"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze the selected weekly calendar regime logic from a walk-forward selection.csv "
            "and sweep only structural calendar parameters over a requested replay window."
        )
    )
    parser.add_argument("--selection-csv", type=Path, default=DEFAULT_SELECTION_CSV)
    parser.add_argument("--entry-start-date", type=date.fromisoformat, default=DEFAULT_ENTRY_START_DATE)
    parser.add_argument("--entry-end-date", type=date.fromisoformat, default=DEFAULT_ENTRY_END_DATE)
    parser.add_argument(
        "--replay-data-end",
        type=date.fromisoformat,
        help="Optional data cutoff used to allow exits after the entry window. Defaults to entry_end_date + 14 days.",
    )
    parser.add_argument("--short-dtes", default=",".join(str(item) for item in DEFAULT_SHORT_DTES))
    parser.add_argument("--far-dtes", default=",".join(str(item) for item in DEFAULT_FAR_DTES))
    parser.add_argument("--max-holding-days-grid", default=",".join(str(item) for item in DEFAULT_MAX_HOLDING_DAYS))
    parser.add_argument(
        "--profit-targets",
        default=DEFAULT_PROFIT_TARGETS,
        help=(
            "Optional comma-separated profit target percentages to override the selected strategy targets. "
            "Examples: 10,20,30. Leave empty to keep each selected strategy's saved take-profit."
        ),
    )
    parser.add_argument("--dte-tolerance-days", type=int, default=DEFAULT_DTE_TOLERANCE_DAYS)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument(
        "--entry-cadence",
        choices=("weekly", "daily"),
        default=DEFAULT_ENTRY_CADENCE,
        help="Entry cadence used to sample bars inside the replay window. Defaults to weekly (Fridays).",
    )
    parser.add_argument(
        "--adjustment-policy",
        default=DEFAULT_ADJUSTMENT_POLICY,
        help=(
            "Optional calendar adjustment policy name. "
            "Examples: roll_same_strike_once, recenter_short_once. "
            "Leave empty to use the baseline close-at-short-expiration behavior."
        ),
    )
    parser.add_argument(
        "--weight-source",
        choices=("selection", "equal"),
        default=DEFAULT_WEIGHT_SOURCE,
        help="Use saved weights from selection.csv or equal-weight the fixed symbol basket.",
    )
    parser.add_argument("--top-k", type=int, help="Optional limit on how many rows to read from selection.csv.")
    parser.add_argument(
        "--output-prefix",
        type=Path,
        help="Optional output prefix. Defaults to logs/weekly_calendar_policy_structural_grid.",
    )
    return parser.parse_args()


def _resolve_adjustment_policy(raw_value: str | None) -> CalendarAdjustmentPolicy | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    available = {policy.name: policy for policy in default_calendar_adjustment_policies()}
    try:
        return available[value]
    except KeyError as exc:
        raise SystemExit(
            f"Unknown adjustment policy: {value}. Available values: {', '.join(sorted(available))}"
        ) from exc


def _variant_run_label(variant: StructuralVariant, adjustment_policy: CalendarAdjustmentPolicy | None) -> str:
    if adjustment_policy is None:
        return variant.label
    return f"{variant.label}_{adjustment_policy.name}"


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_structural_variants(
    *,
    short_dtes: tuple[int, ...],
    far_dtes: tuple[int, ...],
    max_holding_days_grid: tuple[int, ...],
    profit_target_pcts: tuple[int, ...] = (),
) -> list[StructuralVariant]:
    profit_target_grid: tuple[int | None, ...]
    if profit_target_pcts:
        profit_target_grid = tuple(profit_target_pcts)
    else:
        profit_target_grid = (None,)
    variants = [
        StructuralVariant(
            short_dte=short_dte,
            far_leg_target_dte=far_dte,
            max_holding_days=max_holding_days,
            profit_target_pct=profit_target_pct,
        )
        for short_dte in short_dtes
        for far_dte in far_dtes
        for max_holding_days in max_holding_days_grid
        for profit_target_pct in profit_target_grid
        if far_dte > short_dte
    ]
    if not variants:
        raise SystemExit("No valid structural variants. far_dte must be greater than short_dte.")
    return variants


def _load_selected_candidates(
    *,
    selection_csv: Path,
    top_k: int | None,
    weight_source: str,
) -> list[dict[str, object]]:
    rows = list(csv.DictReader(selection_csv.open(newline="", encoding="utf-8")))
    rows.sort(key=lambda row: int(row.get("rank") or 0))
    if top_k is not None:
        rows = rows[:top_k]
    if not rows:
        raise SystemExit(f"No rows found in selection CSV: {selection_csv}")

    use_selection_weights = (
        weight_source == "selection"
        and all((_safe_float(row.get("weight_pct")) or 0.0) > 0.0 for row in rows)
    )
    symbol_count = len(rows)
    equal_weight_pct = 100.0 / symbol_count
    candidates: list[dict[str, object]] = []
    for row in rows:
        output_path = Path(str(row["output_path"]))
        if not output_path.is_absolute():
            output_path = ROOT / output_path
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if use_selection_weights:
            weight_pct = _safe_float(row.get("weight_pct")) or 0.0
            position_multiplier = _safe_float(row.get("position_multiplier"))
            if position_multiplier is None or position_multiplier <= 0.0:
                position_multiplier = weight_pct / 100.0 * symbol_count
        else:
            weight_pct = equal_weight_pct
            position_multiplier = 1.0
        candidates.append(
            {
                "rank": int(row.get("rank") or len(candidates) + 1),
                "symbol": row["symbol"],
                "output_path": output_path,
                "payload": payload,
                "best": dict(payload["combined_best_result"]),
                "selection_row": row,
                "weight_pct": weight_pct,
                "position_multiplier": position_multiplier,
            }
        )
    return candidates


def _build_structural_calendar_config(
    *,
    strategy,
    entry_date: date,
    latest_available_date: date,
    risk_free_curve,
    variant: StructuralVariant,
    dte_tolerance_days: int,
) -> BacktestConfig:
    if strategy.strategy_type == StrategyType.CALENDAR_SPREAD:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=variant.far_leg_target_dte,
            short_call_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    else:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=variant.far_leg_target_dte,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    return BacktestConfig(
        symbol=strategy.symbol,
        strategy_type=strategy.strategy_type.value,
        start_date=entry_date,
        end_date=min(latest_available_date, entry_date + timedelta(days=35)),
        target_dte=variant.short_dte,
        dte_tolerance_days=dte_tolerance_days,
        max_holding_days=variant.max_holding_days,
        account_size=Decimal(str(STARTING_EQUITY)),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        risk_free_rate=risk_free_curve.default_rate,
        risk_free_rate_curve=risk_free_curve,
        dividend_yield=0.0,
        slippage_pct=0.0,
        strategy_overrides=overrides,
        profit_target_pct=float(
            variant.profit_target_pct if variant.profit_target_pct is not None else strategy.profit_target_pct
        ),
        stop_loss_pct=None,
    )


def _display_strategy_label(strategy_label: str, profit_target_pct: int | None) -> str:
    if profit_target_pct is None:
        return strategy_label
    head, separator, suffix = strategy_label.rpartition("_pt")
    if separator and suffix.isdigit():
        return f"{head}{separator}{profit_target_pct}"
    return f"{strategy_label}_pt{profit_target_pct}"


def _compute_entry_windows(*, bundle, entry_dates: list[date], latest_available_date: date) -> dict[date, tuple[list[object], set[date], set[date]]]:
    bars = sorted(bundle.bars, key=lambda bar: bar.trade_date)
    bar_dates = [bar.trade_date for bar in bars]
    bar_date_to_index = {bar_date: index for index, bar_date in enumerate(bar_dates)}
    earnings_dates = tuple(sorted(bundle.earnings_dates))
    ex_dividend_dates = tuple(sorted(bundle.ex_dividend_dates))
    entry_windows: dict[date, tuple[list[object], set[date], set[date]]] = {}
    for entry_date in entry_dates:
        start_index = bar_date_to_index[entry_date]
        end_date = min(latest_available_date, entry_date + timedelta(days=35))
        end_index = bisect_right(bar_dates, end_date)
        window_bars = bars[start_index:end_index]
        entry_windows[entry_date] = (
            window_bars,
            {event_date for event_date in earnings_dates if entry_date <= event_date <= end_date},
            {event_date for event_date in ex_dividend_dates if entry_date <= event_date <= end_date},
        )
    return entry_windows


def _select_entry_dates(
    *,
    bars: list[object],
    entry_start_date: date,
    entry_end_date: date,
    entry_cadence: str,
) -> list[date]:
    if entry_cadence == "daily":
        return [
            bar.trade_date
            for bar in bars
            if entry_start_date <= bar.trade_date <= entry_end_date
        ]
    return [
        bar.trade_date
        for bar in bars
        if entry_start_date <= bar.trade_date <= entry_end_date and bar.trade_date.weekday() == 4
    ]


def _trade_to_ledger_row(
    *,
    variant: StructuralVariant,
    candidate: dict[str, object],
    trade,
    regime: str,
    strategy_label: str,
    adjustment_policy: CalendarAdjustmentPolicy | None,
) -> dict[str, object]:
    quantity = _safe_float(getattr(trade, "quantity", 1.0)) or 1.0
    detail_json = getattr(trade, "detail_json", {}) or {}
    variant_label = _variant_run_label(variant, adjustment_policy)
    entry_debit = _safe_float(detail_json.get("entry_package_market_value"))
    campaign_max_capital = _safe_float(detail_json.get("campaign_max_capital_at_risk"))
    if campaign_max_capital is not None and campaign_max_capital > 0:
        total_capital_required = campaign_max_capital
    else:
        capital_required = _safe_float(detail_json.get("capital_required_per_unit"))
        total_capital_required = None if capital_required is None else capital_required * quantity
    net_pnl = float(trade.net_pnl)
    roi_capital = _trade_roi_on_margin_pct(trade)
    roi_debit = None
    if entry_debit is not None and entry_debit > 0:
        roi_debit = net_pnl / entry_debit * 100.0
    position_multiplier = float(candidate["position_multiplier"])
    weight_pct = float(candidate["weight_pct"])
    adjustment_events = detail_json.get("campaign_adjustment_events") or []
    campaign_roll_count = int(_safe_float(detail_json.get("campaign_roll_count")) or 0)
    return {
        "variant_label": variant_label,
        "short_dte": variant.short_dte,
        "far_leg_target_dte": variant.far_leg_target_dte,
        "max_holding_days": variant.max_holding_days,
        "profit_target_pct": variant.profit_target_pct if variant.profit_target_pct is not None else "",
        "adjustment_policy": adjustment_policy.name if adjustment_policy is not None else "",
        "rank": int(candidate["rank"]),
        "symbol": str(candidate["symbol"]),
        "regime": regime,
        "strategy": _display_strategy_label(strategy_label, variant.profit_target_pct),
        "option_ticker": getattr(trade, "option_ticker", ""),
        "entry_date": trade.entry_date.isoformat(),
        "exit_date": trade.exit_date.isoformat(),
        "quantity": round(quantity, 4),
        "entry_debit": None if entry_debit is None else round(entry_debit, 4),
        "capital_required": None if total_capital_required is None else round(total_capital_required, 4),
        "net_pnl": round(net_pnl, 4),
        "roi_on_debit_pct": None if roi_debit is None else round(roi_debit, 4),
        "roi_on_capital_required_pct": None if roi_capital is None else round(roi_capital, 4),
        "exit_reason": getattr(trade, "exit_reason", ""),
        "adjustment_event_count": len(adjustment_events) if isinstance(adjustment_events, list) else 0,
        "campaign_roll_count": campaign_roll_count,
        "entry_underlying_close": round(float(getattr(trade, "entry_underlying_close", 0.0)), 4),
        "exit_underlying_close": round(float(getattr(trade, "exit_underlying_close", 0.0)), 4),
        "weight_pct": round(weight_pct, 4),
        "position_multiplier": round(position_multiplier, 6),
        "weighted_entry_debit": None if entry_debit is None else round(entry_debit * position_multiplier, 4),
        "weighted_capital_required": None if total_capital_required is None else round(total_capital_required * position_multiplier, 4),
        "weighted_net_pnl": round(net_pnl * position_multiplier, 4),
        "training_median_roi_on_margin_pct": round(float(candidate["best"]["median_roi_on_margin_pct"]), 4),
        "training_trade_count": int(candidate["best"]["trade_count"]),
    }


def _evaluate_symbol_variants(
    *,
    candidate: dict[str, object],
    variants: list[StructuralVariant],
    entry_start_date: date,
    entry_end_date: date,
    replay_data_end: date,
    dte_tolerance_days: int,
    entry_cadence: str,
    adjustment_policy: CalendarAdjustmentPolicy | None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    components = walk_forward._resolve_candidate_components(candidate)
    best = dict(components["best"])
    symbol = str(components["symbol"])
    train_start_date = components["train_start_date"]
    period_config = components["period_config"]
    bull_filter = components["bull_filter"]
    bear_filter = components["bear_filter"]
    bull_strategy = components["bull_strategy"]
    bear_strategy = components["bear_strategy"]
    neutral_strategy = components["neutral_strategy"]

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bundle = _build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=replay_data_end)
    latest_available_date = _resolve_latest_available_date_from_bundle(bundle, replay_data_end)
    curve = _load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
    indicator_cache = _build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = indicator_cache[period_config.label]
    entry_dates = _select_entry_dates(
        bars=bundle.bars,
        entry_start_date=entry_start_date,
        entry_end_date=entry_end_date,
        entry_cadence=entry_cadence,
    )
    entry_windows = _compute_entry_windows(
        bundle=bundle,
        entry_dates=entry_dates,
        latest_available_date=latest_available_date,
    )

    engine = OptionsBacktestEngine()
    base_strategies = (bull_strategy, bear_strategy, neutral_strategy)
    symbol_rows: list[dict[str, object]] = []
    ledger_rows: list[dict[str, object]] = []

    for variant in variants:
        variant_label = _variant_run_label(variant, adjustment_policy)
        trade_maps: dict[str, dict[date, object]] = {}
        for strategy in base_strategies:
            per_date: dict[date, object] = {}
            for entry_date in entry_dates:
                window_bars, window_earnings_dates, window_ex_dividend_dates = entry_windows[entry_date]
                config = _build_structural_calendar_config(
                    strategy=strategy,
                    entry_date=entry_date,
                    latest_available_date=latest_available_date,
                    risk_free_curve=curve,
                    variant=variant,
                    dte_tolerance_days=dte_tolerance_days,
                )
                if adjustment_policy is None:
                    result = engine.run(
                        config=config,
                        bars=window_bars,
                        earnings_dates=window_earnings_dates,
                        ex_dividend_dates=window_ex_dividend_dates,
                        option_gateway=bundle.option_gateway,
                        shared_entry_rule_cache=None,
                    )
                else:
                    result = run_adjusted_calendar_backtest(
                        config=config,
                        bars=window_bars,
                        earnings_dates=window_earnings_dates,
                        ex_dividend_dates=window_ex_dividend_dates,
                        option_gateway=bundle.option_gateway,
                        policy=adjustment_policy,
                        shared_entry_rule_cache=None,
                        force_single_contract=True,
                    )
                trade = next((item for item in result.trades if item.entry_date == entry_date), None)
                if trade is not None:
                    per_date[entry_date] = trade
            trade_maps[strategy.label] = per_date

        variant_ledger: list[dict[str, object]] = []
        for entry_date in entry_dates:
            indicator_row = indicators.get(entry_date)
            bull = bull_filter.matches(indicator_row)
            bear = bear_filter.matches(indicator_row)
            if bull and not bear:
                regime = "bullish"
                selected_strategy = bull_strategy
            elif bear and not bull:
                regime = "bearish"
                selected_strategy = bear_strategy
            else:
                regime = "neutral"
                selected_strategy = neutral_strategy
            trade = trade_maps[selected_strategy.label].get(entry_date)
            if trade is None:
                continue
            variant_ledger.append(
                _trade_to_ledger_row(
                    variant=variant,
                    candidate=candidate,
                    trade=trade,
                    regime=regime,
                    strategy_label=selected_strategy.label,
                    adjustment_policy=adjustment_policy,
                )
            )

        capital_values = [float(item["capital_required"]) for item in variant_ledger if item["capital_required"] is not None]
        roi_values = [float(item["roi_on_capital_required_pct"]) for item in variant_ledger if item["roi_on_capital_required_pct"] is not None]
        total_capital = sum(capital_values)
        total_pnl = sum(float(item["net_pnl"]) for item in variant_ledger)
        position_multiplier = float(candidate["position_multiplier"])
        symbol_rows.append(
            {
                "variant_label": variant_label,
                "short_dte": variant.short_dte,
                "far_leg_target_dte": variant.far_leg_target_dte,
                "max_holding_days": variant.max_holding_days,
                "profit_target_pct": variant.profit_target_pct if variant.profit_target_pct is not None else "",
                "adjustment_policy": adjustment_policy.name if adjustment_policy is not None else "",
                "rank": int(candidate["rank"]),
                "symbol": symbol,
                "weight_pct": round(float(candidate["weight_pct"]), 4),
                "position_multiplier": round(position_multiplier, 6),
                "training_trade_count": int(best["trade_count"]),
                "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
                "trade_count": len(variant_ledger),
                "adjusted_trade_count": sum(1 for row in variant_ledger if int(row["adjustment_event_count"]) > 0),
                "adjustment_event_count": sum(int(row["adjustment_event_count"]) for row in variant_ledger),
                "total_capital_required": round(total_capital, 4),
                "total_net_pnl": round(total_pnl, 4),
                "roi_on_capital_required_pct": round(total_pnl / total_capital * 100.0, 4) if total_capital else 0.0,
                "median_roi_on_capital_required_pct": round(median(roi_values), 4) if roi_values else 0.0,
                "weighted_total_capital_required": round(total_capital * position_multiplier, 4),
                "weighted_total_net_pnl": round(total_pnl * position_multiplier, 4),
            }
        )
        ledger_rows.extend(variant_ledger)
    return symbol_rows, ledger_rows


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _aggregate_variant_outputs(
    *,
    variants: list[StructuralVariant],
    symbol_rows: list[dict[str, object]],
    ledger_rows: list[dict[str, object]],
    adjustment_policy: CalendarAdjustmentPolicy | None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    symbol_rows_by_variant: dict[str, list[dict[str, object]]] = defaultdict(list)
    ledger_rows_by_variant: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in symbol_rows:
        symbol_rows_by_variant[str(row["variant_label"])].append(row)
    for row in ledger_rows:
        ledger_rows_by_variant[str(row["variant_label"])].append(row)

    summary_rows: list[dict[str, object]] = []
    weekly_rows: list[dict[str, object]] = []
    for variant in variants:
        variant_label = _variant_run_label(variant, adjustment_policy)
        variant_symbol_rows = symbol_rows_by_variant[variant_label]
        variant_ledger_rows = sorted(
            ledger_rows_by_variant[variant_label],
            key=lambda row: (str(row["entry_date"]), str(row["symbol"])),
        )
        quarter_total_capital = sum(float(item["weighted_total_capital_required"]) for item in variant_symbol_rows)
        quarter_total_pnl = sum(float(item["weighted_total_net_pnl"]) for item in variant_symbol_rows)
        trade_roi_values = [float(item["roi_on_capital_required_pct"]) for item in variant_ledger_rows if item["roi_on_capital_required_pct"] is not None]
        trade_roi_weights = [float(item["position_multiplier"]) for item in variant_ledger_rows if item["roi_on_capital_required_pct"] is not None]
        average_trade_roi = (
            sum(value * weight for value, weight in zip(trade_roi_values, trade_roi_weights)) / sum(trade_roi_weights)
            if trade_roi_weights
            else 0.0
        )
        median_trade_roi = _weighted_median(trade_roi_values, trade_roi_weights)
        weekly: dict[str, dict[str, float | int | list[float]]] = defaultdict(
            lambda: {
                "trade_count": 0,
                "total_entry_debit": 0.0,
                "total_capital_required": 0.0,
                "total_net_pnl": 0.0,
                "roi_values": [],
                "roi_weights": [],
            }
        )
        for row in variant_ledger_rows:
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

        variant_weekly_rows: list[dict[str, object]] = []
        for week in sorted(weekly):
            agg = weekly[week]
            total_entry_debit = float(agg["total_entry_debit"])
            total_capital_required = float(agg["total_capital_required"])
            total_net_pnl = float(agg["total_net_pnl"])
            median_roi_per_trade_pct = _weighted_median(
                list(agg["roi_values"]),
                list(agg["roi_weights"]),
            )
            variant_weekly_rows.append(
                {
                    "variant_label": variant_label,
                    "short_dte": variant.short_dte,
                    "far_leg_target_dte": variant.far_leg_target_dte,
                    "max_holding_days": variant.max_holding_days,
                    "profit_target_pct": variant.profit_target_pct if variant.profit_target_pct is not None else "",
                    "adjustment_policy": variant_symbol_rows[0].get("adjustment_policy", "") if variant_symbol_rows else "",
                    "entry_week": week,
                    "trade_count": int(agg["trade_count"]),
                    "total_entry_debit": round(total_entry_debit, 4),
                    "total_capital_required": round(total_capital_required, 4),
                    "total_net_pnl": round(total_net_pnl, 4),
                    "roi_on_debit_pct": round(total_net_pnl / total_entry_debit * 100.0, 4) if total_entry_debit > 0 else "",
                    "roi_on_capital_required_pct": round(total_net_pnl / total_capital_required * 100.0, 4) if total_capital_required > 0 else "",
                    "median_roi_per_trade_pct": round(median_roi_per_trade_pct, 4),
                }
            )
        weekly_rows.extend(variant_weekly_rows)
        summary_rows.append(
            {
                "variant_label": variant_label,
                "short_dte": variant.short_dte,
                "far_leg_target_dte": variant.far_leg_target_dte,
                "max_holding_days": variant.max_holding_days,
                "profit_target_pct": variant.profit_target_pct if variant.profit_target_pct is not None else "",
                "adjustment_policy": variant_symbol_rows[0].get("adjustment_policy", "") if variant_symbol_rows else "",
                "symbol_count": len(variant_symbol_rows),
                "trade_count": len(variant_ledger_rows),
                "adjusted_trade_count": sum(int(item["adjusted_trade_count"]) for item in variant_symbol_rows),
                "adjustment_event_count": sum(int(item["adjustment_event_count"]) for item in variant_symbol_rows),
                "total_capital_required": round(quarter_total_capital, 4),
                "total_net_pnl": round(quarter_total_pnl, 4),
                "roi_on_capital_required_pct": round(quarter_total_pnl / quarter_total_capital * 100.0, 4) if quarter_total_capital > 0 else 0.0,
                "average_roi_per_trade_pct": round(average_trade_roi, 4),
                "median_roi_per_trade_pct": round(median_trade_roi, 4),
                "average_weekly_median_roi_per_trade_pct": (
                    round(sum(float(item["median_roi_per_trade_pct"]) for item in variant_weekly_rows) / len(variant_weekly_rows), 4)
                    if variant_weekly_rows
                    else 0.0
                ),
            }
        )
    summary_rows.sort(
        key=lambda row: (
            float(row["median_roi_per_trade_pct"]),
            float(row["roi_on_capital_required_pct"]),
            -float(row["total_capital_required"]),
        ),
        reverse=True,
    )
    weekly_rows.sort(key=lambda row: (str(row["variant_label"]), str(row["entry_week"])))
    return summary_rows, weekly_rows


def main() -> int:
    args = _parse_args()
    replay_data_end = args.replay_data_end or (args.entry_end_date + timedelta(days=14))
    output_prefix = args.output_prefix or _default_output_prefix()
    if not output_prefix.is_absolute():
        output_prefix = ROOT / output_prefix

    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    adjustment_policy = _resolve_adjustment_policy(args.adjustment_policy)

    variants = _build_structural_variants(
        short_dtes=_parse_int_grid(args.short_dtes),
        far_dtes=_parse_int_grid(args.far_dtes),
        max_holding_days_grid=_parse_int_grid(args.max_holding_days_grid),
        profit_target_pcts=_parse_optional_int_grid(args.profit_targets),
    )
    candidates = _load_selected_candidates(
        selection_csv=args.selection_csv,
        top_k=args.top_k,
        weight_source=args.weight_source,
    )

    all_symbol_rows: list[dict[str, object]] = []
    all_ledger_rows: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = {
            executor.submit(
                _evaluate_symbol_variants,
                candidate=candidate,
                variants=variants,
                entry_start_date=args.entry_start_date,
                entry_end_date=args.entry_end_date,
                replay_data_end=replay_data_end,
                dte_tolerance_days=args.dte_tolerance_days,
                entry_cadence=args.entry_cadence,
                adjustment_policy=adjustment_policy,
            ): str(candidate["symbol"])
            for candidate in candidates
        }
        for future in as_completed(futures):
            symbol = futures[future]
            symbol_rows, ledger_rows = future.result()
            all_symbol_rows.extend(symbol_rows)
            all_ledger_rows.extend(ledger_rows)
            print(
                json.dumps(
                    {
                        "symbol": symbol,
                        "variant_count": len(symbol_rows),
                        "trade_count": sum(int(item["trade_count"]) for item in symbol_rows),
                    },
                    sort_keys=True,
                )
            )

    all_symbol_rows.sort(key=lambda row: (str(row["variant_label"]), int(row["rank"]), str(row["symbol"])))
    all_ledger_rows.sort(key=lambda row: (str(row["variant_label"]), str(row["entry_date"]), str(row["symbol"])))
    summary_rows, weekly_rows = _aggregate_variant_outputs(
        variants=variants,
        symbol_rows=all_symbol_rows,
        ledger_rows=all_ledger_rows,
        adjustment_policy=adjustment_policy,
    )

    summary_csv = Path(f"{output_prefix}_summary.csv")
    symbols_csv = Path(f"{output_prefix}_symbols.csv")
    weekly_csv = Path(f"{output_prefix}_weekly.csv")
    ledger_csv = Path(f"{output_prefix}_trade_ledger.csv")

    _write_csv(summary_csv, summary_rows, list(summary_rows[0].keys()) if summary_rows else ["variant_label"])
    _write_csv(symbols_csv, all_symbol_rows, list(all_symbol_rows[0].keys()) if all_symbol_rows else ["variant_label", "symbol"])
    _write_csv(weekly_csv, weekly_rows, list(weekly_rows[0].keys()) if weekly_rows else ["variant_label", "entry_week"])
    _write_csv(ledger_csv, all_ledger_rows, list(all_ledger_rows[0].keys()) if all_ledger_rows else ["variant_label", "symbol"])

    print(
        json.dumps(
            {
                "selection_csv": str(args.selection_csv),
                "entry_window_start": args.entry_start_date.isoformat(),
                "entry_window_end": args.entry_end_date.isoformat(),
                "entry_cadence": args.entry_cadence,
                "replay_data_end": replay_data_end.isoformat(),
                "profit_targets": list(_parse_optional_int_grid(args.profit_targets)),
                "adjustment_policy": adjustment_policy.name if adjustment_policy is not None else None,
                "variant_count": len(variants),
                "symbol_count": len(candidates),
                "summary_csv": str(summary_csv.relative_to(ROOT)).replace("\\", "/"),
                "symbols_csv": str(symbols_csv.relative_to(ROOT)).replace("\\", "/"),
                "weekly_csv": str(weekly_csv.relative_to(ROOT)).replace("\\", "/"),
                "ledger_csv": str(ledger_csv.relative_to(ROOT)).replace("\\", "/"),
                "top_variants": summary_rows[:10],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
