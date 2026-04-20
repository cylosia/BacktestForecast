from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict, deque
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_calendar_take_profit_grid as tp_grid

LOGS = ROOT / "logs"

DEFAULT_BEST_DELTA_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_best_delta_by_symbol.csv"
DEFAULT_DELTA_TRADES_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_trades.csv"
DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_conditional_management_2y_selected_trades.csv"
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_conditional_management_2y_summary.csv"
BASE_BEST_COMBINED_POLICY_LABEL = (
    "best_combined_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit"
    "__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40"
)
BASE_BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    "best_combined_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit"
    "__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40"
    "__symbol_side_52w_lookback_pnl_nonnegative"
)
BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL = (
    "best_combined_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit"
    "__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40"
    "__up_70_75_negative_method_skip"
)
BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL = (
    "best_combined_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit"
    "__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40"
    "__up_70_75_negative_method_skip__abstain_debit_gt_4_half_size"
)
BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL}"
    "__skip_abstain_median25trend__skip_up_mllogreg56_conf_90_100"
)
BEST_COMBINED_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL}"
    "__skip_up_debit_sensitive_methods"
)
# Preferred downstream alias for the current combined policy.
BEST_COMBINED_POLICY_LABEL = BEST_COMBINED_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL
BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
)
NEGATIVE_UP_CONFIDENCE_BUCKET_METHODS = frozenset(
    {
        "mllogreg56",
        "mlgbp64",
        "mlgbp72",
        "median40rsi",
        "vote15rsi",
        "mlgb70",
        "vote30trend",
        "median25trend",
    }
)
DEBIT_SENSITIVE_UP_METHOD_ENTRY_DEBIT_THRESHOLDS: dict[str, float] = {
    "median25": 3.0,
    "median15trend": 2.5,
    "median25rsi": 2.0,
    "mlgb76": 1.2,
    "median30trend": 2.0,
}
POSITION_SIZED_VALUE_COLUMNS = (
    "original_entry_debit",
    "entry_debit",
    "spread_mark",
    "pnl",
    "roll_net_debit",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate conditional management rules across the full 2-year Friday history "
            "for short-IV-greater-than-long-IV weekly call calendars."
        )
    )
    parser.add_argument("--best-delta-csv", type=Path, default=DEFAULT_BEST_DELTA_CSV)
    parser.add_argument("--delta-trades-csv", type=Path, default=DEFAULT_DELTA_TRADES_CSV)
    parser.add_argument("--output-trades-csv", type=Path, default=DEFAULT_OUTPUT_TRADES_CSV)
    parser.add_argument("--output-summary-csv", type=Path, default=DEFAULT_OUTPUT_SUMMARY_CSV)
    parser.add_argument(
        "--max-spot-entry",
        type=float,
        default=None,
        help="Optional maximum allowed spot close on entry date. Example: 1000.",
    )
    parser.add_argument(
        "--abstain-min-entry-debit",
        type=float,
        default=1.5,
        help="Apply conditional management only when abstain entry debit is greater than this threshold.",
    )
    parser.add_argument(
        "--abstain-min-short-iv-pct",
        type=float,
        default=100.0,
        help="Apply conditional management only when abstain short-leg entry IV exceeds this threshold.",
    )
    return parser


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.lower() == "none":
        return None
    return float(text)


def _compat_trade_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "symbol": row["symbol"],
        "entry_date": row["entry_date"],
        "prediction": row["prediction"],
        "selected_method": row["selected_method"],
        "prediction_engine": row["prediction_engine"],
        "confidence_pct": row["confidence_pct"],
        "best_delta_target_pct": row["delta_target_pct"],
        "spot_close_entry": row["spot_close_entry"],
        "short_expiration": row["short_expiration"],
        "long_expiration": row["long_expiration"],
        "short_strike": row["short_strike"],
        "entry_debit": row["entry_debit"],
        "spread_mark": row["spread_mark"],
        "pnl": row["pnl"],
        "roi_pct": row["roi_pct"],
        "short_mark_method": row["short_mark_method"],
        "long_mark_method": row["long_mark_method"],
    }


def _summarize_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    positive_rows = [row for row in rows if float(row["entry_debit"]) > 0]
    total_debit = sum(float(row["entry_debit"]) for row in positive_rows)
    total_pnl = sum(float(row["pnl"]) for row in positive_rows)
    total_pnl_all = sum(float(row["pnl"]) for row in rows)
    roi_values = [float(row["roi_pct"]) for row in positive_rows if row["roi_pct"] is not None]
    return {
        "trade_count": len(rows),
        "positive_debit_count": len(positive_rows),
        "nonpositive_debit_count": len(rows) - len(positive_rows),
        "managed_trade_count": sum(1 for row in rows if int(row["management_applied"]) == 1),
        "total_debit_paid_positive": round(total_debit, 6),
        "total_pnl_positive": round(total_pnl, 6),
        "total_pnl_all_trades": round(total_pnl_all, 6),
        "avg_roi_positive_debit_pct": _round_or_none(mean(roi_values) if roi_values else None),
        "median_roi_positive_debit_pct": _round_or_none(median(roi_values) if roi_values else None),
        "weighted_return_positive_debit_pct": (
            None if total_debit <= 0 else round(total_pnl / total_debit * 100.0, 6)
        ),
        "tested_exit_count": sum(1 for row in rows if row["exit_reason"] == "spot_close_above_short_strike"),
        "stop_loss_exit_count": sum(1 for row in rows if row["exit_reason"] == "stop_loss"),
        "profit_target_exit_count": sum(1 for row in rows if row["exit_reason"] == "profit_target"),
    }


def _load_selected_best_delta_rows(
    *,
    best_delta_csv: Path,
    delta_trades_csv: Path,
) -> list[dict[str, str]]:
    best_delta_by_symbol_prediction = tp_grid._load_best_delta_by_symbol_prediction(best_delta_csv)
    return tp_grid._load_selected_best_delta_trade_rows(
        delta_trades_csv,
        best_delta_by_symbol_prediction=best_delta_by_symbol_prediction,
        limit_symbols=None,
    )


def _load_symbol_cache(
    session: Session,
    *,
    symbol: str,
    trades: list[dict[str, str]],
) -> tuple[
    dict[date, float],
    dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    dict[tuple[str, str, str], list[date]],
]:
    entry_dates = [date.fromisoformat(row["entry_date"]) for row in trades]
    short_expirations = [date.fromisoformat(row["short_expiration"]) for row in trades]
    long_expirations = [date.fromisoformat(row["long_expiration"]) for row in trades]
    spot_by_date = tp_grid._load_underlying_closes(
        session,
        symbol=symbol,
        start_date=min(entry_dates),
        end_date=max(short_expirations),
    )
    ordered_trade_dates = sorted(spot_by_date)
    needed_trade_dates: set[date] = set(entry_dates)
    path_dates_by_trade: dict[tuple[str, str, str], list[date]] = {}
    for row in trades:
        entry_date = date.fromisoformat(row["entry_date"])
        short_expiration = date.fromisoformat(row["short_expiration"])
        path_dates = [
            trade_date
            for trade_date in ordered_trade_dates
            if entry_date < trade_date <= short_expiration
        ]
        path_dates_by_trade[(row["entry_date"], row["symbol"], row["prediction"])] = path_dates
        needed_trade_dates.update(path_dates)
    option_rows_by_date = tp_grid._load_option_rows_for_dates_and_expirations(
        session,
        symbol=symbol,
        trade_dates=needed_trade_dates,
        expirations=set(short_expirations).union(long_expirations),
    )
    return spot_by_date, option_rows_by_date, path_dates_by_trade


def _find_option_close(
    *,
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    trade_date: date,
    expiration_date: date,
    strike_price: float,
) -> float | None:
    expiration_map = option_rows_by_date.get(trade_date)
    if expiration_map is None:
        return None
    for row in expiration_map.get(expiration_date, []):
        if abs(float(row.strike_price) - strike_price) < 1e-9:
            return float(row.close_price)
    return None


def _short_entry_iv_pct(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
) -> float | None:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    short_strike = float(trade_row["short_strike"])
    short_close = _find_option_close(
        option_rows_by_date=option_rows_by_date,
        trade_date=entry_date,
        expiration_date=short_expiration,
        strike_price=short_strike,
    )
    if short_close is None:
        return None
    return tp_grid.delta_grid._estimate_call_iv_pct(
        option_price=short_close,
        spot_price=float(trade_row["spot_close_entry"]),
        strike_price=short_strike,
        trade_date=entry_date,
        expiration_date=short_expiration,
    )


def _with_metadata(
    row: dict[str, object],
    *,
    short_entry_iv_pct: float | None,
    management_applied: bool,
    condition_entry_debit_gt_threshold: bool,
    condition_short_iv_gt_threshold: bool,
    condition_short_iv_gt_110: bool,
    condition_short_iv_gt_130: bool,
    condition_abstain_debit_gt_5_0_iv_35_50: bool,
    condition_abstain_debit_gt_2_0_iv_40_45: bool,
    condition_abstain_debit_gt_3_0_iv_55_65: bool,
    condition_abstain_debit_gt_2_5_iv_55_80: bool,
    condition_abstain_piecewise_moderate_iv: bool,
    condition_abstain_midhigh_iv_tested_exit: bool,
    condition_up_debit_gt_5_5: bool,
    condition_up_short_iv_lt_40: bool,
) -> dict[str, object]:
    enriched = dict(row)
    enriched["entry_year"] = str(row["entry_date"])[:4]
    enriched["short_entry_iv_pct"] = _round_or_none(short_entry_iv_pct)
    enriched["condition_entry_debit_gt_threshold"] = int(condition_entry_debit_gt_threshold)
    enriched["condition_short_iv_gt_threshold"] = int(condition_short_iv_gt_threshold)
    enriched["condition_short_iv_gt_110"] = int(condition_short_iv_gt_110)
    enriched["condition_short_iv_gt_130"] = int(condition_short_iv_gt_130)
    enriched["condition_entry_debit_and_short_iv"] = int(
        condition_entry_debit_gt_threshold and condition_short_iv_gt_threshold
    )
    enriched["condition_entry_debit_and_short_iv110"] = int(
        condition_entry_debit_gt_threshold and condition_short_iv_gt_110
    )
    enriched["condition_entry_debit_and_short_iv130"] = int(
        condition_entry_debit_gt_threshold and condition_short_iv_gt_130
    )
    enriched["condition_abstain_debit_gt_5_0_iv_35_50"] = int(condition_abstain_debit_gt_5_0_iv_35_50)
    enriched["condition_abstain_debit_gt_2_0_iv_40_45"] = int(condition_abstain_debit_gt_2_0_iv_40_45)
    enriched["condition_abstain_debit_gt_3_0_iv_55_65"] = int(condition_abstain_debit_gt_3_0_iv_55_65)
    enriched["condition_abstain_debit_gt_2_5_iv_55_80"] = int(condition_abstain_debit_gt_2_5_iv_55_80)
    enriched["condition_abstain_piecewise_moderate_iv"] = int(condition_abstain_piecewise_moderate_iv)
    enriched["condition_abstain_midhigh_iv_tested_exit"] = int(condition_abstain_midhigh_iv_tested_exit)
    enriched["condition_up_debit_gt_5_5"] = int(condition_up_debit_gt_5_5)
    enriched["condition_up_short_iv_lt_40"] = int(condition_up_short_iv_lt_40)
    enriched["condition_up_debit_and_short_iv_lt_40"] = int(
        condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40
    )
    enriched["management_applied"] = int(management_applied)
    enriched["position_size_weight"] = 1.0
    enriched["position_sizing_rule"] = ""
    return enriched


def _should_apply_first_breach_exit(
    *,
    first_breach_row: dict[str, object],
    is_eligible: bool,
    take_profit_pct: float = 0.0,
    stop_loss_pct: float = 35.0,
) -> bool:
    if not is_eligible:
        return False
    if str(first_breach_row.get("exit_reason")) != "spot_close_above_short_strike":
        return False
    roi_pct = _to_float(str(first_breach_row.get("roi_pct")))
    if roi_pct is None:
        return False
    return roi_pct >= take_profit_pct or roi_pct <= -stop_loss_pct


def _should_apply_piecewise_abstain_tp25_stop35(
    *,
    prediction: str,
    entry_debit: float,
    short_entry_iv_pct: float | None,
) -> bool:
    if prediction != "abstain" or short_entry_iv_pct is None:
        return False
    return (
        (entry_debit > 5.0 and 35.0 <= short_entry_iv_pct < 50.0)
        or (entry_debit > 2.0 and 40.0 <= short_entry_iv_pct < 45.0)
        or (entry_debit > 3.0 and 55.0 <= short_entry_iv_pct < 65.0)
    )


def _should_apply_midhigh_iv_tested_exit(
    *,
    prediction: str,
    entry_debit: float,
    short_entry_iv_pct: float | None,
    already_piecewise_managed: bool,
) -> bool:
    return (
        prediction == "abstain"
        and short_entry_iv_pct is not None
        and entry_debit > 2.5
        and 55.0 <= short_entry_iv_pct < 80.0
        and not already_piecewise_managed
    )


def _derive_symbol_side_lookback_filtered_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    lookback_days: int = 364,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    history_by_key: dict[tuple[str, str], deque[tuple[date, float]]] = defaultdict(deque)
    pnl_sum_by_key: dict[tuple[str, str], float] = defaultdict(float)
    filtered_rows: list[dict[str, object]] = []
    for row in source_rows:
        entry_date = date.fromisoformat(str(row["entry_date"]))
        key = (str(row["symbol"]), str(row["prediction"]))
        cutoff_date = entry_date - timedelta(days=lookback_days)
        while history_by_key[key] and history_by_key[key][0][0] < cutoff_date:
            _, expired_pnl = history_by_key[key].popleft()
            pnl_sum_by_key[key] -= expired_pnl
        if pnl_sum_by_key[key] >= 0.0:
            candidate = dict(row)
            candidate["policy_label"] = derived_policy_label
            filtered_rows.append(candidate)
        if float(row["entry_debit"]) > 0:
            pnl = float(row["pnl"])
            history_by_key[key].append((entry_date, pnl))
            pnl_sum_by_key[key] += pnl
    return filtered_rows


def _is_negative_up_confidence_bucket_method_trade(row: dict[str, object]) -> bool:
    confidence_pct = _to_float(str(row.get("confidence_pct")))
    return (
        str(row.get("prediction")) == "up"
        and confidence_pct is not None
        and 70.0 < confidence_pct <= 75.0
        and str(row.get("selected_method")) in NEGATIVE_UP_CONFIDENCE_BUCKET_METHODS
    )


def _is_abstain_median25trend_trade(row: dict[str, object]) -> bool:
    return (
        str(row.get("prediction")) == "abstain"
        and str(row.get("selected_method")) == "median25trend"
    )


def _is_high_confidence_up_mllogreg56_trade(row: dict[str, object]) -> bool:
    confidence_pct = _to_float(str(row.get("confidence_pct")))
    return (
        str(row.get("prediction")) == "up"
        and str(row.get("selected_method")) == "mllogreg56"
        and confidence_pct is not None
        and 90.0 < confidence_pct <= 100.0
    )


def _is_debit_sensitive_up_method_trade(row: dict[str, object]) -> bool:
    if str(row.get("prediction")) != "up":
        return False
    method = str(row.get("selected_method"))
    entry_debit_threshold = DEBIT_SENSITIVE_UP_METHOD_ENTRY_DEBIT_THRESHOLDS.get(method)
    if entry_debit_threshold is None:
        return False
    raw_entry_debit = row.get("entry_debit")
    entry_debit = _to_float(None if raw_entry_debit is None else str(raw_entry_debit))
    return entry_debit is not None and entry_debit >= entry_debit_threshold


def _scale_position_sized_value(value: object, *, position_size_weight: float) -> object:
    if value in (None, ""):
        return value
    try:
        return _round_or_none(float(value) * position_size_weight)
    except (TypeError, ValueError):
        return value


def _clone_position_sized_trade_row(
    row: dict[str, object],
    *,
    derived_policy_label: str,
    position_size_weight: float,
    position_sizing_rule: str,
) -> dict[str, object]:
    candidate = dict(row)
    candidate["policy_label"] = derived_policy_label
    candidate["position_size_weight"] = _round_or_none(position_size_weight, digits=4)
    candidate["position_sizing_rule"] = position_sizing_rule
    if position_size_weight != 1.0:
        for field in POSITION_SIZED_VALUE_COLUMNS:
            candidate[field] = _scale_position_sized_value(
                candidate.get(field),
                position_size_weight=position_size_weight,
            )
    return candidate


def _derive_targeted_best_combined_variant_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    abstain_half_size_entry_debit_threshold: float | None = None,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        position_size_weight = 1.0
        position_sizing_rule = ""
        if _is_negative_up_confidence_bucket_method_trade(row):
            continue
        if (
            abstain_half_size_entry_debit_threshold is not None
            and str(row.get("prediction")) == "abstain"
            and float(row["entry_debit"]) > abstain_half_size_entry_debit_threshold
        ):
            position_size_weight = 0.5
            position_sizing_rule = (
                f"half_size_abstain_entry_debit_gt_{abstain_half_size_entry_debit_threshold:g}"
            )
        derived_rows.append(
            _clone_position_sized_trade_row(
                row,
                derived_policy_label=derived_policy_label,
                position_size_weight=position_size_weight,
                position_sizing_rule=position_sizing_rule,
            )
        )
    return derived_rows


def _derive_skip_filtered_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    skip_trade_predicates: tuple[Callable[[dict[str, object]], bool], ...],
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        if any(predicate(row) for predicate in skip_trade_predicates):
            continue
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        derived_rows.append(candidate)
    return derived_rows


def main() -> int:
    args = build_parser().parse_args()
    selected_rows_raw = _load_selected_best_delta_rows(
        best_delta_csv=args.best_delta_csv,
        delta_trades_csv=args.delta_trades_csv,
    )
    selected_rows_raw = [row for row in selected_rows_raw if row["prediction"] in {"up", "abstain"}]
    if not selected_rows_raw:
        raise SystemExit("No selected best-delta rows were found.")

    selected_rows = [_compat_trade_row(row) for row in selected_rows_raw]
    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    detail_rows: list[dict[str, object]] = []
    filtered_out_by_prediction: dict[str, list[str]] = defaultdict(list)
    spot_cache: dict[tuple[str, str], float | None] = {}
    symbol_cache: dict[
        str,
        tuple[
            dict[date, float],
            dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
            dict[tuple[str, str, str], list[date]],
        ],
    ] = {}

    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    try:
        with factory() as session:
            total_symbols = len(trades_by_symbol)
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{total_symbols:03d}] {symbol}: loading path data")
                symbol_cache[symbol] = _load_symbol_cache(session, symbol=symbol, trades=symbol_trades)

            for trade_row in selected_rows:
                symbol = trade_row["symbol"].strip().upper()
                prediction = trade_row["prediction"]
                entry_date_text = trade_row["entry_date"]
                entry_date = date.fromisoformat(entry_date_text)
                spot_key = (symbol, entry_date_text)
                if spot_key not in spot_cache:
                    spot_cache[spot_key] = mgmt._load_spot_close(session, symbol=symbol, trade_date=entry_date)
                spot_close_entry = spot_cache[spot_key]
                if args.max_spot_entry is not None and (
                    spot_close_entry is None or spot_close_entry > args.max_spot_entry
                ):
                    filtered_out_by_prediction[prediction].append(symbol)
                    continue

                spot_by_date, option_rows_by_date, path_dates_by_trade = symbol_cache[symbol]
                path_dates = path_dates_by_trade[(trade_row["entry_date"], trade_row["symbol"], trade_row["prediction"])]
                hold_row = mgmt._simulate_hold_to_expiry(
                    trade_row=trade_row,
                    policy_label="hold_best_delta",
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                short_iv_pct = _short_entry_iv_pct(trade_row=trade_row, option_rows_by_date=option_rows_by_date)
                entry_debit = float(trade_row["entry_debit"])
                condition_entry_debit_gt_threshold = (
                    prediction == "abstain" and entry_debit > args.abstain_min_entry_debit
                )
                condition_entry_debit_gt_2_5 = prediction == "abstain" and entry_debit > 2.5
                condition_abstain_debit_gt_5_0_iv_35_50 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 5.0
                    and 35.0 <= short_iv_pct < 50.0
                )
                condition_abstain_debit_gt_2_0_iv_40_45 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 2.0
                    and 40.0 <= short_iv_pct < 45.0
                )
                condition_abstain_debit_gt_3_0_iv_55_65 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 3.0
                    and 55.0 <= short_iv_pct < 65.0
                )
                condition_abstain_debit_gt_2_5_iv_55_80 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 2.5
                    and 55.0 <= short_iv_pct < 80.0
                )
                condition_abstain_piecewise_moderate_iv = _should_apply_piecewise_abstain_tp25_stop35(
                    prediction=prediction,
                    entry_debit=entry_debit,
                    short_entry_iv_pct=short_iv_pct,
                )
                condition_abstain_midhigh_iv_tested_exit = _should_apply_midhigh_iv_tested_exit(
                    prediction=prediction,
                    entry_debit=entry_debit,
                    short_entry_iv_pct=short_iv_pct,
                    already_piecewise_managed=condition_abstain_piecewise_moderate_iv,
                )
                condition_up_debit_gt_5_5 = prediction == "up" and entry_debit > 5.5
                condition_up_short_iv_lt_40 = prediction == "up" and short_iv_pct is not None and short_iv_pct < 40.0
                condition_short_iv_gt_threshold = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and short_iv_pct > args.abstain_min_short_iv_pct
                )
                condition_short_iv_gt_100 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and short_iv_pct > 100.0
                )
                condition_short_iv_gt_110 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and short_iv_pct > 110.0
                )
                condition_short_iv_gt_130 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and short_iv_pct > 130.0
                )
                should_manage = condition_entry_debit_gt_threshold and condition_short_iv_gt_threshold

                detail_rows.append(
                    _with_metadata(
                        hold_row,
                        short_entry_iv_pct=short_iv_pct,
                        management_applied=False,
                        condition_entry_debit_gt_threshold=condition_entry_debit_gt_threshold,
                        condition_short_iv_gt_threshold=condition_short_iv_gt_threshold,
                        condition_short_iv_gt_110=condition_short_iv_gt_110,
                        condition_short_iv_gt_130=condition_short_iv_gt_130,
                        condition_abstain_debit_gt_5_0_iv_35_50=condition_abstain_debit_gt_5_0_iv_35_50,
                        condition_abstain_debit_gt_2_0_iv_40_45=condition_abstain_debit_gt_2_0_iv_40_45,
                        condition_abstain_debit_gt_3_0_iv_55_65=condition_abstain_debit_gt_3_0_iv_55_65,
                        condition_abstain_debit_gt_2_5_iv_55_80=condition_abstain_debit_gt_2_5_iv_55_80,
                        condition_abstain_piecewise_moderate_iv=condition_abstain_piecewise_moderate_iv,
                        condition_abstain_midhigh_iv_tested_exit=condition_abstain_midhigh_iv_tested_exit,
                        condition_up_debit_gt_5_5=condition_up_debit_gt_5_5,
                        condition_up_short_iv_lt_40=condition_up_short_iv_lt_40,
                    )
                )

                tested_row = mgmt._simulate_exit_on_tested_strike_abstain(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                tp25_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=25.0,
                    stop_loss_pct=35.0,
                )
                up_tp75_stop50_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=75.0,
                    stop_loss_pct=50.0,
                )
                condition_abstain_high_iv_or_piecewise_moderate_iv = (
                    (condition_entry_debit_gt_2_5 and condition_short_iv_gt_100)
                    or condition_abstain_piecewise_moderate_iv
                )
                combined_abstain_extended_row = (
                    tp25_row
                    if condition_abstain_high_iv_or_piecewise_moderate_iv
                    else tested_row if condition_abstain_midhigh_iv_tested_exit else hold_row
                )
                should_manage_compromise = _should_apply_first_breach_exit(
                    first_breach_row=tested_row,
                    is_eligible=condition_entry_debit_gt_threshold and condition_short_iv_gt_110,
                )
                should_manage_first_breach_iv130 = _should_apply_first_breach_exit(
                    first_breach_row=tested_row,
                    is_eligible=condition_entry_debit_gt_threshold and condition_short_iv_gt_130,
                )

                policies = [
                    ("cond_tested_exit_debit_and_iv", tested_row, should_manage),
                    (
                        "best_abstain_tested_exit_debit_gt_1_5_short_iv_gt_130",
                        tested_row,
                        condition_entry_debit_gt_threshold and condition_short_iv_gt_130,
                    ),
                    (
                        "best_abstain_first_breach_debit_gt_1_5_short_iv_gt_130_tp0_sl35",
                        tested_row,
                        should_manage_first_breach_iv130,
                    ),
                    (
                        "best_compromise_first_breach_debit_gt_1_5_short_iv_gt_110_tp0_sl35",
                        tested_row,
                        should_manage_compromise,
                    ),
                    (
                        "best_abstain_tp25_stop35_debit_gt_2_5_short_iv_gt_100",
                        tp25_row,
                        condition_entry_debit_gt_2_5 and condition_short_iv_gt_100,
                    ),
                    (
                        "best_abstain_tp25_stop35_piecewise_moderate_iv",
                        tp25_row,
                        condition_abstain_piecewise_moderate_iv,
                    ),
                    (
                        "best_abstain_tp25_stop35_high_iv_or_piecewise_moderate_iv",
                        tp25_row,
                        condition_abstain_high_iv_or_piecewise_moderate_iv,
                    ),
                    (
                        "best_abstain_tested_exit_midhigh_iv_55_80_excluding_piecewise",
                        tested_row,
                        condition_abstain_midhigh_iv_tested_exit,
                    ),
                    (
                        "best_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit",
                        combined_abstain_extended_row,
                        condition_abstain_high_iv_or_piecewise_moderate_iv
                        or condition_abstain_midhigh_iv_tested_exit,
                    ),
                    (
                        "best_up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        up_tp75_stop50_row,
                        condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40,
                    ),
                    (
                        "best_combined_abstain_tp25_stop35_debit_gt_2_5_short_iv_gt_100__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        tp25_row if prediction == "abstain" else up_tp75_stop50_row,
                        (
                            (condition_entry_debit_gt_2_5 and condition_short_iv_gt_100)
                            or (condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40)
                        ),
                    ),
                    (
                        "best_combined_abstain_tp25_stop35_high_iv_or_piecewise_moderate_iv__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        tp25_row if prediction == "abstain" else up_tp75_stop50_row,
                        (
                            (
                                prediction == "abstain"
                                and condition_abstain_high_iv_or_piecewise_moderate_iv
                            )
                            or (
                                prediction == "up"
                                and condition_up_debit_gt_5_5
                                and condition_up_short_iv_lt_40
                            )
                        ),
                    ),
                    (
                        BASE_BEST_COMBINED_POLICY_LABEL,
                        combined_abstain_extended_row if prediction == "abstain" else up_tp75_stop50_row,
                        (
                            (
                                prediction == "abstain"
                                and (
                                    condition_abstain_high_iv_or_piecewise_moderate_iv
                                    or condition_abstain_midhigh_iv_tested_exit
                                )
                            )
                            or (
                                prediction == "up"
                                and condition_up_debit_gt_5_5
                                and condition_up_short_iv_lt_40
                            )
                        ),
                    ),
                    ("cond_tp25_stop35_debit_and_iv", tp25_row, should_manage),
                ]
                for policy_label, managed_row, policy_should_manage in policies:
                    candidate = dict(managed_row if policy_should_manage else hold_row)
                    candidate["policy_label"] = policy_label
                    detail_rows.append(
                        _with_metadata(
                            candidate,
                            short_entry_iv_pct=short_iv_pct,
                            management_applied=policy_should_manage,
                            condition_entry_debit_gt_threshold=condition_entry_debit_gt_threshold,
                            condition_short_iv_gt_threshold=condition_short_iv_gt_threshold,
                            condition_short_iv_gt_110=condition_short_iv_gt_110,
                            condition_short_iv_gt_130=condition_short_iv_gt_130,
                            condition_abstain_debit_gt_5_0_iv_35_50=condition_abstain_debit_gt_5_0_iv_35_50,
                            condition_abstain_debit_gt_2_0_iv_40_45=condition_abstain_debit_gt_2_0_iv_40_45,
                            condition_abstain_debit_gt_3_0_iv_55_65=condition_abstain_debit_gt_3_0_iv_55_65,
                            condition_abstain_debit_gt_2_5_iv_55_80=condition_abstain_debit_gt_2_5_iv_55_80,
                            condition_abstain_piecewise_moderate_iv=condition_abstain_piecewise_moderate_iv,
                            condition_abstain_midhigh_iv_tested_exit=condition_abstain_midhigh_iv_tested_exit,
                            condition_up_debit_gt_5_5=condition_up_debit_gt_5_5,
                            condition_up_short_iv_lt_40=condition_up_short_iv_lt_40,
                        )
                    )
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No 2-year conditional management rows were produced.")

    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            abstain_half_size_entry_debit_threshold=4.0,
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_abstain_median25trend_trade,
                _is_high_confidence_up_mllogreg56_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_debit_sensitive_up_method_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BASE_BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )

    args.output_trades_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_trades_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    summary_rows: list[dict[str, object]] = []
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    yearly_grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    overall_grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail_rows:
        grouped[(str(row["entry_date"])[:10], str(row["prediction"]), str(row["policy_label"]))].append(row)
        yearly_grouped[(str(row["entry_year"]), str(row["prediction"]), str(row["policy_label"]))].append(row)
        overall_grouped[(str(row["prediction"]), str(row["policy_label"]))].append(row)

    for (entry_year, prediction, policy_label), rows in sorted(yearly_grouped.items()):
        summary = _summarize_rows(rows)
        summary_rows.append(
            {
                "summary_scope": "year",
                "entry_period": entry_year,
                "prediction": prediction,
                "policy_label": policy_label,
                **summary,
                "max_spot_entry": args.max_spot_entry,
                "abstain_min_entry_debit": args.abstain_min_entry_debit,
                "abstain_min_short_iv_pct": args.abstain_min_short_iv_pct,
            }
        )

    for (prediction, policy_label), rows in sorted(overall_grouped.items()):
        summary = _summarize_rows(rows)
        summary_rows.append(
            {
                "summary_scope": "all",
                "entry_period": "ALL",
                "prediction": prediction,
                "policy_label": policy_label,
                **summary,
                "max_spot_entry": args.max_spot_entry,
                "abstain_min_entry_debit": args.abstain_min_entry_debit,
                "abstain_min_short_iv_pct": args.abstain_min_short_iv_pct,
            }
        )

    with args.output_summary_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {args.output_trades_csv}")
    print(f"Wrote {args.output_summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
