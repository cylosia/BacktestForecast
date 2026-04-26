from __future__ import annotations

import argparse
import csv
import itertools
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data import vix_regime

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as cond


LOGS = ROOT / "logs"
DEFAULT_SELECTED_TRADES_CSV = (
    LOGS
    / "short_iv_gt_long_best_delta_lookback52warmup_to_2026-04-24_thursday_close_frozen_actual_close_selected_trades.csv"
)
DEFAULT_OUTPUT_CSV = LOGS / "short_iv_gt_long_thursday_8_15_management_reopt_summary.csv"
DEFAULT_OUTPUT_BEST_CSV = LOGS / "short_iv_gt_long_thursday_8_15_management_reopt_best.csv"
DEFAULT_WINDOW_END_DATE = date.fromisoformat("2026-04-23")
DEFAULT_WINDOW_WEEKS = 52
DEFAULT_MAX_SPOT_ENTRY = 1000.0
DEFAULT_VIX_CACHE_CSV = cond.DEFAULT_VIX_CACHE_CSV

ABSTAIN_TP_STOP_PAIRS: tuple[tuple[float, float], ...] = (
    (0.0, 35.0),
    (0.0, 50.0),
    (0.0, 65.0),
    (10.0, 35.0),
    (10.0, 50.0),
    (25.0, 35.0),
    (25.0, 50.0),
    (25.0, 65.0),
    (50.0, 50.0),
    (50.0, 65.0),
)
UP_TP_STOP_PAIRS: tuple[tuple[float, float], ...] = (
    (50.0, 35.0),
    (50.0, 50.0),
    (75.0, 50.0),
    (75.0, 65.0),
    (100.0, 65.0),
    (100.0, 100.0),
)
BASKET_THRESHOLDS: tuple[float | None, ...] = (None, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 120.0)
VIX_THRESHOLDS: tuple[float | None, ...] = (None, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0)
IV_PREMIUM_THRESHOLDS: tuple[float | None, ...] = (None, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0)
FILTER_MODES: tuple[str, ...] = (
    "none",
    "symbol_side_pnl_nonnegative",
    "skip_worst_methods",
    "skip_extended_worst_methods",
    "top35_median_roi_min3_skip_worst_cap12",
    "top43_median_roi_min3_skip_worst_cap12",
    "top55_median_roi_min3_skip_worst_cap12",
    "top43_median_roi_min3_skip_worst_cap12_pnl_over_debit15_min5",
)


@dataclass(frozen=True, slots=True)
class ManagementProfile:
    label: str
    abstain_pair: tuple[float, float] | None
    up_pair: tuple[float, float] | None
    use_current_method_side_overrides: bool = False


@dataclass(slots=True)
class Candidate:
    trade_row: dict[str, str]
    hold_row: dict[str, object]
    tp_stop_rows: dict[tuple[float, float], dict[str, object]]
    marks_by_date: dict[str, dict[str, object]]
    spot_close_entry: float | None
    short_entry_iv_pct: float | None
    short_over_long_atm_iv_premium_pct: float | None
    vix_weekly_change_pct: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reoptimize Thursday 8/15-day short-IV calendar management over the last 52 Thursday entries."
    )
    parser.add_argument("--selected-trades-csv", type=Path, default=DEFAULT_SELECTED_TRADES_CSV)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-best-csv", type=Path, default=DEFAULT_OUTPUT_BEST_CSV)
    parser.add_argument("--window-end-date", type=date.fromisoformat, default=DEFAULT_WINDOW_END_DATE)
    parser.add_argument("--window-weeks", type=int, default=DEFAULT_WINDOW_WEEKS)
    parser.add_argument("--max-spot-entry", type=float, default=DEFAULT_MAX_SPOT_ENTRY)
    parser.add_argument("--vix-cache-csv", type=Path, default=DEFAULT_VIX_CACHE_CSV)
    parser.add_argument("--disable-vix-cache-refresh", action="store_true")
    parser.add_argument("--top-n", type=int, default=12)
    return parser


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise SystemExit("No optimization rows were produced.")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _pair_key(pair: tuple[float, float]) -> str:
    return f"tp{pair[0]:g}_sl{pair[1]:g}"


def _window_entry_dates(*, end_date: date, weeks: int) -> list[date]:
    start_date = end_date - timedelta(days=7 * (weeks - 1))
    return [start_date + timedelta(days=7 * offset) for offset in range(weeks)]


def _all_profiles() -> list[ManagementProfile]:
    profiles = [
        ManagementProfile(
            label="current_method_side",
            abstain_pair=None,
            up_pair=None,
            use_current_method_side_overrides=True,
        )
    ]
    for abstain_pair, up_pair in itertools.product(ABSTAIN_TP_STOP_PAIRS, UP_TP_STOP_PAIRS):
        profiles.append(
            ManagementProfile(
                label=f"abstain_{_pair_key(abstain_pair)}__up_{_pair_key(up_pair)}",
                abstain_pair=abstain_pair,
                up_pair=up_pair,
            )
        )
    return profiles


def _profile_pair_for_trade(profile: ManagementProfile, trade_row: dict[str, str]) -> tuple[float, float]:
    prediction = trade_row["prediction"]
    selected_method = trade_row["selected_method"]
    if profile.use_current_method_side_overrides:
        return cond.resolve_method_side_tp_stop(prediction=prediction, selected_method=selected_method)
    if prediction == "abstain":
        if profile.abstain_pair is None:
            raise ValueError("abstain_pair is required for abstain trades")
        return profile.abstain_pair
    if prediction == "up":
        if profile.up_pair is None:
            raise ValueError("up_pair is required for up trades")
        return profile.up_pair
    raise ValueError(f"Unsupported prediction: {prediction}")


def _should_manage_current_gate(candidate: Candidate) -> bool:
    trade_row = candidate.trade_row
    prediction = trade_row["prediction"]
    entry_debit = float(trade_row["entry_debit"])
    short_iv_pct = candidate.short_entry_iv_pct
    if short_iv_pct is None:
        return False
    if prediction == "abstain":
        return (
            (entry_debit > 2.5 and short_iv_pct > 100.0)
            or (entry_debit > 5.0 and 35.0 <= short_iv_pct < 50.0)
            or (entry_debit > 2.0 and 40.0 <= short_iv_pct < 45.0)
            or (entry_debit > 3.0 and 55.0 <= short_iv_pct < 65.0)
        )
    if prediction == "up":
        return entry_debit > 5.5 and short_iv_pct < 40.0
    return False


def _passes_vix_iv_spot(
    candidate: Candidate,
    *,
    max_spot_entry: float | None,
    vix_threshold_pct: float | None,
    iv_premium_min_pct: float | None,
) -> bool:
    if max_spot_entry is not None and (
        candidate.spot_close_entry is None or candidate.spot_close_entry > max_spot_entry
    ):
        return False
    if vix_threshold_pct is not None and (
        candidate.vix_weekly_change_pct is None or abs(candidate.vix_weekly_change_pct) > vix_threshold_pct
    ):
        return False
    if iv_premium_min_pct is not None and (
        candidate.short_over_long_atm_iv_premium_pct is None
        or candidate.short_over_long_atm_iv_premium_pct < iv_premium_min_pct
    ):
        return False
    return True


def _set_policy(row: dict[str, object], policy_label: str) -> dict[str, object]:
    candidate = dict(row)
    candidate["policy_label"] = policy_label
    candidate.setdefault("position_size_weight", 1.0)
    candidate.setdefault("position_sizing_rule", "")
    return candidate


def _base_rows_for_profile(
    candidates: list[Candidate],
    *,
    profile: ManagementProfile,
    max_spot_entry: float | None,
    vix_threshold_pct: float | None,
    iv_premium_min_pct: float | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for candidate in candidates:
        if not _passes_vix_iv_spot(
            candidate,
            max_spot_entry=max_spot_entry,
            vix_threshold_pct=vix_threshold_pct,
            iv_premium_min_pct=iv_premium_min_pct,
        ):
            continue
        if _should_manage_current_gate(candidate):
            pair = _profile_pair_for_trade(profile, candidate.trade_row)
            selected_row = candidate.tp_stop_rows[pair]
            management_applied = 1
        else:
            selected_row = candidate.hold_row
            management_applied = 0
        row = _set_policy(selected_row, cond.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL)
        row["management_applied"] = management_applied
        rows.append(row)
    return rows


def _promote_method_side_rows(base_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    targeted_rows = cond._derive_targeted_best_combined_variant_rows(
        rows=base_rows,
        source_policy_label=cond.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
        abstain_half_size_entry_debit_threshold=4.0,
    )
    skip_rows_1 = cond._derive_skip_filtered_policy_rows(
        rows=targeted_rows,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
        skip_trade_predicates=(
            cond._is_abstain_median25trend_trade,
            cond._is_high_confidence_up_mllogreg56_trade,
        ),
    )
    skip_rows_2 = cond._derive_skip_filtered_policy_rows(
        rows=skip_rows_1,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
        skip_trade_predicates=(cond._is_debit_sensitive_up_method_trade,),
    )
    return cond._derive_skip_filtered_policy_rows(
        rows=skip_rows_2,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        skip_trade_predicates=(cond._is_debit_sensitive_abstain_method_trade,),
    )


def _copy_with_label(rows: list[dict[str, object]], label: str) -> list[dict[str, object]]:
    copied = []
    for row in rows:
        candidate = dict(row)
        candidate["policy_label"] = label
        copied.append(candidate)
    return copied


def _apply_filter_mode(rows: list[dict[str, object]], *, filter_mode: str) -> list[dict[str, object]]:
    source_label = cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
    filtered_label = f"filtered_{filter_mode}"
    if filter_mode == "none":
        return _copy_with_label(rows, filtered_label)
    if filter_mode == "symbol_side_pnl_nonnegative":
        return cond._derive_symbol_side_lookback_filtered_rows(
            rows=rows,
            source_policy_label=source_label,
            derived_policy_label=filtered_label,
        )
    if filter_mode == "skip_worst_methods":
        return cond._derive_skip_filtered_policy_rows(
            rows=rows,
            source_policy_label=source_label,
            derived_policy_label=filtered_label,
            skip_trade_predicates=(cond._is_worst_method_trade,),
        )
    if filter_mode == "skip_extended_worst_methods":
        return cond._derive_skip_filtered_policy_rows(
            rows=rows,
            source_policy_label=source_label,
            derived_policy_label=filtered_label,
            skip_trade_predicates=(cond._is_extended_worst_method_trade,),
        )

    topk_by_mode = {
        "top35_median_roi_min3_skip_worst_cap12": 35,
        "top43_median_roi_min3_skip_worst_cap12": 43,
        "top55_median_roi_min3_skip_worst_cap12": 55,
        "top43_median_roi_min3_skip_worst_cap12_pnl_over_debit15_min5": 43,
    }
    if filter_mode not in topk_by_mode:
        raise ValueError(f"Unknown filter mode: {filter_mode}")

    skip_label = f"{filtered_label}_skip_worst_source"
    skip_rows = cond._derive_skip_filtered_policy_rows(
        rows=rows,
        source_policy_label=source_label,
        derived_policy_label=skip_label,
        skip_trade_predicates=(cond._is_worst_method_trade,),
    )
    top_rows = cond._derive_symbol_median_roi_topk_rows(
        rows=skip_rows,
        source_policy_label=skip_label,
        derived_policy_label=filtered_label,
        top_k=topk_by_mode[filter_mode],
        min_history_trades=3,
        selected_method_cap=cond.DEFAULT_TOP43_METHOD_CAP,
    )
    if filter_mode == "top43_median_roi_min3_skip_worst_cap12_pnl_over_debit15_min5":
        pnl_label = f"{filtered_label}_pnl_over_debit"
        return cond._derive_symbol_lookback_pnl_over_debit_filtered_rows(
            rows=top_rows,
            source_policy_label=filtered_label,
            derived_policy_label=pnl_label,
            min_history_trades=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES,
            min_pnl_over_debit_pct=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT,
        )
    return top_rows


def _derive_basket_close_rows(
    rows: list[dict[str, object]],
    *,
    threshold_pct: float | None,
    mark_cache: dict[tuple[str, ...], dict[str, dict[str, object]]],
) -> list[dict[str, object]]:
    if threshold_pct is None:
        return _copy_with_label(rows, "no_basket_close")

    marked_rows_by_trade_and_date: dict[tuple[tuple[str, ...], str], dict[str, object]] = {}
    basket_marks_by_week_date: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        entry_date_text = str(row["entry_date"])
        entry_date = date.fromisoformat(entry_date_text)
        trade_key = cond._trade_identity_key(row)
        position_size_weight = cond._to_float(
            None if row.get("position_size_weight") in (None, "") else str(row.get("position_size_weight"))
        )
        if position_size_weight is None:
            position_size_weight = 1.0
        entry_debit = float(row["entry_debit"])
        for mark_date_text, mark in mark_cache.get(trade_key, {}).items():
            scaled_spread_mark = float(mark["spread_mark"]) * position_size_weight
            pnl = scaled_spread_mark - entry_debit
            roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
            marked_rows_by_trade_and_date[(trade_key, mark_date_text)] = {
                "policy_label": f"basket_close_{threshold_pct:g}",
                "exit_date": mark_date_text,
                "spot_close_exit": mark.get("spot_close_exit"),
                "spread_mark": cond._round_or_none(scaled_spread_mark),
                "pnl": cond._round_or_none(pnl),
                "roi_pct": cond._round_or_none(roi_pct),
                "exit_reason": f"basket_close_{threshold_pct:g}",
                "holding_days_calendar": (date.fromisoformat(mark_date_text) - entry_date).days,
                "short_mark_method": mark["short_mark_method"],
                "long_mark_method": mark["long_mark_method"],
            }
            if entry_debit > 0:
                basket_marks_by_week_date[(entry_date_text, mark_date_text)].append((entry_debit, pnl))

    trigger_date_by_week: dict[str, str] = {}
    for entry_date_text in sorted({key[0] for key in basket_marks_by_week_date}):
        candidate_dates = sorted(
            trade_date_text
            for week_text, trade_date_text in basket_marks_by_week_date
            if week_text == entry_date_text
        )
        for trade_date_text in candidate_dates:
            marks = basket_marks_by_week_date[(entry_date_text, trade_date_text)]
            total_debit = sum(entry_debit for entry_debit, _ in marks)
            if total_debit <= 0:
                continue
            total_pnl = sum(pnl for _, pnl in marks)
            if (total_pnl / total_debit) * 100.0 >= threshold_pct:
                trigger_date_by_week[entry_date_text] = trade_date_text
                break

    final_rows: list[dict[str, object]] = []
    for row in rows:
        candidate = dict(row)
        candidate["policy_label"] = f"basket_close_{threshold_pct:g}"
        trigger_date_text = trigger_date_by_week.get(str(row["entry_date"]))
        if trigger_date_text is not None:
            override = marked_rows_by_trade_and_date.get((cond._trade_identity_key(row), trigger_date_text))
            if override is not None:
                candidate.update(override)
        final_rows.append(candidate)
    return final_rows


def _summarize_window(rows: list[dict[str, object]], *, window_entry_dates: list[date]) -> dict[str, object]:
    window_set = {entry_date.isoformat() for entry_date in window_entry_dates}
    window_rows = [row for row in rows if str(row["entry_date"]) in window_set]
    overall = cond._summarize_rows(window_rows)
    weekly_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in window_rows:
        weekly_rows[str(row["entry_date"])].append(row)

    negative_pnl_weeks = 0
    weekly_pnl_series: list[float] = []
    for entry_date in window_entry_dates:
        rows_for_week = weekly_rows.get(entry_date.isoformat(), [])
        if rows_for_week:
            week_summary = cond._summarize_rows(rows_for_week)
            week_pnl = float(week_summary["total_pnl_all_trades"])
        else:
            week_pnl = 0.0
        if week_pnl < 0:
            negative_pnl_weeks += 1
        weekly_pnl_series.append(week_pnl)

    return {
        **overall,
        "active_weeks": sum(1 for entry_date in window_entry_dates if weekly_rows.get(entry_date.isoformat())),
        "zero_trade_weeks": len(window_entry_dates)
        - sum(1 for entry_date in window_entry_dates if weekly_rows.get(entry_date.isoformat())),
        "negative_pnl_weeks": negative_pnl_weeks,
        "max_drawdown_pct": cond._round_or_none(cond._history_max_drawdown_pct(weekly_pnl_series)),
    }


def _evaluate_scenario(
    *,
    candidates: list[Candidate],
    mark_cache: dict[tuple[str, ...], dict[str, dict[str, object]]],
    window_entry_dates: list[date],
    profile: ManagementProfile,
    basket_threshold_pct: float | None,
    vix_threshold_pct: float | None,
    iv_premium_min_pct: float | None,
    filter_mode: str,
    max_spot_entry: float | None,
    stage: str,
) -> dict[str, object]:
    base_rows = _base_rows_for_profile(
        candidates,
        profile=profile,
        max_spot_entry=max_spot_entry,
        vix_threshold_pct=vix_threshold_pct,
        iv_premium_min_pct=iv_premium_min_pct,
    )
    method_side_rows = _promote_method_side_rows(base_rows)
    filtered_rows = _apply_filter_mode(method_side_rows, filter_mode=filter_mode)
    final_rows = _derive_basket_close_rows(
        filtered_rows,
        threshold_pct=basket_threshold_pct,
        mark_cache=mark_cache,
    )
    summary = _summarize_window(final_rows, window_entry_dates=window_entry_dates)
    return {
        "stage": stage,
        "profile_label": profile.label,
        "abstain_tp_stop": "current" if profile.abstain_pair is None else _pair_key(profile.abstain_pair),
        "up_tp_stop": "current" if profile.up_pair is None else _pair_key(profile.up_pair),
        "basket_threshold_pct": "" if basket_threshold_pct is None else basket_threshold_pct,
        "vix_abs_threshold_pct": "" if vix_threshold_pct is None else vix_threshold_pct,
        "iv_premium_min_pct": "" if iv_premium_min_pct is None else iv_premium_min_pct,
        "filter_mode": filter_mode,
        "base_trade_count_all_dates": len(base_rows),
        "method_side_trade_count_all_dates": len(method_side_rows),
        "filtered_trade_count_all_dates": len(filtered_rows),
        **summary,
    }


def _score_pnl(row: dict[str, object]) -> tuple[float, float, int, float]:
    return (
        float(row["total_pnl_positive"] or 0.0),
        float(row["weighted_return_positive_debit_pct"] or 0.0),
        -int(row["negative_pnl_weeks"]),
        float(row["median_roi_positive_debit_pct"] or 0.0),
    )


def _score_efficiency(row: dict[str, object]) -> tuple[float, float, int, float]:
    return (
        float(row["weighted_return_positive_debit_pct"] or 0.0),
        float(row["total_pnl_positive"] or 0.0),
        -int(row["negative_pnl_weeks"]),
        float(row["median_roi_positive_debit_pct"] or 0.0),
    )


def _top_rows(rows: list[dict[str, object]], *, key_name: str, top_n: int) -> list[dict[str, object]]:
    scorer = _score_pnl if key_name == "pnl" else _score_efficiency
    ranked = sorted(rows, key=scorer, reverse=True)
    selected = []
    for rank, row in enumerate(ranked[:top_n], start=1):
        candidate = dict(row)
        candidate["selection"] = f"top_{key_name}_{rank}"
        selected.append(candidate)
    return selected


def _build_candidates(
    *,
    selected_rows: list[dict[str, str]],
    vix_cache_csv: Path,
    disable_vix_cache_refresh: bool,
) -> tuple[list[Candidate], dict[tuple[str, ...], dict[str, dict[str, object]]]]:
    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    entry_dates = sorted({date.fromisoformat(row["entry_date"]) for row in selected_rows})
    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    store = HistoricalMarketDataStore(factory, factory)
    vix_close_by_date = vix_regime.load_vix_close_series(
        start_date=min(entry_dates) - timedelta(days=14),
        end_date=max(entry_dates),
        store=store,
        cache_csv=vix_cache_csv,
        allow_cache_refresh=not disable_vix_cache_refresh,
    )
    vix_snapshots = vix_regime.build_weekly_change_snapshots(
        entry_dates=entry_dates,
        close_by_date=vix_close_by_date,
    )
    missing_vix_dates = [entry_date.isoformat() for entry_date in entry_dates if entry_date not in vix_snapshots]
    if missing_vix_dates:
        raise SystemExit("Missing VIX reference data for entry dates: " + ", ".join(missing_vix_dates))

    all_pairs = set(ABSTAIN_TP_STOP_PAIRS).union(UP_TP_STOP_PAIRS).union(cond.ABSTAIN_METHOD_SIDE_TP_STOP_OVERRIDES.values())
    all_pairs.add((cond.DEFAULT_ABSTAIN_TAKE_PROFIT_PCT, cond.DEFAULT_ABSTAIN_STOP_LOSS_PCT))
    all_pairs.add((cond.DEFAULT_UP_TAKE_PROFIT_PCT, cond.DEFAULT_UP_STOP_LOSS_PCT))

    candidates: list[Candidate] = []
    mark_cache: dict[tuple[str, ...], dict[str, dict[str, object]]] = {}
    try:
        with factory() as session:
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{len(trades_by_symbol):03d}] {symbol}: loading path data")
                (
                    spot_by_date,
                    option_rows_by_date,
                    _put_option_rows_by_date,
                    path_dates_by_trade,
                    _extended_path_dates_by_trade,
                ) = cond._load_symbol_cache(session, symbol=symbol, trades=symbol_trades)
                for trade_row in symbol_trades:
                    trade_key = (trade_row["entry_date"], trade_row["symbol"], trade_row["prediction"])
                    path_dates = path_dates_by_trade[trade_key]
                    hold_row = mgmt._simulate_hold_to_expiry(
                        trade_row=trade_row,
                        policy_label="hold_best_delta",
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                    )
                    tp_stop_rows = {
                        pair: mgmt._simulate_tp_stop(
                            trade_row=trade_row,
                            option_rows_by_date=option_rows_by_date,
                            spot_by_date=spot_by_date,
                            path_dates=path_dates,
                            take_profit_pct=pair[0],
                            stop_loss_pct=pair[1],
                        )
                        for pair in all_pairs
                    }
                    short_iv_pct = cond._short_entry_iv_pct(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                    )
                    (
                        _short_atm_iv_pct,
                        _long_atm_iv_pct,
                        short_over_long_atm_iv_premium_pct,
                    ) = cond._entry_atm_iv_metrics(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                    )
                    entry_date = date.fromisoformat(trade_row["entry_date"])
                    vix_snapshot = vix_snapshots[entry_date]
                    raw_mark_rows: dict[str, dict[str, object]] = {}
                    short_expiration = date.fromisoformat(trade_row["short_expiration"])
                    long_expiration = date.fromisoformat(trade_row["long_expiration"])
                    short_strike = float(trade_row["short_strike"])
                    long_strike = float(trade_row.get("long_strike") or trade_row["short_strike"])
                    for mark_date in path_dates:
                        spot_mark = spot_by_date.get(mark_date)
                        if spot_mark is None:
                            continue
                        mark = mgmt._mark_position(
                            option_rows_by_date=option_rows_by_date,
                            mark_date=mark_date,
                            short_expiration=short_expiration,
                            long_expiration=long_expiration,
                            short_strike=short_strike,
                            long_strike=long_strike,
                            spot_mark=spot_mark,
                        )
                        if mark is None:
                            continue
                        raw_mark_rows[mark_date.isoformat()] = {
                            "spot_close_exit": spot_mark,
                            "spread_mark": mark["spread_mark"],
                            "short_mark_method": mark["short_mark_method"],
                            "long_mark_method": mark["long_mark_method"],
                        }
                    identity = cond._trade_identity_key(hold_row)
                    mark_cache[identity] = raw_mark_rows
                    candidates.append(
                        Candidate(
                            trade_row=trade_row,
                            hold_row=hold_row,
                            tp_stop_rows=tp_stop_rows,
                            marks_by_date=raw_mark_rows,
                            spot_close_entry=spot_by_date.get(entry_date),
                            short_entry_iv_pct=short_iv_pct,
                            short_over_long_atm_iv_premium_pct=short_over_long_atm_iv_premium_pct,
                            vix_weekly_change_pct=vix_snapshot.weekly_change_pct,
                        )
                    )
    finally:
        engine.dispose()
    print(f"Built {len(candidates)} candidates with {sum(len(c.marks_by_date) for c in candidates)} daily marks.")
    return candidates, mark_cache


def main() -> int:
    args = build_parser().parse_args()
    selected_rows = [
        row
        for row in _read_csv_rows(args.selected_trades_csv)
        if row.get("prediction") in {"up", "abstain"}
    ]
    if not selected_rows:
        raise SystemExit("No selected trades were found.")
    window_entry_dates = _window_entry_dates(end_date=args.window_end_date, weeks=args.window_weeks)
    print(
        f"Optimizing {len(selected_rows)} selected trades over "
        f"{window_entry_dates[0].isoformat()} to {window_entry_dates[-1].isoformat()}."
    )
    candidates, mark_cache = _build_candidates(
        selected_rows=selected_rows,
        vix_cache_csv=args.vix_cache_csv,
        disable_vix_cache_refresh=args.disable_vix_cache_refresh,
    )
    profiles = _all_profiles()
    current_profile = profiles[0]

    all_rows: list[dict[str, object]] = []

    stage1_rows = [
        _evaluate_scenario(
            candidates=candidates,
            mark_cache=mark_cache,
            window_entry_dates=window_entry_dates,
            profile=current_profile,
            basket_threshold_pct=basket_threshold,
            vix_threshold_pct=20.0,
            iv_premium_min_pct=10.0,
            filter_mode="none",
            max_spot_entry=args.max_spot_entry,
            stage="basket_threshold_current_tp_sl",
        )
        for basket_threshold in BASKET_THRESHOLDS
    ]
    all_rows.extend(stage1_rows)
    best_stage1 = max(stage1_rows, key=_score_pnl)
    best_stage1_basket = (
        None
        if best_stage1["basket_threshold_pct"] == ""
        else float(best_stage1["basket_threshold_pct"])
    )
    print(f"Stage 1 complete: best PnL basket threshold {best_stage1_basket}.")

    stage2_rows = []
    for profile in profiles:
        for basket_threshold in BASKET_THRESHOLDS:
            stage2_rows.append(
                _evaluate_scenario(
                    candidates=candidates,
                    mark_cache=mark_cache,
                    window_entry_dates=window_entry_dates,
                    profile=profile,
                    basket_threshold_pct=basket_threshold,
                    vix_threshold_pct=20.0,
                    iv_premium_min_pct=10.0,
                    filter_mode="none",
                    max_spot_entry=args.max_spot_entry,
                    stage="tp_sl_and_basket",
                )
            )
    all_rows.extend(stage2_rows)
    top_stage2 = _top_rows(stage2_rows, key_name="pnl", top_n=5) + _top_rows(
        stage2_rows,
        key_name="efficiency",
        top_n=5,
    )
    seed_configs = []
    seen_seed_keys: set[tuple[str, str, str]] = set()
    profile_by_label = {profile.label: profile for profile in profiles}
    for row in top_stage2:
        key = (str(row["profile_label"]), str(row["basket_threshold_pct"]), str(row["filter_mode"]))
        if key in seen_seed_keys:
            continue
        seen_seed_keys.add(key)
        seed_configs.append(
            (
                profile_by_label[str(row["profile_label"])],
                None if row["basket_threshold_pct"] == "" else float(row["basket_threshold_pct"]),
            )
        )
    print(f"Stage 2 complete: {len(stage2_rows)} TP/SL+basket scenarios, {len(seed_configs)} seeds.")

    stage3_rows = []
    for profile, basket_threshold in seed_configs:
        for vix_threshold, iv_threshold in itertools.product(VIX_THRESHOLDS, IV_PREMIUM_THRESHOLDS):
            stage3_rows.append(
                _evaluate_scenario(
                    candidates=candidates,
                    mark_cache=mark_cache,
                    window_entry_dates=window_entry_dates,
                    profile=profile,
                    basket_threshold_pct=basket_threshold,
                    vix_threshold_pct=vix_threshold,
                    iv_premium_min_pct=iv_threshold,
                    filter_mode="none",
                    max_spot_entry=args.max_spot_entry,
                    stage="vix_iv_gates",
                )
            )
    all_rows.extend(stage3_rows)
    top_stage3 = _top_rows(stage3_rows, key_name="pnl", top_n=5) + _top_rows(
        stage3_rows,
        key_name="efficiency",
        top_n=5,
    )
    filter_seeds = []
    seen_filter_keys: set[tuple[str, str, str, str]] = set()
    for row in top_stage3:
        key = (
            str(row["profile_label"]),
            str(row["basket_threshold_pct"]),
            str(row["vix_abs_threshold_pct"]),
            str(row["iv_premium_min_pct"]),
        )
        if key in seen_filter_keys:
            continue
        seen_filter_keys.add(key)
        filter_seeds.append(row)
    print(f"Stage 3 complete: {len(stage3_rows)} VIX/IV scenarios, {len(filter_seeds)} seeds.")

    stage4_rows = []
    for seed in filter_seeds:
        for filter_mode in FILTER_MODES:
            stage4_rows.append(
                _evaluate_scenario(
                    candidates=candidates,
                    mark_cache=mark_cache,
                    window_entry_dates=window_entry_dates,
                    profile=profile_by_label[str(seed["profile_label"])],
                    basket_threshold_pct=None
                    if seed["basket_threshold_pct"] == ""
                    else float(seed["basket_threshold_pct"]),
                    vix_threshold_pct=None
                    if seed["vix_abs_threshold_pct"] == ""
                    else float(seed["vix_abs_threshold_pct"]),
                    iv_premium_min_pct=None
                    if seed["iv_premium_min_pct"] == ""
                    else float(seed["iv_premium_min_pct"]),
                    filter_mode=filter_mode,
                    max_spot_entry=args.max_spot_entry,
                    stage="symbol_method_filters",
                )
            )
    all_rows.extend(stage4_rows)
    print(f"Stage 4 complete: {len(stage4_rows)} filter scenarios.")

    selected_rows_out = []
    selected_rows_out.extend(_top_rows(all_rows, key_name="pnl", top_n=args.top_n))
    selected_rows_out.extend(_top_rows(all_rows, key_name="efficiency", top_n=args.top_n))
    selected_keys = {
        (
            row["stage"],
            row["profile_label"],
            row["basket_threshold_pct"],
            row["vix_abs_threshold_pct"],
            row["iv_premium_min_pct"],
            row["filter_mode"],
        )
        for row in selected_rows_out
    }
    baseline_counter = Counter(
        (
            row["stage"],
            row["profile_label"],
            row["basket_threshold_pct"],
            row["vix_abs_threshold_pct"],
            row["iv_premium_min_pct"],
            row["filter_mode"],
        )
        for row in all_rows
        if row["profile_label"] == "current_method_side"
        and row["basket_threshold_pct"] == 70.0
        and row["vix_abs_threshold_pct"] == 20.0
        and row["iv_premium_min_pct"] == 10.0
        and row["filter_mode"] == "none"
    )
    for row in all_rows:
        key = (
            row["stage"],
            row["profile_label"],
            row["basket_threshold_pct"],
            row["vix_abs_threshold_pct"],
            row["iv_premium_min_pct"],
            row["filter_mode"],
        )
        if key in selected_keys:
            continue
        if baseline_counter[key] > 0:
            baseline = dict(row)
            baseline["selection"] = "baseline_current"
            selected_rows_out.append(baseline)

    _write_csv(args.output_csv, all_rows)
    _write_csv(args.output_best_csv, selected_rows_out)
    print(f"Wrote {args.output_csv}")
    print(f"Wrote {args.output_best_csv}")
    print("Top PnL:")
    for row in _top_rows(all_rows, key_name="pnl", top_n=5):
        print(row)
    print("Top efficiency:")
    for row in _top_rows(all_rows, key_name="efficiency", top_n=5):
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
