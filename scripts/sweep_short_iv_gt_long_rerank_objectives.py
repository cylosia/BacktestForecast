from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as cond

LOGS = ROOT / "logs"

DEFAULT_INPUT_CSV = (
    LOGS
    / "short_iv_gt_long_conditional_management_lookback52warmup_to_2026-04-10_earnings_excl_vix20abs_ivpremium10_selected_trades.csv"
)
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_rerank_objective_sweep_ivpremium10.csv"
DEFAULT_OUTPUT_WEEKLY_CSV = LOGS / "short_iv_gt_long_rerank_objective_sweep_ivpremium10_weeks.csv"


@dataclass(frozen=True)
class HistoryTrade:
    entry_date: date
    roi_pct: float
    pnl: float
    entry_debit: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Re-rank the short-IV>long-IV Friday portfolio under alternate symbol-history "
            "objectives using an existing detail CSV, then reapply the same final pnl/debit gate."
        )
    )
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-summary-csv", type=Path, default=DEFAULT_OUTPUT_SUMMARY_CSV)
    parser.add_argument("--output-weekly-csv", type=Path, default=DEFAULT_OUTPUT_WEEKLY_CSV)
    parser.add_argument("--top-k", type=int, default=43)
    parser.add_argument("--min-history-trades", type=int, default=3)
    parser.add_argument("--lookback-days", type=int, default=364)
    parser.add_argument("--selected-method-cap", type=int, default=cond.DEFAULT_TOP43_METHOD_CAP)
    parser.add_argument(
        "--evaluation-weeks",
        type=int,
        default=52,
        help="Number of trailing weekly entry dates to include in the reported stats.",
    )
    parser.add_argument(
        "--evaluation-end-date",
        type=date.fromisoformat,
        default=None,
        help="Optional end date for the reported weekly window, e.g. 2026-04-10.",
    )
    parser.add_argument(
        "--source-policy-label",
        default=None,
        help="Optional source policy label to rerank from. Defaults to auto-detect from the input file.",
    )
    parser.add_argument(
        "--baseline-policy-label",
        default=None,
        help="Optional final baseline policy label. Defaults to auto-detect from the input file.",
    )
    parser.add_argument(
        "--final-min-history-trades",
        type=int,
        default=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES,
    )
    parser.add_argument(
        "--final-min-pnl-over-debit-pct",
        type=float,
        default=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT,
    )
    return parser


def _load_rows(path: Path) -> list[dict[str, object]]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _detect_baseline_policy_label(rows: list[dict[str, object]]) -> str:
    candidates = sorted(
        {
            str(row["policy_label"])
            for row in rows
            if "__pnl_over_debit_15_min5" in str(row["policy_label"])
            and "__mlgbp72_abstain_" not in str(row["policy_label"])
        },
        key=len,
    )
    if not candidates:
        raise RuntimeError("Could not auto-detect baseline pnl_over_debit_15_min5 policy label.")
    return candidates[0]


def _infer_source_policy_label_from_baseline(baseline_policy_label: str) -> str:
    suffix = "__top43_52w_symbol_median_roi_min3__method_cap12__pnl_over_debit_15_min5"
    if suffix not in baseline_policy_label:
        raise RuntimeError(
            "Could not infer source policy label from baseline label: "
            f"{baseline_policy_label}"
        )
    return baseline_policy_label.removesuffix(suffix)


def _positive_history_trade(row: dict[str, object]) -> HistoryTrade | None:
    entry_debit = cond._to_float(str(row.get("entry_debit")))
    roi_pct = cond._to_float(str(row.get("roi_pct")))
    pnl = cond._to_float(str(row.get("pnl")))
    if entry_debit is None or entry_debit <= 0 or roi_pct is None or pnl is None:
        return None
    return HistoryTrade(
        entry_date=date.fromisoformat(str(row["entry_date"])),
        roi_pct=roi_pct,
        pnl=pnl,
        entry_debit=entry_debit,
    )


def _score_median_roi(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return None if not values else float(median(values))


def _score_pnl_over_debit_times_win_rate(history: list[HistoryTrade]) -> float | None:
    if not history:
        return None
    total_debit = sum(item.entry_debit for item in history)
    if total_debit <= 0:
        return None
    total_pnl = sum(item.pnl for item in history)
    win_rate = sum(1 for item in history if item.pnl > 0) / float(len(history))
    weighted_return_pct = total_pnl / total_debit * 100.0
    return weighted_return_pct * win_rate


def _score_median_minus_negative_p25(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return cond._score_history_by_median_roi_minus_negative_p25(values)


def _score_median_minus_drawdown(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return cond._score_history_by_median_roi_minus_drawdown_pct(values)


def _score_median_minus_cvar10(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return cond._score_history_by_median_roi_minus_cvar10_loss(values)


def _score_profit_factor_guarded(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return cond._score_history_by_profit_factor_guarded(values)


def _score_sortino_guarded(history: list[HistoryTrade]) -> float | None:
    values = [item.roi_pct for item in history]
    return cond._score_history_by_sortino_guarded(values)


def _derive_symbol_ranked_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int,
    lookback_days: int,
    selected_method_cap: int | None,
    score_fn,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    weekly_rows_by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in source_rows:
        weekly_rows_by_date[str(row["entry_date"])].append(row)
    history_by_symbol: dict[str, deque[HistoryTrade]] = defaultdict(deque)
    derived_rows: list[dict[str, object]] = []
    for entry_date_text in sorted(weekly_rows_by_date):
        entry_date = date.fromisoformat(entry_date_text)
        cutoff_date = entry_date - timedelta(days=lookback_days)
        ranked_candidates: list[tuple[dict[str, object], float | None, int]] = []
        for row in weekly_rows_by_date[entry_date_text]:
            symbol = str(row["symbol"])
            history = history_by_symbol[symbol]
            while history and history[0].entry_date < cutoff_date:
                history.popleft()
            history_list = list(history)
            score = None
            if len(history_list) >= min_history_trades:
                score = score_fn(history_list)
            ranked_candidates.append((row, score, len(history_list)))
        ranked_candidates.sort(
            key=lambda item: (
                1 if item[1] is None else 0,
                0.0 if item[1] is None else -float(item[1]),
                -item[2],
                str(item[0]["symbol"]),
                str(item[0]["prediction"]),
            )
        )
        selected_rows: list[dict[str, object]] = []
        selected_count_by_method: dict[str, int] = defaultdict(int)
        for row, _, _ in ranked_candidates:
            selected_method = str(row.get("selected_method"))
            if (
                selected_method_cap is not None
                and selected_count_by_method[selected_method] >= selected_method_cap
            ):
                continue
            selected_rows.append(row)
            selected_count_by_method[selected_method] += 1
            if len(selected_rows) >= top_k:
                break
        for row in selected_rows:
            candidate = dict(row)
            candidate["policy_label"] = derived_policy_label
            derived_rows.append(candidate)
        for row in weekly_rows_by_date[entry_date_text]:
            history_item = _positive_history_trade(row)
            if history_item is not None:
                history_by_symbol[str(row["symbol"])].append(history_item)
    return derived_rows


def _filter_rows_to_evaluation_window(
    rows: list[dict[str, object]],
    *,
    policy_label: str,
    evaluation_weeks: int,
    evaluation_end_date: date | None,
) -> list[dict[str, object]]:
    selected_rows = [row for row in rows if str(row["policy_label"]) == policy_label]
    if not selected_rows or evaluation_weeks <= 0:
        return []
    latest_entry_date = (
        evaluation_end_date
        if evaluation_end_date is not None
        else max(date.fromisoformat(str(row["entry_date"])) for row in rows)
    )
    window_start = latest_entry_date - timedelta(days=7 * (evaluation_weeks - 1))
    return [
        row
        for row in selected_rows
        if date.fromisoformat(str(row["entry_date"])) >= window_start
    ]


def _weekly_rows_for_policy(
    rows: list[dict[str, object]],
    policy_label: str,
    *,
    evaluation_weeks: int,
    evaluation_end_date: date | None,
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in _filter_rows_to_evaluation_window(
        rows,
        policy_label=policy_label,
        evaluation_weeks=evaluation_weeks,
        evaluation_end_date=evaluation_end_date,
    ):
        grouped[str(row["entry_date"])].append(row)
    weekly_rows: list[dict[str, object]] = []
    for entry_date in sorted(grouped):
        summary = cond._summarize_rows(grouped[entry_date])
        weekly_rows.append(
            {
                "entry_date": entry_date,
                "policy_label": policy_label,
                **summary,
            }
        )
    return weekly_rows


def _overall_summary(
    rows: list[dict[str, object]],
    policy_label: str,
    *,
    evaluation_weeks: int,
    evaluation_end_date: date | None,
) -> dict[str, object]:
    selected_rows = _filter_rows_to_evaluation_window(
        rows,
        policy_label=policy_label,
        evaluation_weeks=evaluation_weeks,
        evaluation_end_date=evaluation_end_date,
    )
    summary = cond._summarize_rows(selected_rows)
    weekly_rows = _weekly_rows_for_policy(
        rows,
        policy_label,
        evaluation_weeks=evaluation_weeks,
        evaluation_end_date=evaluation_end_date,
    )
    negative_weeks = sum(
        1
        for row in weekly_rows
        if row["weighted_return_positive_debit_pct"] is not None
        and float(row["weighted_return_positive_debit_pct"]) < 0
    )
    active_weeks = len(weekly_rows)
    win_count = sum(1 for row in selected_rows if cond._to_float(str(row.get("pnl"))) is not None and float(row["pnl"]) > 0)
    loss_count = sum(1 for row in selected_rows if cond._to_float(str(row.get("pnl"))) is not None and float(row["pnl"]) < 0)
    flat_count = sum(1 for row in selected_rows if cond._to_float(str(row.get("pnl"))) is not None and abs(float(row["pnl"])) <= 1e-9)
    summary.update(
        {
            "policy_label": policy_label,
            "active_weeks": active_weeks,
            "negative_weeks": negative_weeks,
            "wins": win_count,
            "losses": loss_count,
            "flat": flat_count,
            "avg_trades_per_active_week": (
                None if active_weeks <= 0 else round(len(selected_rows) / float(active_weeks), 6)
            ),
        }
    )
    return summary


def main() -> int:
    args = build_parser().parse_args()
    rows = _load_rows(args.input_csv)
    baseline_policy_label = args.baseline_policy_label or _detect_baseline_policy_label(rows)
    source_policy_label = args.source_policy_label or _infer_source_policy_label_from_baseline(
        baseline_policy_label
    )

    objective_specs = [
        ("baseline_current", baseline_policy_label, None),
        ("median_roi_reranked", "__rerank_median_roi", _score_median_roi),
        ("ev_pnl_over_debit_x_win_rate", "__rerank_ev_pnl_over_debit_x_win_rate", _score_pnl_over_debit_times_win_rate),
        ("downside_adjusted_negative_p25", "__rerank_downside_adjusted_negative_p25", _score_median_minus_negative_p25),
        ("downside_adjusted_drawdown", "__rerank_downside_adjusted_drawdown", _score_median_minus_drawdown),
        ("downside_adjusted_cvar10", "__rerank_downside_adjusted_cvar10", _score_median_minus_cvar10),
        ("profit_factor_guarded", "__rerank_profit_factor_guarded", _score_profit_factor_guarded),
        ("sortino_guarded", "__rerank_sortino_guarded", _score_sortino_guarded),
    ]

    output_rows = list(rows)
    summary_rows: list[dict[str, object]] = []
    weekly_rows: list[dict[str, object]] = []

    for objective_name, label_or_suffix, score_fn in objective_specs:
        if score_fn is None:
            final_policy_label = label_or_suffix
        else:
            ranked_policy_label = (
                f"{source_policy_label}{label_or_suffix}"
                f"__top{args.top_k}_min{args.min_history_trades}__method_cap{args.selected_method_cap}"
            )
            output_rows.extend(
                _derive_symbol_ranked_rows(
                    rows=output_rows,
                    source_policy_label=source_policy_label,
                    derived_policy_label=ranked_policy_label,
                    top_k=args.top_k,
                    min_history_trades=args.min_history_trades,
                    lookback_days=args.lookback_days,
                    selected_method_cap=args.selected_method_cap,
                    score_fn=score_fn,
                )
            )
            final_policy_label = (
                f"{ranked_policy_label}__pnl_over_debit_{int(args.final_min_pnl_over_debit_pct)}"
                f"_min{args.final_min_history_trades}"
            )
            output_rows.extend(
                cond._derive_symbol_lookback_pnl_over_debit_filtered_rows(
                    rows=output_rows,
                    source_policy_label=ranked_policy_label,
                    derived_policy_label=final_policy_label,
                    lookback_days=args.lookback_days,
                    min_history_trades=args.final_min_history_trades,
                    min_pnl_over_debit_pct=args.final_min_pnl_over_debit_pct,
                )
            )
        summary = _overall_summary(
            output_rows,
            final_policy_label,
            evaluation_weeks=args.evaluation_weeks,
            evaluation_end_date=args.evaluation_end_date,
        )
        summary["objective_name"] = objective_name
        summary_rows.append(summary)
        for weekly_row in _weekly_rows_for_policy(
            output_rows,
            final_policy_label,
            evaluation_weeks=args.evaluation_weeks,
            evaluation_end_date=args.evaluation_end_date,
        ):
            weekly_row["objective_name"] = objective_name
            weekly_rows.append(weekly_row)

    summary_rows.sort(
        key=lambda row: (
            0 if row["weighted_return_positive_debit_pct"] is not None else 1,
            0.0 if row["weighted_return_positive_debit_pct"] is None else -float(row["weighted_return_positive_debit_pct"]),
            -int(row["active_weeks"]),
            str(row["objective_name"]),
        )
    )
    args.output_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_summary_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)
    with args.output_weekly_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(weekly_rows[0].keys()))
        writer.writeheader()
        writer.writerows(weekly_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
