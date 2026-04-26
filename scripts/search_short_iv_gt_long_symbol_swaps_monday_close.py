from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks_monday_close as cond


LOGS = ROOT / "logs"
DEFAULT_BASELINE_SYMBOLS_FILE = ROOT / "weekly-options-over5-median80-mintrades70.txt"
DEFAULT_BASELINE_LEDGERS = (
    LOGS
    / "short_iv_gt_long_conditional_management_monday_close_lookback52warmup_to_2026-04-10_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part2_52weeks_vix20abs_selected_trades.csv",
)
DEFAULT_CHALLENGER_SYMBOL_FILES = (
    ROOT / "weekly-options-over5-minus-median80-mintrades70-part2.txt",
)
DEFAULT_CHALLENGER_LEDGERS = (
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part2_52weeks_vix20abs_selected_trades.csv",
)
DEFAULT_OUTPUT_CSV = LOGS / "short_iv_gt_long_swap_search_monday_close_orig112_vs_part2.csv"
DEFAULT_WINDOW_START = "2025-04-11"
DEFAULT_WINDOW_END = "2026-04-10"


@dataclass(frozen=True)
class PortfolioMetrics:
    trade_count: int
    roi_trade_count: int
    median_roi_pct: float | None
    total_pnl: float
    total_debit: float
    pnl_over_debit_pct: float | None
    negative_weeks: int
    max_drawdown_dollars: float
    max_drawdown_pct: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Search one-for-one symbol swaps for the promoted short-IV portfolio using "
            "existing conditional-management ledgers."
        )
    )
    parser.add_argument("--baseline-symbols-file", type=Path, default=DEFAULT_BASELINE_SYMBOLS_FILE)
    parser.add_argument(
        "--baseline-ledgers",
        type=Path,
        nargs="+",
        default=list(DEFAULT_BASELINE_LEDGERS),
        help="One or more baseline conditional-management ledgers aligned with the current baseline universe.",
    )
    parser.add_argument(
        "--challenger-symbol-files",
        type=Path,
        nargs="+",
        default=list(DEFAULT_CHALLENGER_SYMBOL_FILES),
        help="One or more challenger universe symbol files.",
    )
    parser.add_argument(
        "--challenger-ledgers",
        type=Path,
        nargs="+",
        default=list(DEFAULT_CHALLENGER_LEDGERS),
        help="One or more challenger conditional-management ledgers aligned with --challenger-symbol-files.",
    )
    parser.add_argument("--window-start", default=DEFAULT_WINDOW_START)
    parser.add_argument("--window-end", default=DEFAULT_WINDOW_END)
    parser.add_argument("--incumbent-candidate-count", type=int, default=15)
    parser.add_argument("--challenger-min-trades", type=int, default=10)
    parser.add_argument("--challenger-min-pnl-over-debit-pct", type=float, default=25.0)
    parser.add_argument("--max-negative-weeks", type=int, default=1)
    parser.add_argument("--max-drawdown-pct", type=float, default=1.0)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    return parser


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_symbols(path: Path) -> set[str]:
    return {
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _source_policy_rows(
    rows: list[dict[str, str]],
    *,
    allowed_symbols: set[str],
) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in rows
        if row["policy_label"] == cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
        and row["symbol"].strip().upper() in allowed_symbols
    ]


def _derive_promoted_rows(source_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    skip_rows = cond._derive_skip_filtered_policy_rows(
        rows=source_rows,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
        skip_trade_predicates=(cond._is_worst_method_trade,),
    )
    top_rows = cond._derive_symbol_median_roi_topk_rows(
        rows=skip_rows,
        source_policy_label=cond.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
        top_k=43,
        min_history_trades=3,
        selected_method_cap=cond.DEFAULT_TOP43_METHOD_CAP,
    )
    return cond._derive_symbol_lookback_pnl_over_debit_filtered_rows(
        rows=top_rows,
        source_policy_label=cond.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
        min_history_trades=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES,
        min_pnl_over_debit_pct=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT,
    )


def _portfolio_metrics(
    rows: list[dict[str, object]],
    *,
    window_start: str,
    window_end: str,
) -> PortfolioMetrics:
    window_rows = [
        row
        for row in rows
        if window_start <= str(row["entry_date"]) <= window_end
    ]
    positive_rows = [row for row in window_rows if float(row["entry_debit"]) > 0.0]
    roi_values = [
        float(row["roi_pct"])
        for row in window_rows
        if _to_float(row.get("roi_pct")) is not None
    ]
    weekly = defaultdict(lambda: {"pnl": 0.0})
    for row in window_rows:
        weekly[str(row["entry_date"])]["pnl"] += float(row["pnl"])

    cumulative = 0.0
    peak = 0.0
    max_drawdown_dollars = 0.0
    max_drawdown_pct = 0.0
    negative_weeks = 0
    for entry_date in sorted(weekly):
        cumulative += weekly[entry_date]["pnl"]
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown_dollars:
            max_drawdown_dollars = drawdown
        if peak > 0.0:
            drawdown_pct = (drawdown / peak) * 100.0
            if drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = drawdown_pct
        if weekly[entry_date]["pnl"] < 0.0:
            negative_weeks += 1

    total_pnl = sum(float(row["pnl"]) for row in window_rows)
    total_debit = sum(float(row["entry_debit"]) for row in positive_rows)
    pnl_over_debit_pct = None if total_debit <= 0.0 else (sum(float(row["pnl"]) for row in positive_rows) / total_debit) * 100.0
    return PortfolioMetrics(
        trade_count=len(window_rows),
        roi_trade_count=len(roi_values),
        median_roi_pct=None if not roi_values else median(roi_values),
        total_pnl=total_pnl,
        total_debit=total_debit,
        pnl_over_debit_pct=pnl_over_debit_pct,
        negative_weeks=negative_weeks,
        max_drawdown_dollars=max_drawdown_dollars,
        max_drawdown_pct=max_drawdown_pct,
    )


def _symbol_window_stats(
    rows: list[dict[str, object]],
    *,
    window_start: str,
    window_end: str,
) -> list[dict[str, object]]:
    bucket: dict[str, dict[str, object]] = {}
    for row in rows:
        entry_date = str(row["entry_date"])
        if not (window_start <= entry_date <= window_end):
            continue
        symbol = str(row["symbol"]).strip().upper()
        item = bucket.setdefault(
            symbol,
            {
                "symbol": symbol,
                "trades": 0,
                "roi_values": [],
                "total_pnl": 0.0,
                "total_debit": 0.0,
            },
        )
        item["trades"] = int(item["trades"]) + 1
        item["total_pnl"] = float(item["total_pnl"]) + float(row["pnl"])
        entry_debit = float(row["entry_debit"])
        if entry_debit > 0.0:
            item["total_debit"] = float(item["total_debit"]) + entry_debit
            roi_pct = _to_float(row.get("roi_pct"))
            if roi_pct is not None:
                item["roi_values"].append(roi_pct)
    summary: list[dict[str, object]] = []
    for item in bucket.values():
        total_debit = float(item["total_debit"])
        total_pnl = float(item["total_pnl"])
        roi_values = list(item["roi_values"])
        summary.append(
            {
                "symbol": item["symbol"],
                "trades": int(item["trades"]),
                "total_pnl": total_pnl,
                "total_debit": total_debit,
                "pnl_over_debit_pct": None if total_debit <= 0.0 else (total_pnl / total_debit) * 100.0,
                "median_roi_pct": None if not roi_values else median(roi_values),
            }
        )
    return summary


def _rows_by_symbol(source_rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in source_rows:
        grouped[str(row["symbol"]).strip().upper()].append(row)
    return grouped


def _derive_for_universe(
    universe_symbols: set[str],
    *,
    source_rows_by_symbol: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    combined_rows: list[dict[str, object]] = []
    for symbol in sorted(universe_symbols):
        combined_rows.extend(source_rows_by_symbol.get(symbol, ()))
    return _derive_promoted_rows(combined_rows)


def _score_tuple(metrics: PortfolioMetrics) -> tuple[float, float, float]:
    return (
        float("-inf") if metrics.pnl_over_debit_pct is None else metrics.pnl_over_debit_pct,
        float("-inf") if metrics.median_roi_pct is None else metrics.median_roi_pct,
        metrics.total_pnl,
    )


def main() -> int:
    args = build_parser().parse_args()
    if len(args.challenger_symbol_files) != len(args.challenger_ledgers):
        raise SystemExit("--challenger-symbol-files and --challenger-ledgers must have the same length.")

    baseline_symbols = _read_symbols(args.baseline_symbols_file)
    baseline_all_rows: list[dict[str, str]] = []
    for ledger_path in args.baseline_ledgers:
        baseline_all_rows.extend(_read_rows(ledger_path))
    baseline_source_rows = _source_policy_rows(baseline_all_rows, allowed_symbols=baseline_symbols)
    source_rows_by_symbol = _rows_by_symbol(baseline_source_rows)

    challenger_symbol_to_source: dict[str, str] = {}
    for symbols_file, ledger_path in zip(args.challenger_symbol_files, args.challenger_ledgers):
        challenger_symbols = _read_symbols(symbols_file)
        challenger_rows = _read_rows(ledger_path)
        challenger_source_rows = _source_policy_rows(challenger_rows, allowed_symbols=challenger_symbols)
        challenger_label = symbols_file.stem
        for row in challenger_source_rows:
            symbol = str(row["symbol"]).strip().upper()
            source_rows_by_symbol.setdefault(symbol, []).append(row)
            challenger_symbol_to_source[symbol] = challenger_label

    baseline_promoted_rows = _derive_for_universe(
        baseline_symbols,
        source_rows_by_symbol=source_rows_by_symbol,
    )
    baseline_metrics = _portfolio_metrics(
        baseline_promoted_rows,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    baseline_symbol_stats = _symbol_window_stats(
        baseline_promoted_rows,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    incumbent_candidates = sorted(
        baseline_symbol_stats,
        key=lambda item: (
            float("inf") if item["pnl_over_debit_pct"] is None else float(item["pnl_over_debit_pct"]),
            float(item["total_pnl"]),
            -int(item["trades"]),
            str(item["symbol"]),
        ),
    )[: args.incumbent_candidate_count]

    challenger_symbols = set(challenger_symbol_to_source)
    challenger_promoted_rows = _derive_for_universe(
        challenger_symbols,
        source_rows_by_symbol=source_rows_by_symbol,
    )
    challenger_symbol_stats = _symbol_window_stats(
        challenger_promoted_rows,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    challenger_candidates = [
        item
        for item in challenger_symbol_stats
        if int(item["trades"]) >= args.challenger_min_trades
        and item["pnl_over_debit_pct"] is not None
        and float(item["pnl_over_debit_pct"]) >= args.challenger_min_pnl_over_debit_pct
    ]
    challenger_candidates.sort(
        key=lambda item: (
            -float(item["pnl_over_debit_pct"]),
            -float("-inf" if item["median_roi_pct"] is None else item["median_roi_pct"]),
            -float(item["total_pnl"]),
            str(item["symbol"]),
        )
    )

    candidate_rows: list[dict[str, object]] = []
    for incumbent in incumbent_candidates:
        incumbent_symbol = str(incumbent["symbol"])
        for challenger in challenger_candidates:
            challenger_symbol = str(challenger["symbol"])
            if challenger_symbol in baseline_symbols:
                continue
            swapped_universe = set(baseline_symbols)
            swapped_universe.remove(incumbent_symbol)
            swapped_universe.add(challenger_symbol)
            promoted_rows = _derive_for_universe(
                swapped_universe,
                source_rows_by_symbol=source_rows_by_symbol,
            )
            metrics = _portfolio_metrics(
                promoted_rows,
                window_start=args.window_start,
                window_end=args.window_end,
            )
            feasible = (
                metrics.negative_weeks <= args.max_negative_weeks
                and metrics.max_drawdown_pct <= args.max_drawdown_pct
            )
            improves_pnl_debit = (
                metrics.pnl_over_debit_pct is not None
                and baseline_metrics.pnl_over_debit_pct is not None
                and metrics.pnl_over_debit_pct > baseline_metrics.pnl_over_debit_pct
            )
            improves_median_roi = (
                metrics.median_roi_pct is not None
                and baseline_metrics.median_roi_pct is not None
                and metrics.median_roi_pct > baseline_metrics.median_roi_pct
            )
            improves_total_pnl = metrics.total_pnl > baseline_metrics.total_pnl
            candidate_rows.append(
                {
                    "incumbent_symbol": incumbent_symbol,
                    "incumbent_trades": int(incumbent["trades"]),
                    "incumbent_total_pnl": round(float(incumbent["total_pnl"]), 6),
                    "incumbent_pnl_over_debit_pct": (
                        "" if incumbent["pnl_over_debit_pct"] is None else round(float(incumbent["pnl_over_debit_pct"]), 6)
                    ),
                    "challenger_symbol": challenger_symbol,
                    "challenger_source": challenger_symbol_to_source[challenger_symbol],
                    "challenger_trades": int(challenger["trades"]),
                    "challenger_total_pnl": round(float(challenger["total_pnl"]), 6),
                    "challenger_pnl_over_debit_pct": round(float(challenger["pnl_over_debit_pct"]), 6),
                    "feasible": int(feasible),
                    "improves_pnl_over_debit": int(improves_pnl_debit),
                    "improves_median_roi": int(improves_median_roi),
                    "improves_total_pnl": int(improves_total_pnl),
                    "trade_count": metrics.trade_count,
                    "median_roi_pct": "" if metrics.median_roi_pct is None else round(metrics.median_roi_pct, 6),
                    "total_pnl": round(metrics.total_pnl, 6),
                    "total_debit": round(metrics.total_debit, 6),
                    "pnl_over_debit_pct": "" if metrics.pnl_over_debit_pct is None else round(metrics.pnl_over_debit_pct, 6),
                    "negative_weeks": metrics.negative_weeks,
                    "max_drawdown_dollars": round(metrics.max_drawdown_dollars, 6),
                    "max_drawdown_pct": round(metrics.max_drawdown_pct, 6),
                    "delta_vs_baseline_median_roi_pct": (
                        ""
                        if metrics.median_roi_pct is None or baseline_metrics.median_roi_pct is None
                        else round(metrics.median_roi_pct - baseline_metrics.median_roi_pct, 6)
                    ),
                    "delta_vs_baseline_total_pnl": round(metrics.total_pnl - baseline_metrics.total_pnl, 6),
                    "delta_vs_baseline_pnl_over_debit_pct": (
                        ""
                        if metrics.pnl_over_debit_pct is None or baseline_metrics.pnl_over_debit_pct is None
                        else round(metrics.pnl_over_debit_pct - baseline_metrics.pnl_over_debit_pct, 6)
                    ),
                    "delta_vs_baseline_negative_weeks": metrics.negative_weeks - baseline_metrics.negative_weeks,
                    "delta_vs_baseline_max_drawdown_pct": round(metrics.max_drawdown_pct - baseline_metrics.max_drawdown_pct, 6),
                }
            )

    candidate_rows.sort(
        key=lambda item: (
            -int(item["feasible"]),
            -int(item["improves_pnl_over_debit"]) - int(item["improves_median_roi"]),
            -float("-inf" if item["pnl_over_debit_pct"] == "" else item["pnl_over_debit_pct"]),
            -float("-inf" if item["median_roi_pct"] == "" else item["median_roi_pct"]),
            -float(item["total_pnl"]),
            int(item["negative_weeks"]),
            float(item["max_drawdown_pct"]),
            str(item["incumbent_symbol"]),
            str(item["challenger_symbol"]),
        )
    )

    current_symbols = set(baseline_symbols)
    current_rows = baseline_promoted_rows
    current_metrics = baseline_metrics
    greedy_swaps: list[tuple[str, str, PortfolioMetrics]] = []
    available_challenger_symbols = {str(item["symbol"]) for item in challenger_candidates}
    while True:
        current_symbol_stats = _symbol_window_stats(
            current_rows,
            window_start=args.window_start,
            window_end=args.window_end,
        )
        current_incumbents = sorted(
            current_symbol_stats,
            key=lambda item: (
                float("inf") if item["pnl_over_debit_pct"] is None else float(item["pnl_over_debit_pct"]),
                float(item["total_pnl"]),
                -int(item["trades"]),
                str(item["symbol"]),
            ),
        )[: args.incumbent_candidate_count]
        best_swap: tuple[str, str, PortfolioMetrics, list[dict[str, object]]] | None = None
        for incumbent in current_incumbents:
            incumbent_symbol = str(incumbent["symbol"])
            for challenger in challenger_candidates:
                challenger_symbol = str(challenger["symbol"])
                if challenger_symbol in current_symbols or challenger_symbol not in available_challenger_symbols:
                    continue
                swapped_universe = set(current_symbols)
                swapped_universe.remove(incumbent_symbol)
                swapped_universe.add(challenger_symbol)
                promoted_rows = _derive_for_universe(
                    swapped_universe,
                    source_rows_by_symbol=source_rows_by_symbol,
                )
                metrics = _portfolio_metrics(
                    promoted_rows,
                    window_start=args.window_start,
                    window_end=args.window_end,
                )
                if metrics.negative_weeks > args.max_negative_weeks or metrics.max_drawdown_pct > args.max_drawdown_pct:
                    continue
                if metrics.pnl_over_debit_pct is None or current_metrics.pnl_over_debit_pct is None:
                    continue
                if metrics.pnl_over_debit_pct <= current_metrics.pnl_over_debit_pct:
                    continue
                if metrics.median_roi_pct is None or current_metrics.median_roi_pct is None:
                    continue
                if metrics.median_roi_pct <= current_metrics.median_roi_pct:
                    continue
                if best_swap is None or _score_tuple(metrics) > _score_tuple(best_swap[2]):
                    best_swap = (incumbent_symbol, challenger_symbol, metrics, promoted_rows)
        if best_swap is None:
            break
        incumbent_symbol, challenger_symbol, metrics, promoted_rows = best_swap
        current_symbols.remove(incumbent_symbol)
        current_symbols.add(challenger_symbol)
        current_rows = promoted_rows
        current_metrics = metrics
        available_challenger_symbols.remove(challenger_symbol)
        greedy_swaps.append((incumbent_symbol, challenger_symbol, metrics))

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(candidate_rows[0].keys()) if candidate_rows else []
    if fieldnames:
        with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(candidate_rows)

    print(f"baseline_trade_count={baseline_metrics.trade_count}")
    print(f"baseline_median_roi_pct={'' if baseline_metrics.median_roi_pct is None else round(baseline_metrics.median_roi_pct, 6)}")
    print(f"baseline_total_pnl={round(baseline_metrics.total_pnl, 6)}")
    print(f"baseline_total_debit={round(baseline_metrics.total_debit, 6)}")
    print(f"baseline_pnl_over_debit_pct={'' if baseline_metrics.pnl_over_debit_pct is None else round(baseline_metrics.pnl_over_debit_pct, 6)}")
    print(f"baseline_negative_weeks={baseline_metrics.negative_weeks}")
    print(f"baseline_max_drawdown_pct={round(baseline_metrics.max_drawdown_pct, 6)}")
    print(f"incumbent_candidates={','.join(str(item['symbol']) for item in incumbent_candidates)}")
    print(f"challenger_candidates={','.join(str(item['symbol']) for item in challenger_candidates)}")
    print(f"tested_swaps={len(candidate_rows)}")
    feasible_rows = [row for row in candidate_rows if int(row["feasible"]) == 1]
    print(f"feasible_swaps={len(feasible_rows)}")
    if feasible_rows:
        best = feasible_rows[0]
        print(
            "best_feasible="
            f"{best['incumbent_symbol']}->{best['challenger_symbol']}"
            f" pnl_over_debit={best['pnl_over_debit_pct']}"
            f" median_roi={best['median_roi_pct']}"
            f" total_pnl={best['total_pnl']}"
            f" neg_weeks={best['negative_weeks']}"
            f" max_dd_pct={best['max_drawdown_pct']}"
        )
    print(f"greedy_swap_count={len(greedy_swaps)}")
    for index, (incumbent_symbol, challenger_symbol, metrics) in enumerate(greedy_swaps, start=1):
        print(
            f"greedy_swap_{index}={incumbent_symbol}->{challenger_symbol}"
            f" pnl_over_debit={'' if metrics.pnl_over_debit_pct is None else round(metrics.pnl_over_debit_pct, 6)}"
            f" median_roi={'' if metrics.median_roi_pct is None else round(metrics.median_roi_pct, 6)}"
            f" total_pnl={round(metrics.total_pnl, 6)}"
            f" neg_weeks={metrics.negative_weeks}"
            f" max_dd_pct={round(metrics.max_drawdown_pct, 6)}"
        )
    print(f"output_csv={args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
