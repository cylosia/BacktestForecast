from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_calendar_take_profit_grid as tp_grid
import scripts.evaluate_short_iv_gt_long_conditional_management_2y as cond2y

LOGS = ROOT / "logs"

DEFAULT_OUTPUT_GRID_CSV = LOGS / "short_iv_gt_long_first_breach_management_2y_grid_summary.csv"
DEFAULT_OUTPUT_BEST_CSV = LOGS / "short_iv_gt_long_first_breach_management_2y_grid_best.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Grid test abstain-only first-breach management thresholds over the full 2-year "
            "short-IV-greater-than-long-IV weekly call calendar history."
        )
    )
    parser.add_argument("--best-delta-csv", type=Path, default=cond2y.DEFAULT_BEST_DELTA_CSV)
    parser.add_argument("--delta-trades-csv", type=Path, default=cond2y.DEFAULT_DELTA_TRADES_CSV)
    parser.add_argument("--output-grid-csv", type=Path, default=DEFAULT_OUTPUT_GRID_CSV)
    parser.add_argument("--output-best-csv", type=Path, default=DEFAULT_OUTPUT_BEST_CSV)
    parser.add_argument(
        "--max-spot-entry",
        type=float,
        default=None,
        help="Optional maximum allowed spot close on entry date. Example: 1000.",
    )
    parser.add_argument(
        "--entry-debit-thresholds",
        default="1.5,2.0,2.5,3.0,3.5,4.0",
        help="Comma-separated abstain entry-debit thresholds.",
    )
    parser.add_argument(
        "--short-iv-thresholds",
        default="100,110,120,130,140,150",
        help="Comma-separated abstain short-IV percentage thresholds.",
    )
    parser.add_argument(
        "--first-breach-take-profit-thresholds",
        default="0,10,25,50,75,999",
        help="Comma-separated ROI thresholds for exiting profitable first-breach trades. 999 disables TP.",
    )
    parser.add_argument(
        "--first-breach-stop-loss-thresholds",
        default="20,35,50,75,999",
        help="Comma-separated ROI thresholds for exiting losing first-breach trades. 999 disables SL.",
    )
    return parser


def _parse_thresholds(raw_value: str) -> tuple[float, ...]:
    values = tuple(float(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip())
    if not values:
        raise SystemExit("At least one threshold value is required.")
    return values


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
        "avg_roi_positive_debit_pct": None if not roi_values else round(mean(roi_values), 6),
        "median_roi_positive_debit_pct": None if not roi_values else round(median(roi_values), 6),
        "weighted_return_positive_debit_pct": (
            None if total_debit <= 0 else round(total_pnl / total_debit * 100.0, 6)
        ),
        "first_breach_exit_count": sum(1 for row in rows if row["exit_reason"] == "first_breach_trigger"),
    }


def _to_evaluable_row(row: dict[str, object], *, management_applied: bool) -> dict[str, object]:
    payload = dict(row)
    payload["management_applied"] = int(management_applied)
    return payload


def _simulate_first_breach_row(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[object, dict[object, list[tp_grid.delta_grid.OptionRow]]],
    spot_by_date: dict[object, float],
    path_dates: list[object],
) -> dict[str, object] | None:
    entry_debit = float(trade_row["entry_debit"])
    if entry_debit <= 0:
        return None
    short_expiration = cond2y.date.fromisoformat(trade_row["short_expiration"])
    long_expiration = cond2y.date.fromisoformat(trade_row["long_expiration"])
    short_strike = float(trade_row["short_strike"])
    long_strike = short_strike
    for mark_date in path_dates:
        spot_mark = spot_by_date.get(mark_date)
        if spot_mark is None or spot_mark <= short_strike:
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
        pnl = float(mark["spread_mark"]) - entry_debit
        roi_pct = (pnl / entry_debit) * 100.0
        return mgmt._build_output_row(
            trade_row=trade_row,
            policy_label="first_breach_candidate",
            exit_date=mark_date,
            exit_reason="first_breach_trigger",
            entry_debit=entry_debit,
            spread_mark=float(mark["spread_mark"]),
            pnl=pnl,
            roi_pct=roi_pct,
            spot_close_exit=spot_mark,
            short_strike=short_strike,
            long_strike=long_strike,
            short_mark_method=str(mark["short_mark_method"]),
            long_mark_method=str(mark["long_mark_method"]),
            roll_count=0,
            roll_date=None,
            roll_from_strike=None,
            roll_to_strike=None,
            roll_net_debit=None,
        )
    return None


def main() -> int:
    args = build_parser().parse_args()
    entry_debit_thresholds = _parse_thresholds(args.entry_debit_thresholds)
    short_iv_thresholds = _parse_thresholds(args.short_iv_thresholds)
    first_breach_tp_thresholds = _parse_thresholds(args.first_breach_take_profit_thresholds)
    first_breach_sl_thresholds = _parse_thresholds(args.first_breach_stop_loss_thresholds)

    selected_rows_raw = cond2y._load_selected_best_delta_rows(
        best_delta_csv=args.best_delta_csv,
        delta_trades_csv=args.delta_trades_csv,
    )
    selected_rows_raw = [row for row in selected_rows_raw if row["prediction"] in {"up", "abstain"}]
    if not selected_rows_raw:
        raise SystemExit("No selected best-delta rows were found.")

    selected_rows = [cond2y._compat_trade_row(row) for row in selected_rows_raw]
    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    trade_candidates: list[dict[str, object]] = []
    spot_cache: dict[tuple[str, str], float | None] = {}
    symbol_cache: dict[
        str,
        tuple[
            dict[object, float],
            dict[object, dict[object, list[tp_grid.delta_grid.OptionRow]]],
            dict[tuple[str, str, str], list[object]],
        ],
    ] = {}

    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    try:
        with factory() as session:
            total_symbols = len(trades_by_symbol)
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{total_symbols:03d}] {symbol}: loading path data")
                symbol_cache[symbol] = cond2y._load_symbol_cache(session, symbol=symbol, trades=symbol_trades)

            for trade_row in selected_rows:
                symbol = trade_row["symbol"].strip().upper()
                prediction = trade_row["prediction"]
                entry_date_text = trade_row["entry_date"]
                spot_key = (symbol, entry_date_text)
                if spot_key not in spot_cache:
                    spot_cache[spot_key] = mgmt._load_spot_close(
                        session,
                        symbol=symbol,
                        trade_date=cond2y.date.fromisoformat(entry_date_text),
                    )
                spot_close_entry = spot_cache[spot_key]
                if args.max_spot_entry is not None and (
                    spot_close_entry is None or spot_close_entry > args.max_spot_entry
                ):
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
                first_breach_row = _simulate_first_breach_row(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                short_iv_pct = cond2y._short_entry_iv_pct(trade_row=trade_row, option_rows_by_date=option_rows_by_date)
                trade_candidates.append(
                    {
                        "symbol": symbol,
                        "entry_date": trade_row["entry_date"],
                        "entry_year": trade_row["entry_date"][:4],
                        "prediction": prediction,
                        "original_entry_debit": float(trade_row["entry_debit"]),
                        "short_entry_iv_pct": short_iv_pct,
                        "hold": hold_row,
                        "first_breach": first_breach_row,
                    }
                )
    finally:
        engine.dispose()

    if not trade_candidates:
        raise SystemExit("No trade candidates were produced.")

    hold_abstain = [
        _to_evaluable_row(candidate["hold"], management_applied=False)
        for candidate in trade_candidates
        if candidate["prediction"] == "abstain"
    ]
    hold_combined = [
        _to_evaluable_row(candidate["hold"], management_applied=False)
        for candidate in trade_candidates
    ]
    hold_abstain_summary = _summarize_rows(hold_abstain)
    hold_combined_summary = _summarize_rows(hold_combined)

    grid_rows: list[dict[str, object]] = []
    for entry_debit_threshold in entry_debit_thresholds:
        for short_iv_threshold in short_iv_thresholds:
            for first_breach_tp in first_breach_tp_thresholds:
                for first_breach_sl in first_breach_sl_thresholds:
                    abstain_rows: list[dict[str, object]] = []
                    combined_rows: list[dict[str, object]] = []
                    yearly_abstain: dict[str, list[dict[str, object]]] = defaultdict(list)
                    for candidate in trade_candidates:
                        prediction = str(candidate["prediction"])
                        first_breach_row = candidate["first_breach"]
                        eligible = (
                            prediction == "abstain"
                            and float(candidate["original_entry_debit"]) > entry_debit_threshold
                            and candidate["short_entry_iv_pct"] is not None
                            and float(candidate["short_entry_iv_pct"]) > short_iv_threshold
                        )
                        should_exit = False
                        if eligible and first_breach_row is not None and first_breach_row["roi_pct"] is not None:
                            roi_pct = float(first_breach_row["roi_pct"])
                            tp_enabled = first_breach_tp < 900
                            sl_enabled = first_breach_sl < 900
                            should_exit = (tp_enabled and roi_pct >= first_breach_tp) or (
                                sl_enabled and roi_pct <= -first_breach_sl
                            )
                        selected_row = first_breach_row if should_exit and first_breach_row is not None else candidate["hold"]
                        payload = _to_evaluable_row(selected_row, management_applied=should_exit)
                        combined_rows.append(payload)
                        if prediction == "abstain":
                            abstain_rows.append(payload)
                            yearly_abstain[str(candidate["entry_year"])].append(payload)

                    abstain_summary = _summarize_rows(abstain_rows)
                    combined_summary = _summarize_rows(combined_rows)
                    policy_label = "first_breach_tp_sl"
                    common_fields = {
                        "policy_label": policy_label,
                        "abstain_min_entry_debit": entry_debit_threshold,
                        "abstain_min_short_iv_pct": short_iv_threshold,
                        "first_breach_take_profit_pct": first_breach_tp,
                        "first_breach_stop_loss_pct": first_breach_sl,
                        "max_spot_entry": args.max_spot_entry,
                    }
                    grid_rows.append(
                        {
                            "summary_scope": "all",
                            "entry_period": "ALL",
                            "portfolio_scope": "abstain_only",
                            **common_fields,
                            **abstain_summary,
                            "hold_weighted_return_positive_debit_pct": hold_abstain_summary["weighted_return_positive_debit_pct"],
                            "hold_total_pnl_positive": hold_abstain_summary["total_pnl_positive"],
                            "weighted_return_minus_hold_pct": round(
                                float(abstain_summary["weighted_return_positive_debit_pct"])
                                - float(hold_abstain_summary["weighted_return_positive_debit_pct"]),
                                6,
                            ),
                            "total_pnl_minus_hold": round(
                                float(abstain_summary["total_pnl_positive"]) - float(hold_abstain_summary["total_pnl_positive"]),
                                6,
                            ),
                        }
                    )
                    grid_rows.append(
                        {
                            "summary_scope": "all",
                            "entry_period": "ALL",
                            "portfolio_scope": "combined_up_hold",
                            **common_fields,
                            **combined_summary,
                            "hold_weighted_return_positive_debit_pct": hold_combined_summary["weighted_return_positive_debit_pct"],
                            "hold_total_pnl_positive": hold_combined_summary["total_pnl_positive"],
                            "weighted_return_minus_hold_pct": round(
                                float(combined_summary["weighted_return_positive_debit_pct"])
                                - float(hold_combined_summary["weighted_return_positive_debit_pct"]),
                                6,
                            ),
                            "total_pnl_minus_hold": round(
                                float(combined_summary["total_pnl_positive"]) - float(hold_combined_summary["total_pnl_positive"]),
                                6,
                            ),
                        }
                    )
                    for entry_year, yearly_rows in sorted(yearly_abstain.items()):
                        yearly_summary = _summarize_rows(yearly_rows)
                        hold_year_rows = [row for row in hold_abstain if str(row["entry_date"])[:4] == entry_year]
                        hold_year_summary = _summarize_rows(hold_year_rows)
                        grid_rows.append(
                            {
                                "summary_scope": "year",
                                "entry_period": entry_year,
                                "portfolio_scope": "abstain_only",
                                **common_fields,
                                **yearly_summary,
                                "hold_weighted_return_positive_debit_pct": hold_year_summary[
                                    "weighted_return_positive_debit_pct"
                                ],
                                "hold_total_pnl_positive": hold_year_summary["total_pnl_positive"],
                                "weighted_return_minus_hold_pct": round(
                                    float(yearly_summary["weighted_return_positive_debit_pct"])
                                    - float(hold_year_summary["weighted_return_positive_debit_pct"]),
                                    6,
                                ),
                                "total_pnl_minus_hold": round(
                                    float(yearly_summary["total_pnl_positive"]) - float(hold_year_summary["total_pnl_positive"]),
                                    6,
                                ),
                            }
                        )

    args.output_grid_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_grid_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(grid_rows[0].keys()))
        writer.writeheader()
        writer.writerows(grid_rows)

    best_rows: list[dict[str, object]] = []
    for portfolio_scope in ("abstain_only", "combined_up_hold"):
        subset = [
            row
            for row in grid_rows
            if row["summary_scope"] == "all" and row["portfolio_scope"] == portfolio_scope
        ]
        best_by_weight = max(
            subset,
            key=lambda row: (
                float(row["weighted_return_positive_debit_pct"]),
                float(row["total_pnl_positive"]),
            ),
        )
        best_rows.append(
            {
                "selection_metric": "weighted_return_positive_debit_pct",
                **best_by_weight,
            }
        )
        best_by_pnl = max(
            subset,
            key=lambda row: (
                float(row["total_pnl_positive"]),
                float(row["weighted_return_positive_debit_pct"]),
            ),
        )
        best_rows.append(
            {
                "selection_metric": "total_pnl_positive",
                **best_by_pnl,
            }
        )

    with args.output_best_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(best_rows[0].keys()))
        writer.writeheader()
        writer.writerows(best_rows)

    print(f"Wrote {args.output_grid_csv}")
    print(f"Wrote {args.output_best_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
