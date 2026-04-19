from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_calendar_take_profit_grid as tp_grid

LOGS = ROOT / "logs"

BEST_TP_CSV = LOGS / "short_iv_gt_long_calendar_take_profit_grid_2y_best_target_by_symbol.csv"
TP_TRADES_CSV = LOGS / "short_iv_gt_long_calendar_take_profit_grid_2y_trades.csv"
BEST_DELTA_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_selected_trades.csv"
OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_take_profit_3weeks_selected_trades.csv"
OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_best_delta_take_profit_3weeks_weekly_comparison.csv"

WEEK_SPECS = (
    {
        "entry_date": "2026-03-20",
        "exit_date": "2026-03-27",
        "baseline_csv": LOGS / "predictions_short_iv_gt_long_asof_2026-03-20_with_call_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_03_27",
    },
    {
        "entry_date": "2026-03-27",
        "exit_date": "2026-04-02",
        "baseline_csv": LOGS / "predictions_short_iv_gt_long_asof_2026-03-27_with_call_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_04_02",
    },
    {
        "entry_date": "2026-04-02",
        "exit_date": "2026-04-10",
        "baseline_csv": LOGS / "predictions_36_symbols_asof_2026-04-02_with_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_04_10",
    },
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare 3 short-IV-greater-than-long-IV weekly windows across ATM baseline, "
            "best delta, and best delta plus best take-profit, with an optional spot-entry filter."
        )
    )
    parser.add_argument(
        "--max-spot-entry",
        type=float,
        default=None,
        help="Optional maximum allowed spot close on entry date. Example: 1000.",
    )
    parser.add_argument("--output-trades-csv", type=Path, default=OUTPUT_TRADES_CSV)
    parser.add_argument("--output-summary-csv", type=Path, default=OUTPUT_SUMMARY_CSV)
    return parser


def _load_database_url() -> str:
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL is required.")


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.lower() == "none":
        return None
    return float(text)


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


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
        "total_debit_paid_positive": round(total_debit, 6),
        "total_pnl_positive": round(total_pnl, 6),
        "total_pnl_all_trades": round(total_pnl_all, 6),
        "avg_roi_positive_debit_pct": _round_or_none(mean(roi_values) if roi_values else None),
        "median_roi_positive_debit_pct": _round_or_none(median(roi_values) if roi_values else None),
        "weighted_return_positive_debit_pct": (
            None if total_debit <= 0 else round(total_pnl / total_debit * 100.0, 6)
        ),
    }


def _load_best_tp_by_symbol_prediction(best_tp_csv: Path) -> dict[tuple[str, str], tuple[str, int | None]]:
    mapping: dict[tuple[str, str], tuple[str, int | None]] = {}
    for row in csv.DictReader(best_tp_csv.open(encoding="utf-8")):
        symbol = row["symbol"].strip().upper()
        up_label = row["best_up_take_profit_label"].strip()
        up_pct = _to_float(row["best_up_take_profit_pct"])
        abstain_label = row["best_abstain_take_profit_label"].strip()
        abstain_pct = _to_float(row["best_abstain_take_profit_pct"])
        if up_label:
            mapping[(symbol, "up")] = (up_label, None if up_pct is None else int(up_pct))
        if abstain_label:
            mapping[(symbol, "abstain")] = (abstain_label, None if abstain_pct is None else int(abstain_pct))
    return mapping


def _build_tp_trade_index(tp_trades_csv: Path) -> dict[tuple[str, str, str, str], dict[str, str]]:
    index: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in csv.DictReader(tp_trades_csv.open(encoding="utf-8")):
        key = (
            row["entry_date"],
            row["symbol"].strip().upper(),
            row["prediction"].strip(),
            row["take_profit_label"].strip(),
        )
        index.setdefault(key, row)
    return index


def _build_compatible_trade_row(best_delta_row: dict[str, str]) -> dict[str, str]:
    short_strike = best_delta_row["short_strike"]
    return {
        "entry_date": best_delta_row["entry_date"],
        "symbol": best_delta_row["symbol"],
        "prediction": best_delta_row["prediction"],
        "selected_method": best_delta_row["selected_method"],
        "prediction_engine": best_delta_row["prediction_engine"],
        "confidence_pct": best_delta_row["confidence_pct"],
        "delta_target_pct": best_delta_row["best_delta_target_pct"],
        "spot_close_entry": best_delta_row["spot_close_entry"],
        "short_expiration": best_delta_row["short_expiration"],
        "long_expiration": best_delta_row["long_expiration"],
        "short_strike": short_strike,
        "long_strike": short_strike,
        "entry_debit": best_delta_row["entry_debit"],
        "spread_mark": best_delta_row["spread_mark"],
        "pnl": best_delta_row["pnl"],
        "roi_pct": best_delta_row["roi_pct"],
        "short_mark_method": best_delta_row["short_mark_method"],
        "long_mark_method": best_delta_row["long_mark_method"],
    }


def _load_spot_close(
    session: Session,
    *,
    symbol: str,
    trade_date: date,
) -> float | None:
    return tp_grid._load_underlying_closes(
        session,
        symbol=symbol,
        start_date=trade_date,
        end_date=trade_date,
    ).get(trade_date)


def _load_symbol_path_cache(
    session: Session,
    *,
    symbol: str,
    trades: list[dict[str, str]],
) -> tuple[dict[date, float], dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]], dict[tuple[str, str, str], list[date]]]:
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
    needed_trade_dates: set[date] = set()
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


def main() -> int:
    args = build_parser().parse_args()
    best_tp_by_symbol_prediction = _load_best_tp_by_symbol_prediction(BEST_TP_CSV)
    tp_trade_index = _build_tp_trade_index(TP_TRADES_CSV)
    best_delta_selected_rows = list(csv.DictReader(BEST_DELTA_SELECTED_TRADES_CSV.open(encoding="utf-8")))
    best_delta_selected_index = {
        (
            row["entry_date"],
            row["symbol"].strip().upper(),
            row["prediction"].strip(),
        ): row
        for row in best_delta_selected_rows
    }

    selected_tp_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    engine = create_engine(_load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    symbol_cache: dict[
        str,
        tuple[dict[date, float], dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]], dict[tuple[str, str, str], list[date]]],
    ] = {}
    spot_cache: dict[tuple[str, str], float | None] = {}
    try:
        with factory() as session:
            for week in WEEK_SPECS:
                baseline_rows_raw = list(csv.DictReader(week["baseline_csv"].open(encoding="utf-8")))
                filtered_out_by_prediction: dict[str, list[str]] = {"up": [], "abstain": []}
                baseline_by_prediction = {
                    prediction: [
                        {
                            "symbol": row["symbol"].strip().upper(),
                            "entry_date": week["entry_date"],
                            "exit_date": week["exit_date"],
                            "prediction": prediction,
                            "selected_method": row.get("selected_method", "").strip(),
                            "confidence_pct": _to_float(row.get("confidence_pct")),
                            "entry_debit": float(row["entry_debit"]),
                            "spread_mark": float(row[week["baseline_spread_col"]]),
                            "pnl": float(row["pnl"]),
                            "roi_pct": _to_float(row["roi_pct"]),
                            "source": "atm_baseline",
                            "spot_close_entry": None,
                        }
                        for row in baseline_rows_raw
                        if row["prediction"].strip() == prediction
                    ]
                    for prediction in ("up", "abstain")
                }
                if args.max_spot_entry is not None:
                    for prediction in ("up", "abstain"):
                        filtered_rows: list[dict[str, object]] = []
                        for baseline_row in baseline_by_prediction[prediction]:
                            symbol = str(baseline_row["symbol"])
                            spot_key = (symbol, week["entry_date"])
                            if spot_key not in spot_cache:
                                spot_cache[spot_key] = _load_spot_close(
                                    session,
                                    symbol=symbol,
                                    trade_date=date.fromisoformat(week["entry_date"]),
                                )
                            spot_close_entry = spot_cache[spot_key]
                            baseline_row["spot_close_entry"] = spot_close_entry
                            if spot_close_entry is None or spot_close_entry > args.max_spot_entry:
                                filtered_out_by_prediction[prediction].append(symbol)
                                continue
                            filtered_rows.append(baseline_row)
                        baseline_by_prediction[prediction] = filtered_rows

                best_delta_by_prediction: dict[str, list[dict[str, object]]] = {"up": [], "abstain": []}
                best_tp_by_prediction_rows: dict[str, list[dict[str, object]]] = {"up": [], "abstain": []}
                missing_best_tp: list[str] = []
                missing_tp_price_rows: list[str] = []

                for prediction in ("up", "abstain"):
                    for baseline_row in baseline_by_prediction[prediction]:
                        key = (
                            week["entry_date"],
                            str(baseline_row["symbol"]),
                            prediction,
                        )
                        best_delta_row = best_delta_selected_index.get(key)
                        if best_delta_row is None:
                            continue
                        best_delta_spot_close_entry = _to_float(best_delta_row.get("spot_close_entry"))
                        if args.max_spot_entry is not None and (
                            best_delta_spot_close_entry is None or best_delta_spot_close_entry > args.max_spot_entry
                        ):
                            continue
                        best_delta_by_prediction[prediction].append(
                            {
                                "symbol": best_delta_row["symbol"].strip().upper(),
                                "entry_date": best_delta_row["entry_date"],
                                "exit_date": best_delta_row["exit_date"],
                                "prediction": prediction,
                                "selected_method": best_delta_row["selected_method"],
                                "confidence_pct": _to_float(best_delta_row["confidence_pct"]),
                                "entry_debit": float(best_delta_row["entry_debit"]),
                                "spread_mark": float(best_delta_row["spread_mark"]),
                                "pnl": float(best_delta_row["pnl"]),
                                "roi_pct": _to_float(best_delta_row["roi_pct"]),
                                "source": best_delta_row["source"],
                                "spot_close_entry": best_delta_spot_close_entry,
                            }
                        )

                        best_tp_info = best_tp_by_symbol_prediction.get((str(baseline_row["symbol"]), prediction))
                        if best_tp_info is None:
                            if str(baseline_row["symbol"]) not in missing_best_tp:
                                missing_best_tp.append(str(baseline_row["symbol"]))
                            continue
                        take_profit_label, take_profit_pct = best_tp_info
                        if take_profit_label == "hold_to_expiry":
                            selected_tp_rows.append(
                                {
                                    "symbol": best_delta_row["symbol"].strip().upper(),
                                    "entry_date": best_delta_row["entry_date"],
                                    "exit_date": best_delta_row["exit_date"],
                                    "prediction": prediction,
                                    "selected_method": best_delta_row["selected_method"],
                                    "prediction_engine": best_delta_row["prediction_engine"],
                                    "confidence_pct": _to_float(best_delta_row["confidence_pct"]),
                                    "best_delta_target_pct": int(best_delta_row["best_delta_target_pct"]),
                                    "best_take_profit_label": take_profit_label,
                                    "best_take_profit_pct": "",
                                    "spot_close_entry": best_delta_spot_close_entry,
                                    "entry_debit": float(best_delta_row["entry_debit"]),
                                    "spread_mark": float(best_delta_row["spread_mark"]),
                                    "pnl": float(best_delta_row["pnl"]),
                                    "roi_pct": _to_float(best_delta_row["roi_pct"]),
                                    "exit_reason": "expiration",
                                    "source": "best_delta_hold_to_expiry",
                                }
                            )
                            best_tp_by_prediction_rows[prediction].append(
                                {
                                    "symbol": best_delta_row["symbol"].strip().upper(),
                                    "entry_date": best_delta_row["entry_date"],
                                    "exit_date": best_delta_row["exit_date"],
                                    "prediction": prediction,
                                    "selected_method": best_delta_row["selected_method"],
                                    "confidence_pct": _to_float(best_delta_row["confidence_pct"]),
                                    "spot_close_entry": best_delta_spot_close_entry,
                                    "entry_debit": float(best_delta_row["entry_debit"]),
                                    "spread_mark": float(best_delta_row["spread_mark"]),
                                    "pnl": float(best_delta_row["pnl"]),
                                    "roi_pct": _to_float(best_delta_row["roi_pct"]),
                                    "source": "best_delta_hold_to_expiry",
                                }
                            )
                            continue

                        cached_tp_row = tp_trade_index.get(
                            (
                                week["entry_date"],
                                str(baseline_row["symbol"]),
                                prediction,
                                take_profit_label,
                            )
                        )
                        if cached_tp_row is None:
                            symbol = str(baseline_row["symbol"])
                            if symbol not in symbol_cache:
                                symbol_trades = [
                                    row
                                    for row in best_delta_selected_rows
                                    if row["symbol"].strip().upper() == symbol
                                ]
                                symbol_cache[symbol] = _load_symbol_path_cache(
                                    session,
                                    symbol=symbol,
                                    trades=symbol_trades,
                                )
                            spot_by_date, option_rows_by_date, path_dates_by_trade = symbol_cache[symbol]
                            compatible_trade_row = _build_compatible_trade_row(best_delta_row)
                            simulated = tp_grid._simulate_take_profit_exit(
                                trade_row=compatible_trade_row,
                                take_profit_pct=take_profit_pct,
                                option_rows_by_date=option_rows_by_date,
                                spot_by_date=spot_by_date,
                                path_dates=path_dates_by_trade[
                                    (best_delta_row["entry_date"], best_delta_row["symbol"], best_delta_row["prediction"])
                                ],
                            )
                            if simulated is None:
                                if symbol not in missing_tp_price_rows:
                                    missing_tp_price_rows.append(symbol)
                                continue
                            tp_payload = {
                                "symbol": symbol,
                                "entry_date": simulated["entry_date"],
                                "exit_date": simulated["exit_date"],
                                "prediction": prediction,
                                "selected_method": best_delta_row["selected_method"],
                                "prediction_engine": best_delta_row["prediction_engine"],
                                "confidence_pct": _to_float(best_delta_row["confidence_pct"]),
                                "best_delta_target_pct": int(best_delta_row["best_delta_target_pct"]),
                                "best_take_profit_label": take_profit_label,
                                "best_take_profit_pct": take_profit_pct,
                                "spot_close_entry": _to_float(str(simulated["spot_close_entry"])),
                                "entry_debit": float(simulated["entry_debit"]),
                                "spread_mark": float(simulated["exit_spread_mark"]),
                                "pnl": float(simulated["pnl"]),
                                "roi_pct": _to_float(str(simulated["roi_pct"])),
                                "exit_reason": simulated["exit_reason"],
                                "source": "best_delta_take_profit_on_demand",
                            }
                        else:
                            tp_payload = {
                                "symbol": cached_tp_row["symbol"].strip().upper(),
                                "entry_date": cached_tp_row["entry_date"],
                                "exit_date": cached_tp_row["exit_date"],
                                "prediction": prediction,
                                "selected_method": best_delta_row["selected_method"],
                                "prediction_engine": cached_tp_row["prediction_engine"],
                                "confidence_pct": _to_float(best_delta_row["confidence_pct"]),
                                "best_delta_target_pct": int(best_delta_row["best_delta_target_pct"]),
                                "best_take_profit_label": take_profit_label,
                                "best_take_profit_pct": take_profit_pct,
                                "spot_close_entry": _to_float(cached_tp_row.get("spot_close_entry")),
                                "entry_debit": float(cached_tp_row["entry_debit"]),
                                "spread_mark": float(cached_tp_row["exit_spread_mark"]),
                                "pnl": float(cached_tp_row["pnl"]),
                                "roi_pct": _to_float(cached_tp_row["roi_pct"]),
                                "exit_reason": cached_tp_row["exit_reason"],
                                "source": "best_delta_take_profit_cached",
                            }
                        selected_tp_rows.append(tp_payload)
                        best_tp_by_prediction_rows[prediction].append(
                            {
                                "symbol": tp_payload["symbol"],
                                "entry_date": tp_payload["entry_date"],
                                "exit_date": tp_payload["exit_date"],
                                "prediction": prediction,
                                "selected_method": tp_payload["selected_method"],
                                "confidence_pct": tp_payload["confidence_pct"],
                                "spot_close_entry": tp_payload["spot_close_entry"],
                                "entry_debit": tp_payload["entry_debit"],
                                "spread_mark": tp_payload["spread_mark"],
                                "pnl": tp_payload["pnl"],
                                "roi_pct": tp_payload["roi_pct"],
                                "source": tp_payload["source"],
                            }
                        )

                for prediction in ("up", "abstain"):
                    baseline_summary = _summarize_rows(baseline_by_prediction[prediction])
                    best_delta_summary = _summarize_rows(best_delta_by_prediction[prediction])
                    best_tp_summary = _summarize_rows(best_tp_by_prediction_rows[prediction])
                    summary_rows.append(
                        {
                            "entry_date": week["entry_date"],
                            "exit_date": week["exit_date"],
                            "prediction": prediction,
                            "baseline_trade_count": baseline_summary["trade_count"],
                            "baseline_positive_debit_count": baseline_summary["positive_debit_count"],
                            "baseline_nonpositive_debit_count": baseline_summary["nonpositive_debit_count"],
                            "baseline_total_debit_paid_positive": baseline_summary["total_debit_paid_positive"],
                            "baseline_total_pnl_positive": baseline_summary["total_pnl_positive"],
                            "baseline_total_pnl_all_trades": baseline_summary["total_pnl_all_trades"],
                            "baseline_avg_roi_positive_debit_pct": baseline_summary["avg_roi_positive_debit_pct"],
                            "baseline_median_roi_positive_debit_pct": baseline_summary["median_roi_positive_debit_pct"],
                            "baseline_weighted_return_positive_debit_pct": baseline_summary[
                                "weighted_return_positive_debit_pct"
                            ],
                            "best_delta_trade_count": best_delta_summary["trade_count"],
                            "best_delta_positive_debit_count": best_delta_summary["positive_debit_count"],
                            "best_delta_nonpositive_debit_count": best_delta_summary["nonpositive_debit_count"],
                            "best_delta_total_debit_paid_positive": best_delta_summary["total_debit_paid_positive"],
                            "best_delta_total_pnl_positive": best_delta_summary["total_pnl_positive"],
                            "best_delta_total_pnl_all_trades": best_delta_summary["total_pnl_all_trades"],
                            "best_delta_avg_roi_positive_debit_pct": best_delta_summary["avg_roi_positive_debit_pct"],
                            "best_delta_median_roi_positive_debit_pct": best_delta_summary[
                                "median_roi_positive_debit_pct"
                            ],
                            "best_delta_weighted_return_positive_debit_pct": best_delta_summary[
                                "weighted_return_positive_debit_pct"
                            ],
                            "best_tp_trade_count": best_tp_summary["trade_count"],
                            "best_tp_positive_debit_count": best_tp_summary["positive_debit_count"],
                            "best_tp_nonpositive_debit_count": best_tp_summary["nonpositive_debit_count"],
                            "best_tp_total_debit_paid_positive": best_tp_summary["total_debit_paid_positive"],
                            "best_tp_total_pnl_positive": best_tp_summary["total_pnl_positive"],
                            "best_tp_total_pnl_all_trades": best_tp_summary["total_pnl_all_trades"],
                            "best_tp_avg_roi_positive_debit_pct": best_tp_summary["avg_roi_positive_debit_pct"],
                            "best_tp_median_roi_positive_debit_pct": best_tp_summary[
                                "median_roi_positive_debit_pct"
                            ],
                            "best_tp_weighted_return_positive_debit_pct": best_tp_summary[
                                "weighted_return_positive_debit_pct"
                            ],
                            "best_tp_minus_baseline_avg_roi_pct": (
                                None
                                if baseline_summary["avg_roi_positive_debit_pct"] is None
                                or best_tp_summary["avg_roi_positive_debit_pct"] is None
                                else round(
                                    float(best_tp_summary["avg_roi_positive_debit_pct"])
                                    - float(baseline_summary["avg_roi_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "best_tp_minus_baseline_weighted_return_pct": (
                                None
                                if baseline_summary["weighted_return_positive_debit_pct"] is None
                                or best_tp_summary["weighted_return_positive_debit_pct"] is None
                                else round(
                                    float(best_tp_summary["weighted_return_positive_debit_pct"])
                                    - float(baseline_summary["weighted_return_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "best_tp_minus_best_delta_avg_roi_pct": (
                                None
                                if best_delta_summary["avg_roi_positive_debit_pct"] is None
                                or best_tp_summary["avg_roi_positive_debit_pct"] is None
                                else round(
                                    float(best_tp_summary["avg_roi_positive_debit_pct"])
                                    - float(best_delta_summary["avg_roi_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "best_tp_minus_best_delta_weighted_return_pct": (
                                None
                                if best_delta_summary["weighted_return_positive_debit_pct"] is None
                                or best_tp_summary["weighted_return_positive_debit_pct"] is None
                                else round(
                                    float(best_tp_summary["weighted_return_positive_debit_pct"])
                                    - float(best_delta_summary["weighted_return_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "spot_filter_max_entry": args.max_spot_entry,
                            "filtered_out_symbol_count": len(filtered_out_by_prediction[prediction]),
                            "filtered_out_symbols": ", ".join(sorted(filtered_out_by_prediction[prediction])),
                            "missing_best_tp_symbol_count": len(missing_best_tp),
                            "missing_best_tp_symbols": ", ".join(sorted(missing_best_tp)),
                            "missing_tp_price_row_symbol_count": len(missing_tp_price_rows),
                            "missing_tp_price_row_symbols": ", ".join(sorted(missing_tp_price_rows)),
                        }
                    )
    finally:
        engine.dispose()

    with args.output_trades_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(selected_tp_rows[0].keys()))
        writer.writeheader()
        writer.writerows(selected_tp_rows)

    with args.output_summary_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {args.output_trades_csv}")
    print(f"Wrote {args.output_summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
