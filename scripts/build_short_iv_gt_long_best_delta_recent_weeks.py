from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"

DEFAULT_BASE_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_selected_trades.csv"
DEFAULT_BEST_DELTA_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_best_delta_by_symbol.csv"
DEFAULT_GRID_TRADES_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_trades.csv"
DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_4weeks_selected_trades.csv"
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_best_delta_4weeks_selected_summary.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a recent-weeks best-delta selected-trades CSV by extending an existing "
            "best-delta selection file with additional entry dates from the 2-year delta grid."
        )
    )
    parser.add_argument(
        "--base-selected-trades-csv",
        type=Path,
        default=DEFAULT_BASE_SELECTED_TRADES_CSV,
        help="Existing selected-trades CSV to extend.",
    )
    parser.add_argument(
        "--best-delta-csv",
        type=Path,
        default=DEFAULT_BEST_DELTA_CSV,
        help="Per-symbol best-delta summary CSV.",
    )
    parser.add_argument(
        "--grid-trades-csv",
        type=Path,
        default=DEFAULT_GRID_TRADES_CSV,
        help="Delta-grid trade ledger used to source additional entry dates.",
    )
    parser.add_argument(
        "--add-entry-dates",
        nargs="+",
        required=True,
        help="Entry dates to add, in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output-trades-csv",
        type=Path,
        default=DEFAULT_OUTPUT_TRADES_CSV,
        help="Output CSV for the merged selected trades.",
    )
    parser.add_argument(
        "--output-summary-csv",
        type=Path,
        default=DEFAULT_OUTPUT_SUMMARY_CSV,
        help="Output CSV summarizing counts and missing rows by date/prediction.",
    )
    return parser


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return float(text)


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return int(float(text))


def _load_best_delta_by_symbol_prediction(path: Path) -> dict[tuple[str, str], int]:
    mapping: dict[tuple[str, str], int] = {}
    for row in csv.DictReader(path.open(encoding="utf-8")):
        symbol = row["symbol"].strip().upper()
        best_up = row["best_up_delta_target_pct"].strip()
        best_abstain = row["best_abstain_delta_target_pct"].strip()
        if best_up:
            mapping[(symbol, "up")] = int(best_up)
        if best_abstain:
            mapping[(symbol, "abstain")] = int(best_abstain)
    return mapping


def _build_grid_index(
    path: Path,
) -> tuple[
    dict[tuple[str, str, str, int], dict[str, str]],
    dict[tuple[str, str], set[str]],
]:
    grid_index: dict[tuple[str, str, str, int], dict[str, str]] = {}
    available_predictions_by_date_symbol: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in csv.DictReader(path.open(encoding="utf-8")):
        symbol = row["symbol"].strip().upper()
        prediction = row["prediction"].strip()
        if prediction not in {"up", "abstain"}:
            continue
        entry_date = row["entry_date"]
        delta_target_pct = int(row["delta_target_pct"])
        grid_index.setdefault((entry_date, symbol, prediction, delta_target_pct), row)
        available_predictions_by_date_symbol[(entry_date, symbol)].add(prediction)
    return grid_index, available_predictions_by_date_symbol


def _build_selected_row(grid_row: dict[str, str]) -> dict[str, object]:
    return {
        "symbol": grid_row["symbol"].strip().upper(),
        "entry_date": grid_row["entry_date"],
        "exit_date": grid_row["short_expiration"],
        "prediction": grid_row["prediction"].strip(),
        "selected_method": grid_row["selected_method"],
        "prediction_engine": grid_row["prediction_engine"],
        "confidence_pct": _to_float(grid_row["confidence_pct"]),
        "best_delta_target_pct": _to_int(grid_row["delta_target_pct"]),
        "entry_debit": float(grid_row["entry_debit"]),
        "spread_mark": float(grid_row["spread_mark"]),
        "pnl": float(grid_row["pnl"]),
        "roi_pct": _to_float(grid_row["roi_pct"]),
        "short_strike": float(grid_row["short_strike"]),
        "short_expiration": grid_row["short_expiration"],
        "long_expiration": grid_row["long_expiration"],
        "spot_close_entry": float(grid_row["spot_close_entry"]),
        "spot_close_mark": float(grid_row["spot_close_mark"]),
        "short_mark_method": grid_row["short_mark_method"],
        "long_mark_method": grid_row["long_mark_method"],
        "nonpositive_debit_flag": int(grid_row["nonpositive_debit_flag"]),
        "source": "best_delta_cached_recent_extension",
    }


def main() -> int:
    args = build_parser().parse_args()

    base_rows = list(csv.DictReader(args.base_selected_trades_csv.open(encoding="utf-8")))
    if not base_rows:
        raise SystemExit("Base selected-trades CSV is empty.")

    best_delta_by_symbol_prediction = _load_best_delta_by_symbol_prediction(args.best_delta_csv)
    grid_index, available_predictions_by_date_symbol = _build_grid_index(args.grid_trades_csv)

    merged_rows: list[dict[str, object]] = [dict(row) for row in base_rows]
    existing_keys = {
        (
            str(row["entry_date"]),
            str(row["symbol"]).strip().upper(),
            str(row["prediction"]).strip(),
        )
        for row in merged_rows
    }

    summary_rows: list[dict[str, object]] = []
    added_rows: list[dict[str, object]] = []
    for entry_date in args.add_entry_dates:
        symbols_for_date = sorted(
            symbol
            for candidate_date, symbol in available_predictions_by_date_symbol
            if candidate_date == entry_date
        )
        if not symbols_for_date:
            raise SystemExit(f"No grid rows were found for entry date {entry_date}.")

        for prediction in ("up", "abstain"):
            added_count = 0
            skipped_existing_count = 0
            missing_best_delta_symbols: list[str] = []
            missing_grid_row_symbols: list[str] = []
            for symbol in symbols_for_date:
                if prediction not in available_predictions_by_date_symbol[(entry_date, symbol)]:
                    continue
                key_wo_delta = (entry_date, symbol, prediction)
                if key_wo_delta in existing_keys:
                    skipped_existing_count += 1
                    continue
                best_delta = best_delta_by_symbol_prediction.get((symbol, prediction))
                if best_delta is None:
                    missing_best_delta_symbols.append(symbol)
                    continue
                grid_row = grid_index.get((entry_date, symbol, prediction, best_delta))
                if grid_row is None:
                    missing_grid_row_symbols.append(symbol)
                    continue
                selected_row = _build_selected_row(grid_row)
                merged_rows.append(selected_row)
                added_rows.append(selected_row)
                existing_keys.add(key_wo_delta)
                added_count += 1

            summary_rows.append(
                {
                    "entry_date": entry_date,
                    "prediction": prediction,
                    "symbol_count_with_prediction": sum(
                        1
                        for symbol in symbols_for_date
                        if prediction in available_predictions_by_date_symbol[(entry_date, symbol)]
                    ),
                    "added_trade_count": added_count,
                    "skipped_existing_trade_count": skipped_existing_count,
                    "missing_best_delta_count": len(missing_best_delta_symbols),
                    "missing_best_delta_symbols": ", ".join(missing_best_delta_symbols),
                    "missing_grid_row_count": len(missing_grid_row_symbols),
                    "missing_grid_row_symbols": ", ".join(missing_grid_row_symbols),
                }
            )

    merged_rows.sort(
        key=lambda row: (
            str(row["entry_date"]),
            str(row["symbol"]).strip().upper(),
            str(row["prediction"]).strip(),
        )
    )
    added_rows.sort(
        key=lambda row: (
            str(row["entry_date"]),
            str(row["symbol"]).strip().upper(),
            str(row["prediction"]).strip(),
        )
    )

    args.output_trades_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_trades_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(merged_rows[0].keys()))
        writer.writeheader()
        writer.writerows(merged_rows)

    with args.output_summary_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {args.output_trades_csv}")
    print(f"Wrote {args.output_summary_csv}")
    print(f"Added {len(added_rows)} rows across {len(args.add_entry_dates)} entry dates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
