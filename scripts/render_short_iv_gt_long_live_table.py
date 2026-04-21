from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as management

LOGS = ROOT / "logs"
OCC_TICKER_PATTERN = re.compile(r"^O:(.+?)(\d{6})([CP])(\d{8})$")
DISPLAY_FIELDS = ("symbol", "delta", "option_spread", "entry_debit", "TP", "SL")


def _default_input_csv() -> Path | None:
    candidates = sorted(
        LOGS.glob("short_iv_gt_long_live_top40_*.csv"),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    return candidates[0] if candidates else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render a standardized short-IV-greater-than-long-IV live selection table "
            "with normalized option-spread labels and TP/SL columns."
        )
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=_default_input_csv(),
        help="Live selection CSV to format. Defaults to the newest short_iv_gt_long_live_top40_*.csv in logs/.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional output CSV path for the normalized display rows.",
    )
    return parser


def _parse_occ_ticker(ticker: str) -> tuple[str, str, str, str]:
    match = OCC_TICKER_PATTERN.fullmatch(ticker)
    if match is None:
        raise ValueError(f"Unrecognized OCC option ticker: {ticker}")
    symbol, expiry, contract_type, raw_strike = match.groups()
    strike = f"{int(raw_strike) / 1000.0:.2f}"
    return symbol, expiry, contract_type, strike


def _format_option_spread(short_ticker: str, long_ticker: str) -> str:
    short_symbol, short_expiry, short_type, short_strike = _parse_occ_ticker(short_ticker)
    long_symbol, long_expiry, long_type, long_strike = _parse_occ_ticker(long_ticker)
    if short_symbol == long_symbol:
        return (
            f"{short_symbol} {short_expiry} {short_type} {short_strike} - "
            f"{long_expiry} {long_type} {long_strike}"
        )
    return (
        f"{short_symbol} {short_expiry} {short_type} {short_strike} - "
        f"{long_symbol} {long_expiry} {long_type} {long_strike}"
    )


def _build_display_row(row: dict[str, str]) -> dict[str, str]:
    take_profit_pct, stop_loss_pct = management.resolve_method_side_tp_stop(
        prediction=str(row["prediction"]).strip(),
        selected_method=str(row["selected_method"]).strip(),
    )
    return {
        "symbol": str(row["symbol"]).strip(),
        "delta": str(row["best_delta_target_pct"]).strip(),
        "option_spread": _format_option_spread(
            short_ticker=str(row["short_option_ticker"]).strip(),
            long_ticker=str(row["long_option_ticker"]).strip(),
        ),
        "entry_debit": f"{float(row['entry_debit']):.2f}",
        "TP": f"{take_profit_pct:.0f}",
        "SL": f"{stop_loss_pct:.0f}",
    }


def _format_table(rows: list[dict[str, str]]) -> str:
    headers = list(DISPLAY_FIELDS)
    widths: dict[str, int] = {
        field: max(len(field), *(len(str(row[field])) for row in rows))
        for field in headers
    }
    header_line = " ".join(
        field.rjust(widths[field]) if field in {"delta", "entry_debit", "TP", "SL"} else field.ljust(widths[field])
        for field in headers
    )
    separator_line = " ".join("-" * widths[field] for field in headers)
    body_lines = []
    for row in rows:
        body_lines.append(
            " ".join(
                str(row[field]).rjust(widths[field])
                if field in {"delta", "entry_debit", "TP", "SL"}
                else str(row[field]).ljust(widths[field])
                for field in headers
            )
        )
    return "\n".join([header_line, separator_line, *body_lines])


def main() -> int:
    args = build_parser().parse_args()
    if args.input_csv is None:
        raise SystemExit("No default live top40 CSV was found. Provide --input-csv.")
    if not args.input_csv.exists():
        raise SystemExit(f"Input CSV not found: {args.input_csv}")

    source_rows = list(csv.DictReader(args.input_csv.open(encoding="utf-8")))
    if not source_rows:
        raise SystemExit(f"Input CSV is empty: {args.input_csv}")
    display_rows = [_build_display_row(row) for row in source_rows]

    print(_format_table(display_rows))

    if args.output_csv is not None:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.output_csv.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(DISPLAY_FIELDS))
            writer.writeheader()
            writer.writerows(display_rows)
        print(f"\nWrote {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
