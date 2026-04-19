from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_calendar_delta_grid as delta_grid

from backtestforecast.models import HistoricalOptionDayBar, HistoricalUnderlyingDayBar

DEFAULT_BEST_DELTA_CSV = ROOT / "logs" / "short_iv_gt_long_calendar_delta_grid_2y_best_delta_by_symbol.csv"
DEFAULT_DELTA_TRADES_CSV = ROOT / "logs" / "short_iv_gt_long_calendar_delta_grid_2y_trades.csv"
DEFAULT_OUTPUT_PREFIX = ROOT / "logs" / "short_iv_gt_long_calendar_take_profit_grid_2y"
DEFAULT_TAKE_PROFIT_PCTS = (25, 50, 75, 100, 150, 200)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Grid test take-profit exits for short-IV-greater-than-long-IV call calendars "
            "using each symbol's best delta for up and abstain."
        )
    )
    parser.add_argument("--best-delta-csv", type=Path, default=DEFAULT_BEST_DELTA_CSV)
    parser.add_argument("--delta-trades-csv", type=Path, default=DEFAULT_DELTA_TRADES_CSV)
    parser.add_argument("--output-prefix", type=Path, default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--take-profit-pcts",
        default=",".join(str(value) for value in DEFAULT_TAKE_PROFIT_PCTS),
        help="Comma-separated take-profit percentages. Defaults to 25,50,75,100,150,200.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="SQLAlchemy database URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument("--limit-symbols", type=int, default=None)
    return parser


def _load_database_url(explicit_value: str) -> str:
    if explicit_value:
        return explicit_value
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL is required. Provide --database-url or configure .env.")


def _parse_take_profit_pcts(raw_value: str) -> tuple[int, ...]:
    values = tuple(int(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip())
    if not values:
        raise SystemExit("--take-profit-pcts must contain at least one integer.")
    return values


def _load_best_delta_by_symbol_prediction(best_delta_csv: Path) -> dict[tuple[str, str], int]:
    mapping: dict[tuple[str, str], int] = {}
    for row in csv.DictReader(best_delta_csv.open(encoding="utf-8")):
        symbol = row["symbol"].strip().upper()
        best_up = row["best_up_delta_target_pct"].strip()
        best_abstain = row["best_abstain_delta_target_pct"].strip()
        if best_up:
            mapping[(symbol, "up")] = int(best_up)
        if best_abstain:
            mapping[(symbol, "abstain")] = int(best_abstain)
    return mapping


def _load_selected_best_delta_trade_rows(
    delta_trades_csv: Path,
    *,
    best_delta_by_symbol_prediction: dict[tuple[str, str], int],
    limit_symbols: int | None,
) -> list[dict[str, str]]:
    rows = list(csv.DictReader(delta_trades_csv.open(encoding="utf-8")))
    selected_symbols: set[str] | None = None
    if limit_symbols is not None:
        ordered_symbols = sorted({row["symbol"].strip().upper() for row in rows})
        selected_symbols = set(ordered_symbols[:limit_symbols])
    selected: list[dict[str, str]] = []
    for row in rows:
        symbol = row["symbol"].strip().upper()
        prediction = row["prediction"].strip()
        if prediction not in {"up", "abstain"}:
            continue
        if selected_symbols is not None and symbol not in selected_symbols:
            continue
        target_delta = best_delta_by_symbol_prediction.get((symbol, prediction))
        if target_delta is None:
            continue
        if int(row["delta_target_pct"]) != target_delta:
            continue
        selected.append(row)
    return selected


def _load_underlying_closes(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> dict[date, float]:
    stmt = (
        select(HistoricalUnderlyingDayBar.trade_date, HistoricalUnderlyingDayBar.close_price)
        .where(HistoricalUnderlyingDayBar.symbol == symbol)
        .where(HistoricalUnderlyingDayBar.trade_date >= start_date)
        .where(HistoricalUnderlyingDayBar.trade_date <= end_date)
        .order_by(HistoricalUnderlyingDayBar.trade_date)
    )
    return {
        trade_date: float(close_price)
        for trade_date, close_price in session.execute(stmt).all()
    }


def _load_option_rows_for_dates_and_expirations(
    session: Session,
    *,
    symbol: str,
    trade_dates: set[date],
    expirations: set[date],
) -> dict[date, dict[date, list[delta_grid.OptionRow]]]:
    if not trade_dates or not expirations:
        return {}
    stmt = (
        select(HistoricalOptionDayBar)
        .where(HistoricalOptionDayBar.underlying_symbol == symbol)
        .where(HistoricalOptionDayBar.trade_date.in_(sorted(trade_dates)))
        .where(HistoricalOptionDayBar.expiration_date.in_(sorted(expirations)))
        .where(HistoricalOptionDayBar.contract_type == "call")
        .order_by(
            HistoricalOptionDayBar.trade_date,
            HistoricalOptionDayBar.expiration_date,
            HistoricalOptionDayBar.strike_price,
        )
    )
    grouped: dict[date, dict[date, list[delta_grid.OptionRow]]] = defaultdict(lambda: defaultdict(list))
    for row in session.execute(stmt).scalars():
        grouped[row.trade_date][row.expiration_date].append(
            delta_grid.OptionRow(
                option_ticker=row.option_ticker,
                trade_date=row.trade_date,
                expiration_date=row.expiration_date,
                strike_price=float(row.strike_price),
                close_price=float(row.close_price),
            )
        )
    return {
        trade_date: {expiration: list(items) for expiration, items in expiration_map.items()}
        for trade_date, expiration_map in grouped.items()
    }


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _simulate_take_profit_exit(
    *,
    trade_row: dict[str, str],
    take_profit_pct: int | None,
    option_rows_by_date: dict[date, dict[date, list[delta_grid.OptionRow]]],
    spot_by_date: dict[date, float],
    path_dates: list[date],
) -> dict[str, object]:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    short_strike = float(trade_row["short_strike"])
    long_strike = float(trade_row["long_strike"])
    entry_debit = float(trade_row["entry_debit"])
    baseline_spread_mark = float(trade_row["spread_mark"])
    baseline_pnl = float(trade_row["pnl"])
    baseline_roi_pct = None if not trade_row["roi_pct"].strip() else float(trade_row["roi_pct"])

    exit_date = short_expiration
    exit_reason = "expiration"
    exit_spread_mark = baseline_spread_mark
    exit_pnl = baseline_pnl
    exit_roi_pct = baseline_roi_pct
    short_mark_method = trade_row["short_mark_method"]
    long_mark_method = trade_row["long_mark_method"]

    if take_profit_pct is not None and entry_debit > 0:
        for mark_date in path_dates:
            expiration_map = option_rows_by_date.get(mark_date)
            spot_mark = spot_by_date.get(mark_date)
            if expiration_map is None or spot_mark is None:
                continue
            short_rows = {
                row.strike_price: row
                for row in expiration_map.get(short_expiration, [])
            }
            long_rows = {
                row.strike_price: row
                for row in expiration_map.get(long_expiration, [])
            }
            if mark_date != short_expiration and (not short_rows or not long_rows):
                continue
            current_short_mark, current_short_method = delta_grid._mark_call_leg(
                rows_by_strike=short_rows,
                target_strike=short_strike,
                spot_mark=spot_mark,
                is_expiring_leg=(mark_date == short_expiration),
            )
            current_long_mark, current_long_method = delta_grid._mark_call_leg(
                rows_by_strike=long_rows,
                target_strike=long_strike,
                spot_mark=spot_mark,
                is_expiring_leg=False,
            )
            if current_short_mark is None or current_long_mark is None:
                continue
            current_spread_mark = current_long_mark - current_short_mark
            current_pnl = current_spread_mark - entry_debit
            current_roi_pct = (current_pnl / entry_debit) * 100.0
            if current_roi_pct >= take_profit_pct:
                exit_date = mark_date
                exit_reason = "profit_target"
                exit_spread_mark = current_spread_mark
                exit_pnl = current_pnl
                exit_roi_pct = current_roi_pct
                short_mark_method = current_short_method
                long_mark_method = current_long_method
                break
    elif entry_debit <= 0:
        exit_reason = "nonpositive_debit"

    return {
        "entry_date": entry_date.isoformat(),
        "symbol": trade_row["symbol"].strip().upper(),
        "prediction": trade_row["prediction"],
        "selected_method": trade_row["selected_method"],
        "prediction_engine": trade_row["prediction_engine"],
        "confidence_pct": trade_row["confidence_pct"],
        "delta_target_pct": int(trade_row["delta_target_pct"]),
        "take_profit_label": "hold_to_expiry" if take_profit_pct is None else f"tp_{take_profit_pct}",
        "take_profit_pct": "" if take_profit_pct is None else take_profit_pct,
        "exit_date": exit_date.isoformat(),
        "exit_reason": exit_reason,
        "holding_days_calendar": (exit_date - entry_date).days,
        "spot_close_entry": float(trade_row["spot_close_entry"]),
        "spot_close_exit": _round_or_none(spot_by_date.get(exit_date)),
        "short_expiration": trade_row["short_expiration"],
        "long_expiration": trade_row["long_expiration"],
        "short_strike": short_strike,
        "long_strike": long_strike,
        "entry_debit": entry_debit,
        "exit_spread_mark": round(exit_spread_mark, 6),
        "pnl": round(exit_pnl, 6),
        "roi_pct": _round_or_none(exit_roi_pct),
        "short_mark_method": short_mark_method,
        "long_mark_method": long_mark_method,
        "nonpositive_debit_flag": int(entry_debit <= 0),
    }


def _summarize_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    positive_rows = [row for row in rows if float(row["entry_debit"]) > 0]
    total_debit = sum(float(row["entry_debit"]) for row in positive_rows)
    total_pnl = sum(float(row["pnl"]) for row in positive_rows)
    roi_values = [float(row["roi_pct"]) for row in positive_rows if row["roi_pct"] is not None]
    return {
        "trade_count": len(rows),
        "positive_debit_count": len(positive_rows),
        "nonpositive_debit_count": len(rows) - len(positive_rows),
        "profit_target_exit_count": sum(1 for row in rows if row["exit_reason"] == "profit_target"),
        "expiration_exit_count": sum(1 for row in rows if row["exit_reason"] == "expiration"),
        "total_debit_paid_positive": round(total_debit, 6),
        "total_pnl_positive": round(total_pnl, 6),
        "avg_roi_positive_debit_pct": _round_or_none(mean(roi_values) if roi_values else None),
        "median_roi_positive_debit_pct": _round_or_none(median(roi_values) if roi_values else None),
        "weighted_return_positive_debit_pct": (
            None if total_debit <= 0 else round(total_pnl / total_debit * 100.0, 6)
        ),
        "avg_holding_days_calendar": _round_or_none(mean(float(row["holding_days_calendar"]) for row in rows) if rows else None),
    }


def main() -> int:
    args = build_parser().parse_args()
    take_profit_pcts = _parse_take_profit_pcts(args.take_profit_pcts)
    database_url = _load_database_url(args.database_url)
    best_delta_by_symbol_prediction = _load_best_delta_by_symbol_prediction(args.best_delta_csv)
    selected_trade_rows = _load_selected_best_delta_trade_rows(
        args.delta_trades_csv,
        best_delta_by_symbol_prediction=best_delta_by_symbol_prediction,
        limit_symbols=args.limit_symbols,
    )
    if not selected_trade_rows:
        raise SystemExit("No best-delta trades were selected.")

    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_trade_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    engine = create_engine(database_url, future=True)
    detail_rows: list[dict[str, object]] = []
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            total_symbols = len(trades_by_symbol)
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{total_symbols:03d}] {symbol}: loading path data")
                entry_dates = [date.fromisoformat(row["entry_date"]) for row in symbol_trades]
                short_expirations = [date.fromisoformat(row["short_expiration"]) for row in symbol_trades]
                long_expirations = [date.fromisoformat(row["long_expiration"]) for row in symbol_trades]
                min_entry_date = min(entry_dates)
                max_short_expiration = max(short_expirations)
                spot_by_date = _load_underlying_closes(
                    session,
                    symbol=symbol,
                    start_date=min_entry_date,
                    end_date=max_short_expiration,
                )
                ordered_trade_dates = sorted(spot_by_date)
                path_dates_by_trade: dict[tuple[str, str, int], list[date]] = {}
                needed_trade_dates: set[date] = set()
                for row in symbol_trades:
                    entry_date = date.fromisoformat(row["entry_date"])
                    short_expiration = date.fromisoformat(row["short_expiration"])
                    path_dates = [
                        trade_date
                        for trade_date in ordered_trade_dates
                        if entry_date < trade_date <= short_expiration
                    ]
                    path_dates_by_trade[
                        (
                            row["entry_date"],
                            row["prediction"],
                            int(row["delta_target_pct"]),
                        )
                    ] = path_dates
                    needed_trade_dates.update(path_dates)
                option_rows_by_date = _load_option_rows_for_dates_and_expirations(
                    session,
                    symbol=symbol,
                    trade_dates=needed_trade_dates,
                    expirations=set(short_expirations).union(long_expirations),
                )
                print(f"  {symbol}: simulating take-profit grid")
                for row in symbol_trades:
                    key = (
                        row["entry_date"],
                        row["prediction"],
                        int(row["delta_target_pct"]),
                    )
                    path_dates = path_dates_by_trade[key]
                    detail_rows.append(
                        _simulate_take_profit_exit(
                            trade_row=row,
                            take_profit_pct=None,
                            option_rows_by_date=option_rows_by_date,
                            spot_by_date=spot_by_date,
                            path_dates=path_dates,
                        )
                    )
                    for take_profit_pct in take_profit_pcts:
                        detail_rows.append(
                            _simulate_take_profit_exit(
                                trade_row=row,
                                take_profit_pct=take_profit_pct,
                                option_rows_by_date=option_rows_by_date,
                                spot_by_date=spot_by_date,
                                path_dates=path_dates,
                            )
                        )
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No take-profit grid rows were produced.")

    detail_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_trades.csv")
    aggregate_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_aggregate_summary.csv")
    per_symbol_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_per_symbol_summary.csv")
    best_target_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_best_target_by_symbol.csv")
    detail_csv.parent.mkdir(parents=True, exist_ok=True)

    with detail_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    aggregate_map: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    per_symbol_map: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail_rows:
        aggregate_map[(str(row["prediction"]), str(row["take_profit_label"]))].append(row)
        per_symbol_map[(str(row["symbol"]), str(row["prediction"]), str(row["take_profit_label"]))].append(row)

    aggregate_rows: list[dict[str, object]] = []
    for (prediction, take_profit_label), rows in aggregate_map.items():
        summary = _summarize_rows(rows)
        aggregate_rows.append(
            {
                "symbol": "ALL",
                "prediction": prediction,
                "take_profit_label": take_profit_label,
                "take_profit_pct": rows[0]["take_profit_pct"],
                **summary,
            }
        )
    aggregate_rows.sort(key=lambda row: (row["prediction"], row["take_profit_label"]))
    with aggregate_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(aggregate_rows[0].keys()))
        writer.writeheader()
        writer.writerows(aggregate_rows)

    per_symbol_rows: list[dict[str, object]] = []
    for (symbol, prediction, take_profit_label), rows in per_symbol_map.items():
        summary = _summarize_rows(rows)
        per_symbol_rows.append(
            {
                "symbol": symbol,
                "prediction": prediction,
                "take_profit_label": take_profit_label,
                "take_profit_pct": rows[0]["take_profit_pct"],
                **summary,
            }
        )
    per_symbol_rows.sort(key=lambda row: (row["symbol"], row["prediction"], row["take_profit_label"]))
    with per_symbol_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(per_symbol_rows[0].keys()))
        writer.writeheader()
        writer.writerows(per_symbol_rows)

    best_target_map: dict[tuple[str, str], dict[str, object]] = {}
    for row in per_symbol_rows:
        weighted_return = row["weighted_return_positive_debit_pct"]
        if weighted_return is None:
            continue
        key = (str(row["symbol"]), str(row["prediction"]))
        incumbent = best_target_map.get(key)
        candidate_score = (
            float(weighted_return),
            int(row["positive_debit_count"]),
            float(row["total_pnl_positive"]),
            1 if row["take_profit_label"] == "hold_to_expiry" else 0,
            -1 if row["take_profit_pct"] == "" else -int(row["take_profit_pct"]),
        )
        if incumbent is None:
            best_target_map[key] = row
            continue
        incumbent_score = (
            float(incumbent["weighted_return_positive_debit_pct"]),
            int(incumbent["positive_debit_count"]),
            float(incumbent["total_pnl_positive"]),
            1 if incumbent["take_profit_label"] == "hold_to_expiry" else 0,
            -1 if incumbent["take_profit_pct"] == "" else -int(incumbent["take_profit_pct"]),
        )
        if candidate_score > incumbent_score:
            best_target_map[key] = row

    best_target_rows: list[dict[str, object]] = []
    for symbol in sorted(trades_by_symbol):
        up_row = best_target_map.get((symbol, "up"))
        abstain_row = best_target_map.get((symbol, "abstain"))
        best_target_rows.append(
            {
                "symbol": symbol,
                "best_up_take_profit_label": None if up_row is None else up_row["take_profit_label"],
                "best_up_take_profit_pct": None if up_row is None else up_row["take_profit_pct"],
                "best_up_weighted_return_positive_debit_pct": (
                    None if up_row is None else up_row["weighted_return_positive_debit_pct"]
                ),
                "best_up_avg_roi_positive_debit_pct": None if up_row is None else up_row["avg_roi_positive_debit_pct"],
                "best_up_trade_count": None if up_row is None else up_row["trade_count"],
                "best_abstain_take_profit_label": None if abstain_row is None else abstain_row["take_profit_label"],
                "best_abstain_take_profit_pct": (
                    None if abstain_row is None else abstain_row["take_profit_pct"]
                ),
                "best_abstain_weighted_return_positive_debit_pct": (
                    None if abstain_row is None else abstain_row["weighted_return_positive_debit_pct"]
                ),
                "best_abstain_avg_roi_positive_debit_pct": (
                    None if abstain_row is None else abstain_row["avg_roi_positive_debit_pct"]
                ),
                "best_abstain_trade_count": None if abstain_row is None else abstain_row["trade_count"],
            }
        )
    with best_target_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(best_target_rows[0].keys()))
        writer.writeheader()
        writer.writerows(best_target_rows)

    print(f"Wrote {detail_csv}")
    print(f"Wrote {aggregate_csv}")
    print(f"Wrote {per_symbol_csv}")
    print(f"Wrote {best_target_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
