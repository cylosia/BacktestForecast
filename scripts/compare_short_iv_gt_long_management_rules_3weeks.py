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

BEST_DELTA_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_selected_trades.csv"
DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_management_rules_3weeks_selected_trades.csv"
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_management_rules_3weeks_weekly_summary.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare short-IV-greater-than-long-IV weekly call-calendar management rules "
            "across the 3 recent weekly windows."
        )
    )
    parser.add_argument(
        "--max-spot-entry",
        type=float,
        default=None,
        help="Optional maximum allowed spot close on entry date. Example: 1000.",
    )
    parser.add_argument("--selected-trades-csv", type=Path, default=BEST_DELTA_SELECTED_TRADES_CSV)
    parser.add_argument("--output-trades-csv", type=Path, default=DEFAULT_OUTPUT_TRADES_CSV)
    parser.add_argument("--output-summary-csv", type=Path, default=DEFAULT_OUTPUT_SUMMARY_CSV)
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
        "avg_holding_days_calendar": _round_or_none(
            mean(float(row["holding_days_calendar"]) for row in rows) if rows else None
        ),
        "profit_target_exit_count": sum(1 for row in rows if row["exit_reason"] == "profit_target"),
        "stop_loss_exit_count": sum(1 for row in rows if row["exit_reason"] == "stop_loss"),
        "tested_exit_count": sum(1 for row in rows if row["exit_reason"] == "spot_close_above_short_strike"),
        "roll_adjustment_count": sum(1 for row in rows if int(row["roll_count"]) > 0),
        "expiration_exit_count": sum(1 for row in rows if row["exit_reason"] == "expiration"),
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


def _mark_position(
    *,
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    mark_date: date,
    short_expiration: date,
    long_expiration: date,
    short_strike: float,
    long_strike: float,
    spot_mark: float,
) -> dict[str, object] | None:
    expiration_map = option_rows_by_date.get(mark_date)
    if expiration_map is None:
        return None
    short_rows_by_strike = {
        row.strike_price: row
        for row in expiration_map.get(short_expiration, [])
    }
    long_rows_by_strike = {
        row.strike_price: row
        for row in expiration_map.get(long_expiration, [])
    }
    short_mark, short_mark_method = tp_grid.delta_grid._mark_call_leg(
        rows_by_strike=short_rows_by_strike,
        target_strike=short_strike,
        spot_mark=spot_mark,
        is_expiring_leg=(mark_date == short_expiration),
    )
    long_mark, long_mark_method = tp_grid.delta_grid._mark_call_leg(
        rows_by_strike=long_rows_by_strike,
        target_strike=long_strike,
        spot_mark=spot_mark,
        is_expiring_leg=False,
    )
    if short_mark is None or long_mark is None:
        return None
    return {
        "short_mark": short_mark,
        "long_mark": long_mark,
        "spread_mark": long_mark - short_mark,
        "short_mark_method": short_mark_method,
        "long_mark_method": long_mark_method,
    }


def _pick_roll_short_strike(
    *,
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    mark_date: date,
    short_expiration: date,
    current_short_strike: float,
    spot_mark: float,
) -> tuple[float, float] | None:
    expiration_map = option_rows_by_date.get(mark_date)
    if expiration_map is None:
        return None
    short_rows = [
        row
        for row in expiration_map.get(short_expiration, [])
        if row.close_price > 0 and row.strike_price > current_short_strike
    ]
    if not short_rows:
        return None
    candidates_at_or_above_spot = [row for row in short_rows if row.strike_price >= spot_mark]
    if candidates_at_or_above_spot:
        chosen = min(candidates_at_or_above_spot, key=lambda row: (row.strike_price - spot_mark, row.strike_price))
    else:
        chosen = min(short_rows, key=lambda row: (abs(row.strike_price - spot_mark), row.strike_price))
    return chosen.strike_price, chosen.close_price


def _build_output_row(
    *,
    trade_row: dict[str, str],
    policy_label: str,
    exit_date: date,
    exit_reason: str,
    entry_debit: float,
    spread_mark: float,
    pnl: float,
    roi_pct: float | None,
    spot_close_exit: float | None,
    short_strike: float,
    long_strike: float,
    short_mark_method: str,
    long_mark_method: str,
    roll_count: int,
    roll_date: date | None,
    roll_from_strike: float | None,
    roll_to_strike: float | None,
    roll_net_debit: float | None,
) -> dict[str, object]:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    return {
        "symbol": trade_row["symbol"].strip().upper(),
        "entry_date": trade_row["entry_date"],
        "exit_date": exit_date.isoformat(),
        "prediction": trade_row["prediction"],
        "policy_label": policy_label,
        "selected_method": trade_row["selected_method"],
        "prediction_engine": trade_row["prediction_engine"],
        "confidence_pct": _to_float(trade_row["confidence_pct"]),
        "best_delta_target_pct": int(trade_row["best_delta_target_pct"]),
        "spot_close_entry": float(trade_row["spot_close_entry"]),
        "spot_close_exit": _round_or_none(spot_close_exit),
        "original_entry_debit": float(trade_row["entry_debit"]),
        "entry_debit": round(entry_debit, 6),
        "short_expiration": trade_row["short_expiration"],
        "long_expiration": trade_row["long_expiration"],
        "short_strike": round(short_strike, 6),
        "long_strike": round(long_strike, 6),
        "spread_mark": round(spread_mark, 6),
        "pnl": round(pnl, 6),
        "roi_pct": _round_or_none(roi_pct),
        "exit_reason": exit_reason,
        "holding_days_calendar": (exit_date - entry_date).days,
        "short_mark_method": short_mark_method,
        "long_mark_method": long_mark_method,
        "roll_count": roll_count,
        "roll_date": "" if roll_date is None else roll_date.isoformat(),
        "roll_from_strike": _round_or_none(roll_from_strike),
        "roll_to_strike": _round_or_none(roll_to_strike),
        "roll_net_debit": _round_or_none(roll_net_debit),
    }


def _simulate_hold_to_expiry(
    *,
    trade_row: dict[str, str],
    policy_label: str,
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    spot_by_date: dict[date, float],
    path_dates: list[date],
) -> dict[str, object]:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    short_strike = float(trade_row["short_strike"])
    long_strike = short_strike
    entry_debit = float(trade_row["entry_debit"])
    if entry_debit <= 0:
        spread_mark = float(trade_row["spread_mark"])
        pnl = float(trade_row["pnl"])
        return _build_output_row(
            trade_row=trade_row,
            policy_label=policy_label,
            exit_date=short_expiration,
            exit_reason="nonpositive_debit",
            entry_debit=entry_debit,
            spread_mark=spread_mark,
            pnl=pnl,
            roi_pct=None,
            spot_close_exit=spot_by_date.get(short_expiration),
            short_strike=short_strike,
            long_strike=long_strike,
            short_mark_method=trade_row["short_mark_method"],
            long_mark_method=trade_row["long_mark_method"],
            roll_count=0,
            roll_date=None,
            roll_from_strike=None,
            roll_to_strike=None,
            roll_net_debit=None,
        )
    final_mark = None
    final_spot = None
    for mark_date in path_dates:
        spot_mark = spot_by_date.get(mark_date)
        if spot_mark is None:
            continue
        mark = _mark_position(
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
        final_mark = (mark_date, spot_mark, mark)
    if final_mark is None:
        spread_mark = float(trade_row["spread_mark"])
        pnl = float(trade_row["pnl"])
        roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
        return _build_output_row(
            trade_row=trade_row,
            policy_label=policy_label,
            exit_date=short_expiration,
            exit_reason="expiration",
            entry_debit=entry_debit,
            spread_mark=spread_mark,
            pnl=pnl,
            roi_pct=roi_pct,
            spot_close_exit=spot_by_date.get(short_expiration),
            short_strike=short_strike,
            long_strike=long_strike,
            short_mark_method=trade_row["short_mark_method"],
            long_mark_method=trade_row["long_mark_method"],
            roll_count=0,
            roll_date=None,
            roll_from_strike=None,
            roll_to_strike=None,
            roll_net_debit=None,
        )
    _, final_spot, mark = final_mark
    pnl = float(mark["spread_mark"]) - entry_debit
    roi_pct = (pnl / entry_debit) * 100.0
    return _build_output_row(
        trade_row=trade_row,
        policy_label=policy_label,
        exit_date=short_expiration,
        exit_reason="expiration",
        entry_debit=entry_debit,
        spread_mark=float(mark["spread_mark"]),
        pnl=pnl,
        roi_pct=roi_pct,
        spot_close_exit=final_spot,
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


def _simulate_exit_on_tested_strike_abstain(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    spot_by_date: dict[date, float],
    path_dates: list[date],
) -> dict[str, object]:
    if trade_row["prediction"] != "abstain":
        return _simulate_hold_to_expiry(
            trade_row=trade_row,
            policy_label="abstain_tested_exit",
            option_rows_by_date=option_rows_by_date,
            spot_by_date=spot_by_date,
            path_dates=path_dates,
        )
    entry_debit = float(trade_row["entry_debit"])
    if entry_debit <= 0:
        return _simulate_hold_to_expiry(
            trade_row=trade_row,
            policy_label="abstain_tested_exit",
            option_rows_by_date=option_rows_by_date,
            spot_by_date=spot_by_date,
            path_dates=path_dates,
        )
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    short_strike = float(trade_row["short_strike"])
    long_strike = short_strike
    for mark_date in path_dates:
        spot_mark = spot_by_date.get(mark_date)
        if spot_mark is None:
            continue
        mark = _mark_position(
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
        if spot_mark > short_strike:
            pnl = float(mark["spread_mark"]) - entry_debit
            roi_pct = (pnl / entry_debit) * 100.0
            return _build_output_row(
                trade_row=trade_row,
                policy_label="abstain_tested_exit",
                exit_date=mark_date,
                exit_reason="spot_close_above_short_strike",
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
    return _simulate_hold_to_expiry(
        trade_row=trade_row,
        policy_label="abstain_tested_exit",
        option_rows_by_date=option_rows_by_date,
        spot_by_date=spot_by_date,
        path_dates=path_dates,
    )


def _simulate_tp_stop(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    spot_by_date: dict[date, float],
    path_dates: list[date],
    take_profit_pct: float,
    stop_loss_pct: float,
) -> dict[str, object]:
    policy_label = f"tp{int(take_profit_pct)}_stop{int(stop_loss_pct)}"
    entry_debit = float(trade_row["entry_debit"])
    if entry_debit <= 0:
        return _simulate_hold_to_expiry(
            trade_row=trade_row,
            policy_label=policy_label,
            option_rows_by_date=option_rows_by_date,
            spot_by_date=spot_by_date,
            path_dates=path_dates,
        )
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    short_strike = float(trade_row["short_strike"])
    long_strike = short_strike
    for mark_date in path_dates:
        spot_mark = spot_by_date.get(mark_date)
        if spot_mark is None:
            continue
        mark = _mark_position(
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
        if roi_pct >= take_profit_pct:
            exit_reason = "profit_target"
        elif roi_pct <= -stop_loss_pct:
            exit_reason = "stop_loss"
        else:
            continue
        return _build_output_row(
            trade_row=trade_row,
            policy_label=policy_label,
            exit_date=mark_date,
            exit_reason=exit_reason,
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
    return _simulate_hold_to_expiry(
        trade_row=trade_row,
        policy_label=policy_label,
        option_rows_by_date=option_rows_by_date,
        spot_by_date=spot_by_date,
        path_dates=path_dates,
    )


def _simulate_up_roll_short_once(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    spot_by_date: dict[date, float],
    path_dates: list[date],
) -> dict[str, object]:
    policy_label = "up_roll_short_once"
    entry_debit = float(trade_row["entry_debit"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    original_short_strike = float(trade_row["short_strike"])
    long_strike = original_short_strike
    if entry_debit <= 0 or trade_row["prediction"] != "up":
        return _simulate_hold_to_expiry(
            trade_row=trade_row,
            policy_label=policy_label,
            option_rows_by_date=option_rows_by_date,
            spot_by_date=spot_by_date,
            path_dates=path_dates,
        )

    current_short_strike = original_short_strike
    adjusted_entry_debit = entry_debit
    roll_count = 0
    roll_date: date | None = None
    roll_from_strike: float | None = None
    roll_to_strike: float | None = None
    roll_net_debit: float | None = None
    final_mark_date = short_expiration
    final_spot = spot_by_date.get(short_expiration)
    final_mark = None

    for mark_date in path_dates:
        spot_mark = spot_by_date.get(mark_date)
        if spot_mark is None:
            continue
        mark = _mark_position(
            option_rows_by_date=option_rows_by_date,
            mark_date=mark_date,
            short_expiration=short_expiration,
            long_expiration=long_expiration,
            short_strike=current_short_strike,
            long_strike=long_strike,
            spot_mark=spot_mark,
        )
        if mark is None:
            continue
        final_mark_date = mark_date
        final_spot = spot_mark
        final_mark = mark
        if roll_count == 0 and mark_date < short_expiration and spot_mark > current_short_strike:
            current_short_mark = float(mark["short_mark"])
            replacement = _pick_roll_short_strike(
                option_rows_by_date=option_rows_by_date,
                mark_date=mark_date,
                short_expiration=short_expiration,
                current_short_strike=current_short_strike,
                spot_mark=spot_mark,
            )
            if replacement is None:
                continue
            new_short_strike, new_short_close = replacement
            adjusted_entry_debit += current_short_mark - new_short_close
            roll_count = 1
            roll_date = mark_date
            roll_from_strike = current_short_strike
            roll_to_strike = new_short_strike
            roll_net_debit = current_short_mark - new_short_close
            current_short_strike = new_short_strike

    if final_mark is None:
        spread_mark = float(trade_row["spread_mark"])
        pnl = spread_mark - adjusted_entry_debit
        roi_pct = None if adjusted_entry_debit <= 0 else (pnl / adjusted_entry_debit) * 100.0
        return _build_output_row(
            trade_row=trade_row,
            policy_label=policy_label,
            exit_date=short_expiration,
            exit_reason="expiration",
            entry_debit=adjusted_entry_debit,
            spread_mark=spread_mark,
            pnl=pnl,
            roi_pct=roi_pct,
            spot_close_exit=spot_by_date.get(short_expiration),
            short_strike=current_short_strike,
            long_strike=long_strike,
            short_mark_method=trade_row["short_mark_method"],
            long_mark_method=trade_row["long_mark_method"],
            roll_count=roll_count,
            roll_date=roll_date,
            roll_from_strike=roll_from_strike,
            roll_to_strike=roll_to_strike,
            roll_net_debit=roll_net_debit,
        )

    pnl = float(final_mark["spread_mark"]) - adjusted_entry_debit
    roi_pct = None if adjusted_entry_debit <= 0 else (pnl / adjusted_entry_debit) * 100.0
    return _build_output_row(
        trade_row=trade_row,
        policy_label=policy_label,
        exit_date=final_mark_date,
        exit_reason="expiration",
        entry_debit=adjusted_entry_debit,
        spread_mark=float(final_mark["spread_mark"]),
        pnl=pnl,
        roi_pct=roi_pct,
        spot_close_exit=final_spot,
        short_strike=current_short_strike,
        long_strike=long_strike,
        short_mark_method=str(final_mark["short_mark_method"]),
        long_mark_method=str(final_mark["long_mark_method"]),
        roll_count=roll_count,
        roll_date=roll_date,
        roll_from_strike=roll_from_strike,
        roll_to_strike=roll_to_strike,
        roll_net_debit=roll_net_debit,
    )


def main() -> int:
    args = build_parser().parse_args()
    selected_rows = list(csv.DictReader(args.selected_trades_csv.open(encoding="utf-8")))
    selected_rows = [row for row in selected_rows if row["prediction"] in {"up", "abstain"}]
    if not selected_rows:
        raise SystemExit("No selected trade rows were found.")

    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    detail_rows: list[dict[str, object]] = []
    filtered_out_by_week_prediction: dict[tuple[str, str], list[str]] = defaultdict(list)
    spot_cache: dict[tuple[str, str], float | None] = {}
    symbol_cache: dict[
        str,
        tuple[
            dict[date, float],
            dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
            dict[tuple[str, str, str], list[date]],
        ],
    ] = {}

    engine = create_engine(_load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    try:
        with factory() as session:
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{len(trades_by_symbol):03d}] {symbol}: loading path data")
                symbol_cache[symbol] = _load_symbol_path_cache(session, symbol=symbol, trades=symbol_trades)
            for row in selected_rows:
                symbol = row["symbol"].strip().upper()
                prediction = row["prediction"]
                entry_date_text = row["entry_date"]
                entry_date = date.fromisoformat(entry_date_text)
                spot_key = (symbol, entry_date_text)
                if spot_key not in spot_cache:
                    spot_cache[spot_key] = _load_spot_close(session, symbol=symbol, trade_date=entry_date)
                spot_close_entry = spot_cache[spot_key]
                if args.max_spot_entry is not None and (
                    spot_close_entry is None or spot_close_entry > args.max_spot_entry
                ):
                    filtered_out_by_week_prediction[(entry_date_text, prediction)].append(symbol)
                    continue
                spot_by_date, option_rows_by_date, path_dates_by_trade = symbol_cache[symbol]
                path_dates = path_dates_by_trade[(row["entry_date"], row["symbol"], row["prediction"])]
                detail_rows.append(
                    _simulate_hold_to_expiry(
                        trade_row=row,
                        policy_label="hold_best_delta",
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                    )
                )
                detail_rows.append(
                    _simulate_exit_on_tested_strike_abstain(
                        trade_row=row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                    )
                )
                detail_rows.append(
                    _simulate_tp_stop(
                        trade_row=row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=25.0,
                        stop_loss_pct=35.0,
                    )
                )
                detail_rows.append(
                    _simulate_tp_stop(
                        trade_row=row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=50.0,
                        stop_loss_pct=35.0,
                    )
                )
                detail_rows.append(
                    _simulate_up_roll_short_once(
                        trade_row=row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                    )
                )
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No management-rule trade rows were produced.")

    args.output_trades_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_trades_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    summary_rows: list[dict[str, object]] = []
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    overall_grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail_rows:
        grouped[(str(row["entry_date"]), str(row["prediction"]), str(row["policy_label"]))].append(row)
        overall_grouped[(str(row["prediction"]), str(row["policy_label"]))].append(row)

    for (entry_date_text, prediction, policy_label), rows in sorted(grouped.items()):
        summary = _summarize_rows(rows)
        entry_dates = {str(row["entry_date"]) for row in rows}
        exit_dates = {str(row["exit_date"]) for row in rows}
        summary_rows.append(
            {
                "entry_date": entry_date_text,
                "exit_date": ", ".join(sorted(exit_dates)),
                "prediction": prediction,
                "policy_label": policy_label,
                **summary,
                "spot_filter_max_entry": args.max_spot_entry,
                "filtered_out_symbol_count": len(filtered_out_by_week_prediction[(entry_date_text, prediction)]),
                "filtered_out_symbols": ", ".join(sorted(filtered_out_by_week_prediction[(entry_date_text, prediction)])),
                "group_entry_date_count": len(entry_dates),
            }
        )

    for (prediction, policy_label), rows in sorted(overall_grouped.items()):
        summary = _summarize_rows(rows)
        summary_rows.append(
            {
                "entry_date": "ALL",
                "exit_date": "MULTI",
                "prediction": prediction,
                "policy_label": policy_label,
                **summary,
                "spot_filter_max_entry": args.max_spot_entry,
                "filtered_out_symbol_count": "",
                "filtered_out_symbols": "",
                "group_entry_date_count": len({str(row["entry_date"]) for row in rows}),
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
