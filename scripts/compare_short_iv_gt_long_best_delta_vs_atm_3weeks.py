from __future__ import annotations

import csv
import os
import sys
from datetime import date
from pathlib import Path
from statistics import mean, median

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LOGS = ROOT / "logs"

import scripts.evaluate_short_iv_gt_long_calendar_delta_grid as delta_grid

from backtestforecast.models import HistoricalUnderlyingDayBar

BEST_DELTA_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_best_delta_by_symbol.csv"
GRID_TRADES_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2y_trades.csv"
GRID_TRADES_2026_04_02_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_2026-04-02_trades.csv"
OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_selected_trades.csv"
OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_weekly_comparison.csv"

WEEK_SPECS = (
    {
        "entry_date": "2026-03-20",
        "exit_date": "2026-03-27",
        "baseline_csv": LOGS / "predictions_short_iv_gt_long_asof_2026-03-20_with_call_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_03_27",
        "grid_trades_csv": GRID_TRADES_CSV,
    },
    {
        "entry_date": "2026-03-27",
        "exit_date": "2026-04-02",
        "baseline_csv": LOGS / "predictions_short_iv_gt_long_asof_2026-03-27_with_call_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_04_02",
        "grid_trades_csv": GRID_TRADES_CSV,
    },
    {
        "entry_date": "2026-04-02",
        "exit_date": "2026-04-10",
        "baseline_csv": LOGS / "predictions_36_symbols_asof_2026-04-02_with_calendar_pnl.csv",
        "baseline_spread_col": "spread_price_2026_04_10",
        "grid_trades_csv": GRID_TRADES_2026_04_02_CSV,
    },
)


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return float(text)


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


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


def _load_spot_close(
    session: Session,
    *,
    symbol: str,
    trade_date: date,
) -> float | None:
    stmt = select(HistoricalUnderlyingDayBar.close_price).where(
        HistoricalUnderlyingDayBar.symbol == symbol,
        HistoricalUnderlyingDayBar.trade_date == trade_date,
    )
    value = session.execute(stmt).scalar_one_or_none()
    return None if value is None else float(value)


def _compute_trade_row_on_demand(
    session: Session,
    *,
    symbol: str,
    entry_date: str,
    exit_date: str,
    prediction: str,
    best_delta: int,
    selected_method: str,
    confidence_pct: float | None,
) -> dict[str, object] | None:
    entry_date_obj = date.fromisoformat(entry_date)
    exit_date_obj = date.fromisoformat(exit_date)
    spot_entry = _load_spot_close(session, symbol=symbol, trade_date=entry_date_obj)
    spot_exit = _load_spot_close(session, symbol=symbol, trade_date=exit_date_obj)
    if spot_entry is None or spot_exit is None:
        return None
    option_rows_by_date = delta_grid._load_option_rows_for_dates(
        session,
        symbol=symbol,
        trade_dates={entry_date_obj, exit_date_obj},
    )
    weekly_candidates = delta_grid._select_weekly_calendar_candidates(
        symbol=symbol,
        friday_dates=[entry_date_obj],
        spot_by_date={
            entry_date_obj: spot_entry,
            exit_date_obj: spot_exit,
        },
        option_rows_by_date=option_rows_by_date,
        short_dte_max=10,
        gap_dte_max=10,
    )
    if not weekly_candidates:
        return None
    candidate = weekly_candidates[0]
    entry_rows_by_expiration = option_rows_by_date.get(entry_date_obj, {})
    picked = delta_grid._pick_calendar_rows_for_delta(
        entry_rows_by_expiration=entry_rows_by_expiration,
        short_expiration=candidate.short_expiration,
        long_expiration=candidate.long_expiration,
        spot_close_entry=candidate.spot_close_entry,
        entry_date=candidate.entry_date,
        common_atm_strike=candidate.common_atm_strike,
        delta_target_pct=best_delta,
    )
    if picked is None:
        return None
    short_row, long_row, _resolved_short_delta = picked
    mark_rows_by_expiration = option_rows_by_date.get(exit_date_obj, {})
    short_mark_rows_by_strike = {
        row.strike_price: row
        for row in mark_rows_by_expiration.get(candidate.short_expiration, [])
    }
    long_mark_rows_by_strike = {
        row.strike_price: row
        for row in mark_rows_by_expiration.get(candidate.long_expiration, [])
    }
    short_mark, short_mark_method = delta_grid._mark_call_leg(
        rows_by_strike=short_mark_rows_by_strike,
        target_strike=short_row.strike_price,
        spot_mark=spot_exit,
        is_expiring_leg=True,
    )
    long_mark, long_mark_method = delta_grid._mark_call_leg(
        rows_by_strike=long_mark_rows_by_strike,
        target_strike=long_row.strike_price,
        spot_mark=spot_exit,
        is_expiring_leg=False,
    )
    if short_mark is None or long_mark is None:
        return None
    entry_debit = long_row.close_price - short_row.close_price
    spread_mark = long_mark - short_mark
    pnl = spread_mark - entry_debit
    roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
    return {
        "symbol": symbol,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "prediction": prediction,
        "selected_method": selected_method,
        "prediction_engine": "",
        "confidence_pct": confidence_pct,
        "best_delta_target_pct": best_delta,
        "entry_debit": entry_debit,
        "spread_mark": spread_mark,
        "pnl": pnl,
        "roi_pct": roi_pct,
        "short_strike": short_row.strike_price,
        "short_expiration": candidate.short_expiration.isoformat(),
        "long_expiration": candidate.long_expiration.isoformat(),
        "spot_close_entry": candidate.spot_close_entry,
        "spot_close_mark": spot_exit,
        "short_mark_method": short_mark_method,
        "long_mark_method": long_mark_method,
        "nonpositive_debit_flag": int(entry_debit <= 0),
        "source": "best_delta_on_demand",
    }


def _summarize_rows(
    *,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    positive_rows = [row for row in rows if float(row["entry_debit"]) > 0]
    roi_values = [float(row["roi_pct"]) for row in positive_rows if row["roi_pct"] is not None]
    total_debit_positive = sum(float(row["entry_debit"]) for row in positive_rows)
    total_pnl_positive = sum(float(row["pnl"]) for row in positive_rows)
    total_pnl_all = sum(float(row["pnl"]) for row in rows)
    return {
        "trade_count": len(rows),
        "positive_debit_count": len(positive_rows),
        "nonpositive_debit_count": len(rows) - len(positive_rows),
        "total_debit_paid_positive": round(total_debit_positive, 6),
        "total_pnl_positive": round(total_pnl_positive, 6),
        "total_pnl_all_trades": round(total_pnl_all, 6),
        "avg_roi_positive_debit_pct": _round_or_none(mean(roi_values) if roi_values else None),
        "median_roi_positive_debit_pct": _round_or_none(median(roi_values) if roi_values else None),
        "weighted_return_positive_debit_pct": (
            None if total_debit_positive <= 0 else round(total_pnl_positive / total_debit_positive * 100.0, 6)
        ),
    }


def main() -> int:
    best_delta_rows = list(csv.DictReader(BEST_DELTA_CSV.open(encoding="utf-8")))
    grid_rows_by_file = {
        GRID_TRADES_CSV: list(csv.DictReader(GRID_TRADES_CSV.open(encoding="utf-8"))),
        GRID_TRADES_2026_04_02_CSV: list(csv.DictReader(GRID_TRADES_2026_04_02_CSV.open(encoding="utf-8"))),
    }
    grid_index_by_file: dict[Path, dict[tuple[str, str, int], dict[str, str]]] = {}
    for path, rows in grid_rows_by_file.items():
        grid_index: dict[tuple[str, str, int], dict[str, str]] = {}
        for row in rows:
            key = (
                row["entry_date"],
                row["symbol"].strip().upper(),
                int(row["delta_target_pct"]),
            )
            grid_index.setdefault(key, row)
        grid_index_by_file[path] = grid_index
    best_delta_by_symbol_prediction: dict[tuple[str, str], int] = {}
    for row in best_delta_rows:
        symbol = row["symbol"].strip().upper()
        best_up = row["best_up_delta_target_pct"].strip()
        best_abstain = row["best_abstain_delta_target_pct"].strip()
        if best_up:
            best_delta_by_symbol_prediction[(symbol, "up")] = int(best_up)
        if best_abstain:
            best_delta_by_symbol_prediction[(symbol, "abstain")] = int(best_abstain)

    selected_trade_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    engine = create_engine(_load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    try:
        with factory() as session:
            for week in WEEK_SPECS:
                baseline_rows = list(csv.DictReader(week["baseline_csv"].open(encoding="utf-8")))
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
                        }
                        for row in baseline_rows
                        if row["prediction"].strip() == prediction
                    ]
                    for prediction in ("up", "abstain")
                }

                selected_for_week: list[dict[str, object]] = []
                missing_best_delta: list[str] = []
                missing_price_rows: list[str] = []
                grid_index = grid_index_by_file[week["grid_trades_csv"]]
                for prediction in ("up", "abstain"):
                    for baseline_row in baseline_by_prediction[prediction]:
                        symbol = str(baseline_row["symbol"])
                        best_delta = best_delta_by_symbol_prediction.get((symbol, prediction))
                        if best_delta is None:
                            if symbol not in missing_best_delta:
                                missing_best_delta.append(symbol)
                            continue
                        grid_row = grid_index.get((week["entry_date"], symbol, best_delta))
                        if grid_row is not None:
                            selected_row = {
                                "symbol": symbol,
                                "entry_date": week["entry_date"],
                                "exit_date": week["exit_date"],
                                "prediction": prediction,
                                "selected_method": baseline_row["selected_method"],
                                "prediction_engine": grid_row["prediction_engine"],
                                "confidence_pct": baseline_row["confidence_pct"],
                                "best_delta_target_pct": best_delta,
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
                                "source": "best_delta_cached",
                            }
                        else:
                            selected_row = _compute_trade_row_on_demand(
                                session,
                                symbol=symbol,
                                entry_date=week["entry_date"],
                                exit_date=week["exit_date"],
                                prediction=prediction,
                                best_delta=best_delta,
                                selected_method=str(baseline_row["selected_method"]),
                                confidence_pct=(
                                    None
                                    if baseline_row["confidence_pct"] is None
                                    else float(baseline_row["confidence_pct"])
                                ),
                            )
                            if selected_row is None:
                                if symbol not in missing_price_rows:
                                    missing_price_rows.append(symbol)
                                continue
                        selected_for_week.append(selected_row)
                        selected_trade_rows.append(selected_row)

                for prediction in ("up", "abstain"):
                    baseline_bucket = baseline_by_prediction[prediction]
                    best_delta_bucket = [row for row in selected_for_week if row["prediction"] == prediction]
                    baseline_summary = _summarize_rows(rows=baseline_bucket)
                    best_delta_summary = _summarize_rows(rows=best_delta_bucket)
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
                            "best_delta_median_roi_positive_debit_pct": best_delta_summary["median_roi_positive_debit_pct"],
                            "best_delta_weighted_return_positive_debit_pct": best_delta_summary[
                                "weighted_return_positive_debit_pct"
                            ],
                            "delta_avg_roi_positive_debit_pct": (
                                None
                                if baseline_summary["avg_roi_positive_debit_pct"] is None
                                or best_delta_summary["avg_roi_positive_debit_pct"] is None
                                else round(
                                    float(best_delta_summary["avg_roi_positive_debit_pct"])
                                    - float(baseline_summary["avg_roi_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "delta_weighted_return_positive_debit_pct": (
                                None
                                if baseline_summary["weighted_return_positive_debit_pct"] is None
                                or best_delta_summary["weighted_return_positive_debit_pct"] is None
                                else round(
                                    float(best_delta_summary["weighted_return_positive_debit_pct"])
                                    - float(baseline_summary["weighted_return_positive_debit_pct"]),
                                    6,
                                )
                            ),
                            "missing_best_delta_symbol_count": len(missing_best_delta),
                            "missing_best_delta_symbols": ", ".join(sorted(missing_best_delta)),
                            "missing_price_row_symbol_count": len(missing_price_rows),
                            "missing_price_row_symbols": ", ".join(sorted(missing_price_rows)),
                        }
                    )
    finally:
        engine.dispose()

    with OUTPUT_TRADES_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(selected_trade_rows[0].keys()))
        writer.writeheader()
        writer.writerows(selected_trade_rows)

    with OUTPUT_SUMMARY_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {OUTPUT_TRADES_CSV}")
    print(f"Wrote {OUTPUT_SUMMARY_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
