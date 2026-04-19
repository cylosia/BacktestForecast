from __future__ import annotations

import csv
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from backtestforecast.backtests.rules import implied_volatility_from_price
from backtestforecast.backtests.strategies.common import _approx_bsm_delta, choose_atm_strike
from backtestforecast.models import HistoricalOptionDayBar, HistoricalUnderlyingDayBar

LOGS = ROOT / "logs"
WEEK_SPECS = (
    {
        "entry_date": date.fromisoformat("2026-03-20"),
        "expiration_date": date.fromisoformat("2026-03-27"),
        "predictions_csv": LOGS / "predictions_short_iv_le_long_asof_2026-03-20_with_call_calendar_pnl.csv",
    },
    {
        "entry_date": date.fromisoformat("2026-03-27"),
        "expiration_date": date.fromisoformat("2026-04-02"),
        "predictions_csv": LOGS / "predictions_short_iv_le_long_asof_2026-03-27_with_call_calendar_pnl.csv",
    },
    {
        "entry_date": date.fromisoformat("2026-04-02"),
        "expiration_date": date.fromisoformat("2026-04-10"),
        "predictions_csv": LOGS / "predictions_short_iv_le_long_asof_2026-04-02_with_calendar_pnl.csv",
    },
)
DELTA_TARGETS = (20, 25, 30, 35)
WIDTH_STEPS = (1, 2, 3)
DETAIL_CSV = LOGS / "short_iv_le_long_bull_call_grid_trades.csv"
SUMMARY_CSV = LOGS / "short_iv_le_long_bull_call_grid_summary.csv"


@dataclass(frozen=True, slots=True)
class OptionRow:
    option_ticker: str
    trade_date: date
    expiration_date: date
    strike_price: float
    close_price: float


def _database_url() -> str:
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL is required.")


def _load_underlying_close(session: Session, symbol: str, trade_date: date) -> float | None:
    stmt = select(HistoricalUnderlyingDayBar.close_price).where(
        HistoricalUnderlyingDayBar.symbol == symbol,
        HistoricalUnderlyingDayBar.trade_date == trade_date,
    )
    value = session.execute(stmt).scalar_one_or_none()
    return None if value is None else float(value)


def _load_call_rows(session: Session, symbol: str, trade_date: date, expiration_date: date) -> list[OptionRow]:
    stmt = (
        select(HistoricalOptionDayBar)
        .where(HistoricalOptionDayBar.underlying_symbol == symbol)
        .where(HistoricalOptionDayBar.trade_date == trade_date)
        .where(HistoricalOptionDayBar.expiration_date == expiration_date)
        .where(HistoricalOptionDayBar.contract_type == "call")
    )
    return [
        OptionRow(
            option_ticker=row.option_ticker,
            trade_date=row.trade_date,
            expiration_date=row.expiration_date,
            strike_price=float(row.strike_price),
            close_price=float(row.close_price),
        )
        for row in session.execute(stmt).scalars()
    ]


def _intrinsic_call(strike_price: float, spot_price: float) -> float:
    return max(spot_price - strike_price, 0.0)


def _call_delta_from_price(
    *,
    option_price: float,
    spot_price: float,
    strike_price: float,
    trade_date: date,
    expiration_date: date,
) -> float | None:
    if option_price <= 0 or spot_price <= 0 or strike_price <= 0:
        return None
    dte_days = max((expiration_date - trade_date).days, 1)
    iv = implied_volatility_from_price(
        option_price=option_price,
        underlying_price=spot_price,
        strike_price=strike_price,
        time_to_expiry_years=dte_days / 365.0,
        option_type="call",
        risk_free_rate=0.045,
        dividend_yield=0.0,
    )
    if iv is None:
        return None
    return _approx_bsm_delta(
        spot=spot_price,
        strike=strike_price,
        dte_days=dte_days,
        contract_type="call",
        vol=iv,
        risk_free_rate=0.045,
        dividend_yield=0.0,
    )


def _pick_short_row_by_delta(
    *,
    entry_rows: list[OptionRow],
    spot_price: float,
    entry_date: date,
    expiration_date: date,
    delta_target_pct: int,
) -> tuple[OptionRow, float] | None:
    valid_rows = [row for row in entry_rows if row.close_price > 0]
    if not valid_rows:
        return None
    atm_strike = choose_atm_strike([row.strike_price for row in valid_rows], spot_price)
    candidate_rows = [row for row in valid_rows if row.strike_price >= atm_strike]
    if not candidate_rows:
        candidate_rows = valid_rows
    scored: list[tuple[float, float, OptionRow]] = []
    target = delta_target_pct / 100.0
    for row in candidate_rows:
        delta = _call_delta_from_price(
            option_price=row.close_price,
            spot_price=spot_price,
            strike_price=row.strike_price,
            trade_date=entry_date,
            expiration_date=expiration_date,
        )
        if delta is None:
            continue
        scored.append((abs(delta - target), -row.strike_price, row))
    if not scored:
        return None
    _, _, chosen = min(scored)
    chosen_delta = _call_delta_from_price(
        option_price=chosen.close_price,
        spot_price=spot_price,
        strike_price=chosen.strike_price,
        trade_date=entry_date,
        expiration_date=expiration_date,
    )
    return chosen, chosen_delta if chosen_delta is not None else float("nan")


def _pick_long_row_by_width(
    *,
    entry_rows: list[OptionRow],
    short_strike: float,
    width_steps: int,
) -> OptionRow | None:
    rows_by_strike = {row.strike_price: row for row in entry_rows if row.close_price > 0}
    sorted_strikes = sorted(rows_by_strike)
    if short_strike not in rows_by_strike:
        return None
    short_index = sorted_strikes.index(short_strike)
    long_index = short_index - width_steps
    if long_index < 0:
        return None
    return rows_by_strike[sorted_strikes[long_index]]


def _mark_leg(
    *,
    exact_rows_by_strike: dict[float, OptionRow],
    target_strike: float,
    spot_mark: float,
) -> tuple[float | None, str]:
    exact = exact_rows_by_strike.get(target_strike)
    if exact is not None:
        return exact.close_price, "exact"
    intrinsic = _intrinsic_call(target_strike, spot_mark)
    if not exact_rows_by_strike:
        return intrinsic, "expiry_intrinsic"
    nearest_strike = min(exact_rows_by_strike, key=lambda strike: (abs(strike - target_strike), strike))
    nearest = exact_rows_by_strike[nearest_strike]
    adjusted = nearest.close_price + (_intrinsic_call(target_strike, spot_mark) - _intrinsic_call(nearest_strike, spot_mark))
    return max(adjusted, 0.0), f"nearest_strike_intrinsic_adjusted({nearest_strike})"


def main() -> int:
    engine = create_engine(_database_url(), future=True)
    detail_rows: list[dict[str, object]] = []
    try:
        with Session(engine) as session:
            for week_spec in WEEK_SPECS:
                entry_date = week_spec["entry_date"]
                expiration_date = week_spec["expiration_date"]
                mark_date = expiration_date
                prediction_rows = list(csv.DictReader(week_spec["predictions_csv"].open(encoding="utf-8")))
                for prediction_row in prediction_rows:
                    prediction = prediction_row["prediction"].strip()
                    if prediction not in {"up", "abstain"}:
                        continue
                    symbol = prediction_row["symbol"].strip().upper()
                    spot_entry = _load_underlying_close(session, symbol, entry_date)
                    spot_mark = _load_underlying_close(session, symbol, mark_date)
                    if spot_entry is None or spot_mark is None:
                        continue
                    entry_rows = _load_call_rows(session, symbol, entry_date, expiration_date)
                    mark_rows = _load_call_rows(session, symbol, mark_date, expiration_date)
                    if not entry_rows:
                        continue
                    mark_rows_by_strike = {row.strike_price: row for row in mark_rows}
                    for delta_target in DELTA_TARGETS:
                        short_pick = _pick_short_row_by_delta(
                            entry_rows=entry_rows,
                            spot_price=spot_entry,
                            entry_date=entry_date,
                            expiration_date=expiration_date,
                            delta_target_pct=delta_target,
                        )
                        if short_pick is None:
                            continue
                        short_row, resolved_short_delta = short_pick
                        for width_steps in WIDTH_STEPS:
                            long_row = _pick_long_row_by_width(
                                entry_rows=entry_rows,
                                short_strike=short_row.strike_price,
                                width_steps=width_steps,
                            )
                            if long_row is None:
                                continue
                            entry_debit = long_row.close_price - short_row.close_price
                            short_mark, short_mark_method = _mark_leg(
                                exact_rows_by_strike=mark_rows_by_strike,
                                target_strike=short_row.strike_price,
                                spot_mark=spot_mark,
                            )
                            long_mark, long_mark_method = _mark_leg(
                                exact_rows_by_strike=mark_rows_by_strike,
                                target_strike=long_row.strike_price,
                                spot_mark=spot_mark,
                            )
                            if short_mark is None or long_mark is None:
                                continue
                            spread_mark = long_mark - short_mark
                            pnl = spread_mark - entry_debit
                            roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
                            detail_rows.append(
                                {
                                    "entry_date": entry_date.isoformat(),
                                    "expiration_date": expiration_date.isoformat(),
                                    "symbol": symbol,
                                    "prediction": prediction,
                                    "confidence_pct": prediction_row.get("confidence_pct", ""),
                                    "selected_method": prediction_row.get("selected_method", ""),
                                    "delta_target_pct": delta_target,
                                    "width_steps": width_steps,
                                    "spot_close_entry": spot_entry,
                                    "spot_close_mark": spot_mark,
                                    "short_strike": short_row.strike_price,
                                    "long_strike": long_row.strike_price,
                                    "resolved_short_delta": round(resolved_short_delta, 6),
                                    "short_option_ticker": short_row.option_ticker,
                                    "long_option_ticker": long_row.option_ticker,
                                    "short_close_entry": short_row.close_price,
                                    "long_close_entry": long_row.close_price,
                                    "entry_debit": round(entry_debit, 6),
                                    "short_close_mark": round(short_mark, 6),
                                    "long_close_mark": round(long_mark, 6),
                                    "short_mark_method": short_mark_method,
                                    "long_mark_method": long_mark_method,
                                    "spread_mark": round(spread_mark, 6),
                                    "pnl": round(pnl, 6),
                                    "roi_pct": None if roi_pct is None else round(roi_pct, 6),
                                }
                            )
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No bull call spread trades were produced.")

    DETAIL_CSV.parent.mkdir(parents=True, exist_ok=True)
    with DETAIL_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    summary_map: dict[tuple[str, str, int, int], dict[str, object]] = {}
    for row in detail_rows:
        key = (
            row["entry_date"],
            row["prediction"],
            int(row["delta_target_pct"]),
            int(row["width_steps"]),
        )
        summary = summary_map.setdefault(
            key,
            {
                "entry_date": row["entry_date"],
                "prediction": row["prediction"],
                "delta_target_pct": row["delta_target_pct"],
                "width_steps": row["width_steps"],
                "trade_count": 0,
                "positive_debit_count": 0,
                "total_debit_paid": 0.0,
                "total_pnl": 0.0,
                "roi_sum": 0.0,
                "win_count": 0,
                "nonpositive_debit_symbols": [],
            },
        )
        summary["trade_count"] = int(summary["trade_count"]) + 1
        pnl = float(row["pnl"])
        summary["total_pnl"] = float(summary["total_pnl"]) + pnl
        if pnl > 0:
            summary["win_count"] = int(summary["win_count"]) + 1
        entry_debit = float(row["entry_debit"])
        if entry_debit > 0:
            summary["positive_debit_count"] = int(summary["positive_debit_count"]) + 1
            summary["total_debit_paid"] = float(summary["total_debit_paid"]) + entry_debit
            summary["roi_sum"] = float(summary["roi_sum"]) + float(row["roi_pct"])
        else:
            summary["nonpositive_debit_symbols"].append(row["symbol"])

    aggregate_map: dict[tuple[str, int, int], dict[str, object]] = {}
    summary_rows: list[dict[str, object]] = []
    for summary in summary_map.values():
        positive_debit_count = int(summary["positive_debit_count"])
        total_debit_paid = float(summary["total_debit_paid"])
        total_pnl = float(summary["total_pnl"])
        avg_roi = None if positive_debit_count == 0 else float(summary["roi_sum"]) / positive_debit_count
        weighted_return = None if total_debit_paid == 0 else total_pnl / total_debit_paid * 100.0
        summary_row = {
            **summary,
            "avg_roi_positive_debit_pct": None if avg_roi is None else round(avg_roi, 6),
            "weighted_return_pct": None if weighted_return is None else round(weighted_return, 6),
            "win_rate_pct": round(int(summary["win_count"]) / int(summary["trade_count"]) * 100.0, 6),
            "nonpositive_debit_symbols": ", ".join(summary["nonpositive_debit_symbols"]),
        }
        summary_rows.append(summary_row)

        agg_key = (
            str(summary["prediction"]),
            int(summary["delta_target_pct"]),
            int(summary["width_steps"]),
        )
        aggregate = aggregate_map.setdefault(
            agg_key,
            {
                "entry_date": "ALL",
                "prediction": summary["prediction"],
                "delta_target_pct": summary["delta_target_pct"],
                "width_steps": summary["width_steps"],
                "trade_count": 0,
                "positive_debit_count": 0,
                "total_debit_paid": 0.0,
                "total_pnl": 0.0,
                "roi_sum": 0.0,
                "win_count": 0,
                "nonpositive_debit_symbols": [],
            },
        )
        aggregate["trade_count"] = int(aggregate["trade_count"]) + int(summary["trade_count"])
        aggregate["positive_debit_count"] = int(aggregate["positive_debit_count"]) + positive_debit_count
        aggregate["total_debit_paid"] = float(aggregate["total_debit_paid"]) + total_debit_paid
        aggregate["total_pnl"] = float(aggregate["total_pnl"]) + total_pnl
        aggregate["roi_sum"] = float(aggregate["roi_sum"]) + float(summary["roi_sum"])
        aggregate["win_count"] = int(aggregate["win_count"]) + int(summary["win_count"])
        aggregate["nonpositive_debit_symbols"].extend(summary["nonpositive_debit_symbols"])

    for aggregate in aggregate_map.values():
        positive_debit_count = int(aggregate["positive_debit_count"])
        total_debit_paid = float(aggregate["total_debit_paid"])
        total_pnl = float(aggregate["total_pnl"])
        avg_roi = None if positive_debit_count == 0 else float(aggregate["roi_sum"]) / positive_debit_count
        weighted_return = None if total_debit_paid == 0 else total_pnl / total_debit_paid * 100.0
        summary_rows.append(
            {
                **aggregate,
                "avg_roi_positive_debit_pct": None if avg_roi is None else round(avg_roi, 6),
                "weighted_return_pct": None if weighted_return is None else round(weighted_return, 6),
                "win_rate_pct": round(int(aggregate["win_count"]) / int(aggregate["trade_count"]) * 100.0, 6),
                "nonpositive_debit_symbols": ", ".join(aggregate["nonpositive_debit_symbols"]),
            }
        )

    summary_rows.sort(
        key=lambda row: (
            row["prediction"],
            row["entry_date"],
            int(row["delta_target_pct"]),
            int(row["width_steps"]),
        )
    )
    with SUMMARY_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {DETAIL_CSV}")
    print(f"Wrote {SUMMARY_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
