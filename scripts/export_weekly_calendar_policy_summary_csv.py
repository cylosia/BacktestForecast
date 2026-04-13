from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from sqlalchemy import func

from backtestforecast.db.session import create_readonly_session
from backtestforecast.models import HistoricalOptionDayBar, HistoricalUnderlyingDayBar


ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "logs"

SYMBOLS = [
    "AGQ",
    "AMDL",
    "BITX",
    "BOIL",
    "KOLD",
    "CONL",
    "DPST",
    "ETHU",
    "FAS",
    "LABD",
    "LABU",
    "METU",
    "MSTU",
    "MSTX",
    "MSTZ",
    "NAIL",
    "NUGT",
    "NVDL",
    "NVDX",
    "SCO",
    "SOXL",
    "SOXS",
    "SPXL",
    "SPXS",
    "SPXU",
    "SQQQ",
    "SSO",
    "TMF",
    "TNA",
    "TQQQ",
    "TSLL",
    "TZA",
    "UPRO",
    "UVIX",
    "UVXY",
    "YINN",
    "ZSL",
]


def find_result_file(symbol: str) -> Path:
    symbol_lower = symbol.lower()
    candidates = [
        *sorted(LOGS_DIR.glob(f"{symbol_lower}_weekly_calendar_policy_two_stage_*.json")),
        *sorted(LOGS_DIR.glob(f"{symbol_lower}_weekly_calendar_policy_refine_periods_*.json")),
        *sorted(LOGS_DIR.glob(f"{symbol_lower}_weekly_calendar_policy_grid_indicator_periods_*.json")),
        *sorted(LOGS_DIR.glob(f"{symbol_lower}_weekly_calendar_policy_grid_*.json")),
    ]
    if not candidates:
        raise FileNotFoundError(f"No weekly calendar policy result file found for {symbol}")
    return candidates[0]


def pick_sections(payload: dict) -> tuple[dict, dict, str]:
    if "combined_best_result" in payload:
        return (
            payload["combined_best_result"],
            payload["combined_best_result_by_total_roi_pct"],
            "two_stage",
        )
    if "best_result" in payload:
        return (
            payload["best_result"],
            payload["best_result_by_total_roi_pct"],
            "refine",
        )
    raise KeyError("Unsupported result payload format")


def flatten_section(prefix: str, section: dict, include_stage: bool) -> dict:
    row = {
        f"{prefix}_indicator_periods": section["indicator_periods"],
        f"{prefix}_roc_period": section["roc_period"],
        f"{prefix}_adx_period": section["adx_period"],
        f"{prefix}_rsi_period": section["rsi_period"],
        f"{prefix}_bull_filter": section["bull_filter"],
        f"{prefix}_bear_filter": section["bear_filter"],
        f"{prefix}_bull_strategy": section["bull_strategy"],
        f"{prefix}_bear_strategy": section["bear_strategy"],
        f"{prefix}_neutral_strategy": section["neutral_strategy"],
        f"{prefix}_trade_count": section["trade_count"],
        f"{prefix}_total_net_pnl": section["total_net_pnl"],
        f"{prefix}_total_roi_pct": section["total_roi_pct"],
        f"{prefix}_average_roi_on_margin_pct": section["average_roi_on_margin_pct"],
        f"{prefix}_median_roi_on_margin_pct": section["median_roi_on_margin_pct"],
        f"{prefix}_win_rate_pct": section["win_rate_pct"],
        f"{prefix}_average_win": section["average_win"],
        f"{prefix}_average_loss": section["average_loss"],
        f"{prefix}_overlap_signal_count": section["overlap_signal_count"],
        f"{prefix}_selection_bullish": section["selection_counts"]["bullish"],
        f"{prefix}_selection_bearish": section["selection_counts"]["bearish"],
        f"{prefix}_selection_neutral": section["selection_counts"]["neutral"],
        f"{prefix}_entered_bullish": section["entered_counts"]["bullish"],
        f"{prefix}_entered_bearish": section["entered_counts"]["bearish"],
        f"{prefix}_entered_neutral": section["entered_counts"]["neutral"],
    }
    if include_stage:
        row[f"{prefix}_stage"] = section.get("stage", "")
    return row


def fetch_date_spans(symbols: list[str]) -> dict[str, dict[str, str]]:
    spans: dict[str, dict[str, str]] = {}
    with create_readonly_session() as session:
        underlying_rows = (
            session.query(
                HistoricalUnderlyingDayBar.symbol,
                func.min(HistoricalUnderlyingDayBar.trade_date),
                func.max(HistoricalUnderlyingDayBar.trade_date),
            )
            .filter(HistoricalUnderlyingDayBar.symbol.in_(symbols))
            .group_by(HistoricalUnderlyingDayBar.symbol)
            .all()
        )
        option_rows = (
            session.query(
                HistoricalOptionDayBar.underlying_symbol,
                func.min(HistoricalOptionDayBar.trade_date),
                func.max(HistoricalOptionDayBar.trade_date),
            )
            .filter(HistoricalOptionDayBar.underlying_symbol.in_(symbols))
            .group_by(HistoricalOptionDayBar.underlying_symbol)
            .all()
        )
    for symbol in symbols:
        spans[symbol] = {
            "underlying_start": "",
            "underlying_end": "",
            "options_start": "",
            "options_end": "",
        }
    for symbol, start, end in underlying_rows:
        spans[symbol]["underlying_start"] = start.isoformat() if start else ""
        spans[symbol]["underlying_end"] = end.isoformat() if end else ""
    for symbol, start, end in option_rows:
        spans[symbol]["options_start"] = start.isoformat() if start else ""
        spans[symbol]["options_end"] = end.isoformat() if end else ""
    return spans


def build_rows(symbols: list[str]) -> list[dict]:
    spans = fetch_date_spans(symbols)
    rows: list[dict] = []
    for symbol in symbols:
        result_file = find_result_file(symbol)
        payload = json.loads(result_file.read_text())
        best_avg, best_total, source_type = pick_sections(payload)
        include_stage = source_type == "two_stage"
        period = payload.get("period", {})
        row = {
            "symbol": symbol,
            "source_file": str(result_file.relative_to(ROOT)).replace("\\", "/"),
            "source_type": source_type,
            "selection_objective": payload.get("selection_objective", "average"),
            "period_start": period.get("start", ""),
            "period_requested_end": period.get("requested_end", ""),
            "period_latest_available_date": period.get("latest_available_date", ""),
            **spans[symbol],
        }
        row.update(flatten_section("best_avg", best_avg, include_stage))
        row.update(flatten_section("best_total", best_total, include_stage))
        rows.append(row)
    return rows


def main() -> None:
    output_path = LOGS_DIR / "weekly_calendar_policy_summary_37_symbols.csv"
    rows = build_rows(SYMBOLS)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(output_path)


if __name__ == "__main__":
    main()
