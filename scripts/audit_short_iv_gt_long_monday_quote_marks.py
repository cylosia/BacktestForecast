from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[0]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.integrations.massive_client import MassiveClient

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks_monday_close as cond


LOGS = ROOT / "logs"
DEFAULT_SYMBOLS_FILE = ROOT / "weekly-options-over5-median80-mintrades70.txt"
DEFAULT_WINDOW_START = "2025-04-14"
DEFAULT_WINDOW_END = "2026-04-06"
DEFAULT_ROI_THRESHOLD_PCT = -100.0
DEFAULT_POLICY_TOP_K = 43
DEFAULT_OUTPUT_CSV = LOGS / "short_iv_gt_long_monday_quote_audit_roi_lt_neg100.csv"
DEFAULT_OUTPUT_SUMMARY_JSON = LOGS / "short_iv_gt_long_monday_quote_audit_roi_lt_neg100_summary.json"
DEFAULT_CACHE_DIR = LOGS / "massive_intraday_option_quotes" / "manual_checks"
DEFAULT_LEDGERS = (
    LOGS / "short_iv_gt_long_conditional_management_monday_close_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part1_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part2_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part3_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part4_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
    LOGS / "short_iv_gt_long_conditional_management_monday_close_part5_lookback52warmup_to_2026-04-06_vix20abs_selected_trades.csv",
)
DEFAULT_SELECTED_TRADES_CSVS = (
    LOGS / "short_iv_gt_long_best_delta_monday_close_lookback52warmup_to_2026-04-06_selected_trades.csv",
    LOGS / "short_iv_gt_long_best_delta_monday_close_part1_lookback52warmup_to_2026-04-06_selected_trades.csv",
    LOGS / "short_iv_gt_long_best_delta_monday_close_part2_lookback52warmup_to_2026-04-06_selected_trades.csv",
    LOGS / "short_iv_gt_long_best_delta_monday_close_part3_lookback52warmup_to_2026-04-06_selected_trades.csv",
    LOGS / "short_iv_gt_long_best_delta_monday_close_part4_lookback52warmup_to_2026-04-06_selected_trades.csv",
    LOGS / "short_iv_gt_long_best_delta_monday_close_part5_lookback52warmup_to_2026-04-06_selected_trades.csv",
)
DEFAULT_GRID_TRADES_CSVS = (
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_lookback52warmup_to_2026-04-06_trades.csv",
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_part1_lookback52warmup_to_2026-04-06_trades.csv",
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_part2_lookback52warmup_to_2026-04-06_trades.csv",
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_part3_lookback52warmup_to_2026-04-06_trades.csv",
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_part4_lookback52warmup_to_2026-04-06_trades.csv",
    LOGS / "short_iv_gt_long_calendar_delta_grid_monday_close_part5_lookback52warmup_to_2026-04-06_trades.csv",
)

_EASTERN_TZ = ZoneInfo("America/New_York")
_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(16, 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit promoted Monday-close short-IV calendar trades with ROI below a threshold "
            "against raw Massive option quotes."
        )
    )
    parser.add_argument("--symbols-file", type=Path, default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--ledgers", type=Path, nargs="+", default=list(DEFAULT_LEDGERS))
    parser.add_argument("--selected-trades-csvs", type=Path, nargs="+", default=list(DEFAULT_SELECTED_TRADES_CSVS))
    parser.add_argument("--grid-trades-csvs", type=Path, nargs="+", default=list(DEFAULT_GRID_TRADES_CSVS))
    parser.add_argument("--window-start", default=DEFAULT_WINDOW_START)
    parser.add_argument("--window-end", default=DEFAULT_WINDOW_END)
    parser.add_argument("--roi-threshold-pct", type=float, default=DEFAULT_ROI_THRESHOLD_PCT)
    parser.add_argument("--top-k", type=int, default=DEFAULT_POLICY_TOP_K)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-summary-json", type=Path, default=DEFAULT_OUTPUT_SUMMARY_JSON)
    parser.add_argument("--quote-cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    return parser


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
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
    if not text or text.lower() == "none":
        return None
    return float(text)


def _session_bounds_ns(trade_date: date) -> tuple[int, int]:
    session_open = datetime.combine(trade_date, _MARKET_OPEN, tzinfo=_EASTERN_TZ)
    session_close = datetime.combine(trade_date, _MARKET_CLOSE, tzinfo=_EASTERN_TZ)
    return int(session_open.timestamp() * 1_000_000_000), int(session_close.timestamp() * 1_000_000_000)


def _raw_quote_timestamp_ns(row: dict[str, object]) -> int | None:
    for key in ("participant_timestamp", "sip_timestamp", "trf_timestamp"):
        raw = row.get(key)
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _is_valid_raw_quote_row(row: dict[str, object], trade_date: date) -> bool:
    bid = _to_float(row.get("bid_price"))
    ask = _to_float(row.get("ask_price"))
    timestamp_ns = _raw_quote_timestamp_ns(row)
    if bid is None or ask is None or timestamp_ns is None:
        return False
    if bid < 0.0 or ask < 0.0:
        return False
    if not math.isfinite(bid) or not math.isfinite(ask):
        return False
    if bid > ask:
        return False
    session_open_ns, session_close_ns = _session_bounds_ns(trade_date)
    return session_open_ns <= timestamp_ns <= session_close_ns


def _load_raw_quotes_for_date(
    client: MassiveClient,
    *,
    option_ticker: str,
    trade_date: date,
    raw_quote_cache: dict[tuple[str, date], list[dict[str, object]]],
) -> list[dict[str, object]]:
    cache_key = (option_ticker, trade_date)
    cached = raw_quote_cache.get(cache_key)
    if cached is not None:
        return cached
    rows = client._get_paginated_json(
        f"/v3/quotes/{quote(option_ticker, safe='')}",
        params={"timestamp": trade_date.isoformat(), "sort": "participant_timestamp.asc", "limit": 50000},
    )
    normalized = [dict(row) for row in rows if isinstance(row, dict)]
    raw_quote_cache[cache_key] = normalized
    return normalized


def _last_raw_regular_session_quote(
    client: MassiveClient,
    *,
    option_ticker: str,
    trade_date: date,
    raw_quote_cache: dict[tuple[str, date], list[dict[str, object]]],
) -> tuple[dict[str, object] | None, int, int]:
    rows = _load_raw_quotes_for_date(
        client,
        option_ticker=option_ticker,
        trade_date=trade_date,
        raw_quote_cache=raw_quote_cache,
    )
    filtered = [row for row in rows if _is_valid_raw_quote_row(row, trade_date)]
    if not filtered:
        return None, 0, len(rows)
    chosen = max(filtered, key=lambda row: _raw_quote_timestamp_ns(row) or -1)
    return dict(chosen), len(filtered), len(rows)


def _quote_bid(row: dict[str, object] | None) -> float | None:
    return None if row is None else _to_float(row.get("bid_price"))


def _quote_ask(row: dict[str, object] | None) -> float | None:
    return None if row is None else _to_float(row.get("ask_price"))


def _quote_mid(row: dict[str, object] | None) -> float | None:
    bid = _quote_bid(row)
    ask = _quote_ask(row)
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def _safe_spread(long_value: float | None, short_value: float | None) -> float | None:
    if long_value is None or short_value is None:
        return None
    return long_value - short_value


def _trade_key(row: dict[str, object]) -> tuple[str, str, str, str]:
    return (
        str(row["symbol"]).strip().upper(),
        str(row["entry_date"]),
        str(row["prediction"]),
        str(row["selected_method"]),
    )


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


def _derive_promoted_rows(
    source_rows: list[dict[str, object]],
    *,
    top_k: int,
) -> list[dict[str, object]]:
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
        top_k=top_k,
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


def _build_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str, str, str], dict[str, str]]:
    lookup: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in rows:
        key = _trade_key(row)
        lookup.setdefault(key, dict(row))
    return lookup


def _parse_reference_strike(mark_method: str | None) -> float | None:
    if not mark_method or not mark_method.startswith("nearest_strike_intrinsic_adjusted("):
        return None
    try:
        return float(mark_method.split("(", 1)[1].rstrip(")"))
    except ValueError:
        return None


def _load_local_exit_mark(
    *,
    promoted_row: dict[str, object],
    selected_row: dict[str, str],
    grid_row: dict[str, str],
    session: Session,
    symbol_path_cache: dict[str, tuple[dict[date, float], dict[date, dict[date, list[object]]], dict[tuple[str, str, str], list[date]]]],
    trades_by_symbol: dict[str, list[dict[str, str]]],
) -> dict[str, object]:
    symbol = str(promoted_row["symbol"]).strip().upper()
    if str(promoted_row.get("exit_reason")) == "expiration":
        return {
            "local_short_exit_price": _to_float(grid_row.get("short_close_mark")),
            "local_long_exit_price": _to_float(grid_row.get("long_close_mark")),
            "local_short_exit_method": grid_row.get("short_mark_method"),
            "local_long_exit_method": grid_row.get("long_mark_method"),
            "local_short_reference_strike": None,
            "local_short_reference_close": None,
            "local_long_reference_strike": None,
            "local_long_reference_close": None,
        }

    if symbol not in symbol_path_cache:
        symbol_path_cache[symbol] = mgmt._load_symbol_path_cache(
            session,
            symbol=symbol,
            trades=trades_by_symbol[symbol],
        )
    spot_by_date, option_rows_by_date, _ = symbol_path_cache[symbol]
    exit_date = date.fromisoformat(str(promoted_row["exit_date"]))
    spot_mark = spot_by_date.get(exit_date)
    if spot_mark is None:
        return {
            "local_short_exit_price": None,
            "local_long_exit_price": None,
            "local_short_exit_method": None,
            "local_long_exit_method": None,
            "local_short_reference_strike": None,
            "local_short_reference_close": None,
            "local_long_reference_strike": None,
            "local_long_reference_close": None,
        }
    mark = mgmt._mark_position(
        option_rows_by_date=option_rows_by_date,
        mark_date=exit_date,
        short_expiration=date.fromisoformat(selected_row["short_expiration"]),
        long_expiration=date.fromisoformat(selected_row["long_expiration"]),
        short_strike=float(selected_row["short_strike"]),
        long_strike=float(grid_row["long_strike"]),
        spot_mark=spot_mark,
    )
    if mark is None:
        return {
            "local_short_exit_price": None,
            "local_long_exit_price": None,
            "local_short_exit_method": None,
            "local_long_exit_method": None,
            "local_short_reference_strike": None,
            "local_short_reference_close": None,
            "local_long_reference_strike": None,
            "local_long_reference_close": None,
        }

    expiration_map = option_rows_by_date.get(exit_date, {})
    short_rows_by_strike = {
        row.strike_price: row
        for row in expiration_map.get(date.fromisoformat(selected_row["short_expiration"]), [])
    }
    long_rows_by_strike = {
        row.strike_price: row
        for row in expiration_map.get(date.fromisoformat(selected_row["long_expiration"]), [])
    }
    short_reference_strike = _parse_reference_strike(str(mark["short_mark_method"]))
    long_reference_strike = _parse_reference_strike(str(mark["long_mark_method"]))
    short_reference_close = None
    long_reference_close = None
    if short_reference_strike is not None and short_reference_strike in short_rows_by_strike:
        short_reference_close = short_rows_by_strike[short_reference_strike].close_price
    if long_reference_strike is not None and long_reference_strike in long_rows_by_strike:
        long_reference_close = long_rows_by_strike[long_reference_strike].close_price
    return {
        "local_short_exit_price": float(mark["short_mark"]),
        "local_long_exit_price": float(mark["long_mark"]),
        "local_short_exit_method": str(mark["short_mark_method"]),
        "local_long_exit_method": str(mark["long_mark_method"]),
        "local_short_reference_strike": short_reference_strike,
        "local_short_reference_close": short_reference_close,
        "local_long_reference_strike": long_reference_strike,
        "local_long_reference_close": long_reference_close,
    }


def _round_or_none(value: float | None) -> float | str:
    return "" if value is None else round(value, 6)


def main() -> int:
    args = build_parser().parse_args()
    if len(args.ledgers) == 0:
        raise SystemExit("At least one ledger is required.")

    active_symbols = _read_symbols(args.symbols_file)
    ledger_rows: list[dict[str, str]] = []
    for path in args.ledgers:
        ledger_rows.extend(_read_csv_rows(path))
    source_rows = _source_policy_rows(ledger_rows, allowed_symbols=active_symbols)
    promoted_rows = _derive_promoted_rows(source_rows, top_k=args.top_k)
    target_rows = [
        dict(row)
        for row in promoted_rows
        if args.window_start <= str(row["entry_date"]) <= args.window_end
        and _to_float(row.get("roi_pct")) is not None
        and float(row["roi_pct"]) <= args.roi_threshold_pct
    ]
    target_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"])))

    selected_lookup: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for path in args.selected_trades_csvs:
        selected_lookup.update(_build_lookup(_read_csv_rows(path)))
    grid_lookup: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for path in args.grid_trades_csvs:
        grid_lookup.update(_build_lookup(_read_csv_rows(path)))

    targeted_selected_rows_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in target_rows:
        key = _trade_key(row)
        selected_row = selected_lookup.get(key)
        if selected_row is not None:
            targeted_selected_rows_by_symbol[key[0]].append(selected_row)

    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    raw_quote_cache: dict[tuple[str, date], list[dict[str, object]]] = {}
    symbol_path_cache: dict[str, tuple[dict[date, float], dict[date, dict[date, list[object]]], dict[tuple[str, str, str], list[date]]]] = {}
    detail_rows: list[dict[str, object]] = []

    with factory() as session, MassiveClient() as client:
        for promoted_row in target_rows:
            key = _trade_key(promoted_row)
            selected_row = selected_lookup.get(key)
            grid_row = grid_lookup.get(key)
            if selected_row is None or grid_row is None:
                continue

            local_exit = _load_local_exit_mark(
                promoted_row=promoted_row,
                selected_row=selected_row,
                grid_row=grid_row,
                session=session,
                symbol_path_cache=symbol_path_cache,
                trades_by_symbol=targeted_selected_rows_by_symbol,
            )

            entry_date = date.fromisoformat(str(promoted_row["entry_date"]))
            exit_date = date.fromisoformat(str(promoted_row["exit_date"]))
            short_entry_quote, short_entry_filtered_count, short_entry_raw_count = _last_raw_regular_session_quote(
                client,
                option_ticker=str(grid_row["short_option_ticker"]),
                trade_date=entry_date,
                raw_quote_cache=raw_quote_cache,
            )
            long_entry_quote, long_entry_filtered_count, long_entry_raw_count = _last_raw_regular_session_quote(
                client,
                option_ticker=str(grid_row["long_option_ticker"]),
                trade_date=entry_date,
                raw_quote_cache=raw_quote_cache,
            )
            short_exit_quote, short_exit_filtered_count, short_exit_raw_count = _last_raw_regular_session_quote(
                client,
                option_ticker=str(grid_row["short_option_ticker"]),
                trade_date=exit_date,
                raw_quote_cache=raw_quote_cache,
            )
            long_exit_quote, long_exit_filtered_count, long_exit_raw_count = _last_raw_regular_session_quote(
                client,
                option_ticker=str(grid_row["long_option_ticker"]),
                trade_date=exit_date,
                raw_quote_cache=raw_quote_cache,
            )

            raw_entry_natural_debit = _safe_spread(_quote_ask(long_entry_quote), _quote_bid(short_entry_quote))
            raw_exit_natural_close_value = _safe_spread(_quote_bid(long_exit_quote), _quote_ask(short_exit_quote))
            raw_entry_mid_spread = _safe_spread(_quote_mid(long_entry_quote), _quote_mid(short_entry_quote))
            raw_exit_mid_spread = _safe_spread(_quote_mid(long_exit_quote), _quote_mid(short_exit_quote))
            raw_natural_pnl = _safe_spread(raw_exit_natural_close_value, raw_entry_natural_debit)
            raw_mid_pnl = _safe_spread(raw_exit_mid_spread, raw_entry_mid_spread)
            raw_natural_roi_pct = (
                None
                if raw_entry_natural_debit is None or raw_entry_natural_debit <= 0.0 or raw_natural_pnl is None
                else (raw_natural_pnl / raw_entry_natural_debit) * 100.0
            )
            raw_mid_roi_pct = (
                None
                if raw_entry_mid_spread is None or raw_entry_mid_spread <= 0.0 or raw_mid_pnl is None
                else (raw_mid_pnl / raw_entry_mid_spread) * 100.0
            )

            local_entry_debit = _to_float(grid_row.get("entry_debit"))
            local_exit_spread = _safe_spread(local_exit["local_long_exit_price"], local_exit["local_short_exit_price"])
            local_exit_mid_vs_quote_diff = (
                None
                if local_exit_spread is None or raw_exit_mid_spread is None
                else local_exit_spread - raw_exit_mid_spread
            )
            local_exit_natural_vs_quote_diff = (
                None
                if local_exit_spread is None or raw_exit_natural_close_value is None
                else local_exit_spread - raw_exit_natural_close_value
            )
            detail_rows.append(
                {
                    "symbol": key[0],
                    "entry_date": key[1],
                    "exit_date": promoted_row["exit_date"],
                    "prediction": key[2],
                    "selected_method": key[3],
                    "exit_reason": promoted_row["exit_reason"],
                    "stored_pnl": _round_or_none(_to_float(promoted_row.get("pnl"))),
                    "stored_roi_pct": _round_or_none(_to_float(promoted_row.get("roi_pct"))),
                    "local_entry_debit": _round_or_none(local_entry_debit),
                    "local_short_entry_price": _round_or_none(_to_float(grid_row.get("short_close_entry"))),
                    "local_long_entry_price": _round_or_none(_to_float(grid_row.get("long_close_entry"))),
                    "local_short_exit_price": _round_or_none(local_exit["local_short_exit_price"]),
                    "local_long_exit_price": _round_or_none(local_exit["local_long_exit_price"]),
                    "local_exit_spread": _round_or_none(local_exit_spread),
                    "local_short_exit_method": local_exit["local_short_exit_method"] or "",
                    "local_long_exit_method": local_exit["local_long_exit_method"] or "",
                    "local_short_reference_strike": _round_or_none(local_exit["local_short_reference_strike"]),
                    "local_short_reference_close": _round_or_none(local_exit["local_short_reference_close"]),
                    "local_long_reference_strike": _round_or_none(local_exit["local_long_reference_strike"]),
                    "local_long_reference_close": _round_or_none(local_exit["local_long_reference_close"]),
                    "short_option_ticker": grid_row["short_option_ticker"],
                    "long_option_ticker": grid_row["long_option_ticker"],
                    "raw_short_entry_bid": _round_or_none(_quote_bid(short_entry_quote)),
                    "raw_short_entry_ask": _round_or_none(_quote_ask(short_entry_quote)),
                    "raw_long_entry_bid": _round_or_none(_quote_bid(long_entry_quote)),
                    "raw_long_entry_ask": _round_or_none(_quote_ask(long_entry_quote)),
                    "raw_short_exit_bid": _round_or_none(_quote_bid(short_exit_quote)),
                    "raw_short_exit_ask": _round_or_none(_quote_ask(short_exit_quote)),
                    "raw_long_exit_bid": _round_or_none(_quote_bid(long_exit_quote)),
                    "raw_long_exit_ask": _round_or_none(_quote_ask(long_exit_quote)),
                    "raw_entry_natural_debit": _round_or_none(raw_entry_natural_debit),
                    "raw_exit_natural_close_value": _round_or_none(raw_exit_natural_close_value),
                    "raw_entry_mid_spread": _round_or_none(raw_entry_mid_spread),
                    "raw_exit_mid_spread": _round_or_none(raw_exit_mid_spread),
                    "raw_natural_pnl": _round_or_none(raw_natural_pnl),
                    "raw_natural_roi_pct": _round_or_none(raw_natural_roi_pct),
                    "raw_mid_pnl": _round_or_none(raw_mid_pnl),
                    "raw_mid_roi_pct": _round_or_none(raw_mid_roi_pct),
                    "local_exit_minus_raw_natural_close": _round_or_none(local_exit_natural_vs_quote_diff),
                    "local_exit_minus_raw_mid_spread": _round_or_none(local_exit_mid_vs_quote_diff),
                    "short_entry_filtered_quote_count": short_entry_filtered_count,
                    "long_entry_filtered_quote_count": long_entry_filtered_count,
                    "short_exit_filtered_quote_count": short_exit_filtered_count,
                    "long_exit_filtered_quote_count": long_exit_filtered_count,
                    "short_entry_raw_quote_count": short_entry_raw_count,
                    "long_entry_raw_quote_count": long_entry_raw_count,
                    "short_exit_raw_quote_count": short_exit_raw_count,
                    "long_exit_raw_quote_count": long_exit_raw_count,
                    "short_entry_timestamp_ns": "" if short_entry_quote is None else (_raw_quote_timestamp_ns(short_entry_quote) or ""),
                    "long_entry_timestamp_ns": "" if long_entry_quote is None else (_raw_quote_timestamp_ns(long_entry_quote) or ""),
                    "short_exit_timestamp_ns": "" if short_exit_quote is None else (_raw_quote_timestamp_ns(short_exit_quote) or ""),
                    "long_exit_timestamp_ns": "" if long_exit_quote is None else (_raw_quote_timestamp_ns(long_exit_quote) or ""),
                }
            )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_summary_json.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(detail_rows[0].keys()) if detail_rows else []
    if fieldnames:
        with args.output_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(detail_rows)

    exit_reason_counts = Counter(str(row["exit_reason"]) for row in detail_rows)
    local_long_method_counts = Counter(str(row["local_long_exit_method"]) for row in detail_rows)
    raw_natural_available = [row for row in detail_rows if row["raw_natural_roi_pct"] != ""]
    raw_mid_available = [row for row in detail_rows if row["raw_mid_roi_pct"] != ""]
    sign_flip_symbols = [
        row["symbol"]
        for row in raw_natural_available
        if float(row["stored_pnl"]) < 0.0 and float(row["raw_natural_pnl"]) >= 0.0
    ]
    summary = {
        "window_start": args.window_start,
        "window_end": args.window_end,
        "roi_threshold_pct": args.roi_threshold_pct,
        "target_trade_count": len(detail_rows),
        "exit_reason_counts": dict(exit_reason_counts),
        "local_long_exit_method_counts": dict(local_long_method_counts),
        "raw_natural_roi_available_count": len(raw_natural_available),
        "raw_mid_roi_available_count": len(raw_mid_available),
        "raw_natural_roi_lt_neg100_count": sum(1 for row in raw_natural_available if float(row["raw_natural_roi_pct"]) <= -100.0),
        "raw_mid_roi_lt_neg100_count": sum(1 for row in raw_mid_available if float(row["raw_mid_roi_pct"]) <= -100.0),
        "stored_negative_pnl_but_raw_natural_nonnegative_count": len(sign_flip_symbols),
        "stored_negative_pnl_but_raw_natural_nonnegative_symbols": sign_flip_symbols,
    }
    args.output_summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print(f"wrote_detail_csv={args.output_csv}")
    print(f"wrote_summary_json={args.output_summary_json}")
    print(f"target_trade_count={len(detail_rows)}")
    print(f"raw_natural_roi_available_count={len(raw_natural_available)}")
    print(f"raw_mid_roi_available_count={len(raw_mid_available)}")
    print(f"stored_negative_pnl_but_raw_natural_nonnegative_count={len(sign_flip_symbols)}")
    if sign_flip_symbols:
        print(f"stored_negative_pnl_but_raw_natural_nonnegative_symbols={','.join(sign_flip_symbols)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
