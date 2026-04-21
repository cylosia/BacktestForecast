from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import fmean, median
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.market_data import vix_regime  # noqa: E402
from run_highVIX_uvix_uvxy_vxx_put_calendar_weekly import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_SYMBOLS,
    DEFAULT_VIX_WEEKLY_CHANGE_MIN_PCT,
    FAR_LEG_TARGET_DTE,
    IGNORED_ASSIGNMENT_EXIT_REASON,
    StrategyConfig,
    TESTED_SHORT_STRIKE_EXIT_REASON,
    TARGET_DTE,
    _build_bundle,
    _build_calendar_config,
    _build_store,
    _extract_short_put_strike_from_trade,
    _install_highvix_put_short_strike_management,
    _load_risk_free_curve,
    _resolve_common_latest_available_date,
    _summarize_records,
    _trade_roi_on_margin_pct,
    _parse_symbols,
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)


DEFAULT_DELTA_VALUES = (30, 40, 50)
DEFAULT_PROFIT_TARGET_PCTS = (25, 50, 75, 100)
DEFAULT_OUTPUT_CSV = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_put_calendar_weekly_grid.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_put_calendar_weekly_grid.json"

GRID_FIELDS = [
    "delta_target",
    "profit_target_pct",
    "eligible_high_vix_friday_count",
    "trade_count",
    "trade_count_with_roi",
    "median_roi_on_margin_pct",
    "average_roi_on_margin_pct",
    "median_weekly_median_roi_on_margin_pct",
    "min_weekly_median_roi_on_margin_pct",
    "max_weekly_median_roi_on_margin_pct",
    "total_net_pnl",
    "win_rate_pct",
    "entered_counts_by_symbol",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Grid test the highVIX weekly put-calendar replay for UVIX, UVXY, and VXX, "
            "ranking delta/profit-target combinations by median ROI per trade."
        )
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbols. Defaults to UVIX,UVXY,VXX.",
    )
    parser.add_argument(
        "--requested-end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Requested end date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for entry dates. Defaults to 365.",
    )
    parser.add_argument(
        "--vix-weekly-change-min-pct",
        type=float,
        default=DEFAULT_VIX_WEEKLY_CHANGE_MIN_PCT,
        help="Minimum Friday-over-Friday VIX close increase required to allow entries. Defaults to 20.",
    )
    parser.add_argument(
        "--delta-values",
        default=",".join(str(value) for value in DEFAULT_DELTA_VALUES),
        help="Comma-separated short-put delta targets. Defaults to 30,40,50.",
    )
    parser.add_argument(
        "--profit-target-pcts",
        default=",".join(str(value) for value in DEFAULT_PROFIT_TARGET_PCTS),
        help="Comma-separated profit-target percentages. Defaults to 25,50,75,100.",
    )
    parser.add_argument(
        "--vix-cache-csv",
        type=Path,
        default=vix_regime.DEFAULT_VIX_CACHE_CSV,
        help="Optional VIX cache CSV path. Defaults to logs/reference/vixcls_cache.csv.",
    )
    parser.add_argument(
        "--allow-vix-cache-refresh",
        action="store_true",
        help="Allow refreshing the VIX cache from FRED when DB/cache coverage is missing.",
    )
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    return parser.parse_args()


def _parse_int_values(raw_value: str) -> tuple[int, ...]:
    values: list[int] = []
    seen: set[int] = set()
    for chunk in raw_value.split(","):
        text = chunk.strip()
        if not text:
            continue
        value = int(text)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise SystemExit("At least one integer value is required.")
    return tuple(values)


def _compute_weekly_median_rows(trade_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows_by_entry_date: dict[str, list[float]] = defaultdict(list)
    for row in trade_rows:
        roi = row.get("roi_on_margin_pct")
        if roi is None:
            continue
        rows_by_entry_date[str(row["entry_date"])].append(float(roi))
    weekly_rows: list[dict[str, object]] = []
    for entry_date in sorted(rows_by_entry_date):
        roi_values = sorted(rows_by_entry_date[entry_date])
        weekly_rows.append(
            {
                "entry_date": entry_date,
                "trade_count": len(roi_values),
                "median_roi_on_margin_pct": round(median(roi_values), 4),
                "roi_values": [round(value, 4) for value in roi_values],
            }
        )
    return weekly_rows


def _grid_row_rank_key(row: dict[str, object]) -> tuple[float, float, float, float]:
    median_roi = row.get("median_roi_on_margin_pct")
    return (
        float("-inf") if median_roi is None else float(median_roi),
        float(row.get("total_net_pnl") or 0.0),
        float(row.get("win_rate_pct") or 0.0),
        float(row.get("trade_count") or 0.0),
    )


def _run_single_combo(
    *,
    engine: OptionsBacktestEngine,
    symbols: tuple[str, ...],
    bundles: dict[str, Any],
    eligible_entry_dates: list[date],
    delta_target: int,
    profit_target_pct: int,
    common_latest_available_date: date,
    risk_free_curve: Any,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    trade_rows: list[dict[str, object]] = []
    entered_counts_by_symbol = {symbol: 0 for symbol in symbols}

    for symbol in symbols:
        strategy = StrategyConfig(
            symbol=symbol,
            delta_target=delta_target,
            profit_target_pct=float(profit_target_pct),
        )
        bundle = bundles[symbol]
        for entry_date in eligible_entry_dates:
            config = _build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                replay_data_end=common_latest_available_date,
                risk_free_curve=risk_free_curve,
            )
            result = engine.run(
                config=config,
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                ex_dividend_dates=bundle.ex_dividend_dates,
                option_gateway=bundle.option_gateway,
                shared_entry_rule_cache=bundle.entry_rule_cache,
            )
            trade = next((item for item in result.trades if item.entry_date == entry_date), None)
            if trade is None:
                continue
            entered_counts_by_symbol[symbol] += 1
            roi_on_margin_pct = _trade_roi_on_margin_pct(trade)
            short_strike = _extract_short_put_strike_from_trade(trade)
            trade_rows.append(
                {
                    "symbol": symbol,
                    "entry_date": trade.entry_date.isoformat(),
                    "exit_date": trade.exit_date.isoformat(),
                    "option_ticker": trade.option_ticker,
                    "short_strike": None if short_strike is None else round(float(short_strike), 4),
                    "delta_target": delta_target,
                    "profit_target_pct": float(profit_target_pct),
                    "net_pnl": round(float(trade.net_pnl), 4),
                    "roi_on_margin_pct": None if roi_on_margin_pct is None else round(float(roi_on_margin_pct), 4),
                    "entry_reason": trade.entry_reason,
                    "exit_reason": trade.exit_reason,
                }
            )

    trade_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"])))
    summary = _summarize_records(trade_rows)
    weekly_median_rows = _compute_weekly_median_rows(trade_rows)
    weekly_medians = [float(row["median_roi_on_margin_pct"]) for row in weekly_median_rows]
    roi_trade_count = sum(1 for row in trade_rows if row.get("roi_on_margin_pct") is not None)

    grid_row = {
        "delta_target": delta_target,
        "profit_target_pct": profit_target_pct,
        "eligible_high_vix_friday_count": len(eligible_entry_dates),
        "trade_count": summary["trade_count"],
        "trade_count_with_roi": roi_trade_count,
        "median_roi_on_margin_pct": summary["median_roi_on_margin_pct"] if roi_trade_count else None,
        "average_roi_on_margin_pct": summary["average_roi_on_margin_pct"] if roi_trade_count else None,
        "median_weekly_median_roi_on_margin_pct": round(median(weekly_medians), 4) if weekly_medians else None,
        "min_weekly_median_roi_on_margin_pct": round(min(weekly_medians), 4) if weekly_medians else None,
        "max_weekly_median_roi_on_margin_pct": round(max(weekly_medians), 4) if weekly_medians else None,
        "total_net_pnl": summary["total_net_pnl"],
        "win_rate_pct": summary["win_rate_pct"],
        "entered_counts_by_symbol": json.dumps(entered_counts_by_symbol, sort_keys=True),
    }
    return grid_row, trade_rows, weekly_median_rows


def _write_grid_csv(*, output_csv: Path, rows: list[dict[str, object]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=GRID_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _parse_args()
    symbols = _parse_symbols(args.symbols)
    delta_values = _parse_int_values(args.delta_values)
    profit_target_pcts = _parse_int_values(args.profit_target_pcts)

    engine_module.logger = type(
        "_SilentLogger",
        (),
        {
            "info": staticmethod(lambda *a, **k: None),
            "warning": staticmethod(lambda *a, **k: None),
            "debug": staticmethod(lambda *a, **k: None),
        },
    )()
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    _install_highvix_put_short_strike_management()

    store = _build_store()
    common_latest_available_date = _resolve_common_latest_available_date(
        store,
        symbols=symbols,
        requested_end_date=args.requested_end_date,
    )
    entry_end_date = common_latest_available_date - timedelta(days=FAR_LEG_TARGET_DTE)
    entry_start_date = entry_end_date - timedelta(days=args.lookback_days)

    risk_free_curve = _load_risk_free_curve(
        store,
        start_date=entry_start_date - timedelta(days=30),
        end_date=common_latest_available_date,
    )
    bundles = {
        symbol: _build_bundle(
            store,
            symbol=symbol,
            start_date=entry_start_date,
            end_date=common_latest_available_date,
        )
        for symbol in symbols
    }
    friday_dates_by_symbol = {
        symbol: {
            bar.trade_date
            for bar in bundle.bars
            if entry_start_date <= bar.trade_date <= entry_end_date and bar.trade_date.weekday() == 4
        }
        for symbol, bundle in bundles.items()
    }
    common_fridays = sorted(set.intersection(*friday_dates_by_symbol.values()))
    if not common_fridays:
        raise SystemExit(
            f"No common Friday bars found for {', '.join(symbols)} between {entry_start_date} and {entry_end_date}."
        )

    vix_close_by_date = vix_regime.load_vix_close_series(
        start_date=min(common_fridays) - timedelta(days=14),
        end_date=max(common_fridays),
        store=store,
        cache_csv=args.vix_cache_csv,
        allow_cache_refresh=args.allow_vix_cache_refresh,
    )
    vix_snapshots = vix_regime.build_weekly_change_snapshots(
        entry_dates=common_fridays,
        close_by_date=vix_close_by_date,
    )
    eligible_entry_dates = [
        trade_date
        for trade_date in common_fridays
        if trade_date in vix_snapshots
        and vix_snapshots[trade_date].weekly_change_pct is not None
        and float(vix_snapshots[trade_date].weekly_change_pct) >= args.vix_weekly_change_min_pct
    ]
    if not eligible_entry_dates:
        raise SystemExit("No eligible highVIX Friday entry dates were found.")

    engine = OptionsBacktestEngine()
    total_combos = len(delta_values) * len(profit_target_pcts)
    grid_rows: list[dict[str, object]] = []
    best_payload: dict[str, object] | None = None

    for delta_target in delta_values:
        for profit_target_pct in profit_target_pcts:
            print(
                f"[combo {len(grid_rows) + 1:02d}/{total_combos:02d}] "
                f"delta={delta_target} profit_target_pct={profit_target_pct}"
            )
            grid_row, trade_rows, weekly_median_rows = _run_single_combo(
                engine=engine,
                symbols=symbols,
                bundles=bundles,
                eligible_entry_dates=eligible_entry_dates,
                delta_target=delta_target,
                profit_target_pct=profit_target_pct,
                common_latest_available_date=common_latest_available_date,
                risk_free_curve=risk_free_curve,
            )
            grid_rows.append(grid_row)
            candidate_payload = {
                "grid_row": grid_row,
                "trade_rows": trade_rows,
                "weekly_median_rows": weekly_median_rows,
            }
            if best_payload is None or _grid_row_rank_key(grid_row) > _grid_row_rank_key(best_payload["grid_row"]):
                best_payload = candidate_payload

    grid_rows.sort(key=_grid_row_rank_key, reverse=True)
    _write_grid_csv(output_csv=args.output_csv, rows=grid_rows)

    eligible_high_vix_fridays = [
        {
            "entry_date": entry_date.isoformat(),
            "weekly_change_pct": round(float(vix_snapshots[entry_date].weekly_change_pct), 4),
            "vix_close": round(float(vix_snapshots[entry_date].close_price), 4),
            "vix_prior_close": round(float(vix_snapshots[entry_date].prior_close_price), 4),
        }
        for entry_date in eligible_entry_dates
    ]

    payload = {
        "period": {
            "entry_start_date": entry_start_date.isoformat(),
            "entry_end_date": entry_end_date.isoformat(),
            "requested_end_date": args.requested_end_date.isoformat(),
            "common_latest_available_date": common_latest_available_date.isoformat(),
            "lookback_days": int(args.lookback_days),
        },
        "policy": {
            "symbols": list(symbols),
            "entry_day": "Friday",
            "vix_weekly_change_min_pct": float(args.vix_weekly_change_min_pct),
            "delta_values": list(delta_values),
            "profit_target_pcts": list(profit_target_pcts),
            "short_leg_target_dte": TARGET_DTE,
            "far_leg_target_dte": FAR_LEG_TARGET_DTE,
            "metric": "median_roi_on_margin_pct",
            "tie_breakers": ["total_net_pnl", "win_rate_pct", "trade_count"],
            "ignored_assignment_exit_reason": IGNORED_ASSIGNMENT_EXIT_REASON,
            "tested_short_strike_exit_reason": TESTED_SHORT_STRIKE_EXIT_REASON,
        },
        "selection_counts": {
            "common_friday_count": len(common_fridays),
            "eligible_high_vix_friday_count": len(eligible_entry_dates),
        },
        "eligible_high_vix_fridays": eligible_high_vix_fridays,
        "grid_rows": grid_rows,
        "best_config": None
        if best_payload is None
        else {
            "grid_row": best_payload["grid_row"],
            "weekly_median_rows": best_payload["weekly_median_rows"],
            "trade_rows": best_payload["trade_rows"],
        },
        "top_5_configs": grid_rows[:5],
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload["top_5_configs"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
