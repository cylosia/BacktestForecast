from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, TradeResult  # noqa: E402
from backtestforecast.market_data import vix_regime  # noqa: E402
from backtestforecast.schemas.backtests import StrategyOverrides, StrategyType, StrikeSelection, StrikeSelectionMode  # noqa: E402
from run_highVIX_uvix_uvxy_vxx_put_calendar_weekly import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_SYMBOLS,
    DEFAULT_VIX_WEEKLY_CHANGE_MIN_PCT,
    MAX_HOLDING_DAYS,
    STARTING_EQUITY,
    _build_bundle,
    _build_store,
    _load_risk_free_curve,
    _parse_symbols,
    _resolve_common_latest_available_date,
    _summarize_records,
    _trade_roi_on_margin_pct,
    _yearly_breakdown,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)


DEFAULT_DELTA_TARGET = 50
DEFAULT_PROFIT_TARGET_PCT = 50.0
DEFAULT_TARGET_DTE = 14
DEFAULT_DTE_TOLERANCE_DAYS = 0

DEFAULT_OUTPUT_JSON = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_long_put_weekly_summary.json"
DEFAULT_OUTPUT_CSV = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_long_put_weekly_trades.csv"

LEDGER_FIELDS = [
    "symbol",
    "entry_date",
    "exit_date",
    "option_ticker",
    "strategy_type",
    "vix_weekly_change_pct",
    "vix_close",
    "vix_prior_close",
    "put_strike",
    "delta_target",
    "target_dte",
    "profit_target_pct",
    "quantity",
    "expiration_date",
    "dte_at_open",
    "holding_period_days",
    "entry_underlying_close",
    "exit_underlying_close",
    "entry_mid",
    "exit_mid",
    "gross_pnl",
    "net_pnl",
    "roi_on_margin_pct",
    "total_commissions",
    "entry_reason",
    "exit_reason",
    "detail_json",
]


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    symbol: str
    delta_target: int
    profit_target_pct: float


def _extract_long_put_strike_from_trade(trade: TradeResult) -> float | None:
    detail_json = getattr(trade, "detail_json", {}) or {}
    for leg in detail_json.get("legs", []):
        if leg.get("asset_type") != "option":
            continue
        if leg.get("side") != "long":
            continue
        if leg.get("contract_type") != "put":
            continue
        strike = leg.get("strike_price")
        return None if strike is None else float(strike)
    return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a one-off highVIX weekly long-put replay for UVIX, UVXY, and VXX. "
            "Entries are Friday-only and require VIX weekly change >= the configured threshold."
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
        "--delta-target",
        type=int,
        default=DEFAULT_DELTA_TARGET,
        help="Absolute delta target for the long put. Defaults to 50.",
    )
    parser.add_argument(
        "--profit-target-pct",
        type=float,
        default=DEFAULT_PROFIT_TARGET_PCT,
        help="Profit target percent. Defaults to 50.",
    )
    parser.add_argument(
        "--target-dte",
        type=int,
        default=DEFAULT_TARGET_DTE,
        help="Target days to expiration for the long put. Defaults to 14.",
    )
    parser.add_argument(
        "--dte-tolerance-days",
        type=int,
        default=DEFAULT_DTE_TOLERANCE_DAYS,
        help="Allowed distance from target DTE. Defaults to 0.",
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
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    return parser.parse_args()


def _build_long_put_config(
    *,
    strategy: StrategyConfig,
    entry_date: date,
    replay_data_end: date,
    risk_free_curve: RiskFreeRateCurve,
    target_dte: int,
    dte_tolerance_days: int,
) -> BacktestConfig:
    return BacktestConfig(
        symbol=strategy.symbol,
        strategy_type=StrategyType.LONG_PUT.value,
        start_date=entry_date,
        end_date=min(replay_data_end, entry_date + timedelta(days=35)),
        target_dte=target_dte,
        dte_tolerance_days=dte_tolerance_days,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=Decimal(str(STARTING_EQUITY)),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        risk_free_rate=risk_free_curve.default_rate,
        risk_free_rate_curve=risk_free_curve,
        dividend_yield=0.0,
        slippage_pct=0.0,
        strategy_overrides=StrategyOverrides(
            long_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            )
        ),
        profit_target_pct=float(strategy.profit_target_pct),
        stop_loss_pct=None,
    )


def _write_ledger_csv(*, output_csv: Path, rows: list[dict[str, object]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _parse_args()
    symbols = _parse_symbols(args.symbols)

    engine_module.logger = SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    store = _build_store()
    common_latest_available_date = _resolve_common_latest_available_date(
        store,
        symbols=symbols,
        requested_end_date=args.requested_end_date,
    )
    entry_end_date = common_latest_available_date - timedelta(days=args.target_dte)
    entry_start_date = entry_end_date - timedelta(days=args.lookback_days)
    if entry_start_date > entry_end_date:
        raise SystemExit("Resolved entry window is empty.")

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
    missing_vix_dates = [trade_date.isoformat() for trade_date in common_fridays if trade_date not in vix_snapshots]
    if missing_vix_dates:
        raise SystemExit("Missing VIX reference data for entry dates: " + ", ".join(missing_vix_dates))

    eligible_entry_dates = [
        trade_date
        for trade_date in common_fridays
        if vix_snapshots[trade_date].weekly_change_pct is not None
        and float(vix_snapshots[trade_date].weekly_change_pct) >= args.vix_weekly_change_min_pct
    ]

    engine = OptionsBacktestEngine()
    trade_rows: list[dict[str, object]] = []
    selected_dates_by_symbol = {symbol: 0 for symbol in symbols}
    entered_counts_by_symbol = {symbol: 0 for symbol in symbols}
    skipped_dates_by_symbol: dict[str, list[dict[str, object]]] = {symbol: [] for symbol in symbols}

    for symbol in symbols:
        strategy = StrategyConfig(
            symbol=symbol,
            delta_target=args.delta_target,
            profit_target_pct=args.profit_target_pct,
        )
        bundle = bundles[symbol]
        for entry_date in eligible_entry_dates:
            selected_dates_by_symbol[symbol] += 1
            config = _build_long_put_config(
                strategy=strategy,
                entry_date=entry_date,
                replay_data_end=common_latest_available_date,
                risk_free_curve=risk_free_curve,
                target_dte=args.target_dte,
                dte_tolerance_days=args.dte_tolerance_days,
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
                skipped_dates_by_symbol[symbol].append(
                    {
                        "entry_date": entry_date.isoformat(),
                        "reason": "no_trade_from_engine",
                    }
                )
                continue
            entered_counts_by_symbol[symbol] += 1
            vix_snapshot = vix_snapshots[entry_date]
            put_strike = _extract_long_put_strike_from_trade(trade)
            roi_on_margin_pct = _trade_roi_on_margin_pct(trade)
            trade_rows.append(
                {
                    "symbol": symbol,
                    "entry_date": trade.entry_date.isoformat(),
                    "exit_date": trade.exit_date.isoformat(),
                    "option_ticker": trade.option_ticker,
                    "strategy_type": trade.strategy_type,
                    "vix_weekly_change_pct": None
                    if vix_snapshot.weekly_change_pct is None
                    else round(float(vix_snapshot.weekly_change_pct), 4),
                    "vix_close": round(float(vix_snapshot.close_price), 4),
                    "vix_prior_close": None
                    if vix_snapshot.prior_close_price is None
                    else round(float(vix_snapshot.prior_close_price), 4),
                    "put_strike": None if put_strike is None else round(float(put_strike), 4),
                    "delta_target": args.delta_target,
                    "target_dte": args.target_dte,
                    "profit_target_pct": float(args.profit_target_pct),
                    "quantity": int(trade.quantity),
                    "expiration_date": trade.expiration_date.isoformat(),
                    "dte_at_open": int(trade.dte_at_open),
                    "holding_period_days": int(trade.holding_period_days),
                    "entry_underlying_close": round(float(trade.entry_underlying_close), 4),
                    "exit_underlying_close": round(float(trade.exit_underlying_close), 4),
                    "entry_mid": round(float(trade.entry_mid), 4),
                    "exit_mid": round(float(trade.exit_mid), 4),
                    "gross_pnl": round(float(trade.gross_pnl), 4),
                    "net_pnl": round(float(trade.net_pnl), 4),
                    "roi_on_margin_pct": None if roi_on_margin_pct is None else round(float(roi_on_margin_pct), 4),
                    "total_commissions": round(float(trade.total_commissions), 4),
                    "entry_reason": trade.entry_reason,
                    "exit_reason": trade.exit_reason,
                    "detail_json": json.dumps(trade.detail_json, sort_keys=True, default=str),
                }
            )

    trade_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"])))
    _write_ledger_csv(output_csv=args.output_csv, rows=trade_rows)
    trades_by_symbol: dict[str, list[dict[str, object]]] = {symbol: [] for symbol in symbols}
    for row in trade_rows:
        trades_by_symbol[str(row["symbol"])].append(row)

    eligible_high_vix_fridays = [
        {
            "entry_date": entry_date.isoformat(),
            "effective_trade_date": vix_snapshots[entry_date].effective_trade_date.isoformat(),
            "prior_entry_date": None
            if vix_snapshots[entry_date].prior_entry_date is None
            else vix_snapshots[entry_date].prior_entry_date.isoformat(),
            "prior_effective_trade_date": None
            if vix_snapshots[entry_date].prior_effective_trade_date is None
            else vix_snapshots[entry_date].prior_effective_trade_date.isoformat(),
            "vix_close": round(float(vix_snapshots[entry_date].close_price), 4),
            "vix_prior_close": None
            if vix_snapshots[entry_date].prior_close_price is None
            else round(float(vix_snapshots[entry_date].prior_close_price), 4),
            "weekly_change_pct": None
            if vix_snapshots[entry_date].weekly_change_pct is None
            else round(float(vix_snapshots[entry_date].weekly_change_pct), 4),
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
            "strategy_type": StrategyType.LONG_PUT.value,
            "entry_day": "Friday",
            "vix_weekly_change_min_pct": float(args.vix_weekly_change_min_pct),
            "target_dte": int(args.target_dte),
            "dte_tolerance_days": int(args.dte_tolerance_days),
            "max_holding_days": MAX_HOLDING_DAYS,
            "delta_target": int(args.delta_target),
            "profit_target_pct": float(args.profit_target_pct),
            "position_sizing": "single_contract_per_signal",
        },
        "selection_counts": {
            "common_friday_count": len(common_fridays),
            "eligible_high_vix_friday_count": len(eligible_entry_dates),
            "selected_dates_by_symbol": selected_dates_by_symbol,
            "entered_counts_by_symbol": entered_counts_by_symbol,
        },
        "eligible_high_vix_fridays": eligible_high_vix_fridays,
        "summary": _summarize_records(trade_rows),
        "yearly_breakdown": _yearly_breakdown(trade_rows),
        "symbol_summaries": {
            symbol: _summarize_records(trades_by_symbol.get(symbol, []))
            for symbol in symbols
        },
        "symbol_yearly_breakdown": {
            symbol: _yearly_breakdown(trades_by_symbol.get(symbol, []))
            for symbol in symbols
        },
        "skipped_selected_dates": skipped_dates_by_symbol,
        "trades": trade_rows,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload["summary"], indent=2))
    print(
        json.dumps(
            {
                "eligible_high_vix_friday_count": len(eligible_entry_dates),
                "entered_counts_by_symbol": entered_counts_by_symbol,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
