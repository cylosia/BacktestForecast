from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import fmean, median
from types import SimpleNamespace
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, TradeResult, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.service import HistoricalDataBundle  # noqa: E402
from backtestforecast.market_data import vix_regime  # noqa: E402
from backtestforecast.schemas.backtests import StrategyOverrides, StrategyType, StrikeSelection, StrikeSelectionMode  # noqa: E402
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)


DEFAULT_SYMBOLS = ("UVIX", "UVXY", "VXX")
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_VIX_WEEKLY_CHANGE_MIN_PCT = 20.0
DEFAULT_DELTA_TARGET = 50
DEFAULT_PROFIT_TARGET_PCT = 50.0
STARTING_EQUITY = 100_000.0
TARGET_DTE = 7
FAR_LEG_TARGET_DTE = 14
DTE_TOLERANCE_DAYS = 0
MAX_HOLDING_DAYS = 10
LATEST_LOOKBACK_BUFFER_DAYS = 30
WARMUP_CALENDAR_DAYS = 210 * 3

DEFAULT_OUTPUT_JSON = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_put_calendar_weekly_summary.json"
DEFAULT_OUTPUT_CSV = ROOT / "logs" / "highVIX_uvix_uvxy_vxx_put_calendar_weekly_trades.csv"
IGNORED_ASSIGNMENT_EXIT_REASON = "early_assignment_put_deep_itm"
TESTED_SHORT_STRIKE_EXIT_REASON = "spot_close_below_short_strike"

LEDGER_FIELDS = [
    "symbol",
    "entry_date",
    "exit_date",
    "option_ticker",
    "strategy_type",
    "vix_weekly_change_pct",
    "vix_close",
    "vix_prior_close",
    "short_strike",
    "delta_target",
    "short_leg_target_dte",
    "far_leg_target_dte",
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

_ORIGINAL_CHECK_EARLY_ASSIGNMENT = OptionsBacktestEngine._check_early_assignment.__func__
_ORIGINAL_RESOLVE_EXIT = OptionsBacktestEngine._resolve_exit


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    symbol: str
    delta_target: int
    profit_target_pct: float


def _extract_short_put_strike_from_trade(trade: TradeResult) -> float | None:
    detail_json = getattr(trade, "detail_json", {}) or {}
    for leg in detail_json.get("legs", []):
        if leg.get("asset_type") != "option":
            continue
        if leg.get("side") != "short":
            continue
        if leg.get("contract_type") != "put":
            continue
        strike = leg.get("strike_price")
        return None if strike is None else float(strike)
    return None


def _install_highvix_put_short_strike_management() -> None:
    OptionsBacktestEngine._check_early_assignment = classmethod(_ORIGINAL_CHECK_EARLY_ASSIGNMENT)
    OptionsBacktestEngine._resolve_exit = staticmethod(_ORIGINAL_RESOLVE_EXIT)

    def _patched_check_early_assignment(
        cls,
        *,
        position: object,
        bar: object,
        ex_dividend_dates: set[date],
    ) -> tuple[str | None, dict[str, object] | None]:
        exit_reason, assignment_detail = _ORIGINAL_CHECK_EARLY_ASSIGNMENT(
            cls,
            position=position,
            bar=bar,
            ex_dividend_dates=ex_dividend_dates,
        )
        if exit_reason == IGNORED_ASSIGNMENT_EXIT_REASON:
            return None, None
        return exit_reason, assignment_detail

    def _patched_resolve_exit(
        bar,
        position,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
        *,
        position_value: float = 0.0,
        entry_cost: float = 0.0,
        capital_at_risk: float = 0.0,
        profit_target_pct: float | None = None,
        stop_loss_pct: float | None = None,
        current_bar_index: int | None = None,
    ) -> tuple[bool, str]:
        should_exit, exit_reason = _ORIGINAL_RESOLVE_EXIT(
            bar=bar,
            position=position,
            max_holding_days=max_holding_days,
            backtest_end_date=backtest_end_date,
            last_bar_date=last_bar_date,
            position_value=position_value,
            entry_cost=entry_cost,
            capital_at_risk=capital_at_risk,
            profit_target_pct=profit_target_pct,
            stop_loss_pct=stop_loss_pct,
            current_bar_index=current_bar_index,
        )
        if should_exit:
            return should_exit, exit_reason
        option_legs = getattr(position, "option_legs", None) or []
        short_put_leg = next(
            (
                leg
                for leg in option_legs
                if getattr(leg, "side", 0) < 0 and getattr(leg, "contract_type", None) == "put"
            ),
            None,
        )
        if short_put_leg is None:
            return False, ""
        bar_trade_date = getattr(bar, "trade_date", None)
        entry_date = getattr(position, "entry_date", None)
        if bar_trade_date is None:
            return False, ""
        if entry_date is not None and bar_trade_date <= entry_date:
            return False, ""
        if float(getattr(bar, "close_price")) <= float(getattr(short_put_leg, "strike_price")):
            return True, TESTED_SHORT_STRIKE_EXIT_REASON
        return False, ""

    OptionsBacktestEngine._check_early_assignment = classmethod(_patched_check_early_assignment)
    OptionsBacktestEngine._resolve_exit = staticmethod(_patched_resolve_exit)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a one-off highVIX weekly put-calendar replay for UVIX, UVXY, and VXX. "
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
        help="Absolute delta target for the short put leg. Defaults to 50.",
    )
    parser.add_argument(
        "--profit-target-pct",
        type=float,
        default=DEFAULT_PROFIT_TARGET_PCT,
        help="Profit target percent. Defaults to 50.",
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


def _parse_symbols(raw_value: str) -> tuple[str, ...]:
    symbols: list[str] = []
    seen: set[str] = set()
    for chunk in raw_value.split(","):
        symbol = chunk.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    if not symbols:
        raise SystemExit("At least one symbol is required.")
    return tuple(symbols)


def _build_store() -> HistoricalMarketDataStore:
    return HistoricalMarketDataStore(create_session, create_readonly_session)


def _build_bundle(
    store: HistoricalMarketDataStore,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> HistoricalDataBundle:
    warmup_start = start_date - timedelta(days=WARMUP_CALENDAR_DAYS)
    return HistoricalDataBundle(
        bars=store.get_underlying_day_bars(symbol, warmup_start, end_date),
        earnings_dates=store.list_earnings_event_dates(symbol, warmup_start, end_date),
        ex_dividend_dates=store.list_ex_dividend_dates(symbol, warmup_start, end_date),
        option_gateway=HistoricalOptionGateway(store, symbol),
        data_source="local",
        entry_rule_cache=EntryRuleComputationCache(),
    )


def _resolve_common_latest_available_date(
    store: HistoricalMarketDataStore,
    *,
    symbols: tuple[str, ...],
    requested_end_date: date,
) -> date:
    latest_dates: list[date] = []
    for symbol in symbols:
        bars = store.get_underlying_day_bars(
            symbol,
            requested_end_date - timedelta(days=LATEST_LOOKBACK_BUFFER_DAYS),
            requested_end_date,
        )
        if not bars:
            raise SystemExit(f"Missing recent underlying bars for {symbol}.")
        latest_dates.append(max(bar.trade_date for bar in bars))
    return min(min(latest_dates), requested_end_date)


def _load_risk_free_curve(
    store: HistoricalMarketDataStore,
    *,
    start_date: date,
    end_date: date,
) -> RiskFreeRateCurve:
    local_series = store.get_treasury_yield_series(start_date, end_date)
    default_rate = store.get_average_treasury_yield(start_date, start_date)
    if default_rate is None:
        default_rate = estimate_risk_free_rate(start_date, end_date)
    if local_series:
        ordered_dates = tuple(sorted(local_series))
        ordered_rates = tuple(float(local_series[trade_date]) for trade_date in ordered_dates)
        return RiskFreeRateCurve(
            default_rate=float(default_rate),
            dates=ordered_dates,
            rates=ordered_rates,
        )
    return RiskFreeRateCurve(default_rate=float(default_rate))


def _build_calendar_config(
    *,
    strategy: StrategyConfig,
    entry_date: date,
    replay_data_end: date,
    risk_free_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    return BacktestConfig(
        symbol=strategy.symbol,
        strategy_type=StrategyType.PUT_CALENDAR_SPREAD.value,
        start_date=entry_date,
        end_date=min(replay_data_end, entry_date + timedelta(days=35)),
        target_dte=TARGET_DTE,
        dte_tolerance_days=DTE_TOLERANCE_DAYS,
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
            calendar_far_leg_target_dte=FAR_LEG_TARGET_DTE,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        ),
        profit_target_pct=float(strategy.profit_target_pct),
        stop_loss_pct=None,
    )


def _trade_roi_on_margin_pct(trade: TradeResult) -> float | None:
    capital_required = trade.detail_json.get("capital_required_per_unit")
    if capital_required is None:
        return None
    total_capital = float(capital_required) * float(trade.quantity or 1)
    if total_capital <= 0:
        return None
    return float(trade.net_pnl) / total_capital * 100.0


def _summarize_records(records: list[dict[str, object]]) -> dict[str, object]:
    pnls = [float(item["net_pnl"]) for item in records]
    rois = [float(item["roi_on_margin_pct"]) for item in records if item["roi_on_margin_pct"] is not None]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    total_net_pnl = sum(pnls)
    return {
        "starting_equity": STARTING_EQUITY,
        "ending_equity": round(STARTING_EQUITY + total_net_pnl, 4),
        "total_net_pnl": round(total_net_pnl, 4),
        "total_roi_pct": round(total_net_pnl / STARTING_EQUITY * 100.0, 4),
        "trade_count": len(records),
        "win_rate_pct": round(len(wins) / len(records) * 100.0, 4) if records else 0.0,
        "average_win": round(fmean(wins), 4) if wins else 0.0,
        "average_loss": round(fmean(losses), 4) if losses else 0.0,
        "max_win": round(max(wins), 4) if wins else 0.0,
        "max_loss": round(min(losses), 4) if losses else 0.0,
        "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
        "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
    }


def _yearly_breakdown(records: list[dict[str, object]]) -> list[dict[str, object]]:
    buckets: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        buckets[str(record["entry_date"])[:4]].append(record)
    rows: list[dict[str, object]] = []
    for year in sorted(buckets):
        bucket = buckets[year]
        rois = [float(item["roi_on_margin_pct"]) for item in bucket if item["roi_on_margin_pct"] is not None]
        pnl = sum(float(item["net_pnl"]) for item in bucket)
        rows.append(
            {
                "year": year,
                "trade_count": len(bucket),
                "net_pnl": round(pnl, 4),
                "roi_pct": round(pnl / STARTING_EQUITY * 100.0, 4),
                "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
                "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
            }
        )
    return rows


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
    _install_highvix_put_short_strike_management()

    store = _build_store()
    common_latest_available_date = _resolve_common_latest_available_date(
        store,
        symbols=symbols,
        requested_end_date=args.requested_end_date,
    )
    entry_end_date = common_latest_available_date - timedelta(days=FAR_LEG_TARGET_DTE)
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
    skipped_dates_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)

    for symbol in symbols:
        strategy = StrategyConfig(
            symbol=symbol,
            delta_target=args.delta_target,
            profit_target_pct=args.profit_target_pct,
        )
        bundle = bundles[symbol]
        for entry_date in eligible_entry_dates:
            selected_dates_by_symbol[symbol] += 1
            if entry_date not in friday_dates_by_symbol[symbol]:
                skipped_dates_by_symbol[symbol].append(
                    {
                        "entry_date": entry_date.isoformat(),
                        "reason": "missing_underlying_bar",
                    }
                )
                continue
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
                skipped_dates_by_symbol[symbol].append(
                    {
                        "entry_date": entry_date.isoformat(),
                        "reason": "no_trade_from_engine",
                    }
                )
                continue
            entered_counts_by_symbol[symbol] += 1
            vix_snapshot = vix_snapshots[entry_date]
            short_strike = _extract_short_put_strike_from_trade(trade)
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
                    "short_strike": None if short_strike is None else round(float(short_strike), 4),
                    "delta_target": args.delta_target,
                    "short_leg_target_dte": TARGET_DTE,
                    "far_leg_target_dte": FAR_LEG_TARGET_DTE,
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
                    "roi_on_margin_pct": None
                    if _trade_roi_on_margin_pct(trade) is None
                    else round(float(_trade_roi_on_margin_pct(trade)), 4),
                    "total_commissions": round(float(trade.total_commissions), 4),
                    "entry_reason": trade.entry_reason,
                    "exit_reason": trade.exit_reason,
                    "detail_json": json.dumps(trade.detail_json, sort_keys=True, default=str),
                }
            )

    trade_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"])))
    _write_ledger_csv(output_csv=args.output_csv, rows=trade_rows)

    trades_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)
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
            "entry_day": "Friday",
            "vix_weekly_change_min_pct": float(args.vix_weekly_change_min_pct),
            "short_leg_target_dte": TARGET_DTE,
            "far_leg_target_dte": FAR_LEG_TARGET_DTE,
            "dte_tolerance_days": DTE_TOLERANCE_DAYS,
            "max_holding_days": MAX_HOLDING_DAYS,
            "delta_target": int(args.delta_target),
            "profit_target_pct": float(args.profit_target_pct),
            "requires_short_iv_gt_long_iv": False,
            "position_sizing": "single_contract_per_signal",
            "ignored_assignment_exit_reason": IGNORED_ASSIGNMENT_EXIT_REASON,
            "tested_short_strike_exit_reason": TESTED_SHORT_STRIKE_EXIT_REASON,
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
