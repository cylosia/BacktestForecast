from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.calendar_adjustments import (  # noqa: E402
    default_calendar_adjustment_policies,
    run_adjusted_calendar_backtest,
)
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.pipeline.regime import Regime  # noqa: E402
from backtestforecast.schemas.backtests import RegimeRule, StrategyOverrides, StrikeSelection, StrikeSelectionMode  # noqa: E402
from backtestforecast.services.serialization import serialize_summary, serialize_trade  # noqa: E402

DEFAULT_SYMBOL = "UVXY"
DEFAULT_START = date(2018, 3, 1)
DEFAULT_END = date(2024, 12, 31)
DEFAULT_REGIME_LABEL = "bullish_trending"
DEFAULT_REQUIRED_REGIMES = ("bullish", "trending")
DEFAULT_BLOCKED_REGIMES: tuple[str, ...] = ()
DEFAULT_DELTA_TARGET = 60
DEFAULT_SHORT_DTE = 11
DEFAULT_LONG_DTE = 18
DEFAULT_PROFIT_TARGET_PCT = 20.0
ACCOUNT_SIZE = Decimal("100000")
RISK_PER_TRADE_PCT = Decimal("100")
COMMISSION_PER_CONTRACT = Decimal("0.65")
SLIPPAGE_PCT = 0.0
MAX_HOLDING_DAYS = 120
WARMUP_CALENDAR_DAYS = 210 * 3

DEFAULT_SUMMARY_CSV = ROOT / "logs" / "uvxy_put_calendar_adjustment_policy_compare_summary.csv"
DEFAULT_LEDGER_CSV = ROOT / "logs" / "uvxy_put_calendar_adjustment_policy_compare_ledger.csv"
DEFAULT_SUMMARY_JSON = ROOT / "logs" / "uvxy_put_calendar_adjustment_policy_compare_summary.json"

SUMMARY_FIELDS = [
    "policy_name",
    "trade_count",
    "decided_trades",
    "win_rate",
    "total_net_pnl",
    "total_roi_pct",
    "max_drawdown_pct",
    "profit_factor",
    "payoff_ratio",
    "expectancy",
    "average_holding_period_days",
    "recovery_factor",
    "adjusted_trade_count",
    "adjustment_event_count",
    "warning_codes",
]

LEDGER_FIELDS = [
    "policy_name",
    "option_ticker",
    "strategy_type",
    "underlying_symbol",
    "entry_date",
    "exit_date",
    "expiration_date",
    "quantity",
    "dte_at_open",
    "holding_period_days",
    "entry_underlying_close",
    "exit_underlying_close",
    "entry_mid",
    "exit_mid",
    "gross_pnl",
    "net_pnl",
    "total_commissions",
    "entry_reason",
    "exit_reason",
    "adjustment_event_count",
    "detail_json",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run UVXY calendar adjustment policy comparisons or a single selected policy.",
    )
    parser.add_argument(
        "--policy",
        action="append",
        default=[],
        help="Specific policy name to run. Repeat to run multiple. Defaults to all policies.",
    )
    parser.add_argument("--summary-csv", type=Path, default=None)
    parser.add_argument("--ledger-csv", type=Path, default=None)
    parser.add_argument("--summary-json", type=Path, default=None)
    return parser.parse_args()


def _resolve_selected_policies(selected_names: list[str]) -> list[Any]:
    available = {policy.name: policy for policy in default_calendar_adjustment_policies()}
    if not selected_names:
        return list(available.values())
    selected: list[Any] = []
    seen: set[str] = set()
    for name in selected_names:
        if name not in available:
            raise ValueError(f"unknown policy: {name}")
        if name in seen:
            continue
        seen.add(name)
        selected.append(available[name])
    return selected


def _default_outputs_for_policies(policies: list[Any]) -> tuple[Path, Path, Path]:
    if len(policies) == 1:
        suffix = policies[0].name
        return (
            ROOT / "logs" / f"uvxy_put_calendar_adjustment_policy_{suffix}_summary.csv",
            ROOT / "logs" / f"uvxy_put_calendar_adjustment_policy_{suffix}_ledger.csv",
            ROOT / "logs" / f"uvxy_put_calendar_adjustment_policy_{suffix}_summary.json",
        )
    return DEFAULT_SUMMARY_CSV, DEFAULT_LEDGER_CSV, DEFAULT_SUMMARY_JSON


def _build_store() -> HistoricalMarketDataStore:
    return HistoricalMarketDataStore(
        session_factory=create_session,
        readonly_session_factory=create_readonly_session,
    )


def _build_bundle(
    *,
    store: HistoricalMarketDataStore,
    symbol: str,
    start_date: date,
    end_date: date,
) -> tuple[list[Any], set[date], set[date], HistoricalOptionGateway]:
    warmup_start = start_date - timedelta(days=WARMUP_CALENDAR_DAYS)
    bars = store.get_underlying_day_bars(symbol, warmup_start, end_date)
    earnings_dates = store.list_earnings_event_dates(symbol, warmup_start, end_date)
    ex_dividend_dates = store.list_ex_dividend_dates(symbol, warmup_start, end_date)
    option_gateway = HistoricalOptionGateway(store, symbol)
    return bars, earnings_dates, ex_dividend_dates, option_gateway


def _build_risk_free_rate_curve(
    *,
    store: HistoricalMarketDataStore,
    start_date: date,
    end_date: date,
) -> RiskFreeRateCurve:
    series = store.get_treasury_yield_series(start_date, end_date)
    default_rate = store.get_average_treasury_yield(start_date, start_date)
    if default_rate is None:
        default_rate = estimate_risk_free_rate(start_date, end_date)
    if not series:
        return RiskFreeRateCurve(default_rate=default_rate)
    ordered_dates = tuple(sorted(series))
    ordered_rates = tuple(float(series[trade_date]) for trade_date in ordered_dates)
    return RiskFreeRateCurve(
        default_rate=float(default_rate),
        dates=ordered_dates,
        rates=ordered_rates,
    )


def _build_entry_rules() -> list[RegimeRule]:
    return [
        RegimeRule(
            type="regime",
            required_regimes=[Regime(value) for value in DEFAULT_REQUIRED_REGIMES],
            blocked_regimes=[Regime(value) for value in DEFAULT_BLOCKED_REGIMES],
        )
    ]


def _build_config(
    *,
    risk_free_rate_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    return BacktestConfig(
        symbol=DEFAULT_SYMBOL,
        strategy_type="calendar_spread",
        start_date=DEFAULT_START,
        end_date=DEFAULT_END,
        target_dte=DEFAULT_SHORT_DTE,
        dte_tolerance_days=0,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=ACCOUNT_SIZE,
        risk_per_trade_pct=RISK_PER_TRADE_PCT,
        commission_per_contract=COMMISSION_PER_CONTRACT,
        entry_rules=_build_entry_rules(),
        risk_free_rate=risk_free_rate_curve.default_rate,
        risk_free_rate_curve=risk_free_rate_curve,
        slippage_pct=SLIPPAGE_PCT,
        strategy_overrides=StrategyOverrides(
            calendar_contract_type="put",
            calendar_far_leg_target_dte=DEFAULT_LONG_DTE,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(DEFAULT_DELTA_TARGET)),
            ),
        ),
        profit_target_pct=DEFAULT_PROFIT_TARGET_PCT,
    )


def _write_summary_csv(
    *,
    output_csv: Path,
    rows: list[dict[str, Any]],
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_ledger_csv(
    *,
    output_csv: Path,
    rows: list[dict[str, Any]],
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = _parse_args()
    policies = _resolve_selected_policies(args.policy)
    default_summary_csv, default_ledger_csv, default_summary_json = _default_outputs_for_policies(policies)
    summary_csv = args.summary_csv or default_summary_csv
    ledger_csv = args.ledger_csv or default_ledger_csv
    summary_json = args.summary_json or default_summary_json

    store = _build_store()
    bars, earnings_dates, ex_dividend_dates, option_gateway = _build_bundle(
        store=store,
        symbol=DEFAULT_SYMBOL,
        start_date=DEFAULT_START,
        end_date=DEFAULT_END,
    )
    risk_free_rate_curve = _build_risk_free_rate_curve(
        store=store,
        start_date=DEFAULT_START,
        end_date=DEFAULT_END,
    )
    base_config = _build_config(risk_free_rate_curve=risk_free_rate_curve)

    summary_rows: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
    result_payloads: list[dict[str, Any]] = []
    shared_entry_rule_cache = EntryRuleComputationCache()

    for policy in policies:
        result = run_adjusted_calendar_backtest(
            config=base_config,
            bars=bars,
            earnings_dates=earnings_dates,
            option_gateway=option_gateway,
            policy=policy,
            ex_dividend_dates=ex_dividend_dates,
            shared_entry_rule_cache=shared_entry_rule_cache,
            force_single_contract=True,
        )
        warning_codes = sorted({str(item.get("code")) for item in result.warnings if item.get("code")})
        adjusted_trade_count = sum(
            1
            for trade in result.trades
            if trade.detail_json.get("campaign_adjustment_events")
        )
        adjustment_event_count = sum(
            len(trade.detail_json.get("campaign_adjustment_events", []))
            for trade in result.trades
        )
        summary_rows.append(
            {
                "policy_name": policy.name,
                "trade_count": result.summary.trade_count,
                "decided_trades": result.summary.decided_trades,
                "win_rate": result.summary.win_rate,
                "total_net_pnl": result.summary.total_net_pnl,
                "total_roi_pct": result.summary.total_roi_pct,
                "max_drawdown_pct": result.summary.max_drawdown_pct,
                "profit_factor": result.summary.profit_factor,
                "payoff_ratio": result.summary.payoff_ratio,
                "expectancy": result.summary.expectancy,
                "average_holding_period_days": result.summary.average_holding_period_days,
                "recovery_factor": result.summary.recovery_factor,
                "adjusted_trade_count": adjusted_trade_count,
                "adjustment_event_count": adjustment_event_count,
                "warning_codes": ";".join(warning_codes),
            }
        )
        for trade in result.trades:
            serialized_trade = serialize_trade(trade)
            ledger_rows.append(
                {
                    "policy_name": policy.name,
                    "option_ticker": serialized_trade["option_ticker"],
                    "strategy_type": serialized_trade["strategy_type"],
                    "underlying_symbol": serialized_trade["underlying_symbol"],
                    "entry_date": serialized_trade["entry_date"],
                    "exit_date": serialized_trade["exit_date"],
                    "expiration_date": serialized_trade["expiration_date"],
                    "quantity": serialized_trade["quantity"],
                    "dte_at_open": serialized_trade["dte_at_open"],
                    "holding_period_days": serialized_trade["holding_period_days"],
                    "entry_underlying_close": serialized_trade["entry_underlying_close"],
                    "exit_underlying_close": serialized_trade["exit_underlying_close"],
                    "entry_mid": serialized_trade["entry_mid"],
                    "exit_mid": serialized_trade["exit_mid"],
                    "gross_pnl": serialized_trade["gross_pnl"],
                    "net_pnl": serialized_trade["net_pnl"],
                    "total_commissions": serialized_trade["total_commissions"],
                    "entry_reason": serialized_trade["entry_reason"],
                    "exit_reason": serialized_trade["exit_reason"],
                    "adjustment_event_count": len(trade.detail_json.get("campaign_adjustment_events", [])),
                    "detail_json": json.dumps(serialized_trade["detail_json"], sort_keys=True),
                }
            )
        result_payloads.append(
            {
                "policy_name": policy.name,
                "summary": serialize_summary(result.summary),
                "warning_codes": warning_codes,
                "adjusted_trade_count": adjusted_trade_count,
                "adjustment_event_count": adjustment_event_count,
            }
        )

    _write_summary_csv(output_csv=summary_csv, rows=summary_rows)
    _write_ledger_csv(output_csv=ledger_csv, rows=ledger_rows)

    payload = {
        "symbol": DEFAULT_SYMBOL,
        "start_date": DEFAULT_START.isoformat(),
        "end_date": DEFAULT_END.isoformat(),
        "regime_label": DEFAULT_REGIME_LABEL,
        "required_regimes": list(DEFAULT_REQUIRED_REGIMES),
        "blocked_regimes": list(DEFAULT_BLOCKED_REGIMES),
        "delta_target": DEFAULT_DELTA_TARGET,
        "short_leg_target_dte": DEFAULT_SHORT_DTE,
        "long_leg_target_dte": DEFAULT_LONG_DTE,
        "profit_target_pct": DEFAULT_PROFIT_TARGET_PCT,
        "selected_policies": [policy.name for policy in policies],
        "summary_csv": str(summary_csv),
        "ledger_csv": str(ledger_csv),
        "results": result_payloads,
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
