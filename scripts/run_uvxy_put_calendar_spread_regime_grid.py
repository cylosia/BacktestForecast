from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.calendar_adjustments import (  # noqa: E402
    CalendarAdjustmentPolicy,
    default_calendar_adjustment_policies,
    run_adjusted_calendar_backtest,
)
from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.pipeline.regime import Regime  # noqa: E402
from backtestforecast.schemas.backtests import RegimeRule, StrategyOverrides, StrikeSelection, StrikeSelectionMode  # noqa: E402

from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

DEFAULT_SYMBOL = "UVXY"
DEFAULT_START = date(2018, 3, 1)
DEFAULT_END = date(2024, 12, 31)
ACCOUNT_SIZE = Decimal("100000")
RISK_PER_TRADE_PCT = Decimal("100")
COMMISSION_PER_CONTRACT = Decimal("0.65")
SLIPPAGE_PCT = Decimal("0")
MAX_HOLDING_DAYS = 120
WARMUP_CALENDAR_DAYS = 210 * 3
STATUS_UPDATE_EVERY_BASE_COMBOS = 25
STATUS_UPDATE_MIN_SECONDS = 15.0
CSV_FLUSH_EVERY_BASE_COMBOS = 10

DEFAULT_OUTPUT_CSV = ROOT / "logs" / "uvxy_put_calendar_spread_2018_03_2024_12_regime_grid.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "uvxy_put_calendar_spread_2018_03_2024_12_regime_grid.json"
ADJUSTED_OUTPUT_PREFIX = "uvxy_put_calendar_spread_2018_03_2024_12_all_regimes_grid"

DEFAULT_DELTA_VALUES = list(range(30, 71, 5))
DEFAULT_SHORT_DTE_VALUES = list(range(1, 16))
DEFAULT_LONG_DTE_VALUES = list(range(8, 29))
DEFAULT_PROFIT_VALUES = list(range(20, 201, 10))

GRID_FIELDS = [
    "regime_label",
    "required_regimes",
    "blocked_regimes",
    "delta_target",
    "short_leg_target_dte",
    "long_leg_target_dte",
    "profit_target_pct",
    "status",
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
    "average_dte_at_open",
    "total_commissions",
    "sharpe_ratio",
    "sortino_ratio",
    "cagr_pct",
    "calmar_ratio",
    "max_consecutive_wins",
    "max_consecutive_losses",
    "recovery_factor",
    "warning_codes",
    "error",
]


@dataclass(frozen=True, slots=True)
class RegimeProfile:
    label: str
    required_regimes: tuple[str, ...]
    blocked_regimes: tuple[str, ...] = ()


REGIME_PROFILES = {
    "bearish": RegimeProfile("bearish", ("bearish",)),
    "bearish_not_high_iv": RegimeProfile("bearish_not_high_iv", ("bearish",), ("high_iv",)),
    "bearish_low_iv": RegimeProfile("bearish_low_iv", ("bearish", "low_iv")),
    "bearish_trending": RegimeProfile("bearish_trending", ("bearish", "trending")),
    "bearish_low_iv_trending": RegimeProfile("bearish_low_iv_trending", ("bearish", "low_iv", "trending")),
    "bullish": RegimeProfile("bullish", ("bullish",)),
    "bullish_low_iv": RegimeProfile("bullish_low_iv", ("bullish", "low_iv")),
    "bullish_trending": RegimeProfile("bullish_trending", ("bullish", "trending")),
    "bullish_low_iv_trending": RegimeProfile("bullish_low_iv_trending", ("bullish", "low_iv", "trending")),
    "neutral": RegimeProfile("neutral", ("neutral",)),
    "neutral_low_iv": RegimeProfile("neutral_low_iv", ("neutral", "low_iv")),
    "neutral_trending": RegimeProfile("neutral_trending", ("neutral", "trending")),
    "neutral_low_iv_trending": RegimeProfile("neutral_low_iv_trending", ("neutral", "low_iv", "trending")),
}

DEFAULT_REGIME_LABELS = [
    "bearish",
    "bearish_not_high_iv",
    "bearish_low_iv",
    "bearish_trending",
    "bearish_low_iv_trending",
]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_int_values(value: str | None, *, default: list[int]) -> list[int]:
    if value is None or not value.strip():
        return list(default)
    parsed: list[int] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            pieces = [piece.strip() for piece in part.split(":")]
            range_part = pieces[0]
            step = int(pieces[1]) if len(pieces) == 2 else 1
            start_text, end_text = [piece.strip() for piece in range_part.split("-", 1)]
            start = int(start_text)
            end = int(end_text)
            if step <= 0:
                raise ValueError("range step must be positive")
            parsed.extend(list(range(start, end + 1, step)))
        else:
            parsed.append(int(part))
    if not parsed:
        raise ValueError("at least one integer value is required")
    return sorted(dict.fromkeys(parsed))


def _parse_regime_labels(value: str | None) -> list[str]:
    if value is None or not value.strip():
        return list(DEFAULT_REGIME_LABELS)
    labels = []
    for raw_part in value.split(","):
        label = raw_part.strip()
        if not label:
            continue
        if label not in REGIME_PROFILES:
            raise ValueError(f"unknown regime label: {label}")
        labels.append(label)
    if not labels:
        raise ValueError("at least one regime label is required")
    return labels


def _resolve_adjustment_policy(value: str | None) -> CalendarAdjustmentPolicy | None:
    if value is None or not value.strip():
        return None
    available = {policy.name: policy for policy in default_calendar_adjustment_policies()}
    policy = available.get(value.strip())
    if policy is None:
        raise ValueError(f"unknown adjustment policy: {value}")
    return policy


def _default_adjusted_output_csv(policy: CalendarAdjustmentPolicy) -> Path:
    return ROOT / "logs" / f"{ADJUSTED_OUTPUT_PREFIX}_{policy.name}.csv"


def _default_adjusted_output_json(policy: CalendarAdjustmentPolicy) -> Path:
    return ROOT / "logs" / f"{ADJUSTED_OUTPUT_PREFIX}_{policy.name}.json"


def _build_entry_rules(profile: RegimeProfile) -> list[RegimeRule]:
    return [
        RegimeRule(
            type="regime",
            required_regimes=[Regime(value) for value in profile.required_regimes],
            blocked_regimes=[Regime(value) for value in profile.blocked_regimes],
        )
    ]


def _build_store() -> HistoricalMarketDataStore:
    return HistoricalMarketDataStore(
        session_factory=create_session,
        readonly_session_factory=create_readonly_session,
    )


def _suppress_engine_info_logs() -> None:
    existing_logger = engine_module.logger
    engine_module.logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=getattr(existing_logger, "warning", lambda *args, **kwargs: None),
        debug=getattr(existing_logger, "debug", lambda *args, **kwargs: None),
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


def _build_config(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    profile: RegimeProfile,
    delta_target: int,
    short_leg_target_dte: int,
    long_leg_target_dte: int,
    profit_target_pct: int,
    risk_free_rate_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    return BacktestConfig(
        symbol=symbol,
        strategy_type="calendar_spread",
        start_date=start_date,
        end_date=end_date,
        target_dte=short_leg_target_dte,
        dte_tolerance_days=0,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=ACCOUNT_SIZE,
        risk_per_trade_pct=RISK_PER_TRADE_PCT,
        commission_per_contract=COMMISSION_PER_CONTRACT,
        entry_rules=_build_entry_rules(profile),
        risk_free_rate=risk_free_rate_curve.default_rate,
        risk_free_rate_curve=risk_free_rate_curve,
        slippage_pct=float(SLIPPAGE_PCT),
        strategy_overrides=StrategyOverrides(
            calendar_contract_type="put",
            calendar_far_leg_target_dte=long_leg_target_dte,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_target)),
            ),
        ),
        profit_target_pct=float(profit_target_pct),
    )


def _summary_row(
    *,
    profile: RegimeProfile,
    delta_target: int,
    short_leg_target_dte: int,
    long_leg_target_dte: int,
    profit_target_pct: int,
    result: Any,
) -> dict[str, Any]:
    summary = result.summary
    warning_codes = ";".join(
        sorted({str(item.get("code")) for item in result.warnings if item.get("code")})
    )
    return {
        "regime_label": profile.label,
        "required_regimes": ";".join(profile.required_regimes),
        "blocked_regimes": ";".join(profile.blocked_regimes),
        "delta_target": delta_target,
        "short_leg_target_dte": short_leg_target_dte,
        "long_leg_target_dte": long_leg_target_dte,
        "profit_target_pct": profit_target_pct,
        "status": "ok",
        "trade_count": summary.trade_count,
        "decided_trades": summary.decided_trades,
        "win_rate": summary.win_rate,
        "total_net_pnl": summary.total_net_pnl,
        "total_roi_pct": summary.total_roi_pct,
        "max_drawdown_pct": summary.max_drawdown_pct,
        "profit_factor": summary.profit_factor,
        "payoff_ratio": summary.payoff_ratio,
        "expectancy": summary.expectancy,
        "average_holding_period_days": summary.average_holding_period_days,
        "average_dte_at_open": summary.average_dte_at_open,
        "total_commissions": summary.total_commissions,
        "sharpe_ratio": summary.sharpe_ratio,
        "sortino_ratio": summary.sortino_ratio,
        "cagr_pct": summary.cagr_pct,
        "calmar_ratio": summary.calmar_ratio,
        "max_consecutive_wins": summary.max_consecutive_wins,
        "max_consecutive_losses": summary.max_consecutive_losses,
        "recovery_factor": summary.recovery_factor,
        "warning_codes": warning_codes,
        "error": "",
    }


def _error_row(
    *,
    profile: RegimeProfile,
    delta_target: int,
    short_leg_target_dte: int,
    long_leg_target_dte: int,
    profit_target_pct: int,
    error: Exception,
) -> dict[str, Any]:
    return {
        "regime_label": profile.label,
        "required_regimes": ";".join(profile.required_regimes),
        "blocked_regimes": ";".join(profile.blocked_regimes),
        "delta_target": delta_target,
        "short_leg_target_dte": short_leg_target_dte,
        "long_leg_target_dte": long_leg_target_dte,
        "profit_target_pct": profit_target_pct,
        "status": "error",
        "trade_count": "",
        "decided_trades": "",
        "win_rate": "",
        "total_net_pnl": "",
        "total_roi_pct": "",
        "max_drawdown_pct": "",
        "profit_factor": "",
        "payoff_ratio": "",
        "expectancy": "",
        "average_holding_period_days": "",
        "average_dte_at_open": "",
        "total_commissions": "",
        "sharpe_ratio": "",
        "sortino_ratio": "",
        "cagr_pct": "",
        "calmar_ratio": "",
        "max_consecutive_wins": "",
        "max_consecutive_losses": "",
        "recovery_factor": "",
        "warning_codes": "",
        "error": str(error),
    }


def _row_key(row: dict[str, Any]) -> tuple[str, int, int, int, int]:
    return (
        str(row["regime_label"]),
        int(row["delta_target"]),
        int(row["short_leg_target_dte"]),
        int(row["long_leg_target_dte"]),
        int(row["profit_target_pct"]),
    )


def _base_key(profile: RegimeProfile, delta_target: int, short_leg_target_dte: int, long_leg_target_dte: int) -> tuple[str, int, int, int]:
    return (profile.label, delta_target, short_leg_target_dte, long_leg_target_dte)


def _load_existing_rows(path: Path) -> dict[tuple[str, int, int, int], set[int]]:
    existing: dict[tuple[str, int, int, int], set[int]] = {}
    if not path.exists():
        return existing
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                key = (
                    str(row["regime_label"]),
                    int(row["delta_target"]),
                    int(row["short_leg_target_dte"]),
                    int(row["long_leg_target_dte"]),
                )
                profit_target = int(row["profit_target_pct"])
            except Exception:
                continue
            existing.setdefault(key, set()).add(profit_target)
    return existing


def _append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=GRID_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _update_status(
    *,
    path: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    regime_labels: list[str],
    delta_values: list[int],
    short_dte_values: list[int],
    long_dte_values: list[int],
    profit_values: list[int],
    started_at: float,
    resumed_completed_base_combos: int,
    completed_base_combos: int,
    total_base_combos: int,
    written_rows: int,
    output_csv: Path,
    adjustment_policy: str | None,
    best_row: dict[str, Any] | None,
    last_completed_base_key: tuple[str, int, int, int] | None,
) -> None:
    elapsed_seconds = time.time() - started_at
    session_completed_base_combos = max(completed_base_combos - resumed_completed_base_combos, 0)
    combos_remaining = max(total_base_combos - completed_base_combos, 0)
    avg_seconds_per_base = (elapsed_seconds / session_completed_base_combos) if session_completed_base_combos else None
    eta_seconds = (avg_seconds_per_base * combos_remaining) if avg_seconds_per_base is not None else None
    payload = {
        "symbol": symbol,
        "strategy_type": "calendar_spread",
        "calendar_contract_type": "put",
        "data_source": "historical_flatfile",
        "adjustment_policy": adjustment_policy,
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "regime_labels": regime_labels,
        "delta_values": delta_values,
        "short_leg_target_dte_values": short_dte_values,
        "long_leg_target_dte_values": long_dte_values,
        "profit_target_pct_values": profit_values,
        "output_csv": str(output_csv),
        "total_base_combos": total_base_combos,
        "resumed_completed_base_combos": resumed_completed_base_combos,
        "session_completed_base_combos": session_completed_base_combos,
        "completed_base_combos": completed_base_combos,
        "remaining_base_combos": combos_remaining,
        "written_rows": written_rows,
        "elapsed_seconds": elapsed_seconds,
        "average_seconds_per_base_combo": avg_seconds_per_base,
        "eta_seconds": eta_seconds,
        "last_completed_base_key": last_completed_base_key,
        "best_row_by_total_roi_pct": best_row,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S%z"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _row_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    if row.get("status") != "ok":
        return (-float("inf"), -float("inf"), -float("inf"), -float("inf"))
    return (
        float(row.get("total_roi_pct") or 0.0),
        float(row.get("total_net_pnl") or 0.0),
        -float(row.get("max_drawdown_pct") or 0.0),
        float(row.get("trade_count") or 0.0),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a UVXY put calendar spread grid over local historical data with regime filters, "
            "explicit short-leg and long-leg DTE targets, and multi-profit-target exit lanes."
        )
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", type=_parse_date, default=DEFAULT_START)
    parser.add_argument("--end-date", type=_parse_date, default=DEFAULT_END)
    parser.add_argument("--delta-values", default=None, help="Comma-separated ints and/or ranges like 30-70:5")
    parser.add_argument("--short-dte-values", default=None, help="Comma-separated ints and/or ranges like 1-15")
    parser.add_argument("--long-dte-values", default=None, help="Comma-separated ints and/or ranges like 8-28")
    parser.add_argument("--profit-values", default=None, help="Comma-separated ints and/or ranges like 20-200:10")
    parser.add_argument(
        "--regime-labels",
        default=None,
        help="Comma-separated regime profile labels. Defaults to the five bearish UVXY profiles.",
    )
    parser.add_argument(
        "--adjustment-policy",
        default=None,
        help="Optional calendar adjustment policy name. Example: hold_long_only_if_short_otm",
    )
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    args = parser.parse_args()

    delta_values = _parse_int_values(args.delta_values, default=DEFAULT_DELTA_VALUES)
    short_dte_values = _parse_int_values(args.short_dte_values, default=DEFAULT_SHORT_DTE_VALUES)
    long_dte_values = _parse_int_values(args.long_dte_values, default=DEFAULT_LONG_DTE_VALUES)
    profit_values = _parse_int_values(args.profit_values, default=DEFAULT_PROFIT_VALUES)
    regime_labels = _parse_regime_labels(args.regime_labels)
    adjustment_policy = _resolve_adjustment_policy(args.adjustment_policy)
    if adjustment_policy is not None:
        if args.output_csv == DEFAULT_OUTPUT_CSV:
            args.output_csv = _default_adjusted_output_csv(adjustment_policy)
        if args.output_json == DEFAULT_OUTPUT_JSON:
            args.output_json = _default_adjusted_output_json(adjustment_policy)
    regime_profiles = [REGIME_PROFILES[label] for label in regime_labels]

    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    _suppress_engine_info_logs()

    started_at = time.time()
    existing = _load_existing_rows(args.output_csv)
    written_rows = sum(len(profits) for profits in existing.values())
    best_row: dict[str, Any] | None = None

    # Keep the short/long DTE pair outermost so the calendar-group contract cache
    # stays hot across all deltas and regime profiles for the same expiration pair.
    # Within each pair, keep delta outermost and batch every regime profile
    # together. That lets the engine reuse one structural replay across all
    # regime masks and profit-target lanes for the same delta / DTE pair.
    base_combos = [
        (profile, delta_target, short_leg_target_dte, long_leg_target_dte)
        for short_leg_target_dte in short_dte_values
        for long_leg_target_dte in long_dte_values
        if long_leg_target_dte > short_leg_target_dte
        for delta_target in delta_values
        for profile in regime_profiles
    ]
    total_base_combos = len(base_combos)
    completed_base_combos = sum(
        1
        for profile, delta_target, short_leg_target_dte, long_leg_target_dte in base_combos
        if existing.get(_base_key(profile, delta_target, short_leg_target_dte, long_leg_target_dte), set()) >= set(profit_values)
    )
    resumed_completed_base_combos = completed_base_combos
    combo_groups = [
        (
            delta_target,
            short_leg_target_dte,
            long_leg_target_dte,
            [
                (profile, missing_profit_values)
                for profile in regime_profiles
                if (
                    missing_profit_values := [
                        profit
                        for profit in profit_values
                        if profit not in existing.get(
                            _base_key(profile, delta_target, short_leg_target_dte, long_leg_target_dte),
                            set(),
                        )
                    ]
                )
            ],
        )
        for short_leg_target_dte in short_dte_values
        for long_leg_target_dte in long_dte_values
        if long_leg_target_dte > short_leg_target_dte
        for delta_target in delta_values
    ]
    combo_groups = [group for group in combo_groups if group[3]]

    store = _build_store()
    with store.pinned_readonly_session():
        bars, earnings_dates, ex_dividend_dates, option_gateway = _build_bundle(
            store=store,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        risk_free_rate_curve = _build_risk_free_rate_curve(
            store=store,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        engine = OptionsBacktestEngine()
        shared_entry_rule_cache = EntryRuleComputationCache()
        last_completed_base_key: tuple[str, int, int, int] | None = None
        last_status_update_at = started_at
        pending_rows: list[dict[str, Any]] = []

        _update_status(
            path=args.output_json,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            regime_labels=regime_labels,
            delta_values=delta_values,
            short_dte_values=short_dte_values,
            long_dte_values=long_dte_values,
            profit_values=profit_values,
            started_at=started_at,
            resumed_completed_base_combos=resumed_completed_base_combos,
            completed_base_combos=completed_base_combos,
            total_base_combos=total_base_combos,
            written_rows=written_rows,
            output_csv=args.output_csv,
            adjustment_policy=adjustment_policy.name if adjustment_policy is not None else None,
            best_row=best_row,
            last_completed_base_key=last_completed_base_key,
        )

        for delta_target, short_leg_target_dte, long_leg_target_dte, profile_jobs in combo_groups:
            config_specs = [
                (profile, profit_target_pct)
                for profile, missing_profit_values in profile_jobs
                for profit_target_pct in missing_profit_values
            ]
            configs = [
                _build_config(
                    symbol=args.symbol,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    profile=profile,
                    delta_target=delta_target,
                    short_leg_target_dte=short_leg_target_dte,
                    long_leg_target_dte=long_leg_target_dte,
                    profit_target_pct=profit_target_pct,
                    risk_free_rate_curve=risk_free_rate_curve,
                )
                for profile, profit_target_pct in config_specs
            ]

            if adjustment_policy is None:
                try:
                    results = engine.run_exit_policy_variants(
                        configs=configs,
                        bars=bars,
                        earnings_dates=earnings_dates,
                        option_gateway=option_gateway,
                        ex_dividend_dates=ex_dividend_dates,
                        shared_entry_rule_cache=shared_entry_rule_cache,
                    )
                    rows = [
                        _summary_row(
                            profile=profile,
                            delta_target=delta_target,
                            short_leg_target_dte=short_leg_target_dte,
                            long_leg_target_dte=long_leg_target_dte,
                            profit_target_pct=profit_target_pct,
                            result=result,
                        )
                        for (profile, profit_target_pct), result in zip(config_specs, results, strict=False)
                    ]
                except Exception as exc:
                    rows = [
                        _error_row(
                            profile=profile,
                            delta_target=delta_target,
                            short_leg_target_dte=short_leg_target_dte,
                            long_leg_target_dte=long_leg_target_dte,
                            profit_target_pct=profit_target_pct,
                            error=exc,
                        )
                        for profile, profit_target_pct in config_specs
                    ]
            else:
                rows = []
                for (profile, profit_target_pct), config in zip(config_specs, configs, strict=False):
                    try:
                        result = run_adjusted_calendar_backtest(
                            config=config,
                            bars=bars,
                            earnings_dates=earnings_dates,
                            option_gateway=option_gateway,
                            policy=adjustment_policy,
                            ex_dividend_dates=ex_dividend_dates,
                            shared_entry_rule_cache=shared_entry_rule_cache,
                            force_single_contract=True,
                        )
                        rows.append(
                            _summary_row(
                                profile=profile,
                                delta_target=delta_target,
                                short_leg_target_dte=short_leg_target_dte,
                                long_leg_target_dte=long_leg_target_dte,
                                profit_target_pct=profit_target_pct,
                                result=result,
                            )
                        )
                    except Exception as exc:
                        rows.append(
                            _error_row(
                                profile=profile,
                                delta_target=delta_target,
                                short_leg_target_dte=short_leg_target_dte,
                                long_leg_target_dte=long_leg_target_dte,
                                profit_target_pct=profit_target_pct,
                                error=exc,
                            )
                        )

            for row in rows:
                row_profile = REGIME_PROFILES[str(row["regime_label"])]
                row_base_key = _base_key(
                    row_profile,
                    int(row["delta_target"]),
                    int(row["short_leg_target_dte"]),
                    int(row["long_leg_target_dte"]),
                )
                existing.setdefault(row_base_key, set()).add(int(row["profit_target_pct"]))
                written_rows += 1
                if best_row is None or _row_rank_key(row) > _row_rank_key(best_row):
                    best_row = row
            pending_rows.extend(rows)
            completed_base_combos += len(profile_jobs)
            last_completed_base_key = _base_key(
                profile_jobs[-1][0],
                delta_target,
                short_leg_target_dte,
                long_leg_target_dte,
            )

            session_completed_base_combos = max(completed_base_combos - resumed_completed_base_combos, 0)
            elapsed_seconds = time.time() - started_at
            avg_seconds_per_base = elapsed_seconds / session_completed_base_combos if session_completed_base_combos else 0.0
            remaining = max(total_base_combos - completed_base_combos, 0)
            eta_seconds = avg_seconds_per_base * remaining if session_completed_base_combos else None
            now = time.time()
            should_update_status = (
                session_completed_base_combos <= 3
                or session_completed_base_combos % CSV_FLUSH_EVERY_BASE_COMBOS == 0
                or session_completed_base_combos % STATUS_UPDATE_EVERY_BASE_COMBOS == 0
                or completed_base_combos == total_base_combos
                or (now - last_status_update_at) >= STATUS_UPDATE_MIN_SECONDS
            )
            if should_update_status:
                if pending_rows:
                    _append_rows(args.output_csv, pending_rows)
                    pending_rows.clear()
                print(
                    json.dumps(
                        {
                            "progress": f"{completed_base_combos}/{total_base_combos}",
                            "session_completed_base_combos": session_completed_base_combos,
                            "current": {
                                "delta_target": delta_target,
                                "short_leg_target_dte": short_leg_target_dte,
                                "long_leg_target_dte": long_leg_target_dte,
                                "regime_labels": [profile.label for profile, _ in profile_jobs],
                            },
                            "adjustment_policy": adjustment_policy.name if adjustment_policy is not None else None,
                            "avg_seconds_per_base_combo": round(avg_seconds_per_base, 4),
                            "eta_seconds": None if eta_seconds is None else round(eta_seconds, 2),
                            "grouped_base_combo_count": len(profile_jobs),
                        }
                    )
                )
                _update_status(
                    path=args.output_json,
                    symbol=args.symbol,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    regime_labels=regime_labels,
                    delta_values=delta_values,
                    short_dte_values=short_dte_values,
                    long_dte_values=long_dte_values,
                    profit_values=profit_values,
                    started_at=started_at,
                    resumed_completed_base_combos=resumed_completed_base_combos,
                    completed_base_combos=completed_base_combos,
                    total_base_combos=total_base_combos,
                    written_rows=written_rows,
                    output_csv=args.output_csv,
                    adjustment_policy=adjustment_policy.name if adjustment_policy is not None else None,
                    best_row=best_row,
                    last_completed_base_key=last_completed_base_key,
                )
                last_status_update_at = now

        if pending_rows:
            _append_rows(args.output_csv, pending_rows)
            pending_rows.clear()
        _update_status(
            path=args.output_json,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            regime_labels=regime_labels,
            delta_values=delta_values,
            short_dte_values=short_dte_values,
            long_dte_values=long_dte_values,
            profit_values=profit_values,
            started_at=started_at,
            resumed_completed_base_combos=resumed_completed_base_combos,
            completed_base_combos=completed_base_combos,
            total_base_combos=total_base_combos,
            written_rows=written_rows,
            output_csv=args.output_csv,
            adjustment_policy=adjustment_policy.name if adjustment_policy is not None else None,
            best_row=best_row,
            last_completed_base_key=last_completed_base_key,
        )


if __name__ == "__main__":
    main()
