from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.summary import build_summary  # noqa: E402
from backtestforecast.backtests.types import EquityPointResult, TradeResult  # noqa: E402
from backtestforecast.config import invalidate_settings  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.service import HistoricalDataBundle  # noqa: E402
from backtestforecast.pipeline.regime import Regime, build_regime_snapshots  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    CreateBacktestRunRequest,
    RegimeRule,
    StrategyOverrides,
    StrategyType,
    StrikeSelection,
    StrikeSelectionMode,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService  # noqa: E402
from backtestforecast.services.serialization import serialize_summary, serialize_trade  # noqa: E402


DEFAULT_LEDGER_CSV = ROOT / "logs" / "uvxy_put_calendar_spread_post_2018_rule_book_2020_05_2021_04_trade_ledger.csv"
DEFAULT_SUMMARY_JSON = ROOT / "logs" / "uvxy_put_calendar_spread_post_2018_rule_book_2020_05_2021_04_summary.json"
RULE_BOOK_PATH = ROOT / "docs" / "runbooks" / "uvxy-long-put-post-2018-rule-book.md"

REQUESTED_START = date(2020, 5, 1)
REQUESTED_END = date(2021, 4, 30)
ACCOUNT_SIZE = Decimal("100000")
RISK_PER_TRADE_PCT = Decimal("100")
COMMISSION_PER_CONTRACT = Decimal("0.65")
SLIPPAGE_PCT = Decimal("0")
PROFIT_TARGET_PCT = Decimal("20")
TARGET_DTE = 1
DTE_TOLERANCE_DAYS = 0
MAX_HOLDING_DAYS = 120
STARTING_EQUITY = Decimal("100000")

LEDGER_FIELDS = [
    "branch",
    "required_regimes",
    "blocked_regimes",
    "entry_regimes",
    "delta_target",
    "target_dte",
    "profit_target_pct",
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
    "detail_json",
]

_ORIGINAL_ATTACH_POSITION_QUOTE_SERIES = OptionsBacktestEngine._attach_position_quote_series
_ORIGINAL_RESOLVE_POSITION_SIZE = OptionsBacktestEngine._resolve_position_size


@dataclass(frozen=True, slots=True)
class RuleBranch:
    name: str
    delta_target: int
    required_regimes: tuple[str, ...]
    blocked_regimes: tuple[str, ...]


@dataclass(slots=True)
class SelectedTradeRun:
    branch: RuleBranch
    regimes: frozenset[Regime]
    trade: TradeResult
    equity_curve: list[EquityPointResult]
    warnings: list[dict[str, Any]]


LOW_IV_BRANCH = RuleBranch(
    name="bearish_low_iv",
    delta_target=15,
    required_regimes=("bearish", "low_iv"),
    blocked_regimes=(),
)
FALLBACK_BRANCH = RuleBranch(
    name="bearish_not_high_iv_non_low_iv",
    delta_target=20,
    required_regimes=("bearish",),
    blocked_regimes=("high_iv", "low_iv"),
)


def _install_quote_series_expiration_cap() -> None:
    def _capped_attach_position_quote_series(
        position: Any,
        *,
        option_gateway: Any,
        start_date: date,
        end_date: date,
    ) -> None:
        capped_end_date = end_date
        option_legs = getattr(position, "option_legs", None) or []
        expiration_dates = [
            expiration_date
            for expiration_date in (
                getattr(leg, "expiration_date", None)
                for leg in option_legs
            )
            if isinstance(expiration_date, date)
        ]
        if expiration_dates:
            capped_end_date = min(end_date, max(expiration_dates))
        return _ORIGINAL_ATTACH_POSITION_QUOTE_SERIES(
            position,
            option_gateway=option_gateway,
            start_date=start_date,
            end_date=capped_end_date,
        )

    OptionsBacktestEngine._attach_position_quote_series = staticmethod(_capped_attach_position_quote_series)


def _install_single_contract_position_sizing() -> None:
    def _single_contract_resolve_position_size(
        available_cash: Decimal | float,
        account_size: float,
        risk_per_trade_pct: float,
        capital_required_per_unit: float,
        max_loss_per_unit: float | None,
        entry_cost_per_unit: float = 0.0,
        commission_per_unit: float = 0.0,
        slippage_pct: float = 0.0,
        gross_notional_per_unit: float = 0.0,
    ) -> int:
        resolved = _ORIGINAL_RESOLVE_POSITION_SIZE(
            available_cash=available_cash,
            account_size=account_size,
            risk_per_trade_pct=risk_per_trade_pct,
            capital_required_per_unit=capital_required_per_unit,
            max_loss_per_unit=max_loss_per_unit,
            entry_cost_per_unit=entry_cost_per_unit,
            commission_per_unit=commission_per_unit,
            slippage_pct=slippage_pct,
            gross_notional_per_unit=gross_notional_per_unit,
        )
        return 1 if resolved >= 1 else 0

    OptionsBacktestEngine._resolve_position_size = staticmethod(_single_contract_resolve_position_size)


def _build_regime_entry_rule(*, required: tuple[str, ...], blocked: tuple[str, ...]) -> list[RegimeRule]:
    return [
        RegimeRule(
            type="regime",
            required_regimes=[Regime(value) for value in required],
            blocked_regimes=[Regime(value) for value in blocked],
        )
    ]


def _build_request(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    delta_target: int,
    required_regimes: tuple[str, ...],
    blocked_regimes: tuple[str, ...],
) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(
        symbol=symbol,
        strategy_type=StrategyType.CALENDAR_SPREAD,
        start_date=start_date,
        end_date=end_date,
        target_dte=TARGET_DTE,
        dte_tolerance_days=DTE_TOLERANCE_DAYS,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=ACCOUNT_SIZE,
        risk_per_trade_pct=RISK_PER_TRADE_PCT,
        commission_per_contract=COMMISSION_PER_CONTRACT,
        slippage_pct=SLIPPAGE_PCT,
        profit_target_pct=PROFIT_TARGET_PCT,
        entry_rules=_build_regime_entry_rule(required=required_regimes, blocked=blocked_regimes),
        strategy_overrides=StrategyOverrides(
            calendar_contract_type="put",
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_target)),
            )
        ),
    )


def _select_branch(regimes: frozenset[Regime] | None) -> RuleBranch | None:
    if regimes is None or Regime.BEARISH not in regimes:
        return None
    if Regime.LOW_IV in regimes:
        return LOW_IV_BRANCH
    if Regime.HIGH_IV in regimes:
        return None
    return FALLBACK_BRANCH


def _resolve_effective_end_date(
    *,
    symbol: str,
) -> tuple[date, date, HistoricalDataBundle]:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    warmup_start = REQUESTED_START - timedelta(days=210 * 3)
    in_window_dates = store.get_underlying_day_bars(symbol, REQUESTED_START, REQUESTED_END)
    trade_dates = [bar.trade_date for bar in in_window_dates]
    if len(trade_dates) < 2:
        raise RuntimeError("Not enough local UVXY bars in the requested window to build a safe 1 DTE replay.")
    effective_end_date = trade_dates[-2]
    settlement_end_date = trade_dates[-1]
    bars = store.get_underlying_day_bars(symbol, warmup_start, settlement_end_date)
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=store.list_earnings_event_dates(symbol, warmup_start, settlement_end_date),
        ex_dividend_dates=store.list_ex_dividend_dates(symbol, warmup_start, settlement_end_date),
        option_gateway=HistoricalOptionGateway(store, symbol),
        data_source="historical_flatfile",
        warnings=[],
        entry_rule_cache=EntryRuleComputationCache(),
    )
    return effective_end_date, settlement_end_date, bundle


def _has_exact_next_day_put_expiration(
    *,
    bundle: HistoricalDataBundle,
    trade_date: date,
) -> bool:
    option_gateway = bundle.option_gateway
    store = getattr(option_gateway, "store", None)
    if store is None:
        preferred_fetch = getattr(option_gateway, "list_contracts_for_preferred_expiration", None)
        if not callable(preferred_fetch):
            return False
        try:
            contracts = preferred_fetch(
                entry_date=trade_date,
                contract_type="put",
                target_dte=TARGET_DTE,
                dte_tolerance_days=DTE_TOLERANCE_DAYS,
            )
        except Exception:
            return False
        return bool(contracts) and contracts[0].expiration_date == (trade_date + timedelta(days=1))
    expirations = store.list_available_option_expirations(
        symbol="UVXY",
        as_of_date=trade_date,
        contract_type="put",
        expiration_dates=[trade_date + timedelta(days=1)],
    )
    return bool(expirations)


def _trade_date_lookup(bundle: Any) -> tuple[list[date], dict[date, frozenset[Regime] | None]]:
    snapshots = build_regime_snapshots(
        "UVXY",
        bundle.bars,
        earnings_dates=bundle.earnings_dates,
    )
    trade_dates: list[date] = []
    regimes_by_date: dict[date, frozenset[Regime] | None] = {}
    for bar, snapshot in zip(bundle.bars, snapshots, strict=False):
        if REQUESTED_START <= bar.trade_date <= bar.trade_date:
            pass
        regimes_by_date[bar.trade_date] = None if snapshot is None else snapshot.regimes
        if REQUESTED_START <= bar.trade_date <= REQUESTED_END:
            trade_dates.append(bar.trade_date)
    return trade_dates, regimes_by_date


def _merge_warning_lists(*warning_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for warning_list in warning_lists:
        for warning in warning_list:
            code = str(warning.get("code", ""))
            message = str(warning.get("message", ""))
            key = (code, message)
            if key in seen:
                continue
            seen.add(key)
            merged.append({"code": code, "message": message})
    return merged


def _build_global_equity_curve(
    *,
    bundle: Any,
    selected_runs: list[SelectedTradeRun],
    effective_end_date: date,
) -> list[EquityPointResult]:
    if not selected_runs:
        running_peak = STARTING_EQUITY
        curve: list[EquityPointResult] = []
        for bar in bundle.bars:
            if REQUESTED_START <= bar.trade_date <= effective_end_date:
                drawdown = Decimal("0")
                curve.append(
                    EquityPointResult(
                        trade_date=bar.trade_date,
                        equity=STARTING_EQUITY,
                        cash=STARTING_EQUITY,
                        position_value=Decimal("0"),
                        drawdown_pct=drawdown,
                    )
                )
        return curve

    final_curve_end = max(effective_end_date, selected_runs[-1].trade.exit_date)
    global_dates = [
        bar.trade_date
        for bar in bundle.bars
        if REQUESTED_START <= bar.trade_date <= final_curve_end
    ]

    curve: list[EquityPointResult] = []
    baseline_equity = STARTING_EQUITY
    running_peak = STARTING_EQUITY
    selected_index = 0
    active_run: SelectedTradeRun | None = None
    active_baseline = STARTING_EQUITY
    active_curve_map: dict[date, EquityPointResult] = {}

    for trade_date in global_dates:
        if active_run is None and selected_index < len(selected_runs):
            candidate = selected_runs[selected_index]
            if trade_date == candidate.trade.entry_date:
                active_run = candidate
                active_baseline = baseline_equity
                active_curve_map = {
                    point.trade_date: point
                    for point in candidate.equity_curve
                    if candidate.trade.entry_date <= point.trade_date <= candidate.trade.exit_date
                }

        if active_run is not None and active_run.trade.entry_date <= trade_date <= active_run.trade.exit_date:
            local_point = active_curve_map[trade_date]
            equity = active_baseline + (local_point.equity - STARTING_EQUITY)
            cash = active_baseline + (local_point.cash - STARTING_EQUITY)
            position_value = local_point.position_value
            if trade_date == active_run.trade.exit_date:
                baseline_equity = active_baseline + active_run.trade.net_pnl
                active_run = None
                active_curve_map = {}
                selected_index += 1
        else:
            equity = baseline_equity
            cash = baseline_equity
            position_value = Decimal("0")

        if equity > running_peak:
            running_peak = equity
        drawdown_pct = (
            (running_peak - equity) / running_peak * Decimal("100")
            if running_peak > 0
            else Decimal("0")
        )
        curve.append(
            EquityPointResult(
                trade_date=trade_date,
                equity=equity,
                cash=cash,
                position_value=position_value,
                drawdown_pct=drawdown_pct,
            )
        )

    return curve


def _branch_summary(trades: list[TradeResult]) -> dict[str, Any]:
    trade_count = len(trades)
    total_net_pnl = sum((trade.net_pnl for trade in trades), Decimal("0"))
    decided = [trade for trade in trades if trade.net_pnl != 0]
    wins = [trade for trade in decided if trade.net_pnl > 0]
    average_purchase_price = (
        sum((trade.entry_mid for trade in trades), Decimal("0")) / Decimal(trade_count)
        if trade_count > 0
        else None
    )
    average_holding_days = (
        sum(trade.holding_period_days for trade in trades) / trade_count
        if trade_count > 0
        else 0.0
    )
    return {
        "trade_count": trade_count,
        "decided_trades": len(decided),
        "win_rate": (len(wins) / len(decided) * 100.0) if decided else 0.0,
        "total_net_pnl": float(total_net_pnl),
        "average_purchase_price": float(average_purchase_price) if average_purchase_price is not None else None,
        "average_holding_period_days": average_holding_days,
    }


def _write_ledger(
    *,
    output_csv: Path,
    selected_runs: list[SelectedTradeRun],
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS)
        writer.writeheader()
        for selected in selected_runs:
            trade_row = serialize_trade(selected.trade)
            writer.writerow(
                {
                    "branch": selected.branch.name,
                    "required_regimes": ";".join(selected.branch.required_regimes),
                    "blocked_regimes": ";".join(selected.branch.blocked_regimes),
                    "entry_regimes": ";".join(sorted(regime.value for regime in selected.regimes)),
                    "delta_target": selected.branch.delta_target,
                    "target_dte": TARGET_DTE,
                    "profit_target_pct": float(PROFIT_TARGET_PCT),
                    "option_ticker": trade_row["option_ticker"],
                    "strategy_type": trade_row["strategy_type"],
                    "underlying_symbol": trade_row["underlying_symbol"],
                    "entry_date": trade_row["entry_date"],
                    "exit_date": trade_row["exit_date"],
                    "expiration_date": trade_row["expiration_date"],
                    "quantity": trade_row["quantity"],
                    "dte_at_open": trade_row["dte_at_open"],
                    "holding_period_days": trade_row["holding_period_days"],
                    "entry_underlying_close": trade_row["entry_underlying_close"],
                    "exit_underlying_close": trade_row["exit_underlying_close"],
                    "entry_mid": trade_row["entry_mid"],
                    "exit_mid": trade_row["exit_mid"],
                    "gross_pnl": trade_row["gross_pnl"],
                    "net_pnl": trade_row["net_pnl"],
                    "total_commissions": trade_row["total_commissions"],
                    "entry_reason": trade_row["entry_reason"],
                    "exit_reason": trade_row["exit_reason"],
                    "detail_json": json.dumps(trade_row["detail_json"], sort_keys=True),
                }
            )


def main() -> None:
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    os.environ["BACKTEST_OPTION_PREFETCH_ENABLED"] = "false"
    invalidate_settings()

    output_csv = DEFAULT_LEDGER_CSV
    output_json = DEFAULT_SUMMARY_JSON
    output_json.parent.mkdir(parents=True, exist_ok=True)

    with BacktestExecutionService() as service:
        effective_end_date, last_available_trade_date, bundle = _resolve_effective_end_date(symbol="UVXY")
        trade_dates = [
            bar.trade_date
            for bar in bundle.bars
            if REQUESTED_START <= bar.trade_date <= effective_end_date
        ]
        snapshots = build_regime_snapshots(
            "UVXY",
            bundle.bars,
            earnings_dates=bundle.earnings_dates,
        )
        regimes_by_date = {
            bar.trade_date: (None if snapshot is None else snapshot.regimes)
            for bar, snapshot in zip(bundle.bars, snapshots, strict=False)
        }

        representative_request = _build_request(
            symbol="UVXY",
            start_date=REQUESTED_START,
            end_date=effective_end_date,
            delta_target=FALLBACK_BRANCH.delta_target,
            required_regimes=FALLBACK_BRANCH.required_regimes,
            blocked_regimes=FALLBACK_BRANCH.blocked_regimes,
        )
        resolved_parameters, risk_free_rate_curve = service.resolve_execution_inputs(representative_request)

        selected_runs: list[SelectedTradeRun] = []
        evaluation_rows: list[dict[str, Any]] = []
        last_exit_date: date | None = None
        exact_1dte_entry_days = 0

        for trade_date in trade_dates:
            if last_exit_date is not None and trade_date <= last_exit_date:
                continue
            regimes = regimes_by_date.get(trade_date)
            branch = _select_branch(regimes)
            if branch is None or regimes is None:
                continue
            if not _has_exact_next_day_put_expiration(bundle=bundle, trade_date=trade_date):
                continue
            exact_1dte_entry_days += 1

            request = _build_request(
                symbol="UVXY",
                start_date=trade_date,
                end_date=trade_date + timedelta(days=1),
                delta_target=branch.delta_target,
                required_regimes=branch.required_regimes,
                blocked_regimes=branch.blocked_regimes,
            )
            result = service.execute_request(
                request,
                bundle=bundle,
                resolved_parameters=resolved_parameters,
                risk_free_rate_curve=risk_free_rate_curve,
            )
            matching_trades = [trade for trade in result.trades if trade.entry_date == trade_date]
            evaluation_rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "branch": branch.name,
                    "regimes": sorted(regime.value for regime in regimes),
                    "matching_trade_count": len(matching_trades),
                    "warning_codes": sorted({str(item.get("code")) for item in result.warnings if item.get("code")}),
                }
            )
            if not matching_trades:
                continue
            trade = matching_trades[0]
            selected_runs.append(
                SelectedTradeRun(
                    branch=branch,
                    regimes=regimes,
                    trade=trade,
                    equity_curve=result.equity_curve,
                    warnings=result.warnings,
                )
            )
            last_exit_date = trade.exit_date

    selected_trades = [selected.trade for selected in selected_runs]
    combined_curve = _build_global_equity_curve(
        bundle=bundle,
        selected_runs=selected_runs,
        effective_end_date=effective_end_date,
    )
    combined_warnings = _merge_warning_lists(
        *[selected.warnings for selected in selected_runs],
        [
            {
                "code": "window_clamped_for_missing_settlement_bar",
                "message": (
                    f"Requested end date {REQUESTED_END.isoformat()} was clamped to "
                    f"{effective_end_date.isoformat()} because the latest available trade date was "
                    f"{last_available_trade_date.isoformat()} and a clean 1 DTE replay requires a later settlement bar."
                ),
            }
        ],
    )
    summary = build_summary(
        float(ACCOUNT_SIZE),
        float(combined_curve[-1].equity) if combined_curve else float(ACCOUNT_SIZE),
        selected_trades,
        combined_curve,
        risk_free_rate_curve=risk_free_rate_curve,
        warnings=combined_warnings,
    )

    _write_ledger(output_csv=output_csv, selected_runs=selected_runs)

    branch_to_trades: dict[str, list[TradeResult]] = {
        LOW_IV_BRANCH.name: [],
        FALLBACK_BRANCH.name: [],
    }
    for selected in selected_runs:
        branch_to_trades[selected.branch.name].append(selected.trade)

    payload = {
        "rule_book_path": str(RULE_BOOK_PATH),
        "requested_start_date": REQUESTED_START.isoformat(),
        "requested_end_date": REQUESTED_END.isoformat(),
        "effective_entry_end_date": effective_end_date.isoformat(),
        "last_available_trade_date": last_available_trade_date.isoformat(),
        "strategy_type": StrategyType.CALENDAR_SPREAD.value,
        "symbol": "UVXY",
        "account_size": float(ACCOUNT_SIZE),
        "risk_per_trade_pct": float(RISK_PER_TRADE_PCT),
        "commission_per_contract": float(COMMISSION_PER_CONTRACT),
        "slippage_pct": float(SLIPPAGE_PCT),
        "target_dte": TARGET_DTE,
        "profit_target_pct": float(PROFIT_TARGET_PCT),
        "trade_ledger_csv": str(output_csv),
        "summary": serialize_summary(summary),
        "rule_book_filters": {
            "requires_exact_next_day_expiration": True,
            "exact_1dte_entry_days": exact_1dte_entry_days,
        },
        "branch_breakdown": {
            branch_name: _branch_summary(trades)
            for branch_name, trades in branch_to_trades.items()
        },
        "warnings": combined_warnings,
        "trade_selection_evaluations": evaluation_rows,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
