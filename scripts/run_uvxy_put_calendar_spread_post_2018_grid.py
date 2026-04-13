from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.summary import build_summary  # noqa: E402
from backtestforecast.backtests.types import EquityPointResult, TradeResult  # noqa: E402
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
from backtestforecast.services.serialization import serialize_summary  # noqa: E402

from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    ACCOUNT_SIZE,
    COMMISSION_PER_CONTRACT,
    FALLBACK_BRANCH,
    LOW_IV_BRANCH,
    MAX_HOLDING_DAYS,
    PROFIT_TARGET_PCT,
    REQUESTED_END,
    REQUESTED_START,
    RISK_PER_TRADE_PCT,
    RULE_BOOK_PATH,
    SLIPPAGE_PCT,
    _build_global_equity_curve,
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
    _merge_warning_lists,
    _resolve_effective_end_date,
    _select_branch,
)


DEFAULT_OUTPUT_CSV = ROOT / "logs" / "uvxy_put_calendar_spread_post_2018_rule_book_2020_05_2021_04_delta_5_50_step5_dte_1_7_grid.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "uvxy_put_calendar_spread_post_2018_rule_book_2020_05_2021_04_delta_5_50_step5_dte_1_7_grid.json"

DELTA_VALUES = list(range(5, 51, 5))
DTE_VALUES = list(range(1, 8))

GRID_FIELDS = [
    "delta_target",
    "target_dte",
    "trade_count",
    "decided_trades",
    "win_rate",
    "total_net_pnl",
    "total_roi_pct",
    "max_drawdown_pct",
    "profit_factor",
    "expectancy",
    "average_holding_period_days",
    "average_dte_at_open",
    "exact_entry_days",
    "eligible_entry_days_without_trade",
    "branch_low_iv_trade_count",
    "branch_low_iv_net_pnl",
    "branch_fallback_trade_count",
    "branch_fallback_net_pnl",
    "warning_codes",
]


@dataclass(slots=True)
class SelectedTradeRun:
    branch_name: str
    regimes: frozenset[Regime]
    trade: TradeResult
    equity_curve: list[EquityPointResult]
    warnings: list[dict[str, Any]]


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
    target_dte: int,
    delta_target: int,
    required_regimes: tuple[str, ...],
    blocked_regimes: tuple[str, ...],
) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(
        symbol=symbol,
        strategy_type=StrategyType.CALENDAR_SPREAD,
        start_date=start_date,
        end_date=end_date,
        target_dte=target_dte,
        dte_tolerance_days=0,
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
            ),
        ),
    )


def _has_exact_put_expiration(
    *,
    bundle: Any,
    trade_date: date,
    target_dte: int,
) -> bool:
    store = getattr(bundle.option_gateway, "store", None)
    target_expiration = trade_date + timedelta(days=target_dte)
    if store is None:
        preferred_fetch = getattr(bundle.option_gateway, "list_contracts_for_preferred_expiration", None)
        if not callable(preferred_fetch):
            return False
        try:
            contracts = preferred_fetch(
                entry_date=trade_date,
                contract_type="put",
                target_dte=target_dte,
                dte_tolerance_days=0,
            )
        except Exception:
            return False
        return bool(contracts) and contracts[0].expiration_date == target_expiration
    expirations = store.list_available_option_expirations(
        symbol="UVXY",
        as_of_date=trade_date,
        contract_type="put",
        expiration_dates=[target_expiration],
    )
    return bool(expirations)


def _branch_trade_metrics(trades: list[TradeResult]) -> tuple[int, float]:
    return len(trades), float(sum((trade.net_pnl for trade in trades), Decimal("0")))


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=GRID_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    output_csv = DEFAULT_OUTPUT_CSV
    output_json = DEFAULT_OUTPUT_JSON
    output_json.parent.mkdir(parents=True, exist_ok=True)

    with BacktestExecutionService() as service:
        _, last_available_trade_date, bundle = _resolve_effective_end_date(symbol="UVXY")
        trade_dates = [
            bar.trade_date
            for bar in bundle.bars
            if REQUESTED_START <= bar.trade_date <= last_available_trade_date
        ]
        regime_snapshots = build_regime_snapshots(
            "UVXY",
            bundle.bars,
            earnings_dates=bundle.earnings_dates,
        )
        regimes_by_date = {
            bar.trade_date: (None if snapshot is None else snapshot.regimes)
            for bar, snapshot in zip(bundle.bars, regime_snapshots, strict=False)
        }

        rows: list[dict[str, Any]] = []
        best_row: dict[str, Any] | None = None

        for target_dte in DTE_VALUES:
            for delta_target in DELTA_VALUES:
                representative_request = _build_request(
                    symbol="UVXY",
                    start_date=REQUESTED_START,
                    end_date=min(REQUESTED_END, last_available_trade_date),
                    target_dte=target_dte,
                    delta_target=delta_target,
                    required_regimes=FALLBACK_BRANCH.required_regimes,
                    blocked_regimes=FALLBACK_BRANCH.blocked_regimes,
                )
                resolved_parameters, risk_free_rate_curve = service.resolve_execution_inputs(representative_request)
                selected_runs: list[SelectedTradeRun] = []
                last_exit_date: date | None = None
                exact_entry_days = 0

                for trade_date in trade_dates:
                    if last_exit_date is not None and trade_date <= last_exit_date:
                        continue
                    regimes = regimes_by_date.get(trade_date)
                    branch = _select_branch(regimes)
                    if branch is None or regimes is None:
                        continue
                    if not _has_exact_put_expiration(bundle=bundle, trade_date=trade_date, target_dte=target_dte):
                        continue
                    exact_entry_days += 1

                    request = _build_request(
                        symbol="UVXY",
                        start_date=trade_date,
                        end_date=trade_date + timedelta(days=target_dte),
                        target_dte=target_dte,
                        delta_target=delta_target,
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
                    if not matching_trades:
                        continue
                    trade = matching_trades[0]
                    selected_runs.append(
                        SelectedTradeRun(
                            branch_name=branch.name,
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
                    effective_end_date=last_available_trade_date,
                )
                combined_warnings = _merge_warning_lists(*[selected.warnings for selected in selected_runs])
                summary = build_summary(
                    float(ACCOUNT_SIZE),
                    float(combined_curve[-1].equity) if combined_curve else float(ACCOUNT_SIZE),
                    selected_trades,
                    combined_curve,
                    risk_free_rate_curve=risk_free_rate_curve,
                    warnings=combined_warnings,
                )

                low_iv_trades = [selected.trade for selected in selected_runs if selected.branch_name == LOW_IV_BRANCH.name]
                fallback_trades = [selected.trade for selected in selected_runs if selected.branch_name == FALLBACK_BRANCH.name]
                low_iv_count, low_iv_pnl = _branch_trade_metrics(low_iv_trades)
                fallback_count, fallback_pnl = _branch_trade_metrics(fallback_trades)

                row = {
                    "delta_target": delta_target,
                    "target_dte": target_dte,
                    "trade_count": summary.trade_count,
                    "decided_trades": summary.decided_trades,
                    "win_rate": summary.win_rate,
                    "total_net_pnl": summary.total_net_pnl,
                    "total_roi_pct": summary.total_roi_pct,
                    "max_drawdown_pct": summary.max_drawdown_pct,
                    "profit_factor": summary.profit_factor,
                    "expectancy": summary.expectancy,
                    "average_holding_period_days": summary.average_holding_period_days,
                    "average_dte_at_open": summary.average_dte_at_open,
                    "exact_entry_days": exact_entry_days,
                    "eligible_entry_days_without_trade": exact_entry_days - summary.trade_count,
                    "branch_low_iv_trade_count": low_iv_count,
                    "branch_low_iv_net_pnl": low_iv_pnl,
                    "branch_fallback_trade_count": fallback_count,
                    "branch_fallback_net_pnl": fallback_pnl,
                    "warning_codes": ";".join(sorted({str(item.get("code")) for item in combined_warnings if item.get("code")})),
                }
                rows.append(row)

                if best_row is None:
                    best_row = row
                else:
                    if (
                        float(row["total_roi_pct"] or 0.0),
                        float(row["total_net_pnl"] or 0.0),
                        -float(row["max_drawdown_pct"] or 0.0),
                        float(row["trade_count"] or 0.0),
                    ) > (
                        float(best_row["total_roi_pct"] or 0.0),
                        float(best_row["total_net_pnl"] or 0.0),
                        -float(best_row["max_drawdown_pct"] or 0.0),
                        float(best_row["trade_count"] or 0.0),
                    ):
                        best_row = row

        _write_rows(output_csv, rows)

        payload = {
            "rule_book_path": str(RULE_BOOK_PATH),
            "strategy_type": StrategyType.CALENDAR_SPREAD.value,
            "calendar_contract_type": "put",
            "requested_start_date": REQUESTED_START.isoformat(),
            "requested_end_date": REQUESTED_END.isoformat(),
            "last_available_trade_date": last_available_trade_date.isoformat(),
            "delta_values": DELTA_VALUES,
            "target_dte_values": DTE_VALUES,
            "profit_target_pct": float(PROFIT_TARGET_PCT),
            "trade_grid_csv": str(output_csv),
            "row_count": len(rows),
            "best_combo_by_total_roi_pct": best_row,
            "rows": rows,
        }
        output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
