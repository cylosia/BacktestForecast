from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import date
from decimal import Decimal
from itertools import product
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.schemas.backtests import (  # noqa: E402
    CreateBacktestRunRequest,
    StrategyOverrides,
    StrategyType,
    StrikeSelection,
    StrikeSelectionMode,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService  # noqa: E402
from backtestforecast.services.serialization import serialize_summary  # noqa: E402


DEFAULT_OUTPUT_CSV = ROOT / "logs" / "uvxy_long_put_2015_delta_dte_take_profit_grid.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "uvxy_long_put_2015_delta_dte_take_profit_grid.json"
SUMMARY_FIELDS = [
    "trade_count",
    "decided_trades",
    "win_rate",
    "total_roi_pct",
    "average_win_amount",
    "average_loss_amount",
    "average_holding_period_days",
    "average_dte_at_open",
    "max_drawdown_pct",
    "total_commissions",
    "total_net_pnl",
    "starting_equity",
    "ending_equity",
    "profit_factor",
    "payoff_ratio",
    "expectancy",
    "sharpe_ratio",
    "sortino_ratio",
    "cagr_pct",
    "calmar_ratio",
    "max_consecutive_wins",
    "max_consecutive_losses",
    "recovery_factor",
]
CSV_FIELDS = [
    "symbol",
    "strategy_type",
    "start_date",
    "end_date",
    "delta_target",
    "target_dte",
    "dte_tolerance_days",
    "max_holding_days",
    "profit_target_pct",
    "account_size",
    "risk_per_trade_pct",
    "commission_per_contract",
    "slippage_pct",
    "status",
    "error_type",
    "error_message",
    "data_source",
    "warning_count",
    "warning_codes",
    "elapsed_s",
    *SUMMARY_FIELDS,
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the UVXY 2015 long-put delta/DTE/take-profit grid and export summary rows to CSV."
    )
    parser.add_argument("--symbol", default="UVXY")
    parser.add_argument("--strategy", choices=[StrategyType.LONG_PUT.value], default=StrategyType.LONG_PUT.value)
    parser.add_argument("--start-date", default="2015-01-02")
    parser.add_argument("--end-date", default="2015-12-31")
    parser.add_argument("--delta-start", type=int, default=5)
    parser.add_argument("--delta-end", type=int, default=95)
    parser.add_argument("--delta-step", type=int, default=5)
    parser.add_argument("--dte-start", type=int, default=1)
    parser.add_argument("--dte-end", type=int, default=30)
    parser.add_argument("--dte-step", type=int, default=1)
    parser.add_argument("--profit-start", type=int, default=10)
    parser.add_argument("--profit-end", type=int, default=100)
    parser.add_argument("--profit-step", type=int, default=10)
    parser.add_argument("--dte-tolerance-days", type=int, default=0)
    parser.add_argument("--max-holding-days", type=int, default=120)
    parser.add_argument("--account-size", default="100000")
    parser.add_argument("--risk-per-trade-pct", default="100")
    parser.add_argument("--commission-per-contract", default="0.65")
    parser.add_argument("--slippage-pct", default="0")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    return parser.parse_args()


def _inclusive_range(start: int, end: int, step: int) -> list[int]:
    if step <= 0:
        raise ValueError("step must be positive")
    if end < start:
        raise ValueError("end must be >= start")
    return list(range(start, end + 1, step))


def _build_request(
    *,
    symbol: str,
    strategy_type: StrategyType,
    start_date: date,
    end_date: date,
    target_dte: int,
    dte_tolerance_days: int,
    max_holding_days: int,
    account_size: Decimal,
    risk_per_trade_pct: Decimal,
    commission_per_contract: Decimal,
    slippage_pct: Decimal,
    delta_target: int | None,
) -> CreateBacktestRunRequest:
    strategy_overrides = None
    if delta_target is not None:
        strategy_overrides = StrategyOverrides(
            long_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_target)),
            )
        )
    return CreateBacktestRunRequest(
        symbol=symbol,
        strategy_type=strategy_type,
        start_date=start_date,
        end_date=end_date,
        target_dte=target_dte,
        dte_tolerance_days=dte_tolerance_days,
        max_holding_days=max_holding_days,
        account_size=account_size,
        risk_per_trade_pct=risk_per_trade_pct,
        commission_per_contract=commission_per_contract,
        entry_rules=[],
        slippage_pct=slippage_pct,
        strategy_overrides=strategy_overrides,
    )


def _warning_codes(warnings: list[dict[str, Any]]) -> str:
    return ";".join(
        sorted(
            {
                str(item.get("code"))
                for item in warnings
                if isinstance(item, dict) and item.get("code")
            }
        )
    )


def _base_row(
    *,
    symbol: str,
    strategy_type: StrategyType,
    start_date: date,
    end_date: date,
    delta_target: int,
    target_dte: int,
    dte_tolerance_days: int,
    max_holding_days: int,
    profit_target_pct: int,
    account_size: Decimal,
    risk_per_trade_pct: Decimal,
    commission_per_contract: Decimal,
    slippage_pct: Decimal,
    elapsed_s: float,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "strategy_type": strategy_type.value,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "delta_target": delta_target,
        "target_dte": target_dte,
        "dte_tolerance_days": dte_tolerance_days,
        "max_holding_days": max_holding_days,
        "profit_target_pct": profit_target_pct,
        "account_size": float(account_size),
        "risk_per_trade_pct": float(risk_per_trade_pct),
        "commission_per_contract": float(commission_per_contract),
        "slippage_pct": float(slippage_pct),
        "elapsed_s": round(elapsed_s, 4),
    }


def main() -> None:
    args = _parse_args()
    symbol = args.symbol.strip().upper()
    strategy_type = StrategyType(args.strategy)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    account_size = Decimal(args.account_size)
    risk_per_trade_pct = Decimal(args.risk_per_trade_pct)
    commission_per_contract = Decimal(args.commission_per_contract)
    slippage_pct = Decimal(args.slippage_pct)
    delta_values = _inclusive_range(args.delta_start, args.delta_end, args.delta_step)
    dte_values = _inclusive_range(args.dte_start, args.dte_end, args.dte_step)
    profit_values = _inclusive_range(args.profit_start, args.profit_end, args.profit_step)
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    representative_request = _build_request(
        symbol=symbol,
        strategy_type=strategy_type,
        start_date=start_date,
        end_date=end_date,
        target_dte=max(dte_values),
        dte_tolerance_days=args.dte_tolerance_days,
        max_holding_days=args.max_holding_days,
        account_size=account_size,
        risk_per_trade_pct=risk_per_trade_pct,
        commission_per_contract=commission_per_contract,
        slippage_pct=slippage_pct,
        delta_target=max(delta_values),
    )

    total_base_runs = len(delta_values) * len(dte_values)
    total_rows = total_base_runs * len(profit_values)
    started = time.perf_counter()
    rows_written = 0
    ok_rows = 0
    error_rows = 0
    error_examples: list[dict[str, Any]] = []

    print(
        (
            f"START symbol={symbol} strategy={strategy_type.value} start={start_date.isoformat()} "
            f"end={end_date.isoformat()} base_runs={total_base_runs} rows={total_rows}"
        ),
        flush=True,
    )

    with BacktestExecutionService() as service:
        bundle = service.market_data_service.prepare_backtest(representative_request)
        resolved_parameters, risk_free_rate_curve = service.resolve_execution_inputs(representative_request)

        with output_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()

            for combo_index, (delta_target, target_dte) in enumerate(product(delta_values, dte_values), start=1):
                request = _build_request(
                    symbol=symbol,
                    strategy_type=strategy_type,
                    start_date=start_date,
                    end_date=end_date,
                    target_dte=target_dte,
                    dte_tolerance_days=args.dte_tolerance_days,
                    max_holding_days=args.max_holding_days,
                    account_size=account_size,
                    risk_per_trade_pct=risk_per_trade_pct,
                    commission_per_contract=commission_per_contract,
                    slippage_pct=slippage_pct,
                    delta_target=delta_target,
                )
                combo_start = time.perf_counter()
                try:
                    results = service.execute_exit_policy_variants(
                        request,
                        exit_policies=[(Decimal(str(profit_target_pct)), None) for profit_target_pct in profit_values],
                        bundle=bundle,
                        resolved_parameters=resolved_parameters,
                        risk_free_rate_curve=risk_free_rate_curve,
                    )
                    combo_elapsed = time.perf_counter() - combo_start
                    for profit_target_pct, result in zip(profit_values, results, strict=True):
                        summary = serialize_summary(result.summary)
                        row = _base_row(
                            symbol=symbol,
                            strategy_type=strategy_type,
                            start_date=start_date,
                            end_date=end_date,
                            delta_target=delta_target,
                            target_dte=target_dte,
                            dte_tolerance_days=args.dte_tolerance_days,
                            max_holding_days=args.max_holding_days,
                            profit_target_pct=profit_target_pct,
                            account_size=account_size,
                            risk_per_trade_pct=risk_per_trade_pct,
                            commission_per_contract=commission_per_contract,
                            slippage_pct=slippage_pct,
                            elapsed_s=combo_elapsed / max(1, len(profit_values)),
                        )
                        row.update(
                            {
                                "status": "ok",
                                "error_type": "",
                                "error_message": "",
                                "data_source": result.data_source,
                                "warning_count": len(result.warnings),
                                "warning_codes": _warning_codes(result.warnings),
                            }
                        )
                        row.update(summary)
                        writer.writerow(row)
                        rows_written += 1
                        ok_rows += 1
                except Exception as exc:
                    combo_elapsed = time.perf_counter() - combo_start
                    for profit_target_pct in profit_values:
                        row = _base_row(
                            symbol=symbol,
                            strategy_type=strategy_type,
                            start_date=start_date,
                            end_date=end_date,
                            delta_target=delta_target,
                            target_dte=target_dte,
                            dte_tolerance_days=args.dte_tolerance_days,
                            max_holding_days=args.max_holding_days,
                            profit_target_pct=profit_target_pct,
                            account_size=account_size,
                            risk_per_trade_pct=risk_per_trade_pct,
                            commission_per_contract=commission_per_contract,
                            slippage_pct=slippage_pct,
                            elapsed_s=combo_elapsed / max(1, len(profit_values)),
                        )
                        row.update(
                            {
                                "status": "error",
                                "error_type": type(exc).__name__,
                                "error_message": str(exc),
                                "data_source": "",
                                "warning_count": 0,
                                "warning_codes": "",
                            }
                        )
                        writer.writerow(row)
                        rows_written += 1
                        error_rows += 1
                    if len(error_examples) < 20:
                        error_examples.append(
                            {
                                "delta_target": delta_target,
                                "target_dte": target_dte,
                                "error_type": type(exc).__name__,
                                "error_message": str(exc),
                            }
                        )
                handle.flush()

                if combo_index == 1 or combo_index % 25 == 0 or combo_index == total_base_runs:
                    elapsed_s = time.perf_counter() - started
                    print(
                        (
                            f"PROGRESS combo={combo_index}/{total_base_runs} "
                            f"delta={delta_target} dte={target_dte} rows_written={rows_written} "
                            f"elapsed_s={elapsed_s:.2f}"
                        ),
                        flush=True,
                    )

    total_elapsed_s = time.perf_counter() - started
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "symbol": symbol,
        "strategy_type": strategy_type.value,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "delta_values": delta_values,
        "dte_values": dte_values,
        "profit_target_values": profit_values,
        "dte_tolerance_days": args.dte_tolerance_days,
        "max_holding_days": args.max_holding_days,
        "account_size": float(account_size),
        "risk_per_trade_pct": float(risk_per_trade_pct),
        "commission_per_contract": float(commission_per_contract),
        "slippage_pct": float(slippage_pct),
        "base_runs": total_base_runs,
        "rows_expected": total_rows,
        "rows_written": rows_written,
        "ok_rows": ok_rows,
        "error_rows": error_rows,
        "total_elapsed_s": round(total_elapsed_s, 4),
        "csv_path": str(output_csv),
        "error_examples": error_examples,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        (
            f"DONE rows_written={rows_written} ok_rows={ok_rows} error_rows={error_rows} "
            f"elapsed_s={total_elapsed_s:.2f} csv={output_csv} json={output_json}"
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
