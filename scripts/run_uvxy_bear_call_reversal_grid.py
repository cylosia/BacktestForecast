from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ["HISTORICAL_DATA_LOCAL_PREFERRED"] = "true"
os.environ["HISTORICAL_DATA_T_MINUS_ONE_ONLY"] = "false"
os.environ["MAX_BACKTEST_WINDOW_DAYS"] = "5000"
os.environ["BACKTEST_OPTION_PREFETCH_ENABLED"] = "false"

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleEvaluator  # noqa: E402
from backtestforecast.config import invalidate_settings  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    ComparisonOperator,
    CreateBacktestRunRequest,
    IndicatorTrendDirection,
    IndicatorThresholdRule,
    IndicatorTrendRule,
    RsiSeriesSpec,
    SpreadWidthConfig,
    SpreadWidthMode,
    StrategyOverrides,
    StrategyType,
    StrikeSelection,
    StrikeSelectionMode,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService  # noqa: E402
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.utils.dates import market_date_today  # noqa: E402


DEFAULT_OUTPUT_CSV = ROOT / "logs" / "uvxy_bear_call_reversal_grid_2018_present.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "uvxy_bear_call_reversal_grid_2018_present.json"
DEFAULT_START = date(2018, 3, 1)
DEFAULT_END = market_date_today()
ACCOUNT_SIZE = Decimal("100000")
RISK_PER_TRADE_PCT = Decimal("100")
COMMISSION_PER_CONTRACT = Decimal("0.65")
SLIPPAGE_PCT = Decimal("0")
MAX_HOLDING_DAYS = 10
DTE_TOLERANCE_DAYS = 5
RSI_PERIOD = 5
EMA_PERIOD = 0
SPIKE_PERSISTENCE_BARS = 0

DEFAULT_RSI_THRESHOLDS = "65,70,75,80,85"
DEFAULT_FALLING_BARS = "2,3"
DEFAULT_TARGET_DTES = "21,28,35"
DEFAULT_SHORT_DELTAS = "20,25,30"
DEFAULT_WIDTH_STEPS = "1,2"
DEFAULT_PROFIT_TARGETS = "35,50,65"
DEFAULT_STOP_LOSSES = "100,150"

CSV_FIELDS = [
    "rsi_period",
    "rsi_threshold",
    "rsi_persistence_bars",
    "falling_bars",
    "ema_period",
    "target_dte",
    "short_call_delta",
    "width_steps",
    "profit_target_pct",
    "stop_loss_pct",
    "trade_count",
    "decided_trades",
    "eligible_entry_days",
    "win_rate",
    "total_roi_pct",
    "average_win_amount",
    "average_loss_amount",
    "total_commissions",
    "total_net_pnl",
    "starting_equity",
    "ending_equity",
    "expectancy",
    "profit_factor",
    "payoff_ratio",
    "max_drawdown_pct",
    "average_holding_period_days",
    "average_dte_at_open",
    "sharpe_ratio",
    "sortino_ratio",
    "cagr_pct",
    "calmar_ratio",
    "max_consecutive_wins",
    "max_consecutive_losses",
    "recovery_factor",
    "warning_count",
    "warning_codes",
    "quality_score",
]

_ORIGINAL_ATTACH_POSITION_QUOTE_SERIES = OptionsBacktestEngine._attach_position_quote_series
_ORIGINAL_RESOLVE_POSITION_SIZE = OptionsBacktestEngine._resolve_position_size


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


def _parse_int_list(value: str) -> list[int]:
    values = sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    if not values:
        raise ValueError("expected at least one integer value")
    return values


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


def _safe_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quality_score(row: dict[str, Any]) -> float:
    trade_count = float(row["trade_count"])
    expectancy = _safe_float(row["expectancy"]) or 0.0
    profit_factor = _safe_float(row["profit_factor"]) or 0.0
    max_drawdown_pct = _safe_float(row["max_drawdown_pct"]) or 0.0
    total_net_pnl = _safe_float(row["total_net_pnl"]) or 0.0
    if trade_count <= 0:
        return float("-inf")
    score = total_net_pnl + (expectancy * trade_count * 0.5) + (profit_factor * 200.0) - (max_drawdown_pct * 40.0)
    if trade_count < 12:
        score -= (12 - trade_count) * 100.0
    return score


def _build_entry_rules(*, rsi_threshold: int, falling_bars: int) -> list[Any]:
    rsi_series = RsiSeriesSpec(indicator="rsi", period=RSI_PERIOD)
    return [
        IndicatorThresholdRule(
            type="indicator_threshold",
            series=rsi_series,
            operator=ComparisonOperator.GTE,
            level=Decimal(str(rsi_threshold)),
        ),
        IndicatorTrendRule(
            type="indicator_trend",
            series=rsi_series,
            direction=IndicatorTrendDirection.FALLING,
            bars=falling_bars,
        ),
    ]


def _build_request(
    *,
    start_date: date,
    end_date: date,
    target_dte: int,
    short_call_delta: int,
    width_steps: int,
    rsi_threshold: int,
    falling_bars: int,
) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(
        symbol="UVXY",
        strategy_type=StrategyType.BEAR_CALL_CREDIT_SPREAD,
        start_date=start_date,
        end_date=end_date,
        target_dte=target_dte,
        dte_tolerance_days=DTE_TOLERANCE_DAYS,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=ACCOUNT_SIZE,
        risk_per_trade_pct=RISK_PER_TRADE_PCT,
        commission_per_contract=COMMISSION_PER_CONTRACT,
        slippage_pct=SLIPPAGE_PCT,
        entry_rules=_build_entry_rules(rsi_threshold=rsi_threshold, falling_bars=falling_bars),
        strategy_overrides=StrategyOverrides(
            short_call_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(short_call_delta)),
            ),
            spread_width=SpreadWidthConfig(
                mode=SpreadWidthMode.STRIKE_STEPS,
                value=Decimal(str(width_steps)),
            ),
        ),
    )


def _entry_rule_metadata(
    *,
    service: BacktestExecutionService,
    request: CreateBacktestRunRequest,
    bundle: Any,
    resolved_parameters: Any,
    risk_free_rate_curve: Any,
) -> dict[str, Any]:
    config = service._build_config(
        request=request,
        parameters=resolved_parameters,
        risk_free_rate_curve=risk_free_rate_curve,
    )
    evaluator = EntryRuleEvaluator(
        config=config,
        bars=bundle.bars,
        earnings_dates=bundle.earnings_dates,
        option_gateway=bundle.option_gateway,
        shared_cache=bundle.entry_rule_cache,
    )
    mask = evaluator.build_entry_allowed_mask()
    eligible_entry_days = sum(
        1
        for index, bar in enumerate(bundle.bars)
        if request.start_date <= bar.trade_date <= request.end_date and index < len(mask) and mask[index]
    )
    return {
        "eligible_entry_days": eligible_entry_days,
    }


def _prewarm_calls_for_dte(
    *,
    bundle: Any,
    start_date: date,
    end_date: date,
    target_dte: int,
) -> dict[str, int] | None:
    gateway = bundle.option_gateway
    if not isinstance(gateway, HistoricalOptionGateway):
        return None
    warmed_dates = 0
    warmed_contracts = 0
    for bar in bundle.bars:
        if not (start_date <= bar.trade_date <= end_date):
            continue
        try:
            contracts = gateway.list_contracts(
                entry_date=bar.trade_date,
                contract_type="call",
                target_dte=target_dte,
                dte_tolerance_days=DTE_TOLERANCE_DAYS,
            )
        except Exception:
            continue
        warmed_dates += 1
        warmed_contracts += len(contracts)
    return {
        "trade_dates": warmed_dates,
        "contracts": warmed_contracts,
    }


def _top_rows(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    qualified = [row for row in rows if int(row["trade_count"]) >= 12 and (_safe_float(row["expectancy"]) or 0.0) > 0]
    target = qualified or rows
    ordered = sorted(target, key=lambda row: (_safe_float(row["quality_score"]) or float("-inf")), reverse=True)
    return ordered[:limit]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the UVXY bear-call reversal grid from March 2018 to present.")
    parser.add_argument("--start-date", default=DEFAULT_START.isoformat())
    parser.add_argument("--end-date", default=DEFAULT_END.isoformat())
    parser.add_argument("--rsi-thresholds", default=DEFAULT_RSI_THRESHOLDS)
    parser.add_argument("--falling-bars", default=DEFAULT_FALLING_BARS)
    parser.add_argument("--target-dtes", default=DEFAULT_TARGET_DTES)
    parser.add_argument("--short-deltas", default=DEFAULT_SHORT_DELTAS)
    parser.add_argument("--width-steps", default=DEFAULT_WIDTH_STEPS)
    parser.add_argument("--profit-targets", default=DEFAULT_PROFIT_TARGETS)
    parser.add_argument("--stop-losses", default=DEFAULT_STOP_LOSSES)
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    return parser.parse_args()


def main() -> None:
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    invalidate_settings()

    args = _parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    rsi_thresholds = _parse_int_list(args.rsi_thresholds)
    falling_bars_values = _parse_int_list(args.falling_bars)
    target_dtes = _parse_int_list(args.target_dtes)
    short_deltas = _parse_int_list(args.short_deltas)
    width_steps_values = _parse_int_list(args.width_steps)
    profit_targets = _parse_int_list(args.profit_targets)
    stop_losses = _parse_int_list(args.stop_losses)
    exit_policies = [
        (Decimal(str(profit_target_pct)), Decimal(str(stop_loss_pct)))
        for profit_target_pct in profit_targets
        for stop_loss_pct in stop_losses
    ]

    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    representative_request = _build_request(
        start_date=start_date,
        end_date=end_date,
        target_dte=max(target_dtes),
        short_call_delta=max(short_deltas),
        width_steps=max(width_steps_values),
        rsi_threshold=min(rsi_thresholds),
        falling_bars=min(falling_bars_values),
    )

    total_base_runs = (
        len(rsi_thresholds)
        * len(falling_bars_values)
        * len(target_dtes)
        * len(short_deltas)
        * len(width_steps_values)
    )
    rows_written = 0
    all_rows: list[dict[str, Any]] = []
    started = time.perf_counter()

    with BacktestExecutionService() as service:
        bundle = service.market_data_service.prepare_backtest(representative_request)
        actual_trade_dates = [bar.trade_date for bar in bundle.bars if start_date <= bar.trade_date <= end_date]
        if not actual_trade_dates:
            raise RuntimeError("No UVXY bars found in the requested date range.")
        effective_end_date = actual_trade_dates[-1]
        if effective_end_date < end_date:
            end_date = effective_end_date
        resolved_parameters, risk_free_rate_curve = service.resolve_execution_inputs(representative_request)

        with output_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()

            completed_base_runs = 0
            for target_dte in target_dtes:
                prewarm_summary = _prewarm_calls_for_dte(
                    bundle=bundle,
                    start_date=start_date,
                    end_date=end_date,
                    target_dte=target_dte,
                )
                print(
                    f"PREWARM target_dte={target_dte} "
                    f"trade_dates={0 if prewarm_summary is None else prewarm_summary['trade_dates']} "
                    f"contracts={0 if prewarm_summary is None else prewarm_summary['contracts']}"
                )

                for width_steps in width_steps_values:
                    for short_call_delta in short_deltas:
                        for rsi_threshold in rsi_thresholds:
                            for falling_bars in falling_bars_values:
                                completed_base_runs += 1
                                request = _build_request(
                                    start_date=start_date,
                                    end_date=end_date,
                                    target_dte=target_dte,
                                    short_call_delta=short_call_delta,
                                    width_steps=width_steps,
                                    rsi_threshold=rsi_threshold,
                                    falling_bars=falling_bars,
                                )
                                combo_started = time.perf_counter()
                                entry_metadata = _entry_rule_metadata(
                                    service=service,
                                    request=request,
                                    bundle=bundle,
                                    resolved_parameters=resolved_parameters,
                                    risk_free_rate_curve=risk_free_rate_curve,
                                )
                                results = service.execute_exit_policy_variants(
                                    request,
                                    exit_policies=exit_policies,
                                    bundle=bundle,
                                    resolved_parameters=resolved_parameters,
                                    risk_free_rate_curve=risk_free_rate_curve,
                                )
                                combo_elapsed = time.perf_counter() - combo_started
                                for (profit_target_pct, stop_loss_pct), result in zip(exit_policies, results, strict=True):
                                    row = {
                                        "rsi_period": RSI_PERIOD,
                                        "rsi_threshold": rsi_threshold,
                                        "rsi_persistence_bars": SPIKE_PERSISTENCE_BARS,
                                        "falling_bars": falling_bars,
                                        "ema_period": EMA_PERIOD,
                                        "target_dte": target_dte,
                                        "short_call_delta": short_call_delta,
                                        "width_steps": width_steps,
                                        "profit_target_pct": int(profit_target_pct),
                                        "stop_loss_pct": int(stop_loss_pct),
                                        "eligible_entry_days": entry_metadata["eligible_entry_days"],
                                        "warning_count": len(result.warnings),
                                        "warning_codes": _warning_codes(result.warnings),
                                    }
                                    row.update(serialize_summary(result.summary))
                                    row["quality_score"] = round(_quality_score(row), 4)
                                    writer.writerow(row)
                                    all_rows.append(row)
                                    rows_written += 1
                                handle.flush()
                                elapsed = time.perf_counter() - started
                                print(
                                    f"COMBO {completed_base_runs}/{total_base_runs} "
                                    f"target_dte={target_dte} width_steps={width_steps} short_delta={short_call_delta} "
                                    f"rsi_threshold={rsi_threshold} falling_bars={falling_bars} "
                                    f"rows_written={rows_written} combo_s={combo_elapsed:.2f} elapsed_s={elapsed:.2f}"
                                )
                                status_payload = {
                                    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                    "complete": False,
                                    "start_date": start_date.isoformat(),
                                    "requested_end_date": args.end_date,
                                    "effective_end_date": end_date.isoformat(),
                                    "base_runs": total_base_runs,
                                    "base_runs_completed": completed_base_runs,
                                    "rows_written": rows_written,
                                    "rsi_thresholds": rsi_thresholds,
                                    "falling_bars": falling_bars_values,
                                    "target_dtes": target_dtes,
                                    "short_deltas": short_deltas,
                                    "width_steps": width_steps_values,
                                    "profit_targets": profit_targets,
                                    "stop_losses": stop_losses,
                                    "top_rows": _top_rows(all_rows, limit=10),
                                }
                                output_json.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "complete": True,
        "start_date": start_date.isoformat(),
        "requested_end_date": args.end_date,
        "effective_end_date": end_date.isoformat(),
        "base_runs": total_base_runs,
        "rows_written": rows_written,
        "rsi_thresholds": rsi_thresholds,
        "falling_bars": falling_bars_values,
        "target_dtes": target_dtes,
        "short_deltas": short_deltas,
        "width_steps": width_steps_values,
        "profit_targets": profit_targets,
        "stop_losses": stop_losses,
        "top_rows": _top_rows(all_rows, limit=25),
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
