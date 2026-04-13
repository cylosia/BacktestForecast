from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager, nullcontext
import csv
from functools import wraps
import json
import os
import sys
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
os.environ.setdefault("HISTORICAL_DATA_LOCAL_PREFERRED", "true")
os.environ.setdefault("HISTORICAL_DATA_T_MINUS_ONE_ONLY", "false")

from backtestforecast.backtests.strategies.common import preferred_expiration_dates  # noqa: E402
from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleEvaluator  # noqa: E402
from backtestforecast.config import invalidate_settings  # noqa: E402
from backtestforecast.db import session as db_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.pipeline.regime import Regime  # noqa: E402
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
from sqlalchemy import event  # noqa: E402


DEFAULT_OUTPUT_CSV = ROOT / "logs" / "uvxy_long_put_2015_05_2020_04_delta_dte_take_profit_grid.csv"
DEFAULT_OUTPUT_JSON = ROOT / "logs" / "uvxy_long_put_2015_05_2020_04_delta_dte_take_profit_grid.json"
DEFAULT_ONE_CONTRACT_OUTPUT_CSV = ROOT / "logs" / "uvxy_long_put_2015_05_2020_04_delta_dte_take_profit_grid_one_contract.csv"
DEFAULT_ONE_CONTRACT_OUTPUT_JSON = ROOT / "logs" / "uvxy_long_put_2015_05_2020_04_delta_dte_take_profit_grid_one_contract.json"
DEFAULT_QUERY_PROFILE_JSON = ROOT / "logs" / "uvxy_long_put_2015_05_2020_04_query_profile.json"
REGIME_CHOICES = [regime.value for regime in Regime]
ENTRY_METADATA_FIELDS = [
    "required_regimes",
    "blocked_regimes",
    "eligible_entry_days",
]
DERIVED_FIELDS = [
    "average_purchase_price",
    "roi_pct_per_day",
]
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
    *ENTRY_METADATA_FIELDS,
    *DERIVED_FIELDS,
    *SUMMARY_FIELDS,
]

_ORIGINAL_ATTACH_POSITION_QUOTE_SERIES = OptionsBacktestEngine._attach_position_quote_series
_ORIGINAL_RESOLVE_POSITION_SIZE = OptionsBacktestEngine._resolve_position_size


class _SqlQueryProfiler:
    def __init__(self, *, output_json: Path) -> None:
        self.output_json = output_json
        self._local = threading.local()
        self._patches: list[tuple[type[Any], str, Any]] = []
        self._event_targets: list[Any] = []
        self._scope_stats: dict[str, dict[str, Any]] = defaultdict(self._new_scope_stats)

    @staticmethod
    def _new_scope_stats() -> dict[str, Any]:
        return {
            "calls": 0,
            "wall_s": 0.0,
            "sql_count": 0,
            "sql_s": 0.0,
            "statements": defaultdict(lambda: {"count": 0, "wall_s": 0.0}),
        }

    def _stack(self) -> list[str]:
        stack = getattr(self._local, "scope_stack", None)
        if stack is None:
            stack = []
            self._local.scope_stack = stack
        return stack

    def _current_scope(self) -> str:
        stack = self._stack()
        if not stack:
            return "unscoped"
        return stack[-1]

    @staticmethod
    def _normalize_sql(statement: str) -> str:
        normalized = " ".join(statement.split())
        if len(normalized) > 220:
            return f"{normalized[:217]}..."
        return normalized

    @contextmanager
    def scope(self, name: str):
        stats = self._scope_stats[name]
        stats["calls"] += 1
        stack = self._stack()
        stack.append(name)
        started = time.perf_counter()
        try:
            yield
        finally:
            stats["wall_s"] += time.perf_counter() - started
            stack.pop()

    def _before_cursor_execute(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        context._uvxy_query_profile_started = time.perf_counter()
        context._uvxy_query_profile_scope = self._current_scope()
        context._uvxy_query_profile_statement = self._normalize_sql(statement)

    def _after_cursor_execute(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        started = getattr(context, "_uvxy_query_profile_started", None)
        if started is None:
            return
        elapsed_s = time.perf_counter() - started
        scope_name = getattr(context, "_uvxy_query_profile_scope", "unscoped")
        statement_key = getattr(
            context,
            "_uvxy_query_profile_statement",
            self._normalize_sql(statement),
        )
        stats = self._scope_stats[scope_name]
        stats["sql_count"] += 1
        stats["sql_s"] += elapsed_s
        statement_stats = stats["statements"][statement_key]
        statement_stats["count"] += 1
        statement_stats["wall_s"] += elapsed_s

    def _patch_method(self, cls: type[Any], method_name: str, *, scope_name: str | None = None) -> None:
        original = getattr(cls, method_name)
        profiler = self
        resolved_scope_name = scope_name or f"{cls.__name__}.{method_name}"

        @wraps(original)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            with profiler.scope(resolved_scope_name):
                return original(*args, **kwargs)

        setattr(cls, method_name, wrapped)
        self._patches.append((cls, method_name, original))

    def _install_method_wrappers(self) -> None:
        store_methods = [
            "list_option_contracts_for_expiration",
            "list_option_contracts_for_expirations",
            "list_option_contracts_for_expirations_by_type",
            "list_available_option_expirations",
            "list_available_option_expirations_by_type",
            "get_option_quote_for_date",
            "get_option_quotes_for_date",
            "get_option_quote_series",
        ]
        gateway_methods = [
            "list_contracts_for_preferred_expiration",
            "list_contracts_for_preferred_common_expiration",
            "list_contracts_for_expirations",
            "list_contracts_for_expirations_by_type",
            "list_available_expirations",
            "list_available_expirations_by_type",
            "get_quote",
            "get_quotes",
            "get_quote_series",
        ]
        for method_name in store_methods:
            self._patch_method(HistoricalMarketDataStore, method_name)
        for method_name in gateway_methods:
            self._patch_method(HistoricalOptionGateway, method_name)

    def _install_sqlalchemy_listeners(self) -> None:
        engines = [db_session._get_engine()]
        readonly_engine = db_session._get_readonly_engine()
        if readonly_engine is not None and readonly_engine is not engines[0]:
            engines.append(readonly_engine)
        for engine in engines:
            event.listen(engine, "before_cursor_execute", self._before_cursor_execute)
            event.listen(engine, "after_cursor_execute", self._after_cursor_execute)
            self._event_targets.append(engine)

    def _remove_sqlalchemy_listeners(self) -> None:
        for engine in self._event_targets:
            event.remove(engine, "before_cursor_execute", self._before_cursor_execute)
            event.remove(engine, "after_cursor_execute", self._after_cursor_execute)
        self._event_targets.clear()

    def install(self) -> None:
        self._install_method_wrappers()
        self._install_sqlalchemy_listeners()

    def uninstall(self) -> None:
        self._remove_sqlalchemy_listeners()
        for cls, method_name, original in reversed(self._patches):
            setattr(cls, method_name, original)
        self._patches.clear()

    def write_report(self) -> None:
        ordered_scopes = sorted(
            self._scope_stats.items(),
            key=lambda item: (item[1]["sql_s"], item[1]["wall_s"]),
            reverse=True,
        )
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "scopes": [
                {
                    "scope": scope_name,
                    "calls": stats["calls"],
                    "wall_s": round(stats["wall_s"], 6),
                    "sql_count": stats["sql_count"],
                    "sql_s": round(stats["sql_s"], 6),
                    "avg_sql_ms": round((stats["sql_s"] * 1000.0 / stats["sql_count"]), 3)
                    if stats["sql_count"]
                    else 0.0,
                    "top_statements": [
                        {
                            "sql": statement,
                            "count": statement_stats["count"],
                            "wall_s": round(statement_stats["wall_s"], 6),
                        }
                        for statement, statement_stats in sorted(
                            stats["statements"].items(),
                            key=lambda item: (item[1]["wall_s"], item[1]["count"]),
                            reverse=True,
                        )[:8]
                    ],
                }
                for scope_name, stats in ordered_scopes
            ],
        }
        self.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _emit(message: str) -> None:
    stream = getattr(sys, "stdout", None)
    if stream is None:
        return
    try:
        print(message, flush=True)
    except Exception:
        return


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


def _maybe_profile_scope(profiler: _SqlQueryProfiler | None, name: str):
    if profiler is None:
        return nullcontext()
    return profiler.scope(name)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the UVXY long-put delta/DTE/take-profit grid and export summary rows to CSV."
    )
    parser.add_argument("--symbol", default="UVXY")
    parser.add_argument("--strategy", choices=[StrategyType.LONG_PUT.value], default=StrategyType.LONG_PUT.value)
    parser.add_argument("--start-date", default="2015-05-04")
    parser.add_argument("--end-date", default="2020-04-30")
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
    parser.add_argument("--output-csv", default=str(DEFAULT_ONE_CONTRACT_OUTPUT_CSV))
    parser.add_argument("--output-json", default=str(DEFAULT_ONE_CONTRACT_OUTPUT_JSON))
    parser.add_argument("--query-profile-json", default="")
    parser.add_argument("--enable-service-prefetch", action="store_true")
    parser.add_argument(
        "--require-regime",
        action="append",
        choices=REGIME_CHOICES,
        default=[],
        help="Require a regime label at entry. May be passed multiple times.",
    )
    parser.add_argument(
        "--block-regime",
        action="append",
        choices=REGIME_CHOICES,
        default=[],
        help="Disallow entries when this regime label is present. May be passed multiple times.",
    )
    return parser.parse_args(argv)


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
    entry_rules: list[RegimeRule] | None = None,
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
        entry_rules=list(entry_rules or []),
        slippage_pct=slippage_pct,
        strategy_overrides=strategy_overrides,
    )


def _normalized_regime_labels(values: list[str]) -> list[str]:
    return sorted({value.strip().lower() for value in values if value and value.strip()})


def _build_regime_entry_rules(*, required_labels: list[str], blocked_labels: list[str]) -> list[RegimeRule]:
    if not required_labels and not blocked_labels:
        return []
    return [
        RegimeRule(
            type="regime",
            required_regimes=[Regime(label) for label in required_labels],
            blocked_regimes=[Regime(label) for label in blocked_labels],
        )
    ]


def _entry_rule_metadata(
    *,
    service: BacktestExecutionService,
    request: CreateBacktestRunRequest,
    bundle: Any,
    resolved_parameters: Any,
    risk_free_rate_curve: Any,
    required_labels: list[str],
    blocked_labels: list[str],
) -> dict[str, Any]:
    if not request.entry_rules:
        return {
            "required_regimes": "",
            "blocked_regimes": "",
            "eligible_entry_days": sum(
                1 for bar in bundle.bars if request.start_date <= bar.trade_date <= request.end_date
            ),
        }

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
        "required_regimes": ";".join(required_labels),
        "blocked_regimes": ";".join(blocked_labels),
        "eligible_entry_days": eligible_entry_days,
    }


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
    entry_rule_metadata: dict[str, Any],
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
        "required_regimes": entry_rule_metadata.get("required_regimes", ""),
        "blocked_regimes": entry_rule_metadata.get("blocked_regimes", ""),
        "eligible_entry_days": entry_rule_metadata.get("eligible_entry_days", ""),
    }


def _load_existing_progress(
    output_csv: Path,
    *,
    expected_profit_targets: set[int],
) -> tuple[set[tuple[int, int]], int, int, int]:
    if not output_csv.exists():
        return set(), 0, 0, 0

    combo_to_profit_targets: dict[tuple[int, int], set[int]] = {}
    rows_written = 0
    ok_rows = 0
    error_rows = 0
    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows_written += 1
            try:
                combo_key = (int(row["delta_target"]), int(row["target_dte"]))
                profit_target = int(float(row["profit_target_pct"]))
            except (KeyError, TypeError, ValueError):
                continue
            if profit_target in expected_profit_targets:
                combo_to_profit_targets.setdefault(combo_key, set()).add(profit_target)
            if row.get("status") == "ok":
                ok_rows += 1
            elif row.get("status") == "error":
                error_rows += 1

    completed_combos = {
        combo_key
        for combo_key, seen_profit_targets in combo_to_profit_targets.items()
        if seen_profit_targets == expected_profit_targets
    }
    return completed_combos, rows_written, ok_rows, error_rows


def _trade_bars_for_window(*, bundle: Any, start_date: date, end_date: date) -> list[Any]:
    return [bar for bar in bundle.bars if start_date <= bar.trade_date <= end_date]


def _prewarm_full_exact_contracts_for_dte(
    *,
    bundle: Any,
    start_date: date,
    end_date: date,
    target_dte: int,
    dte_tolerance_days: int,
) -> dict[str, Any] | None:
    gateway = bundle.option_gateway
    if not isinstance(gateway, HistoricalOptionGateway):
        return None

    warmed_dates = 0
    warmed_contracts = 0
    errors: list[str] = []
    for bar in _trade_bars_for_window(bundle=bundle, start_date=start_date, end_date=end_date):
        expirations = preferred_expiration_dates(bar.trade_date, target_dte, dte_tolerance_days)
        try:
            contracts_by_expiration = gateway.list_contracts_for_expirations(
                entry_date=bar.trade_date,
                contract_type="put",
                expiration_dates=expirations,
            )
            warmed_dates += 1
            warmed_contracts += sum(len(contracts) for contracts in contracts_by_expiration.values())
        except Exception as exc:  # pragma: no cover - defensive operational logging
            if len(errors) < 20:
                errors.append(f"{bar.trade_date}: {exc}")

    return {
        "trade_dates": warmed_dates,
        "contracts_fetched": warmed_contracts,
        "error_count": len(errors),
        "errors": errors,
    }


def _round_decimal_metric(value: Decimal | None, *, places: str) -> float | str:
    if value is None:
        return ""
    return float(value.quantize(Decimal(places)))


def _derive_trade_metrics(trades: list[Any]) -> dict[str, float | str]:
    total_contracts = 0
    total_entry_mid = Decimal("0")
    capital_days = Decimal("0")
    total_net_pnl = Decimal("0")

    for trade in trades:
        quantity = int(getattr(trade, "quantity", 0) or 0)
        entry_mid = Decimal(str(getattr(trade, "entry_mid", 0) or 0))
        holding_days = max(int(getattr(trade, "holding_period_days", 0) or 0), 1)
        if quantity <= 0 or entry_mid <= 0:
            continue
        total_contracts += quantity
        total_entry_mid += entry_mid * quantity
        capital_days += entry_mid * Decimal("100") * quantity * holding_days
        total_net_pnl += Decimal(str(getattr(trade, "net_pnl", 0) or 0))

    average_purchase_price = (
        total_entry_mid / Decimal(total_contracts)
        if total_contracts > 0
        else None
    )
    roi_pct_per_day = (
        (total_net_pnl * Decimal("100")) / capital_days
        if capital_days > 0
        else None
    )
    return {
        "average_purchase_price": _round_decimal_metric(average_purchase_price, places="0.0001"),
        "roi_pct_per_day": _round_decimal_metric(roi_pct_per_day, places="0.0001"),
    }


def _write_status_json(
    *,
    output_json: Path,
    output_csv: Path,
    symbol: str,
    strategy_type: StrategyType,
    start_date: date,
    end_date: date,
    delta_values: list[int],
    dte_values: list[int],
    profit_values: list[int],
    dte_tolerance_days: int,
    max_holding_days: int,
    account_size: Decimal,
    risk_per_trade_pct: Decimal,
    commission_per_contract: Decimal,
    slippage_pct: Decimal,
    required_regimes: list[str],
    blocked_regimes: list[str],
    total_base_runs: int,
    total_rows: int,
    rows_written: int,
    ok_rows: int,
    error_rows: int,
    total_elapsed_s: float,
    completed_base_runs: int,
    current_delta: int | None = None,
    current_dte: int | None = None,
    current_prewarm: dict[str, Any] | None = None,
    error_examples: list[dict[str, Any]] | None = None,
    complete: bool = False,
) -> None:
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "complete": complete,
        "symbol": symbol,
        "strategy_type": strategy_type.value,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "delta_values": delta_values,
        "dte_values": dte_values,
        "profit_target_values": profit_values,
        "dte_tolerance_days": dte_tolerance_days,
        "max_holding_days": max_holding_days,
        "account_size": float(account_size),
        "risk_per_trade_pct": float(risk_per_trade_pct),
        "commission_per_contract": float(commission_per_contract),
        "slippage_pct": float(slippage_pct),
        "required_regimes": required_regimes,
        "blocked_regimes": blocked_regimes,
        "base_runs": total_base_runs,
        "base_runs_completed": completed_base_runs,
        "rows_expected": total_rows,
        "rows_written": rows_written,
        "ok_rows": ok_rows,
        "error_rows": error_rows,
        "total_elapsed_s": round(total_elapsed_s, 4),
        "current_delta": current_delta,
        "current_dte": current_dte,
        "current_prewarm": current_prewarm,
        "csv_path": str(output_csv),
        "error_examples": error_examples or [],
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()
    args = _parse_args()
    if not args.enable_service_prefetch:
        os.environ["BACKTEST_OPTION_PREFETCH_ENABLED"] = "false"
        invalidate_settings()
    symbol = args.symbol.strip().upper()
    strategy_type = StrategyType(args.strategy)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    required_regime_labels = _normalized_regime_labels(args.require_regime)
    blocked_regime_labels = _normalized_regime_labels(args.block_regime)
    regime_entry_rules = _build_regime_entry_rules(
        required_labels=required_regime_labels,
        blocked_labels=blocked_regime_labels,
    )
    account_size = Decimal(args.account_size)
    risk_per_trade_pct = Decimal(args.risk_per_trade_pct)
    commission_per_contract = Decimal(args.commission_per_contract)
    slippage_pct = Decimal(args.slippage_pct)
    delta_values = _inclusive_range(args.delta_start, args.delta_end, args.delta_step)
    dte_values = _inclusive_range(args.dte_start, args.dte_end, args.dte_step)
    profit_values = _inclusive_range(args.profit_start, args.profit_end, args.profit_step)
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)
    query_profile_json = Path(args.query_profile_json) if args.query_profile_json else None
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    if query_profile_json is not None:
        query_profile_json.parent.mkdir(parents=True, exist_ok=True)

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
        entry_rules=regime_entry_rules,
    )

    total_base_runs = len(delta_values) * len(dte_values)
    total_rows = total_base_runs * len(profit_values)
    expected_profit_targets = set(profit_values)
    completed_combos, rows_written, ok_rows, error_rows = _load_existing_progress(
        output_csv,
        expected_profit_targets=expected_profit_targets,
    )
    completed_base_runs = len(completed_combos)
    error_examples: list[dict[str, Any]] = []
    started = time.perf_counter()
    query_profiler = _SqlQueryProfiler(output_json=query_profile_json) if query_profile_json is not None else None
    if query_profiler is not None:
        query_profiler.install()

    _emit(
        f"START symbol={symbol} strategy={strategy_type.value} start={start_date.isoformat()} "
        f"end={end_date.isoformat()} base_runs={total_base_runs} rows={total_rows} "
        f"resume_completed={completed_base_runs} "
        f"required_regimes={';'.join(required_regime_labels) or '-'} "
        f"blocked_regimes={';'.join(blocked_regime_labels) or '-'}"
    )
    _write_status_json(
        output_json=output_json,
        output_csv=output_csv,
        symbol=symbol,
        strategy_type=strategy_type,
        start_date=start_date,
        end_date=end_date,
        delta_values=delta_values,
        dte_values=dte_values,
        profit_values=profit_values,
        dte_tolerance_days=args.dte_tolerance_days,
        max_holding_days=args.max_holding_days,
        account_size=account_size,
        risk_per_trade_pct=risk_per_trade_pct,
        commission_per_contract=commission_per_contract,
        slippage_pct=slippage_pct,
        required_regimes=required_regime_labels,
        blocked_regimes=blocked_regime_labels,
        total_base_runs=total_base_runs,
        total_rows=total_rows,
        rows_written=rows_written,
        ok_rows=ok_rows,
        error_rows=error_rows,
        total_elapsed_s=0.0,
        completed_base_runs=completed_base_runs,
        error_examples=error_examples,
        complete=False,
    )
    try:
        with BacktestExecutionService() as service:
            bundle = service.market_data_service.prepare_backtest(representative_request)
            resolved_parameters, risk_free_rate_curve = service.resolve_execution_inputs(representative_request)

            write_header = rows_written == 0
            with output_csv.open("a" if not write_header else "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                if write_header:
                    writer.writeheader()

                processed_base_runs = completed_base_runs
                for target_dte in dte_values:
                    with _maybe_profile_scope(query_profiler, "script.prewarm_full_exact_contracts_for_dte"):
                        prewarm_summary = _prewarm_full_exact_contracts_for_dte(
                            bundle=bundle,
                            start_date=start_date,
                            end_date=end_date,
                            target_dte=target_dte,
                            dte_tolerance_days=args.dte_tolerance_days,
                        )
                    if prewarm_summary is not None:
                        _emit(
                            f"PREWARM dte={target_dte} trade_dates={prewarm_summary['trade_dates']} "
                            f"contracts={prewarm_summary['contracts_fetched']} "
                            f"errors={prewarm_summary['error_count']}"
                        )

                    for delta_target in delta_values:
                        combo_key = (delta_target, target_dte)
                        if combo_key in completed_combos:
                            continue
                        processed_base_runs += 1
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
                            entry_rules=regime_entry_rules,
                        )
                        entry_rule_metadata = _entry_rule_metadata(
                            service=service,
                            request=request,
                            bundle=bundle,
                            resolved_parameters=resolved_parameters,
                            risk_free_rate_curve=risk_free_rate_curve,
                            required_labels=required_regime_labels,
                            blocked_labels=blocked_regime_labels,
                        )

                        combo_start = time.perf_counter()
                        try:
                            with _maybe_profile_scope(query_profiler, "script.execute_exit_policy_variants"):
                                results = service.execute_exit_policy_variants(
                                    request,
                                    exit_policies=[(Decimal(str(profit_target_pct)), None) for profit_target_pct in profit_values],
                                    bundle=bundle,
                                    resolved_parameters=resolved_parameters,
                                    risk_free_rate_curve=risk_free_rate_curve,
                                )
                            combo_elapsed = time.perf_counter() - combo_start
                            for profit_target_pct, result in zip(profit_values, results, strict=True):
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
                                    entry_rule_metadata=entry_rule_metadata,
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
                                row.update(_derive_trade_metrics(result.trades))
                                row.update(serialize_summary(result.summary))
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
                                    entry_rule_metadata=entry_rule_metadata,
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
                        completed_combos.add(combo_key)
                        _write_status_json(
                            output_json=output_json,
                            output_csv=output_csv,
                            symbol=symbol,
                            strategy_type=strategy_type,
                            start_date=start_date,
                            end_date=end_date,
                            delta_values=delta_values,
                            dte_values=dte_values,
                            profit_values=profit_values,
                            dte_tolerance_days=args.dte_tolerance_days,
                            max_holding_days=args.max_holding_days,
                            account_size=account_size,
                            risk_per_trade_pct=risk_per_trade_pct,
                            commission_per_contract=commission_per_contract,
                            slippage_pct=slippage_pct,
                            required_regimes=required_regime_labels,
                            blocked_regimes=blocked_regime_labels,
                            total_base_runs=total_base_runs,
                            total_rows=total_rows,
                            rows_written=rows_written,
                            ok_rows=ok_rows,
                            error_rows=error_rows,
                            total_elapsed_s=time.perf_counter() - started,
                            completed_base_runs=len(completed_combos),
                            current_delta=delta_target,
                            current_dte=target_dte,
                            current_prewarm=prewarm_summary,
                            error_examples=error_examples,
                            complete=False,
                        )

                        if (
                            processed_base_runs == 1
                            or processed_base_runs % 10 == 0
                            or processed_base_runs == total_base_runs
                        ):
                            elapsed_s = time.perf_counter() - started
                            _emit(
                                f"PROGRESS combo={processed_base_runs}/{total_base_runs} "
                                f"delta={delta_target} dte={target_dte} rows_written={rows_written} "
                                f"elapsed_s={elapsed_s:.2f}"
                            )

        total_elapsed_s = time.perf_counter() - started
        _write_status_json(
            output_json=output_json,
            output_csv=output_csv,
            symbol=symbol,
            strategy_type=strategy_type,
            start_date=start_date,
            end_date=end_date,
            delta_values=delta_values,
            dte_values=dte_values,
            profit_values=profit_values,
            dte_tolerance_days=args.dte_tolerance_days,
            max_holding_days=args.max_holding_days,
            account_size=account_size,
            risk_per_trade_pct=risk_per_trade_pct,
            commission_per_contract=commission_per_contract,
            slippage_pct=slippage_pct,
            required_regimes=required_regime_labels,
            blocked_regimes=blocked_regime_labels,
            total_base_runs=total_base_runs,
            total_rows=total_rows,
            rows_written=rows_written,
            ok_rows=ok_rows,
            error_rows=error_rows,
            total_elapsed_s=total_elapsed_s,
            completed_base_runs=len(completed_combos),
            error_examples=error_examples,
            complete=True,
        )
        _emit(
            f"DONE rows_written={rows_written} ok_rows={ok_rows} error_rows={error_rows} "
            f"elapsed_s={total_elapsed_s:.2f} csv={output_csv} json={output_json}"
        )
    finally:
        if query_profiler is not None:
            try:
                query_profiler.write_report()
                _emit(f"QUERY_PROFILE json={query_profile_json}")
            finally:
                query_profiler.uninstall()


if __name__ == "__main__":
    main()
