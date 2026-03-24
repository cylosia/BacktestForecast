from __future__ import annotations

import time as _time
from typing import Any

import structlog
from pydantic import ValidationError as _PydanticValidationError

from backtestforecast.errors import AppError, AppValidationError
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.services.backtest_execution import BacktestExecutionService

logger = structlog.get_logger("services.sweep_genetic_runtime")

_RUNTIME: dict[str, Any] = {}


def init_sweep_genetic_runtime(context: dict[str, Any]) -> None:
    payload = CreateSweepRequest.model_validate(context["payload"])
    strategy_type = context["strategy_type"]
    execution_service = BacktestExecutionService()
    representative = CreateBacktestRunRequest(
        symbol=payload.symbol,
        strategy_type=strategy_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
        target_dte=payload.target_dte,
        dte_tolerance_days=payload.dte_tolerance_days,
        max_holding_days=payload.max_holding_days,
        account_size=payload.account_size,
        risk_per_trade_pct=payload.risk_per_trade_pct,
        commission_per_contract=payload.commission_per_contract,
        entry_rules=payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else [],
        slippage_pct=payload.slippage_pct,
        custom_legs=[],
    )
    bundle = execution_service.market_data_service.prepare_backtest(representative)
    prefetcher = OptionDataPrefetcher()
    prefetcher.prefetch_for_symbol(
        symbol=payload.symbol,
        bars=bundle.bars,
        start_date=payload.start_date,
        end_date=payload.end_date,
        target_dte=payload.target_dte,
        dte_tolerance_days=payload.dte_tolerance_days,
        option_gateway=bundle.option_gateway,
    )
    _RUNTIME.clear()
    _RUNTIME.update({
        "payload": payload,
        "strategy_type": strategy_type,
        "bundle": bundle,
        "execution_service": execution_service,
        "started_at": _time.monotonic(),
        "timeout_seconds": float(context["timeout_seconds"]),
        "score_summary": context["score_summary"],
    })


def evaluate_sweep_individual(individual: list[dict[str, Any]]) -> float:
    from backtestforecast.services.sweeps import SweepService

    payload: CreateSweepRequest = _RUNTIME["payload"]
    strategy_type = _RUNTIME["strategy_type"]
    execution_service: BacktestExecutionService = _RUNTIME["execution_service"]
    bundle = _RUNTIME["bundle"]
    timeout_seconds = _RUNTIME["timeout_seconds"]
    if (_time.monotonic() - _RUNTIME["started_at"]) > timeout_seconds:
        return 0.0
    entry_rules = payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else []
    exit_set = payload.exit_rule_sets[0] if payload.exit_rule_sets else None
    request = CreateBacktestRunRequest(
        symbol=payload.symbol,
        strategy_type=strategy_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
        target_dte=payload.target_dte,
        dte_tolerance_days=payload.dte_tolerance_days,
        max_holding_days=payload.max_holding_days,
        account_size=payload.account_size,
        risk_per_trade_pct=payload.risk_per_trade_pct,
        commission_per_contract=payload.commission_per_contract,
        entry_rules=entry_rules,
        slippage_pct=payload.slippage_pct,
        profit_target_pct=exit_set.profit_target_pct if exit_set else None,
        stop_loss_pct=exit_set.stop_loss_pct if exit_set else None,
        custom_legs=individual,
    )
    try:
        result = execution_service.execute_request(request, bundle=bundle)
        summary = {
            "trade_count": result.summary.trade_count,
            "decided_trades": getattr(result.summary, "decided_trades", result.summary.trade_count),
            "win_rate": result.summary.win_rate,
            "total_roi_pct": result.summary.total_roi_pct,
            "sharpe_ratio": result.summary.sharpe_ratio,
            "max_drawdown_pct": result.summary.max_drawdown_pct,
        }
        return SweepService._score_candidate_from_summary(summary)
    except (AppError, AppValidationError, _PydanticValidationError):
        return 0.0
    except Exception:
        logger.warning("sweep.genetic_fitness_unexpected_error", exc_info=True)
        return 0.0
