from __future__ import annotations

import contextlib
import math
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy.orm import Session

from backtestforecast.config import get_settings
from backtestforecast.schemas.backtests import TradeJsonResponse
from backtestforecast.schemas.sweeps import SweepJobResponse, SweepResultResponse
from backtestforecast.services.serialization import (
    safe_validate_equity_curve,
    safe_validate_json,
    safe_validate_list,
    safe_validate_summary,
    safe_validate_warning_list,
)

logger = structlog.get_logger("services.sweeps")
_heartbeat_failures: int = 0


def sweep_scoring_config() -> dict[str, float]:
    settings = get_settings()
    return {
        "win_rate_weight": settings.sweep_score_win_rate_weight,
        "roi_weight": settings.sweep_score_roi_weight,
        "sharpe_weight": settings.sweep_score_sharpe_weight,
        "drawdown_weight": settings.sweep_score_drawdown_weight,
        "sharpe_multiplier": settings.sweep_score_sharpe_multiplier,
        "min_trades": settings.sweep_score_min_trades,
    }


def update_sweep_heartbeat(session: Session, model_cls, job_id: UUID) -> None:
    global _heartbeat_failures
    from sqlalchemy import update as _hb_update

    nested = None
    try:
        nested = session.begin_nested()
        session.execute(
            _hb_update(model_cls)
            .where(model_cls.id == job_id)
            .values(last_heartbeat_at=datetime.now(UTC))
        )
        nested.commit()
        _heartbeat_failures = 0
    except Exception:
        _heartbeat_failures += 1
        if nested is not None:
            with contextlib.suppress(Exception):
                nested.rollback()
        if _heartbeat_failures >= 3:
            logger.error(
                "sweep.heartbeat_consecutive_failures",
                job_id=str(job_id),
                consecutive_failures=_heartbeat_failures,
                hint="Reaper may kill this job if heartbeat stays stale.",
            )


def score_candidate_from_summary(summary: dict[str, Any], cfg: dict[str, float] | None = None) -> float:
    if cfg is None:
        cfg = sweep_scoring_config()
    win_rate = Decimal(str(summary.get("win_rate", 0)))
    roi = Decimal(str(summary.get("total_roi_pct", 0)))
    drawdown = max(Decimal(str(summary.get("max_drawdown_pct", 0))), Decimal("0"))
    sharpe = Decimal(str(summary.get("sharpe_ratio") or 0))
    trade_count = int(summary.get("trade_count", 0))
    decided_trades = int(summary.get("decided_trades", trade_count) or 0)

    if not all(math.isfinite(float(v)) for v in [win_rate, roi, drawdown, sharpe]):
        return 0.0

    min_trades = int(cfg["min_trades"])
    if decided_trades < min_trades:
        return 0.0

    win_rate_w = Decimal(str(round(cfg["win_rate_weight"], 10)))
    roi_w = Decimal(str(round(cfg["roi_weight"], 10)))
    sharpe_w = Decimal(str(round(cfg["sharpe_weight"], 10)))
    sharpe_m = Decimal(str(round(cfg["sharpe_multiplier"], 10)))
    drawdown_w = Decimal(str(round(cfg["drawdown_weight"], 10)))

    effective_sharpe_w = sharpe_w * sharpe_m
    total_effective = win_rate_w + roi_w + effective_sharpe_w + drawdown_w
    norm = (win_rate_w + roi_w + sharpe_w + drawdown_w) / total_effective if total_effective > 0 else Decimal("1")

    score = (
        win_rate * win_rate_w
        + roi * roi_w
        + sharpe * effective_sharpe_w
        - drawdown * drawdown_w
    ) * norm
    return float(score)


def sweep_job_response(job) -> SweepJobResponse:
    warnings = safe_validate_warning_list(job.warnings_json)
    return SweepJobResponse(
        id=job.id,
        status=job.status,
        symbol=job.symbol,
        mode=job.mode,
        plan_tier_snapshot=job.plan_tier_snapshot,
        candidate_count=job.candidate_count,
        evaluated_candidate_count=job.evaluated_candidate_count,
        result_count=job.result_count,
        prefetch_summary=safe_validate_json(
            job.prefetch_summary_json,
            "prefetch_summary_json",
            default=None,
            response_warnings=warnings,
        ),
        warnings=warnings,
        request_snapshot=safe_validate_json(
            job.request_snapshot_json,
            "request_snapshot_json",
            default={},
            response_warnings=warnings,
        ),
        error_code=job.error_code,
        error_message=job.error_message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
    )


def sweep_result_response(result) -> SweepResultResponse:
    params = result.parameter_snapshot_json or {}
    warnings = safe_validate_warning_list(result.warnings_json)
    params = safe_validate_json(
        params,
        "parameter_snapshot_json",
        default={},
        response_warnings=warnings,
    )
    persisted_trade_count = max(
        int(params.get("trade_count") or 0),
        int((result.summary_json or {}).get("trade_count") or 0),
        len(result.trades_json or []),
    )
    serialized_trade_count = max(
        int(params.get("serialized_trade_count") or 0),
        len(result.trades_json or []),
    )
    persisted_equity_point_count = max(
        int(params.get("equity_point_count") or 0),
        len(result.equity_curve_json or []),
    )
    serialized_equity_point_count = max(
        int(params.get("serialized_equity_point_count") or 0),
        len(result.equity_curve_json or []),
    )
    trades = safe_validate_list(
        TradeJsonResponse,
        result.trades_json,
        "trades_json",
        response_warnings=warnings,
    )
    equity_curve = safe_validate_equity_curve(
        result.equity_curve_json,
        field_name="equity_curve_json",
        response_warnings=warnings,
    )
    return SweepResultResponse(
        id=result.id,
        rank=result.rank,
        score=result.score,
        strategy_type=result.strategy_type,
        delta=params.get("delta"),
        width_mode=params.get("width_mode"),
        width_value=params.get("width_value"),
        entry_rule_set_name=params.get("entry_rule_set_name") or "default",
        exit_rule_set_name=params.get("exit_rule_set_name"),
        profit_target_pct=params.get("profit_target_pct"),
        stop_loss_pct=params.get("stop_loss_pct"),
        parameter_snapshot_json=params,
        summary=safe_validate_summary(
            result.summary_json,
            field_name="summary_json",
            response_warnings=warnings,
        ),
        warnings=warnings,
        trades_json=trades,
        equity_curve=equity_curve,
        trades_truncated=persisted_trade_count > serialized_trade_count,
        trade_items_omitted=max(persisted_trade_count - serialized_trade_count, 0),
        equity_curve_truncated=persisted_equity_point_count > serialized_equity_point_count,
        equity_curve_points_omitted=max(persisted_equity_point_count - serialized_equity_point_count, 0),
    )
