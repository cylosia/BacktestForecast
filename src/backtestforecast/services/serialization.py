"""Shared serialization and validation helpers for scan and sweep services."""
from __future__ import annotations

import math
from contextlib import suppress
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import ValidationError

from backtestforecast.utils import to_decimal

if TYPE_CHECKING:
    from backtestforecast.schemas.backtests import BacktestSummaryResponse

_validate_logger = structlog.get_logger("services.validation")

_ZEROED_SUMMARY = {
    "trade_count": 0, "total_commissions": 0, "total_net_pnl": 0,
    "starting_equity": 0, "ending_equity": 0,
}


def _append_integrity_warning(warnings: list[dict[str, Any]] | None, field_name: str) -> None:
    if warnings is None:
        return
    warning = {
        "code": "stored_payload_invalid",
        "message": f"Stored {field_name} payload was malformed and has been partially omitted or replaced.",
    }
    if warning not in warnings:
        warnings.append(warning)


def safe_validate_summary(
    data: dict,
    *,
    field_name: str = "summary",
    response_warnings: list[dict[str, Any]] | None = None,
) -> BacktestSummaryResponse:
    """Parse summary JSON, returning a zeroed summary on corrupt data.

    Used by both scan and sweep response builders.
    """
    from backtestforecast.schemas.backtests import BacktestSummaryResponse

    try:
        return BacktestSummaryResponse.model_validate(data)
    except Exception:
        with suppress(Exception):
            from backtestforecast.observability.metrics import SCAN_CORRUPT_SUMMARY_TOTAL
            SCAN_CORRUPT_SUMMARY_TOTAL.inc()
        _validate_logger.error(
            "malformed_summary_json",
            keys=list(data.keys()) if isinstance(data, dict) else None,
        )
        _append_integrity_warning(response_warnings, field_name)
        return BacktestSummaryResponse.model_validate(_ZEROED_SUMMARY)


def safe_validate_list(
    model_cls: type,
    items: list | None,
    field_name: str,
    *,
    response_warnings: list[dict[str, Any]] | None = None,
) -> list:
    """Validate a list of JSON dicts against a Pydantic model, skipping malformed entries."""
    if items is None:
        return []
    result = []
    had_malformed = False
    for item in items:
        try:
            result.append(model_cls.model_validate(item))
        except Exception:
            had_malformed = True
            _validate_logger.warning(
                "malformed_json_entry_skipped",
                field=field_name,
                item_keys=list(item.keys()) if isinstance(item, dict) else None,
            )
    if had_malformed:
        _append_integrity_warning(response_warnings, field_name)
    return result


def safe_validate_equity_curve(
    data: list,
    *,
    field_name: str = "equity_curve",
    response_warnings: list[dict[str, Any]] | None = None,
) -> list:
    """Parse equity curve JSON items, skipping corrupt entries."""
    from backtestforecast.schemas.backtests import EquityCurvePointResponse

    results = []
    had_malformed = False
    for item in data:
        try:
            results.append(EquityCurvePointResponse.model_validate(item))
        except Exception:
            had_malformed = True
            _validate_logger.warning(
                "malformed_equity_curve_entry_skipped",
                item_keys=list(item.keys()) if isinstance(item, dict) else None,
            )
            continue
    if had_malformed:
        _append_integrity_warning(response_warnings, field_name)
    return results


def safe_validate_json(
    data: Any,
    label: str,
    *,
    default: Any = None,
    response_warnings: list[dict[str, Any]] | None = None,
) -> Any:
    """Return *data* if it's a dict or list, otherwise *default*."""
    if data is None:
        return default
    if isinstance(data, (dict, list)):
        return data
    _validate_logger.warning("malformed_json_payload_replaced", label=label, got_type=type(data).__name__)
    _append_integrity_warning(response_warnings, label)
    return default


def safe_validate_warning_list(
    items: list | None,
    field_name: str = "warnings",
    *,
    response_warnings: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Validate warning payloads so malformed stored entries never reach responses."""
    if items is None:
        return []
    result: list[dict[str, Any]] = []
    had_malformed = False
    for item in items:
        if not isinstance(item, dict):
            had_malformed = True
            _validate_logger.warning("malformed_warning_entry_skipped", field=field_name, got_type=type(item).__name__)
            continue
        message = item.get("message")
        code = item.get("code")
        if not isinstance(message, str) or not message.strip():
            had_malformed = True
            _validate_logger.warning("malformed_warning_entry_skipped", field=field_name, item_keys=list(item.keys()))
            continue
        normalized = dict(item)
        normalized["message"] = message
        if code is not None:
            normalized["code"] = str(code)
        result.append(normalized)
    if had_malformed:
        _append_integrity_warning(response_warnings, field_name)
    return result


def safe_validate_model(
    model_cls: type,
    data: Any,
    field_name: str,
    *,
    default: Any = None,
    response_warnings: list[dict[str, Any]] | None = None,
    required_keys: set[str] | None = None,
) -> Any:
    """Validate a single object against a Pydantic model, falling back safely."""
    if data is None:
        return default
    if required_keys is not None and (not isinstance(data, dict) or not required_keys.issubset(set(data.keys()))):
        _validate_logger.warning(
            "malformed_model_payload_missing_required_keys",
            field=field_name,
            model=getattr(model_cls, "__name__", str(model_cls)),
            required_keys=sorted(required_keys),
            item_keys=list(data.keys()) if isinstance(data, dict) else None,
            got_type=type(data).__name__,
        )
        _append_integrity_warning(response_warnings, field_name)
        return default
    try:
        return model_cls.model_validate(data)
    except ValidationError:
        _validate_logger.warning(
            "malformed_model_payload_replaced",
            field=field_name,
            model=getattr(model_cls, "__name__", str(model_cls)),
            item_keys=list(data.keys()) if isinstance(data, dict) else None,
            got_type=type(data).__name__,
        )
        _append_integrity_warning(response_warnings, field_name)
        return default


def safe_validate_model_list(
    model_cls: type,
    items: list | None,
    field_name: str,
    *,
    response_warnings: list[dict[str, Any]] | None = None,
) -> list:
    """Validate a list of objects against a Pydantic model, skipping malformed entries."""
    if items is None:
        return []
    result = []
    had_malformed = False
    for item in items:
        try:
            result.append(model_cls.model_validate(item))
        except ValidationError:
            had_malformed = True
            _validate_logger.warning(
                "malformed_model_entry_skipped",
                field=field_name,
                model=getattr(model_cls, "__name__", str(model_cls)),
                item_keys=list(item.keys()) if isinstance(item, dict) else None,
                got_type=type(item).__name__,
            )
    if had_malformed:
        _append_integrity_warning(response_warnings, field_name)
    return result


def _safe_decimal(val: float | Decimal) -> float:
    """Convert to quantized Decimal then float; NaN/Inf -> 0.0."""
    try:
        result = to_decimal(val)
    except (ValueError, ArithmeticError):
        return 0.0
    return float(result) if result is not None else 0.0


def _opt_decimal(val: float | None) -> float | None:
    """Convert to quantized Decimal then float; Inf/NaN -> None."""
    if val is None:
        return None
    try:
        result = to_decimal(val, allow_infinite=True)
    except (ValueError, ArithmeticError):
        return None
    if result is not None and math.isinf(float(result)):
        return None
    return float(result) if result is not None else None


def serialize_summary(summary: Any) -> dict[str, Any]:
    """Convert a backtest summary object to a JSON-safe dict."""
    return {
        "trade_count": summary.trade_count,
        "win_rate": _safe_decimal(summary.win_rate),
        "total_roi_pct": _safe_decimal(summary.total_roi_pct),
        "average_win_amount": _safe_decimal(summary.average_win_amount),
        "average_loss_amount": _safe_decimal(summary.average_loss_amount),
        "average_holding_period_days": _safe_decimal(summary.average_holding_period_days),
        "average_dte_at_open": _safe_decimal(summary.average_dte_at_open),
        "max_drawdown_pct": _safe_decimal(summary.max_drawdown_pct),
        "total_commissions": _safe_decimal(summary.total_commissions),
        "total_net_pnl": _safe_decimal(summary.total_net_pnl),
        "starting_equity": _safe_decimal(summary.starting_equity),
        "ending_equity": _safe_decimal(summary.ending_equity),
        "profit_factor": _opt_decimal(summary.profit_factor),
        "payoff_ratio": _opt_decimal(summary.payoff_ratio),
        "expectancy": _safe_decimal(summary.expectancy),
        "sharpe_ratio": _opt_decimal(summary.sharpe_ratio),
        "sortino_ratio": _opt_decimal(summary.sortino_ratio),
        "cagr_pct": _opt_decimal(summary.cagr_pct),
        "calmar_ratio": _opt_decimal(summary.calmar_ratio),
        "max_consecutive_wins": summary.max_consecutive_wins,
        "max_consecutive_losses": summary.max_consecutive_losses,
        "recovery_factor": _opt_decimal(summary.recovery_factor),
    }


def serialize_trade(trade: Any) -> dict[str, Any]:
    """Convert a backtest trade object to a JSON-safe dict."""
    return {
        "option_ticker": trade.option_ticker,
        "strategy_type": trade.strategy_type,
        "underlying_symbol": trade.underlying_symbol,
        "entry_date": trade.entry_date.isoformat(),
        "exit_date": trade.exit_date.isoformat(),
        "expiration_date": trade.expiration_date.isoformat(),
        "quantity": trade.quantity,
        "dte_at_open": trade.dte_at_open,
        "holding_period_days": trade.holding_period_days,
        "entry_underlying_close": _safe_decimal(trade.entry_underlying_close),
        "exit_underlying_close": _safe_decimal(trade.exit_underlying_close),
        "entry_mid": _safe_decimal(trade.entry_mid),
        "exit_mid": _safe_decimal(trade.exit_mid),
        "gross_pnl": _safe_decimal(trade.gross_pnl),
        "net_pnl": _safe_decimal(trade.net_pnl),
        "total_commissions": _safe_decimal(trade.total_commissions),
        "entry_reason": trade.entry_reason,
        "exit_reason": trade.exit_reason,
        "detail_json": trade.detail_json,
    }


def serialize_equity_point(point: Any) -> dict[str, Any]:
    """Convert a single equity-curve point to a JSON-safe dict."""
    return {
        "trade_date": point.trade_date.isoformat(),
        "equity": _safe_decimal(point.equity),
        "cash": _safe_decimal(point.cash),
        "position_value": _safe_decimal(point.position_value),
        "drawdown_pct": _safe_decimal(point.drawdown_pct),
    }


def downsample_equity_curve(
    curve: list, max_points: int = 500,
) -> list[dict[str, Any]]:
    """Downsample an equity curve to at most *max_points* serialized dicts."""
    n = len(curve)
    if n <= max_points:
        return [serialize_equity_point(p) for p in curve]
    step = max(1, -(-n // max_points))

    max_dd_idx = 0
    max_dd_val = 0
    sampled: list[dict[str, Any]] = []
    for i, point in enumerate(curve):
        dd = point.drawdown_pct
        if dd is not None and dd > max_dd_val:
            max_dd_val = dd
            max_dd_idx = i
        if i % step == 0 or i == n - 1:
            sampled.append(serialize_equity_point(point))

    if max_dd_idx % step != 0 and max_dd_idx != n - 1:
        entry = serialize_equity_point(curve[max_dd_idx])
        insert_pos = next(
            (j for j, s in enumerate(sampled)
             if s.get("trade_date", "") > entry.get("trade_date", "")),
            len(sampled),
        )
        sampled.insert(insert_pos, entry)
    return sampled
