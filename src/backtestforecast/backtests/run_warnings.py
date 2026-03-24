from __future__ import annotations

from typing import Any

from backtestforecast.config import get_settings
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

NAKED_OPTION_STRATEGY_TYPES = {
    "naked_call",
    "naked_put",
    "short_straddle",
    "short_strangle",
    "covered_strangle",
    "jade_lizard",
    "reverse_conversion",
}


def make_warning(
    code: str,
    message: str,
    *,
    severity: str = "warning",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "severity": severity, "message": message}
    if metadata:
        payload["metadata"] = metadata
    return payload


def build_user_warnings(
    request: CreateBacktestRunRequest,
    *,
    resolved_risk_free_rate: float | None = None,
    risk_free_rate_source: str | None = None,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if request.strategy_type.value in NAKED_OPTION_STRATEGY_TYPES:
        warnings.append(
            make_warning(
                "naked_option_margin_only",
                "This strategy is sized using broker-style margin collateral only. Reported sizing can materially understate the true economic downside of naked short-option exposure; review results with separate stress-loss limits.",
                severity="critical",
                metadata={
                    "recommendation": "Add stress-loss sizing or scenario analysis before treating this run as production-ready.",
                    "strategy_type": request.strategy_type.value,
                },
            )
        )
    if request.risk_free_rate is None:
        configured_rfr = get_settings().risk_free_rate
        resolved = resolved_risk_free_rate if resolved_risk_free_rate is not None else configured_rfr
        if risk_free_rate_source == "massive_treasury":
            warnings.append(
                make_warning(
                    "historical_treasury_risk_free_rate",
                    f"Sharpe and Sortino are using a date-varying 3-month Treasury curve from Massive for {request.start_date.isoformat()} through {request.end_date.isoformat()} (anchor/default rate {resolved:.4f}).",
                    metadata={
                        "configured_risk_free_rate": configured_rfr,
                        "resolved_risk_free_rate": resolved,
                        "risk_free_rate_source": risk_free_rate_source,
                        "start_date": request.start_date.isoformat(),
                        "end_date": request.end_date.isoformat(),
                    },
                )
            )
        elif risk_free_rate_source == "configured_fallback":
            warnings.append(
                make_warning(
                    "configured_fallback_risk_free_rate",
                    f"Massive Treasury yields were unavailable for {request.start_date.isoformat()} through {request.end_date.isoformat()}, so Sharpe and Sortino are using the configured server risk-free-rate anchor/default ({configured_rfr:.4f}).",
                    metadata={
                        "configured_risk_free_rate": configured_rfr,
                        "resolved_risk_free_rate": resolved,
                        "risk_free_rate_source": risk_free_rate_source,
                        "start_date": request.start_date.isoformat(),
                        "end_date": request.end_date.isoformat(),
                    },
                )
            )
        else:
            warnings.append(
                make_warning(
                    "resolved_risk_free_rate_recorded",
                    f"Sharpe and Sortino are using the resolved run risk-free-rate anchor recorded at creation time ({resolved:.4f}).",
                    metadata={
                        "configured_risk_free_rate": configured_rfr,
                        "resolved_risk_free_rate": resolved,
                        "risk_free_rate_source": risk_free_rate_source or "unknown",
                        "start_date": request.start_date.isoformat(),
                        "end_date": request.end_date.isoformat(),
                    },
                )
            )
    return warnings


def merge_warnings(*warning_sets: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for warning_set in warning_sets:
        for warning in warning_set or []:
            key = (str(warning.get("code", "")), str(warning.get("message", "")))
            if key in seen:
                continue
            seen.add(key)
            merged.append(warning)
    return merged
