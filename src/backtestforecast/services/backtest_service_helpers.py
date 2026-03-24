from __future__ import annotations

from typing import Any

from backtestforecast.backtests.run_warnings import make_warning
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.models import BacktestRun


def merge_warning_sets(*warning_sets: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
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


def request_payload_from_snapshot(snapshot: dict[str, Any] | None, allowed_fields: set[str]) -> dict[str, Any]:
    payload = snapshot or {}
    return {key: value for key, value in payload.items() if key in allowed_fields}


def resolve_risk_free_rate(run: BacktestRun) -> float | None:
    snapshot = run.input_snapshot_json or {}
    return ResolvedExecutionParameters.from_snapshot(
        {
            **snapshot,
            "risk_free_rate": float(run.risk_free_rate) if run.risk_free_rate is not None else snapshot.get("risk_free_rate"),
        }
    ).risk_free_rate


def resolve_risk_free_rate_curve_points(run: BacktestRun) -> list[dict[str, Any]]:
    snapshot = run.input_snapshot_json or {}
    raw_points = snapshot.get("resolved_risk_free_rate_curve_points")
    if not isinstance(raw_points, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw_points:
        if not isinstance(item, dict):
            continue
        trade_date = item.get("trade_date")
        rate = item.get("rate")
        if not isinstance(trade_date, str):
            continue
        try:
            normalized_rate = float(rate)
        except (TypeError, ValueError):
            continue
        normalized.append({"trade_date": trade_date, "rate": normalized_rate})
    return normalized


def risk_free_rate_curve_payload_warning(run: BacktestRun) -> dict[str, Any] | None:
    snapshot = run.input_snapshot_json or {}
    raw_points = snapshot.get("resolved_risk_free_rate_curve_points")
    if not isinstance(raw_points, list):
        return None
    normalized_count = len(resolve_risk_free_rate_curve_points(run))
    malformed_count = len(raw_points) - normalized_count
    if malformed_count <= 0:
        return None
    return make_warning(
        "risk_free_rate_curve_partial",
        "Some persisted risk-free-rate curve points were malformed and have been omitted from this response.",
        metadata={
            "persisted_points": len(raw_points),
            "returned_points": normalized_count,
            "omitted_points": malformed_count,
        },
    )
