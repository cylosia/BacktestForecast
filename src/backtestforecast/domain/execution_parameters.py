from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.risk_free_rate import TREASURY_YIELD_FIELD, ResolvedRiskFreeRate


@dataclass(frozen=True, slots=True)
class ResolvedExecutionParameters:
    risk_free_rate: float | None
    risk_free_rate_source: str | None
    risk_free_rate_field_name: str | None
    dividend_yield: float
    source_of_truth: str

    @classmethod
    def from_request_resolution(
        cls,
        request: CreateBacktestRunRequest,
        resolved_risk_free_rate: ResolvedRiskFreeRate,
    ) -> ResolvedExecutionParameters:
        dividend_yield = float(request.dividend_yield) if request.dividend_yield is not None else 0.0
        return cls(
            risk_free_rate=resolved_risk_free_rate.rate,
            risk_free_rate_source=resolved_risk_free_rate.source,
            risk_free_rate_field_name=resolved_risk_free_rate.field_name,
            dividend_yield=dividend_yield,
            source_of_truth="request_resolution",
        )

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any] | None) -> ResolvedExecutionParameters:
        snapshot = snapshot or {}
        raw_rate = snapshot.get("risk_free_rate")
        try:
            risk_free_rate = float(raw_rate) if raw_rate is not None else None
        except (TypeError, ValueError):
            risk_free_rate = None
        raw_dividend_yield = snapshot.get("dividend_yield", 0.0)
        try:
            dividend_yield = float(raw_dividend_yield)
        except (TypeError, ValueError):
            dividend_yield = 0.0
        return cls(
            risk_free_rate=risk_free_rate,
            risk_free_rate_source=snapshot.get("resolved_risk_free_rate_source"),
            risk_free_rate_field_name=snapshot.get("resolved_risk_free_rate_field_name") or TREASURY_YIELD_FIELD,
            dividend_yield=dividend_yield,
            source_of_truth="persisted_snapshot" if risk_free_rate is not None else "missing",
        )

    def to_snapshot_fields(self) -> dict[str, Any]:
        return {
            "risk_free_rate": self.risk_free_rate,
            "resolved_risk_free_rate_source": self.risk_free_rate_source,
            "resolved_risk_free_rate_field_name": self.risk_free_rate_field_name,
            "dividend_yield": self.dividend_yield,
        }
