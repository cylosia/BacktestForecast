from __future__ import annotations

from dataclasses import dataclass

import structlog

from backtestforecast.config import get_settings
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

logger = structlog.get_logger("services.risk_free_rate")
TREASURY_YIELD_FIELD = "yield_3_month"


@dataclass(frozen=True, slots=True)
class ResolvedRiskFreeRate:
    rate: float
    source: str
    field_name: str = TREASURY_YIELD_FIELD


def resolve_backtest_risk_free_rate(
    request: CreateBacktestRunRequest,
    *,
    client: MassiveClient | None = None,
) -> ResolvedRiskFreeRate:
    if request.risk_free_rate is not None:
        return ResolvedRiskFreeRate(rate=float(request.risk_free_rate), source="request_override")

    settings = get_settings()
    owns_client = client is None
    massive_client = client or MassiveClient()
    try:
        rate = massive_client.get_average_treasury_yield(
            request.start_date,
            request.end_date,
            field_name=TREASURY_YIELD_FIELD,
        )
        if rate is not None:
            return ResolvedRiskFreeRate(rate=rate, source="massive_treasury")
        logger.warning(
            "risk_free_rate.massive_empty",
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            field_name=TREASURY_YIELD_FIELD,
        )
    except ExternalServiceError:
        logger.warning(
            "risk_free_rate.massive_failed",
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            field_name=TREASURY_YIELD_FIELD,
            exc_info=True,
        )
    finally:
        if owns_client:
            massive_client.close()

    return ResolvedRiskFreeRate(rate=float(settings.risk_free_rate), source="configured_fallback")
