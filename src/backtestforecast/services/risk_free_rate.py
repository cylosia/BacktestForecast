from __future__ import annotations

from dataclasses import dataclass

import structlog

from backtestforecast.backtests.types import RiskFreeRateCurve
from backtestforecast.config import get_settings
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.models import HistoricalTreasuryYield
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

logger = structlog.get_logger("services.risk_free_rate")
TREASURY_YIELD_FIELD = "yield_3_month"


@dataclass(frozen=True, slots=True)
class ResolvedRiskFreeRate:
    rate: float
    source: str
    field_name: str = TREASURY_YIELD_FIELD


def _historical_store() -> HistoricalMarketDataStore:
    return HistoricalMarketDataStore(
        session_factory=create_session,
        readonly_session_factory=create_readonly_session,
    )


def resolve_backtest_risk_free_rate(
    request: CreateBacktestRunRequest,
    *,
    client: MassiveClient | None = None,
) -> ResolvedRiskFreeRate:
    if request.risk_free_rate is not None:
        return ResolvedRiskFreeRate(rate=float(request.risk_free_rate), source="request_override")

    settings = get_settings()
    try:
        local_rate = _historical_store().get_average_treasury_yield(
            request.start_date,
            request.start_date,
            field_name=TREASURY_YIELD_FIELD,
        )
        if local_rate is not None:
            return ResolvedRiskFreeRate(rate=local_rate, source="historical_flatfile_treasury")
    except Exception:
        logger.warning("risk_free_rate.local_store_failed", exc_info=True)
    owns_client = client is None
    massive_client = client or MassiveClient()
    try:
        rate = massive_client.get_average_treasury_yield(
            request.start_date,
            request.start_date,
            field_name=TREASURY_YIELD_FIELD,
        )
        if rate is not None:
            try:
                _historical_store().upsert_treasury_yields(
                    [
                        HistoricalTreasuryYield(
                            trade_date=request.start_date,
                            yield_3_month=rate,
                            source_file_date=request.start_date,
                        )
                    ]
                )
            except Exception:
                logger.warning("risk_free_rate.local_store_upsert_failed", exc_info=True)
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


def build_backtest_risk_free_rate_curve(
    request: CreateBacktestRunRequest,
    *,
    default_rate: float,
    client: MassiveClient | None = None,
) -> RiskFreeRateCurve:
    if request.risk_free_rate is not None:
        rate = float(request.risk_free_rate)
        return RiskFreeRateCurve(default_rate=rate)

    owns_client = client is None
    try:
        local_series = _historical_store().get_treasury_yield_series(
            request.start_date,
            request.end_date,
            field_name=TREASURY_YIELD_FIELD,
        )
        if local_series:
            ordered_dates = tuple(sorted(local_series))
            ordered_rates = tuple(float(local_series[trade_date]) for trade_date in ordered_dates)
            return RiskFreeRateCurve(
                default_rate=default_rate,
                dates=ordered_dates,
                rates=ordered_rates,
            )
    except Exception:
        logger.warning("risk_free_rate.local_curve_failed", exc_info=True)
    massive_client = client or MassiveClient()
    try:
        get_series = getattr(massive_client, "get_treasury_yield_series", None)
        if get_series is None:
            return RiskFreeRateCurve(default_rate=default_rate)
        series = get_series(
            request.start_date,
            request.end_date,
            field_name=TREASURY_YIELD_FIELD,
        )
        if series:
            try:
                _historical_store().upsert_treasury_yields(
                    [
                        HistoricalTreasuryYield(
                            trade_date=trade_date,
                            yield_3_month=rate,
                            source_file_date=trade_date,
                        )
                        for trade_date, rate in series.items()
                    ]
                )
            except Exception:
                logger.warning("risk_free_rate.local_curve_upsert_failed", exc_info=True)
    except ExternalServiceError:
        logger.warning(
            "risk_free_rate.massive_curve_failed",
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            field_name=TREASURY_YIELD_FIELD,
            exc_info=True,
        )
        series = {}
    finally:
        if owns_client:
            massive_client.close()

    if not series:
        return RiskFreeRateCurve(default_rate=default_rate)

    ordered_dates = tuple(sorted(series))
    ordered_rates = tuple(float(series[trade_date]) for trade_date in ordered_dates)
    return RiskFreeRateCurve(
        default_rate=default_rate,
        dates=ordered_dates,
        rates=ordered_rates,
    )
