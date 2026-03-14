from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.schemas.backtests import StrategyType
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.scans import ScanService

router = APIRouter(prefix="/forecasts", tags=["forecasts"])

_TICKER_RE = re.compile(r"^[A-Za-z0-9./^]{1,16}$")


@router.get("/{ticker}", response_model=ForecastEnvelopeResponse)
def get_forecast(
    ticker: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    strategy_type: StrategyType | None = Query(default=None),
    horizon_days: int = Query(default=20, ge=5, le=90),
    settings: Settings = Depends(get_settings),
) -> ForecastEnvelopeResponse:
    if not _TICKER_RE.match(ticker):
        raise ValidationError("Ticker must be 1-10 alphabetic characters.")
    get_rate_limiter().check(
        bucket="forecasts:get",
        actor_key=str(user.id),
        limit=settings.forecast_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    symbol = ticker.upper()
    service = ScanService(db)
    return service.build_forecast(
        user=user,
        symbol=symbol,
        strategy_type=strategy_type.value if strategy_type is not None else None,
        horizon_days=horizon_days,
    )
