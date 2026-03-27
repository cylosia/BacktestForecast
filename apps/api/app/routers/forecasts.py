from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user_readonly
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import AppValidationError, FeatureLockedError
from backtestforecast.models import User
from backtestforecast.schemas.backtests import SYMBOL_ALLOWED_CHARS, StrategyType
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.scans import ScanService

router = APIRouter(prefix="/forecasts", tags=["forecasts"])


def _require_forecasts_enabled(settings: Settings = Depends(get_settings)) -> None:
    if not settings.feature_forecasts_enabled:
        raise FeatureLockedError("Forecasts are temporarily disabled.", required_tier="free")


@router.get("/{ticker}", response_model=ForecastEnvelopeResponse)
def get_forecast(
    ticker: str,
    user: User = Depends(get_current_user_readonly),
    _: None = Depends(_require_forecasts_enabled),
    db: Session = Depends(get_db),
    strategy_type: StrategyType | None = Query(default=None),
    horizon_days: int = Query(default=20, ge=5, le=90),
    settings: Settings = Depends(get_settings),
) -> ForecastEnvelopeResponse:
    symbol = ticker.strip().upper()
    if not SYMBOL_ALLOWED_CHARS.match(symbol):
        raise AppValidationError("Ticker must be 1-16 characters starting with A-Z or ^ and may include digits, ., /, or -.")
    get_rate_limiter().check(
        bucket="forecasts:get",
        actor_key=str(user.id),
        limit=settings.forecast_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    with ScanService(db) as service:
        return service.build_forecast(
            user=user,
            symbol=symbol,
            strategy_type=strategy_type.value if strategy_type is not None else None,
            horizon_days=horizon_days,
        )
