from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.schemas.backtests import StrategyType
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.services.scans import ScanService

router = APIRouter(prefix="/forecasts", tags=["forecasts"])

_TICKER_RE = re.compile(r"^[A-Za-z]{1,10}$")


@router.get("/{ticker}", response_model=ForecastEnvelopeResponse)
def get_forecast(
    ticker: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    strategy_type: StrategyType | None = Query(default=None),
    horizon_days: int = Query(default=20, ge=5, le=90),
) -> ForecastEnvelopeResponse:
    if not _TICKER_RE.match(ticker):
        raise ValidationError("Ticker must be 1-10 alphabetic characters.")
    return ScanService(db).build_forecast(
        user=user,
        symbol=ticker,
        strategy_type=strategy_type.value if strategy_type is not None else None,
        horizon_days=horizon_days,
    )
