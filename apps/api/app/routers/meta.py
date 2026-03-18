from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from apps.api.app.dependencies import _extract_client_ip, get_current_user
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.security import get_rate_limiter

router = APIRouter(tags=["meta"])
logger = structlog.get_logger("api.meta")

API_VERSION = "0.1.0"


def _try_authenticate(request: Request, db: Session) -> User | None:
    """Attempt to authenticate without raising on failure."""
    try:
        return get_current_user(request=request, db=db)
    except Exception:
        return None


@router.get("/meta")
def get_meta(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    settings = get_settings()
    client_ip = _extract_client_ip(request)
    get_rate_limiter().check(
        bucket="meta:read",
        actor_key=client_ip or "unknown",
        limit=120,
        window_seconds=settings.rate_limit_window_seconds,
    )
    result: dict[str, Any] = {
        "service": "backtestforecast-api",
        "version": API_VERSION,
    }
    user = _try_authenticate(request, db)
    if user is not None:
        result["billing_enabled"] = settings.stripe_billing_enabled
        result["features"] = {
            "backtests": settings.feature_backtests_enabled,
            "scanner": settings.feature_scanner_enabled,
            "exports": settings.feature_exports_enabled,
            "forecasts": settings.feature_forecasts_enabled,
            "analysis": settings.feature_analysis_enabled,
            "daily_picks": settings.feature_daily_picks_enabled,
            "billing": settings.feature_billing_enabled,
        }
    if settings.app_env not in ("production", "staging"):
        result["environment"] = settings.app_env
    return result
