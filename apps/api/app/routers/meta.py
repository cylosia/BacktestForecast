from fastapi import APIRouter, Request

from backtestforecast.config import get_settings
from backtestforecast.security import get_rate_limiter

router = APIRouter(tags=["meta"])

API_VERSION = "0.1.0"


from typing import Any


@router.get("/meta")
def get_meta(request: Request) -> dict[str, Any]:
    settings = get_settings()
    client_ip = request.client.host if request.client else "unknown"
    get_rate_limiter().check(
        bucket="meta:read",
        actor_key=client_ip,
        limit=120,
        window_seconds=settings.rate_limit_window_seconds,
    )
    result: dict[str, Any] = {
        "service": "backtestforecast-api",
        "version": API_VERSION,
        "billing_enabled": settings.stripe_billing_enabled,
        "features": {
            "backtests": settings.feature_backtests_enabled,
            "scanner": settings.feature_scanner_enabled,
            "exports": settings.feature_exports_enabled,
            "forecasts": settings.feature_forecasts_enabled,
            "analysis": settings.feature_analysis_enabled,
            "daily_picks": settings.feature_daily_picks_enabled,
            "billing": settings.feature_billing_enabled,
        },
    }
    if settings.app_env not in ("production", "staging"):
        result["environment"] = settings.app_env
    return result
