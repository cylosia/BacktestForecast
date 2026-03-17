from typing import Any

from fastapi import APIRouter, Request

from apps.api.app.dependencies import _extract_client_ip
from backtestforecast.config import get_settings
from backtestforecast.security import get_rate_limiter

router = APIRouter(tags=["meta"])

API_VERSION = "0.1.0"


@router.get("/meta")
def get_meta(request: Request) -> dict[str, Any]:
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
    auth_header = request.headers.get("authorization")
    session_cookie = request.cookies.get("__session")
    if auth_header or session_cookie:
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
