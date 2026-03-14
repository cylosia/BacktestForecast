from fastapi import APIRouter

from backtestforecast.config import get_settings

router = APIRouter(tags=["meta"])

API_VERSION = "0.1.0"


@router.get("/meta")
def get_meta() -> dict[str, str | bool]:
    settings = get_settings()
    result: dict[str, str | bool] = {
        "service": "backtestforecast-api",
        "version": API_VERSION,
        "billing_enabled": settings.stripe_billing_enabled,
    }
    if settings.app_env not in ("production", "staging"):
        result["environment"] = settings.app_env
    return result
