from fastapi import APIRouter

from backtestforecast.config import get_settings

router = APIRouter(tags=["meta"])

API_VERSION = "0.1.0"


@router.get("/meta")
def get_meta() -> dict[str, str]:
    settings = get_settings()
    return {
        "service": "backtestforecast-api",
        "environment": settings.app_env,
        "version": API_VERSION,
        "billing_enabled": str(settings.stripe_billing_enabled).lower(),
    }
