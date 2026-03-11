from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.config import get_settings
from backtestforecast.db.session import ping_database
from backtestforecast.security.rate_limits import ping_redis

router = APIRouter(tags=["health"])

HEALTH_VERSION = "0.1.0"


@router.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok", "service": "api", "version": HEALTH_VERSION}


@router.get("/health/ready")
def ready() -> JSONResponse:
    settings = get_settings()
    redis_up = ping_redis()
    try:
        ping_database()
    except SQLAlchemyError:
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "version": HEALTH_VERSION,
                "environment": settings.app_env,
                "database": "down",
                "redis": "up" if redis_up else "degraded",
            },
        )

    if redis_up:
        rl_mode = "redis"
    elif settings.rate_limit_fail_closed:
        rl_mode = "fail_closed"
    else:
        rl_mode = "in_memory_fallback"

    payload = {
        "status": "ok" if redis_up else "degraded",
        "version": HEALTH_VERSION,
        "environment": settings.app_env,
        "database": "up",
        "redis": "up" if redis_up else "degraded",
        "rate_limit_mode": rl_mode,
    }
    status_code = 200
    return JSONResponse(status_code=status_code, content=payload)
