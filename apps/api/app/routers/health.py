import time
from collections import deque
from threading import Lock

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.config import get_settings
from backtestforecast.db.session import ping_database
from backtestforecast.security.rate_limits import ping_redis

router = APIRouter(tags=["health"])

_health_window: deque[float] = deque()
_health_lock = Lock()
# Per-worker (in-process) limit — not a global cluster-wide rate limit.
# Each Uvicorn/Gunicorn worker enforces this independently.
_HEALTH_MAX_RPM = 120

HEALTH_VERSION = "0.1.0"


@router.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok", "service": "api", "version": HEALTH_VERSION}


@router.get("/health/ready")
def ready(request: Request) -> JSONResponse:
    now = time.monotonic()
    with _health_lock:
        while _health_window and _health_window[0] < now - 60:
            _health_window.popleft()
        if len(_health_window) >= _HEALTH_MAX_RPM:
            return JSONResponse(status_code=429, content={"status": "rate_limited"})
        _health_window.append(now)
    settings = get_settings()
    redis_up = ping_redis()

    db_up = True
    try:
        ping_database()
    except (SQLAlchemyError, OSError):
        db_up = False

    if not db_up:
        content: dict[str, str] = {"status": "degraded", "version": HEALTH_VERSION}
        if settings.app_env not in ("production", "staging"):
            content["environment"] = settings.app_env
            content["database"] = "down"
            content["redis"] = "up" if redis_up else "degraded"
        return JSONResponse(status_code=503, content=content)

    if redis_up:
        rl_mode = "redis"
    elif settings.rate_limit_fail_closed:
        rl_mode = "fail_closed"
    else:
        rl_mode = "in_memory_fallback"

    payload: dict[str, str] = {
        "status": "ok" if redis_up else "degraded",
        "version": HEALTH_VERSION,
    }
    if settings.app_env not in ("production", "staging"):
        payload["environment"] = settings.app_env
        payload["database"] = "up"
        payload["redis"] = "up" if redis_up else "degraded"
        payload["rate_limit_mode"] = rl_mode
    return JSONResponse(status_code=200, content=payload)
