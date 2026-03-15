import time
from collections import deque
from threading import Lock

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.config import get_settings, register_invalidation_callback
from backtestforecast.db.session import ping_database
from backtestforecast.security.rate_limits import ping_redis

router = APIRouter(tags=["health"])


_broker_redis = None
_broker_redis_lock = Lock()


def _invalidate_broker_redis() -> None:
    global _broker_redis
    with _broker_redis_lock:
        client = _broker_redis
        _broker_redis = None
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


register_invalidation_callback(_invalidate_broker_redis)


def _ping_broker_redis() -> bool:
    """Ping the Celery broker Redis (distinct from the rate-limit/cache Redis)."""
    global _broker_redis
    try:
        from redis import Redis
        with _broker_redis_lock:
            if _broker_redis is None:
                settings = get_settings()
                _broker_redis = Redis.from_url(
                    settings.redis_url,
                    socket_timeout=2.0,
                    socket_connect_timeout=2.0,
                )
        return bool(_broker_redis.ping())
    except Exception:
        with _broker_redis_lock:
            if _broker_redis is not None:
                try:
                    _broker_redis.close()
                except Exception:
                    pass
                _broker_redis = None
        return False

_HEALTH_MAX_RPM = 120
_health_window: deque[float] = deque(maxlen=_HEALTH_MAX_RPM + 50)
_health_lock = Lock()

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
    broker_up = _ping_broker_redis()

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
            content["broker"] = "up" if broker_up else "down"
        return JSONResponse(status_code=503, content=content)

    if not broker_up:
        content = {"status": "degraded", "version": HEALTH_VERSION}
        if settings.app_env not in ("production", "staging"):
            content["environment"] = settings.app_env
            content["database"] = "up"
            content["redis"] = "up" if redis_up else "down"
            content["broker"] = "down"
        return JSONResponse(status_code=503, content=content)

    if not redis_up and settings.rate_limit_fail_closed:
        content = {"status": "unavailable", "version": HEALTH_VERSION}
        if settings.app_env not in ("production", "staging"):
            content["environment"] = settings.app_env
            content["database"] = "up"
            content["redis"] = "down"
            content["broker"] = "up"
            content["rate_limit_mode"] = "fail_closed"
        return JSONResponse(status_code=503, content=content)

    if redis_up:
        rl_mode = "redis"
    else:
        rl_mode = "in_memory_fallback"

    all_ok = redis_up and broker_up
    payload: dict[str, str] = {
        "status": "ok" if all_ok else "degraded",
        "version": HEALTH_VERSION,
    }
    if settings.app_env not in ("production", "staging"):
        payload["environment"] = settings.app_env
        payload["database"] = "up"
        payload["redis"] = "up" if redis_up else "degraded"
        payload["broker"] = "up" if broker_up else "down"
        payload["rate_limit_mode"] = rl_mode
    return JSONResponse(status_code=200, content=payload)
