import hmac
import time
from collections import deque
from threading import Lock

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.config import get_settings, register_invalidation_callback
from backtestforecast.db.session import ping_database
from backtestforecast.security.rate_limits import get_rate_limiter, ping_redis

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
            conn = _broker_redis
        return bool(conn.ping())
    except Exception:
        with _broker_redis_lock:
            if _broker_redis is not None:
                try:
                    _broker_redis.close()
                except Exception:
                    pass
                _broker_redis = None
        return False

def _get_redis_pool_stats() -> dict[str, int] | None:
    """Return connection pool stats from the rate-limiter Redis client, or None if unavailable."""
    try:
        rl = get_rate_limiter()
        redis_client = rl.get_redis()
        if redis_client is None:
            return None
        pool = redis_client.connection_pool
        return {
            "current_connections": len(pool._in_use_connections),
            "available_connections": len(pool._available_connections),
            "max_connections": pool.max_connections,
        }
    except Exception:
        return None


def _check_massive_health(settings) -> str:
    """Lightweight Massive API health: check config and circuit breaker state.

    Returns "ok", "degraded" (circuit open/half-open), or "unconfigured".
    """
    if not settings.massive_api_key:
        return "unconfigured"
    try:
        from backtestforecast.integrations.massive_client import _massive_sync_circuit
        from backtestforecast.resilience.circuit_breaker import CircuitState
        state = _massive_sync_circuit.state
        if state == CircuitState.OPEN:
            return "circuit_open"
        if state == CircuitState.HALF_OPEN:
            return "circuit_half_open"
    except Exception:
        return "degraded"
    return "ok"


def _check_migration_drift() -> bool:
    """Return True if DB migration version matches the code's Alembic head."""
    try:
        from alembic.config import Config
        from alembic.runtime.migration import MigrationContext
        from alembic.script import ScriptDirectory
        from backtestforecast.db.session import _get_engine
        from backtestforecast.observability.metrics import MIGRATION_HEAD_MATCH

        config = Config("alembic.ini")
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        with _get_engine().connect() as conn:
            context = MigrationContext.configure(conn)
            current = context.get_current_revision()
        match = current == head
        MIGRATION_HEAD_MATCH.set(1 if match else 0)
        return match
    except (ImportError, FileNotFoundError):
        import structlog
        structlog.get_logger("health").debug("health.migration_check_unavailable", exc_info=True)
        return False
    except Exception:
        import structlog
        structlog.get_logger("health").warning("health.migration_check_failed", exc_info=True)
        return False


_HEALTH_MAX_RPM = 120
_health_window: deque[float] = deque(maxlen=_HEALTH_MAX_RPM + 50)
_health_lock = Lock()

HEALTH_VERSION = "0.1.0"
_SHOW_VERSION_IN_HEALTH = get_settings().app_env not in ("production", "staging")


_LIVE_MAX_RPM = 300
_live_window: deque[float] = deque(maxlen=_LIVE_MAX_RPM + 50)
_live_lock = Lock()


@router.get("/health/live", response_model=None)
def live() -> dict[str, str] | JSONResponse:
    now = time.monotonic()
    with _live_lock:
        while _live_window and _live_window[0] < now - 60:
            _live_window.popleft()
        if len(_live_window) >= _LIVE_MAX_RPM:
            return JSONResponse(status_code=429, content={"status": "rate_limited"})
        _live_window.append(now)
    resp: dict[str, str] = {"status": "ok", "service": "api"}
    if _SHOW_VERSION_IN_HEALTH:
        resp["version"] = HEALTH_VERSION
    return resp


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

    show_details = False
    if settings.metrics_token:
        token = request.headers.get("x-metrics-token", "")
        if token and hmac.compare_digest(token, settings.metrics_token):
            show_details = True

    redis_up = ping_redis()
    broker_up = _ping_broker_redis()

    db_up = True
    try:
        ping_database()
    except (SQLAlchemyError, OSError):
        db_up = False

    if not db_up:
        content: dict[str, str] = {"status": "degraded"}
        if _SHOW_VERSION_IN_HEALTH:
            content["version"] = HEALTH_VERSION
        if show_details:
            content["environment"] = settings.app_env
            content["database"] = "down"
            content["redis"] = "up" if redis_up else "degraded"
            content["broker"] = "up" if broker_up else "down"
        return JSONResponse(status_code=503, content=content)

    if not broker_up:
        content = {"status": "degraded"}
        if _SHOW_VERSION_IN_HEALTH:
            content["version"] = HEALTH_VERSION
        if show_details:
            content["environment"] = settings.app_env
            content["database"] = "up"
            content["redis"] = "up" if redis_up else "down"
            content["broker"] = "down"
        return JSONResponse(status_code=503, content=content)

    if not redis_up and settings.rate_limit_fail_closed:
        content = {"status": "unavailable"}
        if _SHOW_VERSION_IN_HEALTH:
            content["version"] = HEALTH_VERSION
        if show_details:
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

    massive_status = _check_massive_health(settings)

    all_ok = redis_up and broker_up and massive_status in ("ok", "unconfigured")
    payload: dict[str, object] = {
        "status": "ok" if all_ok else "degraded",
    }
    if _SHOW_VERSION_IN_HEALTH:
        payload["version"] = HEALTH_VERSION
    if show_details:
        payload["environment"] = settings.app_env
        payload["database"] = "up"
        payload["redis"] = "up" if redis_up else "degraded"
        payload["broker"] = "up" if broker_up else "down"
        payload["rate_limit_mode"] = rl_mode
        payload["massive_api"] = massive_status
        try:
            from backtestforecast.db.session import get_pool_stats
            payload["pool_stats"] = get_pool_stats()
        except Exception:
            pass
        redis_pool = _get_redis_pool_stats()
        if redis_pool is not None:
            payload["redis_pool_stats"] = redis_pool
        try:
            from backtestforecast.market_data.redis_cache import OptionDataRedisCache
            from backtestforecast.config import get_settings as _gs
            _s = _gs()
            if _s.option_cache_enabled and _s.redis_cache_url:
                _cache = OptionDataRedisCache(_s.redis_cache_url, _s.option_cache_ttl_seconds)
                payload["option_cache_freshness"] = _cache.check_freshness("SPY")
                _cache.close()
        except Exception:
            pass
        if settings.sentry_dsn:
            try:
                import sentry_sdk
                payload["sentry"] = "initialized" if sentry_sdk.is_initialized() else "not_initialized"
            except Exception:
                payload["sentry"] = "unavailable"
        if settings.app_env not in ("development",):
            payload["migration_aligned"] = _check_migration_drift()
        payload["outbox"] = _check_outbox_health()
    return JSONResponse(status_code=200, content=payload)


def _check_outbox_health() -> dict[str, object]:
    """Check for stale outbox messages that may indicate dispatch problems."""
    try:
        from sqlalchemy import func, select

        from backtestforecast.db.session import create_session
        from backtestforecast.models import OutboxMessage

        with create_session() as session:
            pending_count = session.scalar(
                select(func.count(OutboxMessage.id)).where(OutboxMessage.status == "pending")
            ) or 0
            oldest_pending = session.scalar(
                select(func.min(OutboxMessage.created_at)).where(OutboxMessage.status == "pending")
            )
            session.rollback()

        result: dict[str, object] = {"pending_count": pending_count}
        if oldest_pending is not None:
            from datetime import UTC, datetime

            age_seconds = (datetime.now(UTC) - oldest_pending.replace(tzinfo=UTC)).total_seconds()
            result["oldest_pending_age_seconds"] = round(age_seconds, 1)
            if age_seconds > 300:
                result["status"] = "stale"
            elif pending_count > 0:
                result["status"] = "pending"
            else:
                result["status"] = "ok"
        else:
            result["status"] = "ok"
        return result
    except Exception:
        return {"status": "unknown"}
