from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import Response

from apps.api.app.routers import (
    analysis,
    backtests,
    billing,
    catalog,
    daily_picks,
    events,
    exports,
    forecasts,
    health,
    me,
    meta,
    scans,
    templates,
)
from backtestforecast.config import get_settings
from backtestforecast.errors import AppError, FeatureLockedError, QuotaExceededError, RateLimitError
from backtestforecast.security import get_rate_limiter
from backtestforecast.observability import REQUEST_ID_HEADER, configure_logging, get_logger
from backtestforecast.observability.logging import RequestContextMiddleware
from backtestforecast.observability.metrics import API_ERRORS_TOTAL, PrometheusMiddleware, metrics_response
from backtestforecast.security.http import ApiSecurityHeadersMiddleware, RequestBodyLimitMiddleware

settings = get_settings()
configure_logging(settings)
logger = get_logger("api")

if settings.sentry_dsn:
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.app_env,
            traces_sample_rate=settings.sentry_traces_sample_rate,
            send_default_pii=False,
        )
        logger.info("sentry.initialized", environment=settings.app_env)
    except Exception:
        logger.warning("sentry.init_failed", exc_info=True)

_is_dev = settings.app_env in ("development", "test")


@asynccontextmanager
async def _lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    if settings.app_env in ("production", "staging"):
        if not settings.clerk_audience or not settings.clerk_audience.strip():
            raise RuntimeError(
                "CLERK_AUDIENCE must be set to a non-empty value in production/staging. "
                "JWT audience verification will not work without it."
            )
    elif not settings.clerk_audience:
        logger.warning(
            "startup.clerk_audience_missing",
            hint="CLERK_AUDIENCE is not set; JWT audience verification is disabled in development.",
        )

    logger.info("lifespan.startup_complete")
    yield
    logger.info("lifespan.shutdown_started")

    from apps.api.app.routers.events import shutdown_async_redis

    await shutdown_async_redis()

    from backtestforecast.events import _shutdown_redis as _shutdown_sync_redis

    _shutdown_sync_redis()

    from backtestforecast.db.session import _get_engine

    try:
        _get_engine().dispose()
        logger.info("lifespan.db_engine_disposed")
    except Exception:
        logger.warning("lifespan.db_engine_dispose_failed", exc_info=True)

    logger.info("lifespan.shutdown_complete")


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="BacktestForecast API — options backtesting, scanning, forecasting, and portfolio analysis.",
    openapi_url="/openapi.json" if _is_dev else None,
    docs_url="/docs" if _is_dev else None,
    redoc_url="/redoc" if _is_dev else None,
    lifespan=_lifespan,
)


def _custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema["components"] = schema.get("components", {})
    schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Clerk-issued JWT via Authorization header.",
        },
    }
    schema["security"] = [{"BearerAuth": []}]

    error_schema = {
        "type": "object",
        "properties": {
            "error": {
                "type": "object",
                "properties": {
                    "code": {"type": "string"},
                    "message": {"type": "string"},
                    "request_id": {"type": "string", "nullable": True},
                },
                "required": ["code", "message"],
            },
        },
        "required": ["error"],
    }
    schema["components"]["schemas"]["ErrorEnvelope"] = error_schema
    _error_ref = {"schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}
    _error_content = {"content": {"application/json": _error_ref}}
    for path_obj in schema.get("paths", {}).values():
        for method, operation in path_obj.items():
            if not isinstance(operation, dict):
                continue
            responses = operation.setdefault("responses", {})
            responses.setdefault("401", {"description": "Authentication required or session expired.", **_error_content})
            responses.setdefault("403", {"description": "Insufficient permissions or feature locked.", **_error_content})
            responses.setdefault("422", {"description": "Validation error — request payload did not match the expected schema.", **_error_content})
            responses.setdefault("429", {"description": "Rate limit exceeded. See Retry-After header.", **_error_content})
            responses.setdefault("500", {"description": "Unexpected server error.", **_error_content})
            if method in ("get", "delete"):
                responses.setdefault("404", {"description": "Resource not found.", **_error_content})

    app.openapi_schema = schema
    return schema


app.openapi = _custom_openapi

app.add_middleware(ApiSecurityHeadersMiddleware)
app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=settings.request_max_body_bytes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.web_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID", "X-Requested-With", "Accept"],
    expose_headers=["X-Request-ID", "Retry-After", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"],
    max_age=600,
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.api_allowed_hosts)
app.add_middleware(RequestContextMiddleware)
app.add_middleware(PrometheusMiddleware)

app.include_router(health.router)
app.include_router(meta.router, prefix="/v1")
app.include_router(me.router, prefix="/v1")
app.include_router(catalog.router, prefix="/v1")
app.include_router(backtests.router, prefix="/v1")
app.include_router(templates.router, prefix="/v1")
app.include_router(scans.router, prefix="/v1")
app.include_router(forecasts.router, prefix="/v1")
app.include_router(exports.router, prefix="/v1")
app.include_router(daily_picks.router, prefix="/v1")
app.include_router(analysis.router, prefix="/v1")
app.include_router(billing.router, prefix="/v1")
app.include_router(events.router, prefix="/v1")


def _error_payload(request: Request, *, code: str, message: str) -> dict[str, object]:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": getattr(request.state, "request_id", None),
        }
    }


@app.exception_handler(AppError)
def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    API_ERRORS_TOTAL.labels(code=exc.code).inc()
    logger.warning("api.error", code=exc.code, status_code=exc.status_code, message=exc.message)
    payload = _error_payload(request, code=exc.code, message=exc.message)
    if isinstance(exc, (QuotaExceededError, FeatureLockedError)):
        extra: dict[str, str] = {}
        if hasattr(exc, "current_tier"):
            extra["current_tier"] = exc.current_tier
        if hasattr(exc, "required_tier"):
            extra["required_tier"] = exc.required_tier
        if extra:
            payload["error"]["detail"] = extra  # type: ignore[index]
    response = JSONResponse(
        status_code=exc.status_code,
        content=payload,
    )
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    if isinstance(exc, RateLimitError):
        info = getattr(exc, "rate_limit_info", None)
        if info is not None:
            response.headers["Retry-After"] = str(max(info.reset_at - int(time.time()), 1))
            response.headers["X-RateLimit-Limit"] = str(info.limit)
            response.headers["X-RateLimit-Remaining"] = str(info.remaining)
            response.headers["X-RateLimit-Reset"] = str(info.reset_at)
    return response


@app.exception_handler(RequestValidationError)
def request_validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    sanitized = [
        {k: v for k, v in err.items() if k != "input"} for err in exc.errors()
    ]
    logger.warning("api.request_validation_error", errors=sanitized)
    response = JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "request_validation_error",
                "message": "The request payload did not match the expected schema.",
                "request_id": getattr(request.state, "request_id", None),
                "details": sanitized,
            }
        },
    )
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


@app.exception_handler(StarletteHTTPException)
def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else "The request could not be completed."
    logger.warning("api.http_exception", status_code=exc.status_code, detail=detail)
    response = JSONResponse(
        status_code=exc.status_code,
        content=_error_payload(request, code="http_error", message=detail),
    )
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


@app.exception_handler(Exception)
def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    API_ERRORS_TOTAL.labels(code="internal_server_error").inc()
    logger.exception("api.unhandled_exception", exc_info=exc)
    response = JSONResponse(
        status_code=500,
        content=_error_payload(
            request,
            code="internal_server_error",
            message="An unexpected server error occurred.",
        ),
    )
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics(request: Request) -> Response:
    ip_address = request.client.host if request.client else None
    get_rate_limiter().check(bucket="admin", actor_key=ip_address or "unknown", limit=30, window_seconds=60)
    if settings.app_env in ("production", "staging"):
        import hmac as _hmac

        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        if not settings.metrics_token or not token or not _hmac.compare_digest(token, settings.metrics_token):
            return JSONResponse(status_code=403, content={"error": "forbidden"})
    return metrics_response()


@app.get("/admin/dlq", include_in_schema=False)
def dlq_status(request: Request) -> Response:
    """Inspect the dead-letter queue depth and recent items.

    Protected by the same metrics token as /metrics in production.
    """
    ip_address = request.client.host if request.client else None
    get_rate_limiter().check(bucket="admin", actor_key=ip_address or "unknown", limit=30, window_seconds=60)
    if settings.app_env in ("production", "staging"):
        import hmac as _hmac

        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        if not settings.metrics_token or not token or not _hmac.compare_digest(token, settings.metrics_token):
            return JSONResponse(status_code=403, content={"error": "forbidden"})
    try:
        import json

        fallback_redis = None
        r = get_rate_limiter()._get_redis()
        if r is None:
            from redis import Redis
            r = Redis.from_url(settings.redis_url, socket_timeout=5, decode_responses=False)
            fallback_redis = r
        try:
            depth = r.llen("bff:dead_letter_queue")
            recent_raw = r.lrange("bff:dead_letter_queue", 0, 9)

            recent = []
            for raw in recent_raw:
                try:
                    recent.append(json.loads(raw))
                except (ValueError, TypeError, UnicodeDecodeError):
                    recent.append({"raw": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)})
            return JSONResponse(content={"depth": depth, "recent": recent})
        finally:
            if fallback_redis is not None:
                fallback_redis.close()
    except Exception:
        logger.exception("admin.dlq_unavailable")
        return JSONResponse(status_code=503, content={"error": "dlq_unavailable"})


@app.get("/")
def root() -> dict[str, str]:
    payload: dict[str, str] = {
        "service": "backtestforecast-api",
        "status": "ok",
        "health": "/health/ready",
    }
    if _is_dev:
        payload["docs"] = "/docs"
    return payload
