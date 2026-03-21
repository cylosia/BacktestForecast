from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from redis import Redis

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import Response

from apps.api.app.dependencies import _extract_client_ip, reset_token_verifier, reset_trusted_networks
from apps.api.app.routers import (
    account,
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
    sweeps,
    templates,
)
from backtestforecast.config import get_settings, register_invalidation_callback
from backtestforecast.errors import AppError, FeatureLockedError, QuotaExceededError, RateLimitError
from backtestforecast.observability import REQUEST_ID_HEADER, configure_logging, get_logger
from backtestforecast.observability.logging import RequestContextMiddleware
from backtestforecast.observability.metrics import API_ERRORS_TOTAL, PrometheusMiddleware, metrics_response
from backtestforecast.security import get_rate_limiter
from backtestforecast.security.http import (
    ApiSecurityHeadersMiddleware,
    DynamicTrustedHostMiddleware,
    RequestBodyLimitMiddleware,
)
from backtestforecast.version import get_public_version

_startup_settings = get_settings()
configure_logging(_startup_settings)
logger = get_logger("api")

# WARNING: _startup_settings still captures configuration used to build the
# FastAPI app itself (notably CORS configuration and docs exposure) at import
# time. Host validation and body-size enforcement re-read settings per request,
# but CORS/origin behavior still requires a process restart after config
# changes. Per-request code paths (e.g. /metrics auth, /admin/dlq auth) must
# call get_settings() to pick up post-invalidation changes.
settings = _startup_settings

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

@asynccontextmanager
async def _lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    if settings.app_env in ("production", "staging"):
        if not settings.clerk_audience or not settings.clerk_audience.strip():
            raise RuntimeError(
                "CLERK_AUDIENCE must be set to a non-empty value in production/staging. "
                "JWT audience verification will not work without it."
            )
        if not settings.clerk_issuer or not settings.clerk_issuer.strip():
            raise RuntimeError(
                "CLERK_ISSUER must be set to a non-empty value in production/staging. "
                "JWT issuer verification will not work without it."
            )
        if not settings.admin_token or not settings.admin_token.strip():
            raise RuntimeError(
                "ADMIN_TOKEN must be set to a non-empty value in production/staging. "
                "The /admin/dlq endpoint will fall back to metrics_token without it."
            )
        if not settings.clerk_authorized_parties:
            raise RuntimeError(
                "CLERK_AUTHORIZED_PARTIES must be set in production/staging. "
                "Without it, any Clerk application sharing the same JWKS endpoint "
                "could generate valid tokens. Set it to your frontend app's client ID."
            )
    else:
        if not settings.clerk_audience:
            logger.warning(
                "startup.clerk_audience_missing",
                hint="CLERK_AUDIENCE is not set; JWT audience verification is disabled in development. "
                     "Tokens from any Clerk app sharing the same key pair will be accepted.",
            )
        if not settings.clerk_issuer:
            logger.warning(
                "startup.clerk_issuer_missing",
                hint="CLERK_ISSUER is not set; JWT issuer verification is disabled in development.",
            )
        if not settings.clerk_authorized_parties:
            logger.warning(
                "startup.clerk_authorized_parties_missing",
                hint="CLERK_AUTHORIZED_PARTIES is empty; the azp claim will not be checked.",
            )

    if "*" in settings.web_cors_origins and settings.app_env in ("production", "staging"):
        raise RuntimeError(
            "WEB_CORS_ORIGINS must not contain '*' in production/staging when "
            "allow_credentials=True. This would allow any origin to make "
            "credentialed cross-origin requests."
        )

    _cors_hosts = set()
    for origin in settings.web_cors_origins:
        if origin != "*":
            from urllib.parse import urlparse as _parse_origin
            parsed = _parse_origin(origin)
            if parsed.hostname:
                _cors_hosts.add(parsed.hostname)
    _allowed_set = set(settings.api_allowed_hosts)
    if "*" not in _allowed_set:
        _missing_hosts = _cors_hosts - _allowed_set
        if _missing_hosts:
            logger.warning(
                "startup.cors_trustedhost_mismatch",
                cors_hosts_not_in_allowed=sorted(_missing_hosts),
                allowed_hosts=sorted(_allowed_set),
                hint="CORS origins reference hostnames not in API_ALLOWED_HOSTS. "
                     "Browser preflight requests to these origins will be rejected "
                     "with 400 by TrustedHostMiddleware before CORS headers are attached, "
                     "causing opaque CORS errors. Add them to API_ALLOWED_HOSTS_RAW.",
            )

    register_invalidation_callback(reset_trusted_networks)
    register_invalidation_callback(reset_token_verifier)

    register_invalidation_callback(_invalidate_dlq_redis)

    if settings.clerk_jwks_url or settings.clerk_issuer:
        try:
            from apps.api.app.dependencies import get_token_verifier
            verifier = get_token_verifier()
            verifier._get_jwks_client()
            logger.info("lifespan.jwks_cache_warmed")
        except Exception:
            logger.warning("lifespan.jwks_cache_warmup_failed", exc_info=True)

    try:
        from backtestforecast.observability.tracing import init_tracing
        if init_tracing(service_name="backtestforecast-api"):
            logger.info("lifespan.tracing_initialized")
    except Exception:
        logger.debug("lifespan.tracing_init_skipped", exc_info=True)

    logger.info("lifespan.startup_complete")
    yield
    logger.info("lifespan.shutdown_started")

    from apps.api.app.routers.events import shutdown_async_redis

    await shutdown_async_redis()

    from backtestforecast.events import _shutdown_redis as _shutdown_sync_redis

    _shutdown_sync_redis()

    global _dlq_redis
    with _dlq_redis_lock:
        if _dlq_redis is not None:
            try:
                _dlq_redis.close()
                _dlq_redis = None
                logger.info("lifespan.dlq_redis_closed")
            except Exception:
                logger.warning("lifespan.dlq_redis_close_failed", exc_info=True)

    from backtestforecast.db.session import _get_engine

    try:
        _get_engine().dispose()
        logger.info("lifespan.db_engine_disposed")
    except Exception:
        logger.warning("lifespan.db_engine_dispose_failed", exc_info=True)

    logger.info("lifespan.shutdown_complete")


app = FastAPI(
    title=settings.app_name,
    version=get_public_version(),
    description="BacktestForecast API — options backtesting, scanning, forecasting, and portfolio analysis.",
    openapi_url="/openapi.json" if settings.app_env in ("development", "test") else None,
    docs_url="/docs" if settings.app_env in ("development", "test") else None,
    redoc_url="/redoc" if settings.app_env in ("development", "test") else None,
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
                    "detail": {
                        "type": "object",
                        "description": "Present on 403 quota/feature errors. Contains current_tier and/or required_tier.",
                        "properties": {
                            "current_tier": {"type": "string", "enum": ["free", "pro", "premium"]},
                            "required_tier": {"type": "string", "enum": ["free", "pro", "premium"]},
                        },
                        "nullable": True,
                    },
                    "details": {
                        "type": "array",
                        "description": "Present on 422 validation errors. Each item describes one field-level error.",
                        "items": {"type": "object"},
                        "nullable": True,
                    },
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
            responses["422"] = {"description": "Validation error — request payload did not match the expected schema.", **_error_content}
            responses.setdefault("429", {"description": "Rate limit exceeded. See Retry-After header.", **_error_content})
            responses.setdefault("500", {"description": "Unexpected server error.", **_error_content})
            if method in ("get", "delete"):
                responses.setdefault("404", {"description": "Resource not found.", **_error_content})

    app.openapi_schema = schema
    return schema


app.openapi = _custom_openapi


class _CancelledErrorMiddleware:
    """Return 499 when the client disconnects mid-request.

    asyncio.CancelledError is a BaseException (not Exception) in Python 3.9+,
    so it cannot be registered via ``@app.exception_handler``.  This pure-ASGI
    middleware catches it at the outermost layer instead.
    """

    def __init__(self, app_inner: FastAPI) -> None:
        self.app = app_inner

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        response_started = False
        original_send = send

        async def tracked_send(message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await original_send(message)

        try:
            await self.app(scope, receive, tracked_send)
        except asyncio.CancelledError:
            from backtestforecast.observability.metrics import HTTP_REQUESTS_TOTAL
            path = scope.get("path", "/unknown")
            HTTP_REQUESTS_TOTAL.labels(method=scope.get("method", "?"), path=path, status="499").inc()
            if not response_started:
                try:
                    response = JSONResponse(
                        status_code=499,
                        content={
                            "error": {
                                "code": "client_disconnected",
                                "message": "Client closed connection",
                                "request_id": None,
                            }
                        },
                    )
                    await response(scope, receive, original_send)
                except Exception:
                    logger.debug("client_disconnect.response_send_failed", exc_info=True)


# Middleware execution order (outermost to innermost):
# 1. PrometheusMiddleware — records request metrics (including 499s)
# 2. _CancelledErrorMiddleware — converts CancelledError to 499
# 3. RequestContextMiddleware — binds request_id to structlog context
# 4. TrustedHostMiddleware — rejects requests with invalid Host headers
# 5. CORSMiddleware — handles cross-origin preflight and response headers
# 6. RequestBodyLimitMiddleware — enforces max request body size
# 7. ApiSecurityHeadersMiddleware — adds security response headers
#
# Starlette builds middleware LIFO: the last add_middleware call is outermost.
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)
app.add_middleware(ApiSecurityHeadersMiddleware)
app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=lambda: get_settings().request_max_body_bytes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.web_cors_origins,
    allow_credentials=True,
    # PUT intentionally excluded: no API endpoints use PUT. All updates use PATCH.
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID", "X-Requested-With", "Accept"],
    expose_headers=["X-Request-ID", "Retry-After", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"],
    max_age=600,
)
# Note: TrustedHostMiddleware runs BEFORE CORSMiddleware. If a CORS preflight
# request has a Host header not in api_allowed_hosts, it will be rejected with
# 400 before CORS headers are attached. Ensure api_allowed_hosts includes all
# domains that legitimate CORS requests target.
app.add_middleware(DynamicTrustedHostMiddleware, allowed_hosts=lambda: get_settings().api_allowed_hosts)
app.add_middleware(RequestContextMiddleware)
app.add_middleware(_CancelledErrorMiddleware)


class _RequestTimeoutMiddleware:
    """Cancel requests that exceed a configurable wall-clock timeout."""

    _EXEMPT_PREFIXES = ("/v1/events/",)

    def __init__(self, app: object, *, timeout_seconds: int = 60) -> None:
        self.app = app
        self.timeout_seconds = timeout_seconds

    async def __call__(self, scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        if scope["type"] != "http" or any(scope["path"].startswith(p) for p in self._EXEMPT_PREFIXES):
            await self.app(scope, receive, send)
            return

        response_started = False

        async def guarded_send(message: dict) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await asyncio.wait_for(self.app(scope, receive, guarded_send), timeout=self.timeout_seconds)
        except TimeoutError:
            if not response_started:
                from starlette.responses import JSONResponse as _JR

                headers = {}
                if get_settings().app_env not in ("production", "staging"):
                    headers["X-Debug-Timeout"] = str(self.timeout_seconds)
                resp = _JR(status_code=504, content={"error": {"code": "request_timeout", "message": "Request timed out."}}, headers=headers)
                await resp(scope, receive, send)
            else:
                structlog.get_logger("api.timeout").warning(
                    "request.timeout_after_headers_sent",
                    path=scope.get("path", "/unknown"),
                    timeout_seconds=self.timeout_seconds,
                )


app.add_middleware(_RequestTimeoutMiddleware, timeout_seconds=settings.request_timeout_seconds)
app.add_middleware(PrometheusMiddleware)

app.include_router(health.router)
app.include_router(meta.router, prefix="/v1")
app.include_router(me.router, prefix="/v1")
app.include_router(catalog.router, prefix="/v1")
app.include_router(backtests.router, prefix="/v1")
app.include_router(templates.router, prefix="/v1")
app.include_router(scans.router, prefix="/v1")
app.include_router(sweeps.router, prefix="/v1")
app.include_router(forecasts.router, prefix="/v1")
app.include_router(exports.router, prefix="/v1")
app.include_router(daily_picks.router, prefix="/v1")
app.include_router(analysis.router, prefix="/v1")
app.include_router(billing.router, prefix="/v1")
app.include_router(events.router, prefix="/v1")
app.include_router(account.router, prefix="/v1")


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
    from backtestforecast.errors import ConfigurationError as _CfgErr
    API_ERRORS_TOTAL.labels(code=exc.code).inc()
    is_cfg_err = isinstance(exc, _CfgErr)
    log_message = "[redacted]" if is_cfg_err else exc.message
    logger.warning("api.error", code=exc.code, status_code=exc.status_code, message=log_message)
    safe_message = "An internal configuration error occurred." if is_cfg_err else exc.message
    payload = _error_payload(request, code=exc.code, message=safe_message)
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
    if isinstance(exc.detail, dict):
        code = exc.detail.get("code", "http_error")
        message = exc.detail.get("message", "The request could not be completed.")
        logger.warning("api.http_exception", status_code=exc.status_code, code=code, message=message)
        response = JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(request, code=code, message=message),
        )
    else:
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
    ip_address = _extract_client_ip(request)
    get_rate_limiter().check(bucket="admin_metrics", actor_key=ip_address or "unknown", limit=30, window_seconds=60)
    _settings = get_settings()
    if _settings.app_env in ("production", "staging"):
        import hmac as _hmac

        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        if not _settings.metrics_token or not token or not _hmac.compare_digest(token, _settings.metrics_token):
            get_rate_limiter().check(bucket="admin_auth_fail", actor_key=ip_address or "unknown", limit=5, window_seconds=60)
            return JSONResponse(status_code=403, content={"error": {"code": "forbidden", "message": "Forbidden"}})
    return metrics_response()


_dlq_redis: Redis | None = None
_dlq_redis_lock = __import__("threading").Lock()


def _get_dlq_redis():
    global _dlq_redis
    if _dlq_redis is None:
        with _dlq_redis_lock:
            if _dlq_redis is None:
                from redis import Redis
                _dlq_redis = Redis.from_url(
                    get_settings().redis_cache_url,
                    socket_timeout=5,
                    decode_responses=False,
                )
    return _dlq_redis


def _invalidate_dlq_redis() -> None:
    global _dlq_redis
    with _dlq_redis_lock:
        _dlq_redis = None


@app.get("/admin/dlq", include_in_schema=False)
def dlq_status(request: Request) -> Response:
    """Inspect the dead-letter queue depth and recent items.

    Requires the admin token (falls back to metrics token if admin_token is not set).
    """
    ip_address = _extract_client_ip(request)
    get_rate_limiter().check(bucket="admin_dlq", actor_key=ip_address or "unknown", limit=30, window_seconds=60)
    import hmac as _hmac

    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
    _dlq_settings = get_settings()
    expected_token = _dlq_settings.admin_token or _dlq_settings.metrics_token
    if not expected_token or not token or not _hmac.compare_digest(token, expected_token):
        logger.warning("admin.dlq_auth_failed", ip=ip_address)
        return JSONResponse(status_code=403, content={"error": {"code": "forbidden", "message": "Forbidden"}})
    logger.info("admin.dlq_accessed", ip=ip_address)
    try:
        import json

        r = _get_dlq_redis()
        depth = r.llen("bff:dead_letter_queue")
        recent_raw = r.lrange("bff:dead_letter_queue", 0, 9)

        _REDACT_KEYS = frozenset({
            "email", "emails", "password", "secret", "token", "api_key",
            "stripe_customer_id", "stripe_subscription_id", "clerk_user_id",
            "ip_address", "ip_hash", "ip",
            "name", "first_name", "last_name", "full_name",
            "phone", "phone_number", "address", "date_of_birth",
            "ssn", "user_agent", "authorization",
        })

        def _redact_list(items: list) -> list:
            result = []
            for item in items:
                if isinstance(item, dict):
                    result.append(_redact_dict(item))
                elif isinstance(item, list):
                    result.append(_redact_list(item))
                else:
                    result.append(item)
            return result

        def _redact_dict(d: dict) -> dict:
            result = {}
            for k, v in d.items():
                if k in _REDACT_KEYS:
                    result[k] = "[REDACTED]"
                elif isinstance(v, dict):
                    result[k] = _redact_dict(v)
                elif isinstance(v, list):
                    result[k] = _redact_list(v)
                else:
                    result[k] = v
            return result

        _DLQ_ITEM_MAX_BYTES = 65_536

        recent = []
        for raw in recent_raw:
            if len(raw) > _DLQ_ITEM_MAX_BYTES:
                logger.warning("admin.dlq_item_too_large", size=len(raw))
                continue
            try:
                item = json.loads(raw)
                if isinstance(item, dict):
                    item = _redact_dict(item)
                    if "kwargs" in item and isinstance(item["kwargs"], dict):
                        item["kwargs"] = _redact_dict(item["kwargs"])
                recent.append(item)
            except (ValueError, TypeError, UnicodeDecodeError):
                recent.append({"raw": "[binary data]"})
        return JSONResponse(content={"depth": depth, "recent": recent})
    except Exception:
        logger.exception("admin.dlq_unavailable")
        return JSONResponse(status_code=503, content={"error": {"code": "dlq_unavailable", "message": "Dead-letter queue is currently unavailable."}})


@app.get("/")
def root() -> dict[str, str]:
    payload: dict[str, str] = {
        "service": "backtestforecast-api",
        "status": "ok",
        "health": "/health/ready",
    }
    if get_settings().app_env in ("development", "test"):
        payload["docs"] = "/docs"
    return payload
