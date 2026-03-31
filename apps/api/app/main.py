from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from redis import Redis

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
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
    multi_step_backtests,
    multi_symbol_backtests,
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
    DynamicCORSMiddleware,
    DynamicTrustedHostMiddleware,
    RequestBodyLimitMiddleware,
    RuntimeHTTPPolicy,
)
from backtestforecast.version import get_public_version

_startup_settings = get_settings()
configure_logging(_startup_settings)
logger = get_logger("api")

# _startup_settings is reserved for process-start concerns like logging, docs
# exposure, and FastAPI metadata. Request-time HTTP policy is resolved
# separately so host/CORS/body-limit/security-header behavior tracks
# invalidate_settings() without relying on another module-level alias.


def _get_runtime_http_policy() -> RuntimeHTTPPolicy:
    runtime_settings = get_settings()
    return RuntimeHTTPPolicy(
        app_env=runtime_settings.app_env,
        request_max_body_bytes=runtime_settings.request_max_body_bytes,
        trusted_hosts=runtime_settings.api_allowed_hosts,
        cors_origins=runtime_settings.web_cors_origins,
    )

if _startup_settings.sentry_dsn:
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=_startup_settings.sentry_dsn,
            environment=_startup_settings.app_env,
            traces_sample_rate=_startup_settings.sentry_traces_sample_rate,
            send_default_pii=False,
        )
        logger.info("sentry.initialized", environment=_startup_settings.app_env)
    except Exception:
        logger.warning("sentry.init_failed", exc_info=True)

@asynccontextmanager
async def _lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    from backtestforecast.db.session import (
        get_database_timezones,
        get_migration_status,
        get_missing_schema_tables,
    )

    if _startup_settings.app_env in ("production", "staging"):
        if not _startup_settings.clerk_audience or not _startup_settings.clerk_audience.strip():
            raise RuntimeError(
                "CLERK_AUDIENCE must be set to a non-empty value in production/staging. "
                "JWT audience verification will not work without it."
            )
        if not _startup_settings.clerk_issuer or not _startup_settings.clerk_issuer.strip():
            raise RuntimeError(
                "CLERK_ISSUER must be set to a non-empty value in production/staging. "
                "JWT issuer verification will not work without it."
            )
        if not _startup_settings.admin_token or not _startup_settings.admin_token.strip():
            raise RuntimeError(
                "ADMIN_TOKEN must be set to a non-empty value in production/staging. "
                "The /admin/dlq endpoint will fall back to metrics_token without it."
            )
        if not _startup_settings.clerk_authorized_parties:
            raise RuntimeError(
                "CLERK_AUTHORIZED_PARTIES must be set in production/staging. "
                "Without it, any Clerk application sharing the same JWKS endpoint "
                "could generate valid tokens. Set it to your frontend app's client ID."
            )
    else:
        if not _startup_settings.clerk_audience:
            logger.warning(
                "startup.clerk_audience_missing",
                hint="CLERK_AUDIENCE is not set; JWT audience verification is disabled in development. "
                     "Tokens from any Clerk app sharing the same key pair will be accepted.",
            )
        if not _startup_settings.clerk_issuer:
            logger.warning(
                "startup.clerk_issuer_missing",
                hint="CLERK_ISSUER is not set; JWT issuer verification is disabled in development.",
            )
        if not _startup_settings.clerk_authorized_parties:
            logger.warning(
                "startup.clerk_authorized_parties_missing",
                hint="CLERK_AUTHORIZED_PARTIES is empty; the azp claim will not be checked.",
            )

    missing_schema_tables = get_missing_schema_tables()
    if missing_schema_tables:
        if _startup_settings.app_env in ("production", "staging"):
            raise RuntimeError(
                "DATABASE_URL points to an incomplete schema. Missing tables: "
                + ", ".join(missing_schema_tables)
            )
        logger.warning(
            "startup.schema_incomplete",
            missing_tables=list(missing_schema_tables),
            hint="Run Alembic migrations against the configured DATABASE_URL before serving traffic.",
        )

    migration_status = get_migration_status()
    if not migration_status["aligned"]:
        migration_error = migration_status.get("error")
        expected_revision = migration_status["expected_revision"] or "unknown"
        applied_revision = migration_status["applied_revision"] or "none"
        if migration_error:
            if _startup_settings.app_env in ("production", "staging"):
                raise RuntimeError(
                    "Unable to resolve Alembic head for readiness verification. "
                    f"Applied revision: {applied_revision}; error: {migration_error}."
                )
            logger.warning(
                "startup.migration_head_unavailable",
                applied_revision=applied_revision,
                migration_error=migration_error,
                hint="Fix repository bootstrap/import paths so Alembic resolves this checkout's migration head.",
            )
        if _startup_settings.app_env in ("production", "staging"):
            raise RuntimeError(
                "DATABASE_URL points to a schema revision that does not match Alembic head. "
                f"Applied revision: {applied_revision}; expected revision: {expected_revision}."
            )
        logger.warning(
            "startup.migration_drift",
            applied_revision=applied_revision,
            expected_revision=expected_revision,
            migration_error=migration_error,
            hint="Run Alembic migrations against the configured DATABASE_URL before serving traffic.",
        )

    db_timezones = get_database_timezones()
    server_timezone = db_timezones.get("server_timezone")
    if server_timezone and server_timezone.upper() != "UTC":
        logger.warning(
            "startup.database_server_timezone_not_utc",
            database_server_timezone=server_timezone,
            database_session_timezone=db_timezones.get("session_timezone"),
            hint=(
                "App sessions are pinned to UTC, but the database default timezone is not. "
                "Set the database/server timezone to UTC so ad hoc SQL, admin tools, and "
                "non-app scripts do not observe timezone-shifted timestamps."
            ),
        )

    if _startup_settings.feature_exports_enabled and not _startup_settings.s3_bucket:
        logger.warning(
            "startup.export_storage_using_database",
            export_storage_mode="database",
            hint=(
                "FEATURE_EXPORTS_ENABLED is true but S3_BUCKET is not configured, so exports "
                "will store content_bytes in Postgres. This is acceptable for small workloads "
                "but increases database bloat and memory-heavy download paths at scale."
            ),
        )

    if "*" in _startup_settings.web_cors_origins and _startup_settings.app_env in ("production", "staging"):
        raise RuntimeError(
            "WEB_CORS_ORIGINS must not contain '*' in production/staging when "
            "allow_credentials=True. This would allow any origin to make "
            "credentialed cross-origin requests."
        )

    _cors_hosts = set()
    for origin in _startup_settings.web_cors_origins:
        if origin != "*":
            from urllib.parse import urlparse as _parse_origin
            parsed = _parse_origin(origin)
            if parsed.hostname:
                _cors_hosts.add(parsed.hostname)
    _allowed_set = set(_startup_settings.api_allowed_hosts)
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

    _register_startup_invalidation_callbacks()

    logger.info(
        "startup.config_reload_surfaces",
        reloadable=[
            "runtime HTTP policy (trusted hosts, CORS origins, body limits)",
            "security headers app-env resolver",
            "DB/Redis/session factories and cached clients via invalidation callbacks",
            "JWKS/token verifier caches",
        ],
        restart_required=[
            "FastAPI title/version/docs OpenAPI surface",
            "process-start logging/Sentry configuration",
            "Celery beat schedules and worker process env",
            "Next.js baked NEXT_PUBLIC_* bundle values",
        ],
        msg=(
            "invalidate_settings() is process-local and refreshes only runtime-resolved settings. "
            "Startup-built surfaces still require a restart to pick up env changes."
        ),
    )

    if _startup_settings.clerk_jwks_url or _startup_settings.clerk_issuer:
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
        old_redis = _dlq_redis
        _dlq_redis = None
    _close_dlq_redis_client(old_redis, log_event="lifespan.dlq_redis_closed")

    from backtestforecast.db.session import _get_engine

    try:
        _get_engine().dispose()
        logger.info("lifespan.db_engine_disposed")
    except Exception:
        logger.warning("lifespan.db_engine_dispose_failed", exc_info=True)

    logger.info("lifespan.shutdown_complete")


app = FastAPI(
    title=_startup_settings.app_name,
    version=get_public_version(),
    description="BacktestForecast API - options backtesting, scanning, forecasting, and portfolio analysis.",
    openapi_url="/openapi.json" if _startup_settings.app_env in ("development", "test") else None,
    docs_url="/docs" if _startup_settings.app_env in ("development", "test") else None,
    redoc_url="/redoc" if _startup_settings.app_env in ("development", "test") else None,
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
            responses["422"] = {
                "description": "Validation error - request payload did not match the expected schema.",
                **_error_content,
            }
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
# 1. PrometheusMiddleware - records request metrics (including 499s)
# 2. _CancelledErrorMiddleware - converts CancelledError to 499
# 3. RequestContextMiddleware - binds request_id to structlog context
# 4. DynamicTrustedHostMiddleware - rejects requests with invalid Host headers
# 5. DynamicCORSMiddleware - handles cross-origin preflight and response headers
# 6. RequestBodyLimitMiddleware - enforces max request body size
# 7. ApiSecurityHeadersMiddleware - adds security response headers
#
# Starlette builds middleware LIFO: the last add_middleware call is outermost.
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)
app.add_middleware(ApiSecurityHeadersMiddleware, app_env_resolver=lambda: _get_runtime_http_policy().app_env)
app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=lambda: _get_runtime_http_policy().request_max_body_bytes)
app.add_middleware(
    DynamicCORSMiddleware,
    allow_origins=lambda: _get_runtime_http_policy().cors_origins,
    allow_credentials=True,
    # PUT intentionally excluded: no API endpoints use PUT. All updates use PATCH.
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID", "X-Requested-With", "Accept"],
    expose_headers=["X-Request-ID", "Retry-After", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"],
    max_age=600,
)
# Trusted host validation runs before CORS handling, so every browser-facing
# API hostname still needs to be present in API_ALLOWED_HOSTS_RAW.
app.add_middleware(DynamicTrustedHostMiddleware, allowed_hosts=lambda: _get_runtime_http_policy().trusted_hosts)
app.add_middleware(RequestContextMiddleware)
app.add_middleware(_CancelledErrorMiddleware)


class _RequestTimeoutMiddleware:
    """Cancel requests that exceed a configurable wall-clock timeout."""

    _EXEMPT_PREFIXES = ("/v1/events/",)

    def __init__(
        self,
        app: object,
        *,
        timeout_seconds: int = 60,
        timeout_seconds_resolver: Callable[[], int] | None = None,
    ) -> None:
        self.app = app
        self.timeout_seconds = timeout_seconds
        self._timeout_seconds_resolver = timeout_seconds_resolver

    def _resolve_timeout_seconds(self) -> int:
        if self._timeout_seconds_resolver is None:
            return self.timeout_seconds
        return int(self._timeout_seconds_resolver())

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
            timeout_seconds = self._resolve_timeout_seconds()
            await asyncio.wait_for(self.app(scope, receive, guarded_send), timeout=timeout_seconds)
        except TimeoutError:
            if not response_started:
                from starlette.responses import JSONResponse as _JR

                headers = {}
                if get_settings().app_env not in ("production", "staging"):
                    headers["X-Debug-Timeout"] = str(timeout_seconds)
                resp = _JR(status_code=504, content={"error": {"code": "request_timeout", "message": "Request timed out."}}, headers=headers)
                await resp(scope, receive, send)
            else:
                structlog.get_logger("api.timeout").warning(
                    "request.timeout_after_headers_sent",
                    path=scope.get("path", "/unknown"),
                    timeout_seconds=timeout_seconds,
                )


app.add_middleware(
    _RequestTimeoutMiddleware,
    timeout_seconds=_startup_settings.request_timeout_seconds,
    timeout_seconds_resolver=lambda: get_settings().request_timeout_seconds,
)
app.add_middleware(PrometheusMiddleware)

app.include_router(health.router)
app.include_router(meta.router, prefix="/v1")
app.include_router(me.router, prefix="/v1")
app.include_router(catalog.router, prefix="/v1")
app.include_router(backtests.router, prefix="/v1")
app.include_router(multi_symbol_backtests.router, prefix="/v1")
app.include_router(multi_step_backtests.router, prefix="/v1")
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
    sanitized = jsonable_encoder(
        [{k: v for k, v in err.items() if k != "input"} for err in exc.errors()],
        custom_encoder={Exception: lambda value: str(value)},
    )
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
_startup_invalidation_callbacks_registered = False
_startup_invalidation_callbacks_lock = __import__("threading").Lock()


def _register_startup_invalidation_callbacks() -> None:
    global _startup_invalidation_callbacks_registered
    with _startup_invalidation_callbacks_lock:
        if _startup_invalidation_callbacks_registered:
            return
        register_invalidation_callback(reset_trusted_networks)
        register_invalidation_callback(reset_token_verifier)
        register_invalidation_callback(_invalidate_dlq_redis)
        _startup_invalidation_callbacks_registered = True


def _close_dlq_redis_client(redis_client: Redis | None, *, log_event: str) -> None:
    if redis_client is None:
        return
    try:
        redis_client.close()
        logger.info(log_event)
    except Exception:
        logger.warning(f"{log_event}_failed", exc_info=True)


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
    old_redis: Redis | None
    with _dlq_redis_lock:
        old_redis = _dlq_redis
        _dlq_redis = None
    _close_dlq_redis_client(old_redis, log_event="settings_invalidation.dlq_redis_closed")


def _require_admin_token(request: Request, *, rate_limit_bucket: str) -> JSONResponse | None:
    ip_address = _extract_client_ip(request)
    get_rate_limiter().check(bucket=rate_limit_bucket, actor_key=ip_address or "unknown", limit=30, window_seconds=60)
    import hmac as _hmac

    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
    settings = get_settings()
    expected_token = settings.admin_token or settings.metrics_token
    if not expected_token or not token or not _hmac.compare_digest(token, expected_token):
        return JSONResponse(status_code=403, content={"error": {"code": "forbidden", "message": "Forbidden"}})
    return None


class _AdminRemediationRequest(BaseModel):
    action: str = Field(max_length=64)
    job_type: str | None = Field(default=None, max_length=32)
    job_id: str | None = Field(default=None, max_length=64)
    subscription_id: str | None = Field(default=None, max_length=128)
    customer_id: str | None = Field(default=None, max_length=128)
    user_id: str | None = Field(default=None, max_length=64)


@app.get("/admin/dlq", include_in_schema=False)
def dlq_status(request: Request) -> Response:
    """Inspect the dead-letter queue depth and recent items.

    Requires the admin token (falls back to metrics token if admin_token is not set).
    """
    ip_address = _extract_client_ip(request)
    auth_failure = _require_admin_token(request, rate_limit_bucket="admin_dlq")
    if auth_failure is not None:
        logger.warning("admin.dlq_auth_failed", ip=ip_address)
        return auth_failure
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
        from backtestforecast.db.session import create_session as _create_session
        from backtestforecast.services.dispatch_recovery import get_queue_diagnostics

        queue_diagnostics: dict[str, object]
        try:
            with _create_session() as session:
                queue_diagnostics = get_queue_diagnostics(session)
                session.rollback()
        except Exception:
            queue_diagnostics = {"status": "unknown"}

        return JSONResponse(content={"depth": depth, "recent": recent, "queue_diagnostics": queue_diagnostics})
    except Exception:
        logger.exception("admin.dlq_unavailable")
        return JSONResponse(status_code=503, content={"error": {"code": "dlq_unavailable", "message": "Dead-letter queue is currently unavailable."}})


@app.post("/admin/remediation", include_in_schema=False)
def admin_remediation(request: Request, payload: _AdminRemediationRequest) -> Response:
    ip_address = _extract_client_ip(request)
    auth_failure = _require_admin_token(request, rate_limit_bucket="admin_remediation")
    if auth_failure is not None:
        logger.warning("admin.remediation_auth_failed", ip=ip_address)
        return auth_failure

    try:
        from uuid import UUID

        from apps.api.app.routers.account import _dispatch_stripe_cleanup_retry
        from backtestforecast.db.session import create_session as _create_session
        from backtestforecast.models import (
            BacktestRun,
            ExportJob,
            MultiStepRun,
            MultiSymbolRun,
            ScannerJob,
            SweepJob,
            SymbolAnalysis,
        )
        from backtestforecast.services.audit import AuditService
        from backtestforecast.services.job_cancellation import (
            mark_job_cancelled,
            publish_cancellation_event,
            revoke_celery_task,
        )

        if payload.action == "cancel_job":
            model_map = {
                "backtest": (BacktestRun, "backtest"),
                "multi_symbol_backtest": (MultiSymbolRun, "multi_symbol_backtest"),
                "multi_step_backtest": (MultiStepRun, "multi_step_backtest"),
                "export": (ExportJob, "export"),
                "scan": (ScannerJob, "scan"),
                "sweep": (SweepJob, "sweep"),
                "analysis": (SymbolAnalysis, "analysis"),
            }
            if payload.job_type not in model_map or not payload.job_id:
                return JSONResponse(
                    status_code=422,
                    content={"error": {"code": "invalid_request", "message": "cancel_job requires a supported job_type and job_id."}},
                )
            model_cls, job_type = model_map[payload.job_type]
            with _create_session() as session:
                job = session.get(model_cls, UUID(payload.job_id))
                if job is None:
                    return JSONResponse(status_code=404, content={"error": {"code": "not_found", "message": "Job not found."}})
                if getattr(job, "status", None) not in ("queued", "running"):
                    return JSONResponse(
                        status_code=409,
                        content={"error": {"code": "conflict", "message": "Only queued or running jobs can be cancelled."}},
                    )
                task_id = mark_job_cancelled(job, error_code="cancelled_by_support", error_message="Cancelled by support remediation.")
                AuditService(session).record_always(
                    event_type="support.remediation_cancelled_job",
                    subject_type=model_cls.__name__.lower(),
                    subject_id=job.id,
                    user_id=None,
                    ip_address=ip_address,
                    metadata={"job_type": job_type, "reason": "support_remediation"},
                )
                session.commit()
                revoke_celery_task(task_id, job_type=job_type, job_id=job.id)
                publish_cancellation_event(job_type=job_type, job_id=job.id, error_code="cancelled_by_support")
                return JSONResponse(
                    content={
                        "action": "cancel_job",
                        "job_type": job_type,
                        "job_id": str(job.id),
                        "status": job.status,
                    }
                )

        if payload.action == "dispatch_stripe_cleanup":
            if not payload.user_id:
                return JSONResponse(
                    status_code=422,
                    content={"error": {"code": "invalid_request", "message": "dispatch_stripe_cleanup requires user_id."}},
                )
            user_id = UUID(payload.user_id)
            _dispatch_stripe_cleanup_retry(payload.subscription_id, payload.customer_id, user_id, "support_manual_dispatch")
            with _create_session() as session:
                AuditService(session).record_always(
                    event_type="support.remediation_dispatched_stripe_cleanup",
                    subject_type="user",
                    subject_id=user_id,
                    user_id=None,
                    ip_address=ip_address,
                    metadata={
                        "subscription_id_present": payload.subscription_id is not None,
                        "customer_id_present": payload.customer_id is not None,
                    },
                )
                session.commit()
            return JSONResponse(
                content={
                    "action": "dispatch_stripe_cleanup",
                    "user_id": str(user_id),
                    "queued": True,
                }
            )

        return JSONResponse(
            status_code=422,
            content={"error": {"code": "invalid_request", "message": "Unsupported remediation action."}},
        )
    except Exception:
        logger.exception("admin.remediation_failed", action=payload.action, ip=ip_address)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal_error", "message": "Admin remediation failed."}},
        )


@app.get("/")
def root() -> dict[str, str]:
    payload: dict[str, str] = {
        "service": "backtestforecast-api",
        "status": "ok",
        "health": "/health/live",
    }
    if get_settings().app_env in ("development", "test"):
        payload["docs"] = "/docs"
    return payload
