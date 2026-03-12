from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
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
from backtestforecast.errors import AppError
from backtestforecast.observability import REQUEST_ID_HEADER, configure_logging, get_logger
from backtestforecast.observability.logging import RequestContextMiddleware
from backtestforecast.observability.metrics import PrometheusMiddleware, metrics_response
from backtestforecast.security.http import ApiSecurityHeadersMiddleware, RequestBodyLimitMiddleware

settings = get_settings()
configure_logging(settings)
logger = get_logger("api")

_is_dev = settings.app_env in ("development", "test")


@asynccontextmanager
async def _lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    yield
    from apps.api.app.routers.events import shutdown_async_redis

    await shutdown_async_redis()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/docs" if _is_dev else None,
    redoc_url="/redoc" if _is_dev else None,
    lifespan=_lifespan,
)

app.add_middleware(PrometheusMiddleware)
app.add_middleware(ApiSecurityHeadersMiddleware)
app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=settings.request_max_body_bytes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.web_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.api_allowed_hosts)
app.add_middleware(RequestContextMiddleware)

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
    logger.warning("api.error", code=exc.code, status_code=exc.status_code, message=exc.message)
    response = JSONResponse(
        status_code=exc.status_code,
        content=_error_payload(request, code=exc.code, message=exc.message),
    )
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


@app.exception_handler(RequestValidationError)
def request_validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    sanitized = [
        {k: v for k, v in err.items() if k != "input"} for err in exc.errors()
    ]
    logger.warning("api.request_validation_error", errors=sanitized)
    response = JSONResponse(
        status_code=422,
        content=_error_payload(
            request,
            code="request_validation_error",
            message="The request payload did not match the expected schema.",
        ),
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


if settings.app_env != "test":

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics(request: Request) -> Response:
        if settings.app_env in ("production", "staging"):
            import hmac as _hmac

            auth = request.headers.get("Authorization", "")
            token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
            if not token:
                token = request.query_params.get("token", "")
            if not settings.metrics_token or not token or not _hmac.compare_digest(token, settings.metrics_token):
                return JSONResponse(status_code=403, content={"error": "forbidden"})
        return metrics_response()


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "backtestforecast-api",
        "status": "ok",
        "docs": "/docs",
        "health": "/health/ready",
    }
