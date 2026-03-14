from __future__ import annotations

import hashlib
import logging
import re
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from backtestforecast.config import Settings, get_settings

REQUEST_ID_HEADER = "x-request-id"
_SAFE_REQUEST_ID = re.compile(r"^[a-zA-Z0-9\-_.]{1,128}$")


def configure_logging(settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    processors: list[Callable[..., Any]] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
    ]
    if cfg.log_json:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(message)s",
        stream=sys.stdout,
        force=True,
    )
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


class RequestContextMiddleware(BaseHTTPMiddleware):
    """TODO: Convert to pure ASGI middleware to avoid SSE buffering issues
    caused by BaseHTTPMiddleware wrapping the response body iterator."""
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        raw_id = request.headers.get(REQUEST_ID_HEADER)
        request_id = raw_id if (raw_id and _SAFE_REQUEST_ID.match(raw_id)) else str(uuid.uuid4())
        started_at = time.perf_counter()
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            request_path=request.url.path,
            request_method=request.method,
        )
        request.state.request_id = request_id
        logger = structlog.get_logger("api.request")
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            logger.exception("request.failed", duration_ms=duration_ms)
            structlog.contextvars.clear_contextvars()
            raise
        response.headers[REQUEST_ID_HEADER] = request_id
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info("request.completed", status_code=response.status_code, duration_ms=duration_ms)
        structlog.contextvars.clear_contextvars()
        return response


def get_logger(name: str):
    return structlog.get_logger(name)


def hash_ip(value: str | None) -> str | None:
    if not value:
        return None
    settings = get_settings()
    return hashlib.sha256(
        f"{settings.ip_hash_salt}:{value}".encode("utf-8"),
    ).hexdigest()
