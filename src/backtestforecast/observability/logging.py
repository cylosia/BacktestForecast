from __future__ import annotations

import hashlib
import hmac
import logging
import re
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

import structlog
from starlette.datastructures import Headers, MutableHeaders, State
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from backtestforecast.config import Settings, get_settings, register_invalidation_callback

REQUEST_ID_HEADER = "x-request-id"
_SAFE_REQUEST_ID = re.compile(r"^[a-zA-Z0-9\-_.]{1,128}$")

_SENSITIVE_FRAGMENTS = (
    "password", "secret", "token", "api_key", "apikey",
    "aws_secret_access_key", "secret_key", "authorization",
    "cookie", "credentials", "private_key",
)
_REDACTED = "[REDACTED]"


def short_hash(value: str | None) -> str | None:
    """Return a short irreversible hash for correlating sensitive identifiers."""
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _is_sensitive_key(lower_key: str) -> bool:
    return any(fragment in lower_key for fragment in _SENSITIVE_FRAGMENTS)


def _deep_sanitize_value(value: Any, _logger: Any, _method: str) -> Any:
    """Recursively sanitize dicts (and dicts nested within lists)."""
    if isinstance(value, dict):
        return _sanitize_sensitive_keys(_logger, _method, dict(value))
    if isinstance(value, list):
        return [_deep_sanitize_value(item, _logger, _method) for item in value]
    return value


def _sanitize_sensitive_keys(
    _logger: Any, _method: str, event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Drop or redact values whose keys suggest they contain secrets.

    Uses substring matching so compound keys like ``access_token``,
    ``stripe_secret_key``, or ``webhook_secret`` are caught automatically.

    Nested dicts and lists are shallow-copied before mutation to avoid
    corrupting shared objects that other code paths may still reference.
    """
    for key in list(event_dict):
        lower = key.lower()
        if _is_sensitive_key(lower):
            event_dict[key] = _REDACTED
        elif isinstance(event_dict[key], dict):
            event_dict[key] = _sanitize_sensitive_keys(_logger, _method, dict(event_dict[key]))
        elif isinstance(event_dict[key], list):
            event_dict[key] = [_deep_sanitize_value(item, _logger, _method) for item in event_dict[key]]
    return event_dict


def configure_logging(settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    processors: list[Callable[..., Any]] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        _sanitize_sensitive_keys,
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
    structlog.contextvars.bind_contextvars(app_env=cfg.app_env)


class RequestContextMiddleware:
    """Pure ASGI middleware that sets up structured logging context for each
    request without buffering the response body, preserving SSE streaming."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        req_headers = Headers(scope=scope)
        raw_id = req_headers.get(REQUEST_ID_HEADER)
        request_id = raw_id if (raw_id and _SAFE_REQUEST_ID.match(raw_id)) else str(uuid.uuid4())
        started_at = time.perf_counter()

        if "state" not in scope:
            scope["state"] = State()
        state = scope["state"]
        if isinstance(state, dict):
            state["request_id"] = request_id
        else:
            state.request_id = request_id  # type: ignore[union-attr]

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            request_path=scope.get("path", ""),
            request_method=scope.get("method", ""),
        )

        logger = structlog.get_logger("api.request")
        response_status_code: int | None = None

        async def send_with_request_id(message: Message) -> None:
            nonlocal response_status_code
            if message["type"] == "http.response.start":
                response_status_code = message.get("status", 0)
                resp_headers = MutableHeaders(scope=message)
                resp_headers[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            logger.exception("request.failed", duration_ms=duration_ms)
            raise
        else:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            logger.info("request.completed", status_code=response_status_code, duration_ms=duration_ms)
        finally:
            structlog.contextvars.clear_contextvars()


def bind_request_context(request_id: str | None = None, user_id: str | None = None) -> None:
    """Bind request-scoped context vars for structured logging.

    RequestContextMiddleware handles this automatically for HTTP requests.
    Use this helper for non-HTTP entry points (Celery tasks, CLI commands, etc.).
    """
    ctx: dict[str, str] = {}
    if request_id:
        ctx["request_id"] = request_id
    if user_id:
        ctx["user_id"] = user_id
    if ctx:
        structlog.contextvars.bind_contextvars(**ctx)


def get_logger(name: str):
    return structlog.get_logger(name)


_cached_ip_salt: bytes | None = None


def _invalidate_ip_salt() -> None:
    global _cached_ip_salt
    _cached_ip_salt = None


register_invalidation_callback(_invalidate_ip_salt)


def hash_ip(value: str | None) -> str | None:
    global _cached_ip_salt
    if not value:
        return None
    if _cached_ip_salt is None:
        _cached_ip_salt = get_settings().ip_hash_salt.encode("utf-8")
    return hmac.new(
        _cached_ip_salt,
        value.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
