from __future__ import annotations

import re
import time

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

BACKTEST_RUNS_TOTAL = Counter(
    "backtest_runs_total",
    "Total backtest runs by final status",
    ["status"],
)

CELERY_TASKS_TOTAL = Counter(
    "celery_tasks_total",
    "Total Celery task executions",
    ["task_name", "status"],
)

RATE_LIMIT_HITS_TOTAL = Counter(
    "rate_limit_hits_total",
    "Total rate limit rejections",
    ["bucket"],
)

STRIPE_WEBHOOK_EVENTS_TOTAL = Counter(
    "stripe_webhook_events_total",
    "Total Stripe webhook events processed",
    ["event_type", "result"],
)

AUDIT_DEDUPE_CONFLICTS_TOTAL = Counter(
    "audit_dedupe_conflicts_total",
    "Audit event inserts rejected by uq_audit_events_dedup",
)

JOBS_STUCK_REDISPATCHED_TOTAL = Counter(
    "jobs_stuck_redispatched_total",
    "Stale jobs re-dispatched by the reaper",
    ["model"],
)

DUPLICATE_NIGHTLY_RUNS_TOTAL = Counter(
    "duplicate_nightly_runs_total",
    "Nightly pipeline runs rejected as duplicates for an already-succeeded trade_date",
)

DUPLICATE_TASK_EXECUTION_TOTAL = Counter(
    "duplicate_task_execution_total",
    "Celery tasks that were no-ops because the task_id did not match the job's celery_task_id",
    ["task_name"],
)

REDIS_RATE_LIMIT_FALLBACK_TOTAL = Counter(
    "redis_rate_limit_fallback_total",
    "Times rate-limiting fell back to in-memory counters due to Redis unavailability",
    ["bucket"],
)

DLQ_MESSAGES_TOTAL = Counter(
    "dlq_messages_total",
    "Total messages sent to the dead-letter queue",
    ["task_name"],
)

CIRCUIT_BREAKER_TRIPS_TOTAL = Counter(
    "circuit_breaker_trips_total",
    "Total circuit breaker trips (closed -> open transitions)",
    ["service"],
)

DLQ_DEPTH = Gauge(
    "dlq_depth",
    "Current number of messages in the Redis dead-letter queue (bff:dead_letter_queue)",
)

JOBS_STUCK_RUNNING = Gauge(
    "jobs_stuck_running",
    "Jobs in 'running' status longer than the staleness threshold",
    ["model"],
)

DB_POOL_SIZE = Gauge("db_pool_size", "Database connection pool size")
DB_POOL_CHECKED_OUT = Gauge("db_pool_checked_out", "Database connections currently in use")
DB_POOL_OVERFLOW = Gauge("db_pool_overflow", "Database connections in overflow")

S3_STREAM_OPEN = Gauge("s3_stream_open", "Number of currently open S3 body streams")

REAPER_DURATION_SECONDS = Histogram(
    "reaper_duration_seconds",
    "Time spent in reaper task",
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

REDIS_POOL_SIZE = Gauge(
    "redis_pool_size",
    "Redis connection pool size (set via periodic task or middleware from redis.connection_pool)",
)
REDIS_POOL_IN_USE = Gauge(
    "redis_pool_in_use",
    "Redis connections currently checked out from the pool",
)


_RE_UUID = re.compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_RE_INT = re.compile(r"/\d+(?=/|$)")
_DYNAMIC_SEGMENT_PREFIXES = {
    "/symbols/", "/tickers/", "/api/v1/symbols/", "/api/v1/tickers/",
    "/v1/forecasts/", "/forecasts/",
}


_KNOWN_PATH_PREFIXES = frozenset({
    "/v1/backtests", "/v1/scans", "/v1/exports", "/v1/analysis",
    "/v1/forecasts", "/v1/templates", "/v1/me", "/v1/billing",
    "/v1/daily-picks", "/v1/catalog", "/v1/events", "/v1/meta",
    "/health", "/admin", "/metrics", "/meta",
})


def _normalize_path(path: str) -> str:
    """Collapse path parameters to avoid high-cardinality labels."""
    path = _RE_UUID.sub("/{id}", path)
    path = _RE_INT.sub("/{id}", path)
    for prefix in _DYNAMIC_SEGMENT_PREFIXES:
        if prefix in path:
            idx = path.find(prefix) + len(prefix)
            rest = path[idx:]
            slug_end = rest.find("/")
            if slug_end == -1:
                path = path[:idx] + "{symbol}"
            else:
                path = path[:idx] + "{symbol}" + rest[slug_end:]
            break
    if not any(path.startswith(p) for p in _KNOWN_PATH_PREFIXES):
        path = "/unknown"
    return path


class PrometheusMiddleware(BaseHTTPMiddleware):
    """TODO: Convert to pure ASGI middleware to avoid SSE buffering issues
    caused by BaseHTTPMiddleware wrapping the response body iterator."""
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        method = request.method
        start = time.perf_counter()
        status_code = 500

        try:
            response: Response = await call_next(request)
            status_code = response.status_code
        except Exception:
            raise
        finally:
            duration = time.perf_counter() - start
            path = _normalize_path(request.url.path)
            HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status=str(status_code)).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(duration)

        return response


def metrics_response() -> Response:
    return Response(
        content=generate_latest(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
