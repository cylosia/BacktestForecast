from __future__ import annotations

import re
import time

from prometheus_client import Counter, Histogram, generate_latest
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
    "Times rate-limiting fell back to allow-all due to Redis unavailability",
    ["bucket"],
)


_RE_UUID = re.compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_RE_INT = re.compile(r"/\d+(?=/|$)")
_DYNAMIC_SEGMENT_PREFIXES = {
    "/symbols/", "/tickers/", "/api/v1/symbols/", "/api/v1/tickers/",
}


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
    return path


class PrometheusMiddleware(BaseHTTPMiddleware):
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
