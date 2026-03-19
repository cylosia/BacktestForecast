from __future__ import annotations

import re
import time

import structlog

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

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

STRIPE_WEBHOOK_DEDUPE_TOTAL = Counter(
    "stripe_webhook_dedupe_total",
    "Stripe webhook events rejected by uq_stripe_events_event_id",
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
DB_POOL_CHECKED_IN = Gauge("db_pool_checked_in", "Database connections available in the pool")
DB_POOL_CHECKED_OUT = Gauge("db_pool_checked_out", "Database connections currently in use")
DB_POOL_OVERFLOW = Gauge("db_pool_overflow", "Database connections in overflow")
DB_POOL_MAX_OVERFLOW = Gauge("db_pool_max_overflow", "Maximum pool overflow connections configured")
DB_POOL_EXHAUSTION_WARNING = Gauge(
    "db_pool_exhaustion_warning",
    "Set to 1 when pool usage exceeds 80% capacity",
)

S3_STREAM_OPEN = Gauge("s3_stream_open", "Number of currently open S3 body streams")

FORECAST_FALLBACK_TOTAL = Counter(
    "forecast_fallback_total",
    "Forecasts that fell back to 0-analog empty response",
)

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

ACTIVE_SSE_CONNECTIONS = Gauge(
    "active_sse_connections",
    "Number of currently active SSE connections",
)

CELERY_WORKERS_ONLINE = Gauge(
    "celery_workers_online",
    "Number of Celery workers reporting via heartbeat",
)

CACHE_HITS_TOTAL = Counter(
    "cache_hits_total",
    "Cache lookups that returned a hit",
    ["cache"],
)
CACHE_MISSES_TOTAL = Counter(
    "cache_misses_total",
    "Cache lookups that returned a miss",
    ["cache"],
)

CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Current circuit breaker state (0=closed, 1=half-open, 2=open)",
    ["service"],
)

QUEUE_DEPTH = Gauge(
    "queue_depth",
    "Number of messages waiting in a Celery task queue",
    ["queue"],
)

EXPORT_JOBS_TOTAL = Counter(
    "export_jobs_total",
    "Total export jobs by final status",
    ["status"],
)

SCAN_JOBS_TOTAL = Counter(
    "scan_jobs_total",
    "Total scan jobs by final status",
    ["status"],
)

SWEEP_JOBS_TOTAL = Counter(
    "sweep_jobs_total",
    "Total sweep jobs by final status",
    ["status"],
)

ANALYSIS_JOBS_TOTAL = Counter(
    "analysis_jobs_total",
    "Total analysis jobs by final status",
    ["status"],
)

API_ERRORS_TOTAL = Counter(
    "api_errors_total",
    "Total API error responses by error code",
    ["code"],
)

BILLING_EVENTS_TOTAL = Counter(
    "billing_events_total",
    "Total billing events (checkout, subscription changes, cancellations)",
    ["event_type"],
)

ACCOUNT_DELETIONS_TOTAL = Counter(
    "account_deletions_total",
    "Total account deletions",
    ["stripe_cleanup_result"],
)

STRIPE_ORPHAN_CLEANUP_TOTAL = Counter(
    "stripe_orphan_cleanup_total",
    "Async Stripe cleanup attempts after account deletion",
    ["result"],
)

REDIS_CONNECTION_ERRORS_TOTAL = Counter(
    "redis_connection_errors_total",
    "Total Redis connection errors",
    ["operation"],
)

EVENT_PUBLISH_FAILURES_TOTAL = Counter(
    "event_publish_failures_total",
    "Non-Redis failures during event publishing",
)

DAILY_RECOMMENDATIONS_COUNT = Gauge(
    "daily_recommendations_count",
    "Number of rows in the daily_recommendations table",
)

OPTION_CACHE_ENTRIES = Gauge(
    "option_cache_entries",
    "Number of entries across all in-memory option gateway caches",
)

NIGHTLY_PIPELINE_RUNS_TOTAL = Counter(
    "nightly_pipeline_runs_total",
    "Total nightly pipeline runs by status",
    ["status"],
)

DB_STATEMENT_TIMEOUTS_TOTAL = Counter(
    "db_statement_timeouts_total",
    "Total PostgreSQL QueryCanceled exceptions due to statement_timeout",
    ["task_name"],
)

MIGRATION_HEAD_MATCH = Gauge(
    "migration_head_match",
    "1 if the DB migration version matches the code head, 0 if drifted",
)

EXTERNAL_API_REQUESTS_TOTAL = Counter(
    "external_api_requests_total",
    "Total external API requests (Massive, Stripe) by service and result",
    ["service", "result"],
)

EXTERNAL_API_LATENCY_SECONDS = Histogram(
    "external_api_latency_seconds",
    "Latency of external API calls in seconds",
    ["service"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# Fixed set of reason values for SCAN_CANDIDATE_FAILURES_TOTAL to avoid high cardinality.
# Maps unknown AppError codes to "unknown"; use "internal" for non-AppError exceptions.
SCAN_CANDIDATE_FAILURE_REASONS = frozenset({
    "internal", "unknown",
    "validation_error", "data_unavailable", "external_service_error",
    "configuration_error", "not_found", "quota_exceeded", "rate_limited",
    "conflict", "service_unavailable", "authentication_error", "authorization_error",
    "feature_locked",
})


def _normalize_scan_failure_reason(reason: str) -> str:
    """Return a validated reason for SCAN_CANDIDATE_FAILURES_TOTAL labels."""
    return reason if reason in SCAN_CANDIDATE_FAILURE_REASONS else "unknown"


SCAN_CANDIDATE_FAILURES_TOTAL = Counter(
    "backtestforecast_scan_candidate_failures_total",
    "Number of individual scanner candidates that failed evaluation",
    ["reason"],
)

MARKET_DATA_CACHE_HITS = Counter(
    "backtestforecast_market_data_cache_hits_total",
    "Market data cache hits",
    ["cache_type"],
)
MARKET_DATA_CACHE_MISSES = Counter(
    "backtestforecast_market_data_cache_misses_total",
    "Market data cache misses",
    ["cache_type"],
)

SCAN_EXECUTION_DURATION_SECONDS = Histogram(
    "backtestforecast_scan_execution_duration_seconds",
    "Time to execute a scanner job end-to-end",
    buckets=[10, 30, 60, 120, 300, 600],
)

SWEEP_EXECUTION_DURATION_SECONDS = Histogram(
    "sweep_execution_duration_seconds",
    "Time to execute a sweep job end-to-end",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

SWEEP_CANDIDATE_FAILURES_TOTAL = Counter(
    "sweep_candidate_failures_total",
    "Sweep candidates that failed during evaluation",
    ["reason"],
)

BACKTEST_EXECUTION_DURATION_SECONDS = Histogram(
    "backtest_execution_duration_seconds",
    "Time to execute a backtest run end-to-end",
    buckets=[0.1, 0.5, 1, 2.5, 5, 10, 30, 60],
)

EXPORT_EXECUTION_DURATION_SECONDS = Histogram(
    "export_execution_duration_seconds",
    "Time to execute an export job end-to-end",
    buckets=[0.5, 1, 2.5, 5, 10, 30, 60],
)

DISPATCH_RESULTS_TOTAL = Counter(
    "bff_dispatch_results_total",
    "Celery dispatch outcomes",
    ["result", "task_name"],
)

OUTBOX_RECOVERED_TOTAL = Counter(
    "bff_outbox_recovered_total",
    "Tasks recovered via outbox poller after inline dispatch failure",
    ["task_name"],
)

DB_QUERY_DURATION_SECONDS = Histogram(
    "db_query_duration_seconds",
    "Database query latency",
    ["operation"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
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
    "/v1/sweeps", "/v1/account",
    "/health", "/admin", "/metrics", "/meta",
})

import threading as _threading

_unknown_path_counter: int = 0
_unknown_path_lock = _threading.Lock()


def _normalize_path(path: str) -> str:
    """Collapse path parameters to avoid high-cardinality labels."""
    global _unknown_path_counter
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
        with _unknown_path_lock:
            _unknown_path_counter += 1
            count = _unknown_path_counter
        if count % 100 == 1:
            structlog.get_logger("observability.metrics").warning(
                "metrics.unknown_path_prefix",
                original_path=path,
                occurrences=_unknown_path_counter,
            )
        path = "/unknown"
    return path


class PrometheusMiddleware:
    """Pure ASGI middleware that records HTTP request metrics without
    buffering the response body, preserving SSE streaming."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        path = _normalize_path(scope.get("path", "/"))
        start = time.perf_counter()
        status_code = 500

        async def send_with_metrics(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 500)
            await send(message)

        try:
            await self.app(scope, receive, send_with_metrics)
        except Exception:
            raise
        finally:
            duration = time.perf_counter() - start
            HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status=str(status_code)).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(duration)


_logger = structlog.get_logger("observability.metrics")


def _refresh_pool_gauges() -> None:
    """Update DB pool gauges from the engine's live pool statistics."""
    try:
        from backtestforecast.config import get_settings
        from backtestforecast.db.session import get_pool_stats
        stats = get_pool_stats()
        pool_size = stats["pool_size"]
        checked_out = stats["checked_out"]
        overflow = stats["overflow"]
        max_overflow = stats.get("max_overflow", get_settings().db_pool_max_overflow)

        DB_POOL_SIZE.set(pool_size)
        DB_POOL_CHECKED_IN.set(stats["checked_in"])
        DB_POOL_CHECKED_OUT.set(checked_out)
        DB_POOL_OVERFLOW.set(overflow)
        DB_POOL_MAX_OVERFLOW.set(max_overflow)

        total_capacity = pool_size + max_overflow
        if total_capacity > 0:
            usage_ratio = (checked_out + overflow) / total_capacity
            DB_POOL_EXHAUSTION_WARNING.set(1 if usage_ratio > 0.8 else 0)
        else:
            DB_POOL_EXHAUSTION_WARNING.set(0)
    except Exception:
        _logger.debug("pool_gauge_refresh_failed", exc_info=True)


def metrics_response() -> Response:
    import os

    _refresh_pool_gauges()

    prom_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if prom_dir:
        from prometheus_client import CollectorRegistry
        from prometheus_client.multiprocess import MultiProcessCollector

        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        content = generate_latest(registry)
    else:
        content = generate_latest()

    return Response(
        content=content,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
