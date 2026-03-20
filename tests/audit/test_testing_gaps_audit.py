"""Tests closing the Testing Gaps identified in the production-grade audit.

Each test targets a specific gap where the audit found missing coverage.
Tests are structural (inspect source) or behavioral (exercise real logic)
so they run without a live database or Redis.
"""
from __future__ import annotations

import inspect
import re
import warnings
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ============================================================================
# TG1: DLQ Redis URL consistency between worker and API
# ============================================================================

def test_tg1_worker_dlq_uses_redis_cache_url():
    """Worker DLQ writes must use redis_cache_url, not redis_url (broker)."""
    from apps.worker.app.task_base import _get_dlq_redis
    source = inspect.getsource(_get_dlq_redis)
    assert "redis_cache_url" in source, (
        "Worker DLQ _get_dlq_redis must use redis_cache_url. "
        "Using redis_url writes to the Celery broker, making DLQ "
        "invisible to the /admin/dlq API endpoint."
    )
    assert "redis_url" not in source.replace("redis_cache_url", ""), (
        "Worker DLQ _get_dlq_redis should ONLY reference redis_cache_url, "
        "not redis_url."
    )


def test_tg1_api_dlq_uses_redis_cache_url():
    """API /admin/dlq reader must use redis_cache_url."""
    from apps.api.app.main import _get_dlq_redis
    source = inspect.getsource(_get_dlq_redis)
    assert "redis_cache_url" in source, (
        "API _get_dlq_redis must use redis_cache_url to read from "
        "the same Redis instance the worker writes to."
    )


def test_tg1_dlq_queue_depth_metric_reads_cache_redis():
    """DLQ depth metric must be read from redis_cache_url, not broker."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import reap_stale_jobs
    source = inspect.getsource(reap_stale_jobs)
    cache_idx = source.find("_cache_r")
    dlq_idx = source.find("dead_letter_queue")
    assert cache_idx != -1 and dlq_idx != -1, (
        "reap_stale_jobs must read DLQ depth from a separate cache Redis client"
    )


# ============================================================================
# TG2: Account deletion + Stripe cleanup retry
# ============================================================================

def test_tg2_account_delete_dispatches_stripe_cleanup_task():
    """Account deletion must dispatch a Celery task for Stripe cleanup on failure."""
    from apps.api.app.routers.account import _dispatch_stripe_cleanup_retry
    source = inspect.getsource(_dispatch_stripe_cleanup_retry)
    assert "cleanup_stripe_orphan" in source
    assert "countdown" in source


def test_tg2_account_delete_calls_cleanup_export_storage():
    """Account deletion must call _cleanup_export_storage before cascade."""
    from apps.api.app.routers.account import delete_account
    source = inspect.getsource(delete_account)
    storage_idx = source.index("_cleanup_export_storage")
    delete_idx = source.index("db.delete(user)")
    assert storage_idx < delete_idx, (
        "_cleanup_export_storage must run BEFORE db.delete(user) "
        "to prevent orphaned S3 objects."
    )


def test_tg2_account_delete_pii_hashed():
    """Account deletion audit event must not contain plaintext PII."""
    from apps.api.app.routers.account import delete_account
    source = inspect.getsource(delete_account)
    assert "email_hash" in source, "Audit event should contain email_hash, not email"
    assert "clerk_user_id_hash" in source, "Audit event should contain clerk_user_id_hash"
    assert '"email":' not in source or "email_hash" in source


# ============================================================================
# TG3: Concurrent pipeline runs prevented by Redis lock
# ============================================================================

def test_tg3_pipeline_acquires_lock():
    """Nightly pipeline must acquire a Redis lock to prevent concurrent runs."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import nightly_scan_pipeline
    source = inspect.getsource(nightly_scan_pipeline)
    assert "lock" in source.lower()
    assert "bff:pipeline:" in source


def test_tg3_pipeline_lock_exceeds_soft_time_limit():
    """Pipeline Redis lock timeout must exceed the task soft_time_limit."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import nightly_scan_pipeline

    lock_match = re.search(r"timeout=(\d+)", inspect.getsource(nightly_scan_pipeline))
    assert lock_match is not None, "Could not find lock timeout in pipeline task"
    lock_timeout = int(lock_match.group(1))
    soft_limit = nightly_scan_pipeline.soft_time_limit or 1800
    assert lock_timeout > soft_limit, (
        f"Lock timeout ({lock_timeout}s) must exceed soft_time_limit ({soft_limit}s) "
        f"to prevent premature lock release while the task is still running."
    )


# ============================================================================
# TG4: Redis failover — rate limiter fallback behavior
# ============================================================================

def test_tg4_rate_limiter_memory_fallback_halves_limit():
    """When Redis is down with fail_closed, memory fallback should use half the limit."""
    source = inspect.getsource(
        __import__("backtestforecast.security.rate_limits", fromlist=["RateLimiter"]).RateLimiter.check
    )
    assert "limit // 2" in source, (
        "Rate limiter memory fallback must halve the limit to compensate "
        "for per-process (not shared) counting."
    )


def test_tg4_rate_limiter_fail_closed_required_in_production():
    """Production config must enforce rate_limit_fail_closed=true."""
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "rate_limit_fail_closed" in source


# ============================================================================
# TG5: Cursor-based pagination available on all list endpoints
# ============================================================================

@pytest.mark.parametrize("router_module,endpoint_fn", [
    ("apps.api.app.routers.backtests", "list_backtests"),
    ("apps.api.app.routers.scans", "list_scans"),
    ("apps.api.app.routers.sweeps", "list_sweeps"),
    ("apps.api.app.routers.analysis", "list_analyses"),
])
def test_tg5_list_endpoint_accepts_cursor(router_module, endpoint_fn):
    """Every list endpoint must accept a `cursor` query parameter."""
    mod = __import__(router_module, fromlist=[endpoint_fn])
    fn = getattr(mod, endpoint_fn)
    sig = inspect.signature(fn)
    assert "cursor" in sig.parameters, (
        f"{router_module}.{endpoint_fn} must accept a 'cursor' parameter "
        f"for keyset pagination."
    )


@pytest.mark.parametrize("schema_module,response_cls", [
    ("backtestforecast.schemas.backtests", "BacktestRunListResponse"),
    ("backtestforecast.schemas.scans", "ScannerJobListResponse"),
    ("backtestforecast.schemas.sweeps", "SweepJobListResponse"),
    ("backtestforecast.schemas.exports", "ExportJobListResponse"),
    ("backtestforecast.schemas.analysis", "AnalysisListResponse"),
])
def test_tg5_list_response_has_next_cursor(schema_module, response_cls):
    """Every list response schema must include a next_cursor field."""
    mod = __import__(schema_module, fromlist=[response_cls])
    cls = getattr(mod, response_cls)
    assert "next_cursor" in cls.model_fields, (
        f"{response_cls} must include a next_cursor field for cursor pagination."
    )


# ============================================================================
# TG6: SSE connection limits within Redis pool capacity
# ============================================================================

def test_tg6_sse_process_limit_within_redis_pool():
    """SSE per-process connection limit must not exceed Redis pool size."""
    from apps.api.app.routers.events import SSE_MAX_CONNECTIONS_PROCESS
    from backtestforecast.config import get_settings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        settings = get_settings()

    assert SSE_MAX_CONNECTIONS_PROCESS <= settings.sse_redis_max_connections, (
        f"SSE_MAX_CONNECTIONS_PROCESS ({SSE_MAX_CONNECTIONS_PROCESS}) must be <= "
        f"sse_redis_max_connections ({settings.sse_redis_max_connections}). "
        f"Each SSE connection holds a Redis Pub/Sub subscription."
    )


# ============================================================================
# TG7: Billing customer creation race condition handling
# ============================================================================

def test_tg7_get_or_create_customer_handles_race():
    """_get_or_create_customer must handle the race where CAS update returns rowcount=0."""
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._get_or_create_customer)
    assert "rowcount == 0" in source, (
        "_get_or_create_customer must check rowcount == 0 for the race condition "
        "where another request created the customer between our check and write."
    )
    assert "cleanup_stripe_orphan" in source, (
        "_get_or_create_customer must dispatch a cleanup task for the orphaned "
        "Stripe customer when the race is lost."
    )


def test_tg7_no_blocking_sleep_in_customer_creation():
    """_get_or_create_customer must not block with time.sleep."""
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._get_or_create_customer)
    assert "time.sleep" not in source and "_time.sleep" not in source, (
        "_get_or_create_customer must not block the request thread with sleep. "
        "Use a single inline attempt + async Celery dispatch for cleanup."
    )


# ============================================================================
# TG8: Sweep TypeScript types match backend Pydantic schemas
# ============================================================================

def test_tg8_sweep_job_response_fields_present():
    """Every SweepJobResponse field must appear in the TypeScript type definitions."""
    from backtestforecast.schemas.sweeps import SweepJobResponse

    ts_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
    if not ts_path.exists():
        pytest.skip(f"TypeScript file not found: {ts_path}")
    ts_content = ts_path.read_text(encoding="utf-8")

    for field_name, field_info in SweepJobResponse.model_fields.items():
        ts_name = field_info.alias if field_info.alias else field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepJobResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )


def test_tg8_sweep_result_response_fields_present():
    """Every SweepResultResponse field must appear in TypeScript type definitions."""
    from backtestforecast.schemas.sweeps import SweepResultResponse

    ts_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
    if not ts_path.exists():
        pytest.skip(f"TypeScript file not found: {ts_path}")
    ts_content = ts_path.read_text(encoding="utf-8")

    for field_name, field_info in SweepResultResponse.model_fields.items():
        ts_name = field_info.alias if field_info.alias else field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepResultResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )


def test_tg8_create_sweep_request_fields_present():
    """Every CreateSweepRequest field must appear in TypeScript type definitions."""
    from backtestforecast.schemas.sweeps import CreateSweepRequest

    ts_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
    if not ts_path.exists():
        pytest.skip(f"TypeScript file not found: {ts_path}")
    ts_content = ts_path.read_text(encoding="utf-8")

    for field_name in CreateSweepRequest.model_fields:
        assert field_name in ts_content, (
            f"CreateSweepRequest.{field_name} is missing from TypeScript definitions"
        )


# ============================================================================
# TG-EXTRA: Error sanitization covers database connection strings
# ============================================================================

def test_error_sanitization_redacts_postgres_url():
    """sanitize_error_message must redact postgresql:// connection strings."""
    from backtestforecast.schemas.common import sanitize_error_message
    msg = "Connection failed: postgresql+psycopg://user:s3cret@db.example.com:5432/mydb"
    result = sanitize_error_message(msg)
    assert result == "An internal error occurred.", (
        f"postgresql:// URL leaked through sanitization: {result!r}"
    )


def test_error_sanitization_redacts_redis_url():
    """sanitize_error_message must redact redis:// connection strings."""
    from backtestforecast.schemas.common import sanitize_error_message
    msg = "Redis error: redis://default:password@redis.example.com:6379/0"
    result = sanitize_error_message(msg)
    assert result == "An internal error occurred."


def test_error_sanitization_redacts_stripe_keys():
    """sanitize_error_message must redact Stripe API keys."""
    from backtestforecast.schemas.common import sanitize_error_message
    msg = "Stripe error with key sk_live_abc123def456"
    result = sanitize_error_message(msg)
    assert result == "An internal error occurred."


def test_error_sanitization_passes_safe_messages():
    """Safe user-facing error messages must pass through unchanged."""
    from backtestforecast.schemas.common import sanitize_error_message
    msg = "Backtest run not found."
    result = sanitize_error_message(msg)
    assert result == msg


def test_error_sanitization_truncates_long_messages():
    """Messages exceeding 300 chars must be truncated."""
    from backtestforecast.schemas.common import sanitize_error_message
    msg = "A" * 500
    result = sanitize_error_message(msg)
    assert result is not None
    assert len(result) <= 304  # 300 + "..."


# ============================================================================
# TG-EXTRA: DLQ redaction covers Stripe IDs
# ============================================================================

def test_dlq_redaction_covers_stripe_ids():
    """DLQ redaction must cover customer_id and subscription_id."""
    from apps.worker.app.task_base import _DLQ_REDACT_KEYS
    assert "customer_id" in _DLQ_REDACT_KEYS
    assert "subscription_id" in _DLQ_REDACT_KEYS


def test_dlq_redaction_covers_pii_fields():
    """DLQ redaction must cover common PII fields."""
    from apps.worker.app.task_base import _DLQ_REDACT_KEYS
    for key in ("email", "password", "token", "api_key", "ip_address", "phone"):
        assert key in _DLQ_REDACT_KEYS, f"DLQ redaction missing key: {key}"


def test_dlq_error_sanitization_redacts_db_urls():
    """DLQ error sanitizer must redact database connection strings."""
    from apps.worker.app.task_base import _sanitize_error
    msg = "OperationalError: postgresql+psycopg://user:pass@host/db"
    result = _sanitize_error(msg)
    assert "pass@host" not in result
    assert "[REDACTED_URL]" in result


# ============================================================================
# TG-EXTRA: JWKS cache lifespan
# ============================================================================

def test_jwks_cache_lifespan_at_least_one_hour():
    """JWKS key cache should last at least 1 hour to survive Clerk outages."""
    from backtestforecast.auth.verification import ClerkTokenVerifier
    source = inspect.getsource(ClerkTokenVerifier._get_jwks_client)
    match = re.search(r"lifespan=(\d+)", source)
    assert match is not None, "Could not find lifespan parameter in _get_jwks_client"
    lifespan = int(match.group(1))
    assert lifespan >= 3600, (
        f"JWKS cache lifespan is {lifespan}s ({lifespan // 60} min). "
        f"Should be >= 3600s (1 hour) to survive Clerk JWKS endpoint outages."
    )


# ============================================================================
# TG-EXTRA: Webhook body limit adequate
# ============================================================================

def test_webhook_body_limit_at_least_128kb():
    """Stripe webhook body limit must be at least 128 KB."""
    from backtestforecast.security.http import BODY_LIMIT_OVERRIDES
    limit = BODY_LIMIT_OVERRIDES.get("/v1/billing/webhook")
    assert limit is not None, "Webhook path must have a body limit override"
    assert limit >= 131_072, (
        f"Webhook body limit is {limit} bytes ({limit // 1024} KB). "
        f"Should be >= 128 KB for large Stripe event payloads."
    )


# ============================================================================
# MT3: Response body size observability
# ============================================================================

def test_mt3_response_size_histogram_exists():
    """PrometheusMiddleware must track response body size."""
    from backtestforecast.observability.metrics import HTTP_RESPONSE_SIZE_BYTES
    assert HTTP_RESPONSE_SIZE_BYTES is not None


def test_mt3_middleware_tracks_response_bytes():
    """PrometheusMiddleware.send_with_metrics must count response body bytes."""
    from backtestforecast.observability.metrics import PrometheusMiddleware
    source = inspect.getsource(PrometheusMiddleware.__call__)
    assert "response_bytes" in source, (
        "PrometheusMiddleware must track response_bytes "
        "to feed the HTTP_RESPONSE_SIZE_BYTES histogram."
    )
    assert "http.response.body" in source


# ============================================================================
# MT5: Resilience — structural verification of failover paths
# ============================================================================

def test_mt5_events_fallback_persist_on_redis_failure():
    """Event publisher must fall back to direct DB persist when Redis is down."""
    from backtestforecast.events import _fallback_persist_status
    source = inspect.getsource(_fallback_persist_status)
    assert "create_worker_session" in source, (
        "_fallback_persist_status must create a DB session to persist "
        "status directly when Redis Pub/Sub is unavailable."
    )


def test_mt5_rate_limiter_reconnects_after_backoff():
    """Rate limiter must reconnect to Redis after a backoff period."""
    from backtestforecast.security.rate_limits import RateLimiter
    source = inspect.getsource(RateLimiter._get_redis)
    assert "_redis_retry_after" in source, (
        "RateLimiter must implement a retry-after backoff to avoid "
        "thundering herd reconnection attempts."
    )


def test_mt5_db_pool_pre_ping_enabled():
    """Database engine must use pool_pre_ping to detect stale connections."""
    from backtestforecast.db.session import build_engine
    source = inspect.getsource(build_engine)
    assert "pool_pre_ping" in source


def test_mt5_worker_visibility_timeout_exceeds_time_limit():
    """Celery visibility_timeout must exceed task_time_limit to prevent redelivery."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.celery_app import celery_app
    vis = celery_app.conf.broker_transport_options.get("visibility_timeout", 3600)
    hard = celery_app.conf.task_time_limit or 3900
    assert vis > hard, (
        f"visibility_timeout ({vis}s) must exceed task_time_limit ({hard}s) "
        f"to prevent the broker from re-delivering tasks still running."
    )


def test_mt5_outbox_pattern_for_reliable_dispatch():
    """dispatch_celery_task must write an OutboxMessage for crash recovery."""
    from apps.api.app.dispatch import dispatch_celery_task
    source = inspect.getsource(dispatch_celery_task)
    assert "OutboxMessage" in source, (
        "dispatch_celery_task must write an OutboxMessage in the same "
        "DB transaction as the job for reliable delivery."
    )
