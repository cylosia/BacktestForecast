import threading

import structlog
from celery import Celery
from celery.schedules import crontab
from celery.signals import task_postrun, task_prerun, worker_ready, worker_shutdown, worker_shutting_down
from kombu import Queue
from redbeat import RedBeatSchedulerEntry  # noqa: F401 — registers the custom scheduler

from backtestforecast.config import get_settings
from backtestforecast.observability import configure_logging

_shutdown_logger = structlog.get_logger("worker.lifecycle")

settings = get_settings()
configure_logging(settings)

if settings.sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.celery import CeleryIntegration

        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.app_env,
            traces_sample_rate=settings.sentry_traces_sample_rate,
            send_default_pii=False,
            integrations=[CeleryIntegration()],
        )
        _shutdown_logger.info("sentry.initialized", environment=settings.app_env)
    except Exception:
        _shutdown_logger.warning("sentry.init_failed", exc_info=True)

celery_app = Celery(
    "backtestforecast",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["apps.worker.app.tasks"],
)

celery_app.conf.update(
    task_default_queue="research",
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=3600,
    task_time_limit=3900,
    result_expires=7200,
    worker_max_tasks_per_child=200,
    worker_max_memory_per_child=500_000,
    broker_connection_retry_on_startup=True,
    redbeat_redis_url=settings.redis_url,
    # REQUIREMENT: visibility_timeout must exceed the longest task's hard
    # time_limit to prevent the broker from re-delivering tasks that are still
    # running.  Current longest task_time_limit = 3900s (65 min), so we set
    # visibility_timeout = 4200s (70 min).  If you increase task_time_limit,
    # you MUST increase visibility_timeout proportionally.
    broker_transport_options={"visibility_timeout": 4200},
)

celery_app.conf.task_queues = (
    Queue("research"),
    Queue("exports"),
    Queue("maintenance"),
    Queue("pipeline"),
)

celery_app.conf.task_routes = {
    "maintenance.ping": {"queue": "maintenance"},
    "maintenance.reap_stale_jobs": {"queue": "maintenance"},
    "maintenance.reconcile_s3_orphans": {"queue": "maintenance"},
    "backtests.run": {"queue": "research"},
    "scans.run_job": {"queue": "research"},
    "scans.refresh_prioritized": {"queue": "maintenance"},
    "sweeps.run": {"queue": "research"},
    "exports.generate": {"queue": "exports"},
    "pipeline.nightly_scan": {"queue": "pipeline"},
    "analysis.deep_symbol": {"queue": "research"},
    "maintenance.cleanup_audit_events": {"queue": "maintenance"},
    "maintenance.cleanup_daily_recommendations": {"queue": "maintenance"},
    "maintenance.refresh_market_holidays": {"queue": "maintenance"},
    "maintenance.cleanup_outbox": {"queue": "maintenance"},
    "maintenance.poll_outbox": {"queue": "maintenance"},
}

# RedBeat loads these entries on first run and stores them in Redis.
# If RedBeat's Redis is flushed, these entries will be re-created from
# this config on the next beat startup.  This is expected behavior.
celery_app.conf.beat_schedule = {
    "refresh-prioritized-scans-daily": {
        "task": "scans.refresh_prioritized",
        "schedule": crontab(hour=6, minute=30),
    },
    # Runs at 6:00 UTC (~1 AM EST / 2 AM EDT) to ensure end-of-day prices
    # from market data providers are fully finalized before the pipeline
    # consumes them.
    "nightly-scan-pipeline": {
        "task": "pipeline.nightly_scan",
        "schedule": crontab(hour=6, minute=0),
        "kwargs": {"max_recommendations": 20},
    },
    "reap-stale-jobs": {
        "task": "maintenance.reap_stale_jobs",
        "schedule": crontab(minute="*/10"),
    },
    "reconcile-s3-orphans-daily": {
        "task": "maintenance.reconcile_s3_orphans",
        "schedule": crontab(hour=3, minute=30),
    },
    "cleanup-audit-events-weekly": {
        "task": "maintenance.cleanup_audit_events",
        "schedule": crontab(hour=2, minute=0, day_of_week=0),
    },
    "refresh-market-holidays-weekly": {
        "task": "maintenance.refresh_market_holidays",
        "schedule": crontab(hour=1, minute=0, day_of_week=0),
    },
    "cleanup-daily-recommendations-weekly": {
        "task": "maintenance.cleanup_daily_recommendations",
        "schedule": crontab(hour=2, minute=30, day_of_week=0),
    },
    "cleanup-outbox-daily": {
        "task": "maintenance.cleanup_outbox",
        "schedule": crontab(hour=4, minute=0),
    },
}


@task_prerun.connect
def _bind_task_context(task_id, task, *args, **kwargs):  # type: ignore[no-untyped-def]
    structlog.contextvars.clear_contextvars()
    ctx: dict[str, str] = {"task_id": task_id, "task_name": task.name}
    headers = getattr(task.request, "headers", None) or {}
    if isinstance(headers, dict):
        origin_request_id = headers.get("request_id")
        if origin_request_id:
            ctx["origin_request_id"] = origin_request_id
        traceparent = headers.get("traceparent")
        if traceparent:
            ctx["traceparent"] = traceparent
    structlog.contextvars.bind_contextvars(**ctx)


@task_postrun.connect
def _clear_task_context(task_id, task, *args, **kwargs):  # type: ignore[no-untyped-def]
    structlog.contextvars.clear_contextvars()


def _start_worker_metrics_server() -> None:
    """Start a lightweight HTTP server to expose Prometheus metrics from the worker process."""
    import hmac
    import os
    from http.server import HTTPServer, BaseHTTPRequestHandler

    port = int(os.environ.get("WORKER_METRICS_PORT", "9090"))
    metrics_token = settings.metrics_token

    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/metrics":
                if not metrics_token and settings.app_env in ("production", "staging"):
                    self.send_response(403)
                    self.end_headers()
                    return
                if metrics_token:
                    auth = self.headers.get("Authorization", "")
                    token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
                    if not token or not hmac.compare_digest(token, metrics_token):
                        self.send_response(403)
                        self.end_headers()
                        return
                from prometheus_client import generate_latest
                body = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    if not metrics_token:
        _shutdown_logger.warning("worker.metrics_server_no_auth", msg="Metrics server started without authentication token")

    bind_host = os.environ.get("WORKER_METRICS_BIND", "0.0.0.0")
    server = HTTPServer((bind_host, port), _MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _shutdown_logger.info(
        "worker.metrics_server_started",
        port=port,
        auth_enabled=bool(metrics_token),
    )


_worker_heartbeat_key: str | None = None
_heartbeat_thread = None
_heartbeat_stop = threading.Event()


def _start_heartbeat_loop() -> None:
    """Set a TTL key in Redis every 30s so the reaper can count live workers."""
    import os

    global _worker_heartbeat_key
    pid = os.getpid()
    hostname = os.environ.get("HOSTNAME", f"worker-{pid}")
    _worker_heartbeat_key = f"worker:heartbeat:{hostname}:{pid}"

    def _loop() -> None:
        from redis import Redis
        conn: Redis | None = None
        consecutive_errors = 0
        while not _heartbeat_stop.is_set():
            try:
                if conn is None:
                    conn = Redis.from_url(settings.redis_url, socket_timeout=5)
                conn.setex(_worker_heartbeat_key, 90, "1")
                consecutive_errors = 0
            except Exception:
                consecutive_errors += 1
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
            sleep_secs = min(15 * (2 ** consecutive_errors), 30)
            _heartbeat_stop.wait(sleep_secs)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    global _heartbeat_thread
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    _heartbeat_thread = t


@worker_ready.connect
def _on_worker_ready(**kwargs):  # type: ignore[no-untyped-def]
    _shutdown_logger.info("worker.ready")
    try:
        _start_heartbeat_loop()
    except Exception:
        _shutdown_logger.warning("worker.heartbeat_failed", exc_info=True)
    try:
        _start_worker_metrics_server()
    except Exception:
        _shutdown_logger.warning("worker.metrics_server_failed", exc_info=True)
    try:
        _seed_market_holidays()
    except Exception:
        _shutdown_logger.warning("worker.market_holidays_seed_failed", exc_info=True)


def _seed_market_holidays() -> None:
    """Dispatch a one-off holiday refresh so the cache is warm on first boot.

    Uses a SET NX lock so that only the first worker to start dispatches the
    refresh task, avoiding duplicate dispatches from concurrent workers.
    """
    from redis import Redis

    r = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=3)
    try:
        if r.exists("bff:market_holidays"):
            try:
                count = r.scard("bff:market_holidays")
            except Exception:
                count = 1
            if count > 0:
                _shutdown_logger.info("worker.market_holidays_already_cached")
                return
        acquired = r.set("bff:market_holidays_seed_lock", "1", nx=True, ex=300)
        if not acquired:
            _shutdown_logger.info("worker.market_holidays_seed_already_dispatched")
            return
    finally:
        r.close()

    celery_app.send_task("maintenance.refresh_market_holidays")
    _shutdown_logger.info("worker.market_holidays_seed_dispatched")


@worker_shutting_down.connect
def _on_worker_shutting_down(sig, how, exitcode, **kwargs):  # type: ignore[no-untyped-def]
    _shutdown_logger.info(
        "worker.shutting_down",
        signal=str(sig),
        how=how,
        exitcode=exitcode,
    )


@worker_shutdown.connect
def _on_worker_shutdown(**kwargs):  # type: ignore[no-untyped-def]
    _shutdown_logger.info("worker.shutdown_cleanup_started")
    _heartbeat_stop.set()
    if _worker_heartbeat_key:
        try:
            from redis import Redis
            r = Redis.from_url(settings.redis_url, socket_timeout=5)
            try:
                r.delete(_worker_heartbeat_key)
            finally:
                r.close()
        except Exception:
            pass
    try:
        from backtestforecast.db.session import _get_worker_engine

        if _get_worker_engine.cache_info().currsize > 0:
            _get_worker_engine().dispose()
            _shutdown_logger.info("worker.db_engine_disposed")
        else:
            _shutdown_logger.info("worker.db_engine_never_created")
    except Exception:
        _shutdown_logger.warning("worker.db_engine_dispose_failed", exc_info=True)
    try:
        from backtestforecast.events import _shutdown_redis

        _shutdown_redis()
        _shutdown_logger.info("worker.redis_closed")
    except Exception:
        _shutdown_logger.warning("worker.redis_close_failed", exc_info=True)
    _shutdown_logger.info("worker.shutdown_cleanup_complete")
