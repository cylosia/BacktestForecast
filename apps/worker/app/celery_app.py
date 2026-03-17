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
    result_expires=86400,
    worker_max_tasks_per_child=200,
    worker_max_memory_per_child=500_000,
    broker_connection_retry_on_startup=True,
    redbeat_redis_url=settings.redis_url,
    # visibility_timeout must exceed the longest task's hard time_limit
    # to prevent re-delivery of tasks that are still running.  4200s = 70 min.
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
    "exports.generate": {"queue": "exports"},
    "pipeline.nightly_scan": {"queue": "pipeline"},
    "analysis.deep_symbol": {"queue": "research"},
    "maintenance.cleanup_audit_events": {"queue": "maintenance"},
    "maintenance.refresh_market_holidays": {"queue": "maintenance"},
}

# RedBeat loads these entries on first run and stores them in Redis.
# If RedBeat's Redis is flushed, these entries will be re-created from
# this config on the next beat startup.  This is expected behavior.
celery_app.conf.beat_schedule = {
    "refresh-prioritized-scans-daily": {
        "task": "scans.refresh_prioritized",
        "schedule": crontab(hour=6, minute=5),
    },
    # Runs at 4:00 UTC (midnight ET / 11 PM EDT). Market data providers may not
    # have finalized end-of-day prices at this time. Consider running at 6:00 UTC
    # or later if stale close prices are observed in pipeline results.
    "nightly-scan-pipeline": {
        "task": "pipeline.nightly_scan",
        "schedule": crontab(hour=4, minute=0),
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
    structlog.contextvars.bind_contextvars(**ctx)


@task_postrun.connect
def _clear_task_context(task_id, task, *args, **kwargs):  # type: ignore[no-untyped-def]
    structlog.contextvars.clear_contextvars()


def _start_worker_metrics_server() -> None:
    """Start a lightweight HTTP server to expose Prometheus metrics from the worker process."""
    import hmac
    import os
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    port = int(os.environ.get("WORKER_METRICS_PORT", "9090"))
    metrics_token = settings.metrics_token

    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/metrics":
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

    server = HTTPServer(("0.0.0.0", port), _MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _shutdown_logger.info(
        "worker.metrics_server_started",
        port=port,
        auth_enabled=bool(metrics_token),
    )


_worker_heartbeat_key: str | None = None
_heartbeat_thread = None


def _start_heartbeat_loop() -> None:
    """Set a TTL key in Redis every 30s so the reaper can count live workers."""
    import os
    import threading
    import time

    global _worker_heartbeat_key
    pid = os.getpid()
    hostname = os.environ.get("HOSTNAME", f"worker-{pid}")
    _worker_heartbeat_key = f"worker:heartbeat:{hostname}:{pid}"

    def _loop() -> None:
        from redis import Redis
        conn: Redis | None = None
        while True:
            try:
                if conn is None:
                    conn = Redis.from_url(settings.redis_url, socket_timeout=5)
                conn.setex(_worker_heartbeat_key, 90, "1")
            except Exception:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
            time.sleep(30)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


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
    """Dispatch a one-off holiday refresh so the cache is warm on first boot."""
    from redis import Redis

    r = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=3)
    try:
        if r.exists("bff:market_holidays"):
            _shutdown_logger.info("worker.market_holidays_already_cached")
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
    if _worker_heartbeat_key:
        try:
            from redis import Redis
            r = Redis.from_url(settings.redis_url, socket_timeout=5)
            r.delete(_worker_heartbeat_key)
            r.close()
        except Exception:
            pass
    try:
        from backtestforecast.db.session import _get_engine

        if _get_engine.cache_info().currsize > 0:
            _get_engine().dispose()
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
