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
}

# RedBeat loads these entries on first run and stores them in Redis.
# If RedBeat's Redis is flushed, these entries will be re-created from
# this config on the next beat startup.  This is expected behavior.
celery_app.conf.beat_schedule = {
    "refresh-prioritized-scans-daily": {
        "task": "scans.refresh_prioritized",
        "schedule": crontab(hour=6, minute=5),
    },
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


@worker_ready.connect
def _on_worker_ready(**kwargs):  # type: ignore[no-untyped-def]
    _shutdown_logger.info("worker.ready")


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
