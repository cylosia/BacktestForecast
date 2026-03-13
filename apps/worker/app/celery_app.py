from celery import Celery
from celery.schedules import crontab
from kombu import Queue
from redbeat import RedBeatSchedulerEntry  # noqa: F401 — registers the custom scheduler

from backtestforecast.config import get_settings
from backtestforecast.observability import configure_logging

settings = get_settings()
configure_logging(settings)

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
    worker_prefetch_multiplier=1,
    task_soft_time_limit=3600,
    task_time_limit=3900,
    result_expires=86400,
    worker_max_tasks_per_child=200,
    worker_max_memory_per_child=500_000,
    broker_connection_retry_on_startup=True,
    redbeat_redis_url=settings.redis_url,
    # visibility_timeout must exceed the longest task's hard time_limit
    # (currently pipeline.nightly_scan at 1860s) to prevent re-delivery
    # of tasks that are still running.  4200s = 70 minutes.
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
    "backtests.run": {"queue": "research"},
    "scans.run_job": {"queue": "research"},
    "scans.refresh_prioritized": {"queue": "maintenance"},
    "exports.generate": {"queue": "exports"},
    "pipeline.nightly_scan": {"queue": "pipeline"},
    "analysis.deep_symbol": {"queue": "research"},
}

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
}
