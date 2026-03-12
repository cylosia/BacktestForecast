from celery import Celery
from celery.schedules import crontab
from kombu import Queue

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
)

celery_app.conf.task_queues = (
    Queue("research"),
    Queue("market_data"),
    Queue("exports"),
    Queue("maintenance"),
    Queue("pipeline"),
)

celery_app.conf.task_routes = {
    "backtests.run": {"queue": "research"},
    "scans.run_job": {"queue": "research"},
    "scans.refresh_prioritized": {"queue": "maintenance"},
    "exports.generate": {"queue": "exports"},
    "pipeline.nightly_scan": {"queue": "pipeline"},
    "maintenance.reap_stale_jobs": {"queue": "maintenance"},
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
