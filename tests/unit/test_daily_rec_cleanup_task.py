"""Verify the cleanup_daily_recommendations task exists and is registered."""
from __future__ import annotations


def test_cleanup_daily_recommendations_function_exists():
    from apps.worker.app.tasks import cleanup_daily_recommendations

    assert callable(cleanup_daily_recommendations)


def test_cleanup_daily_recommendations_is_registered():
    from apps.worker.app.celery_app import celery_app

    registered_tasks = celery_app.conf.beat_schedule
    assert "cleanup-daily-recommendations-weekly" in registered_tasks

    entry = registered_tasks["cleanup-daily-recommendations-weekly"]
    assert entry["task"] == "maintenance.cleanup_daily_recommendations"


def test_cleanup_daily_recommendations_task_name():
    from apps.worker.app.tasks import cleanup_daily_recommendations

    assert cleanup_daily_recommendations.name == "maintenance.cleanup_daily_recommendations"


def test_cleanup_daily_recommendations_routed_to_maintenance_queue():
    from apps.worker.app.celery_app import celery_app

    routes = celery_app.conf.task_routes
    assert routes.get("maintenance.cleanup_daily_recommendations") == {"queue": "maintenance"}
