from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from backtestforecast.models import TaskResult

pytestmark = pytest.mark.postgres


def _invoke_task(task, *args, **kwargs):
    return task.run(*args, **kwargs)


def _make_task_result(*, task_id: str, created_at: datetime) -> TaskResult:
    return TaskResult(
        task_name="maintenance.example",
        task_id=task_id,
        status="succeeded",
        created_at=created_at,
        completed_at=created_at,
        result_summary_json={},
        retries=0,
    )


def test_cleanup_task_results_deletes_only_expired_rows(postgres_db_session, monkeypatch):
    from apps.worker.app import tasks as tasks_module
    from backtestforecast.config import get_settings

    now = datetime.now(UTC)
    old_row = _make_task_result(task_id="old-task", created_at=now - timedelta(days=45))
    fresh_row = _make_task_result(task_id="fresh-task", created_at=now - timedelta(days=3))
    postgres_db_session.add_all([old_row, fresh_row])
    postgres_db_session.commit()

    class _SessionContext:
        def __enter__(self):
            return postgres_db_session

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr(tasks_module, "create_worker_session", lambda: _SessionContext())

    settings = get_settings()
    original_retention = settings.task_result_cleanup_retention_days
    settings.task_result_cleanup_retention_days = 30
    try:
        result = _invoke_task(tasks_module.cleanup_task_results)
    finally:
        settings.task_result_cleanup_retention_days = original_retention

    remaining_ids = set(postgres_db_session.scalars(select(TaskResult.task_id)))
    assert result["deleted"] == 1
    assert "old-task" not in remaining_ids
    assert "fresh-task" in remaining_ids


def test_cleanup_task_results_registered_and_scheduled():
    from apps.worker.app.celery_app import celery_app
    from apps.worker.app.tasks import cleanup_task_results

    assert cleanup_task_results.name == "maintenance.cleanup_task_results"
    assert celery_app.conf.task_routes["maintenance.cleanup_task_results"] == {"queue": "maintenance"}
    assert celery_app.conf.beat_schedule["cleanup-task-results-daily"]["task"] == "maintenance.cleanup_task_results"
