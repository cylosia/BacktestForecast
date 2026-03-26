"""Verify maintenance.poll_outbox is routed to the recovery queue."""


def test_poll_outbox_routed_to_recovery():
    from apps.worker.app.celery_app import celery_app

    routes = celery_app.conf.task_routes
    assert "maintenance.poll_outbox" in routes
    assert routes["maintenance.poll_outbox"]["queue"] == "recovery"


def test_multi_workflow_tasks_have_dedicated_routes():
    from apps.worker.app.celery_app import celery_app

    routes = celery_app.conf.task_routes
    assert routes["multi_symbol_backtests.run"]["queue"] == "multi_symbol_backtests"
    assert routes["multi_step_backtests.run"]["queue"] == "multi_step_backtests"
