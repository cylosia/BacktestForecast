"""Verify maintenance.poll_outbox is routed to the recovery queue."""


def test_poll_outbox_routed_to_recovery():
    from apps.worker.app.celery_app import celery_app

    routes = celery_app.conf.task_routes
    assert "maintenance.poll_outbox" in routes
    assert routes["maintenance.poll_outbox"]["queue"] == "recovery"
