"""Test that the sweep worker task handles SoftTimeLimitExceeded correctly."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from celery.exceptions import SoftTimeLimitExceeded

pytestmark = pytest.mark.filterwarnings("ignore:MASSIVE_API_KEY:UserWarning")


def _make_session(get_side_effect):
    """Build a MagicMock session that works as a context manager."""
    session = MagicMock()
    session.get.side_effect = get_side_effect
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_timeout_sets_status_failed(mock_create_session, mock_publish):
    """SoftTimeLimitExceeded during sweep must set status='failed' and
    error_code='time_limit_exceeded', then re-raise."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    mock_sweep = MagicMock()
    mock_sweep.user_id = uuid4()
    mock_sweep.status = "running"

    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    def _get(model, uid, **kwargs):
        name = model.__name__
        if name == "SweepJob":
            return mock_sweep
        if name == "User":
            return mock_user
        return None

    mock_create_session.return_value = _make_session(_get)

    mock_service = MagicMock()
    mock_service.run_job.side_effect = SoftTimeLimitExceeded("time limit")
    mock_service.close = MagicMock()

    policy = SimpleNamespace(forecasting_access=True, monthly_sweep_quota=None)

    with (
        patch("apps.worker.app.tasks._validate_task_ownership", return_value=True),
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.SweepService", return_value=mock_service),
        pytest.raises(SoftTimeLimitExceeded),
    ):
        run_sweep(str(job_id))

    assert mock_sweep.status == "failed"
    assert mock_sweep.error_code == "time_limit_exceeded"
    assert mock_sweep.completed_at is not None

    mock_service.close.assert_called_once()


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_timeout_publishes_failed_event(mock_create_session, mock_publish):
    """The handler must publish a status event with error_code metadata."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    mock_sweep = MagicMock()
    mock_sweep.user_id = uuid4()
    mock_sweep.status = "running"

    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    def _get(model, uid, **kwargs):
        name = model.__name__
        if name == "SweepJob":
            return mock_sweep
        if name == "User":
            return mock_user
        return None

    mock_create_session.return_value = _make_session(_get)

    mock_service = MagicMock()
    mock_service.run_job.side_effect = SoftTimeLimitExceeded("time limit")
    mock_service.close = MagicMock()

    policy = SimpleNamespace(forecasting_access=True, monthly_sweep_quota=None)

    with (
        patch("apps.worker.app.tasks._validate_task_ownership", return_value=True),
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.SweepService", return_value=mock_service),
        pytest.raises(SoftTimeLimitExceeded),
    ):
        run_sweep(str(job_id))

    fail_calls = [
        c for c in mock_publish.call_args_list
        if len(c.args) >= 3 and c.args[2] == "failed"
    ]
    assert len(fail_calls) >= 1, "publish_job_status must be called with 'failed'"
    last_fail = fail_calls[-1]
    assert last_fail.kwargs.get("metadata", {}).get("error_code") == "time_limit_exceeded"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_timeout_skips_update_for_terminal_status(mock_create_session, mock_publish):
    """If the job already reached a terminal status before the timeout handler
    runs, the handler must NOT overwrite it."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    mock_sweep = MagicMock()
    mock_sweep.user_id = uuid4()
    mock_sweep.status = "succeeded"

    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    def _get(model, uid, **kwargs):
        name = model.__name__
        if name == "SweepJob":
            return mock_sweep
        if name == "User":
            return mock_user
        return None

    mock_create_session.return_value = _make_session(_get)

    mock_service = MagicMock()
    mock_service.run_job.side_effect = SoftTimeLimitExceeded("time limit")
    mock_service.close = MagicMock()

    policy = SimpleNamespace(forecasting_access=True, monthly_sweep_quota=None)

    with (
        patch("apps.worker.app.tasks._validate_task_ownership", return_value=True),
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.SweepService", return_value=mock_service),
        pytest.raises(SoftTimeLimitExceeded),
    ):
        run_sweep(str(job_id))

    assert mock_sweep.status == "succeeded", (
        "Handler must not overwrite terminal status"
    )
