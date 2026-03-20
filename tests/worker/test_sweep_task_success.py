"""Tests for the run_sweep task happy path and error paths."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from backtestforecast.errors import AppError

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
def test_sweep_success_returns_succeeded(mock_create_session, mock_publish):
    """A sweep that completes without error returns status 'succeeded'."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    mock_sweep = MagicMock()
    mock_sweep.user_id = uuid4()
    mock_sweep.status = "running"

    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    completed_job = MagicMock()
    completed_job.status = "succeeded"
    completed_job.result_count = 5

    def _get(model, uid, **kwargs):
        name = model.__name__
        if name == "SweepJob":
            return mock_sweep
        if name == "User":
            return mock_user
        return None

    mock_create_session.return_value = _make_session(_get)

    mock_service = MagicMock()
    mock_service.run_job.return_value = completed_job
    mock_service.close = MagicMock()

    policy = SimpleNamespace(forecasting_access=True, monthly_sweep_quota=None)

    with (
        patch("apps.worker.app.tasks._validate_task_ownership", return_value=True),
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.SweepService", return_value=mock_service),
    ):
        result = run_sweep(str(job_id))

    assert result["status"] == "succeeded"
    assert result["job_id"] == str(job_id)
    assert result["result_count"] == 5

    mock_service.run_job.assert_called_once()
    mock_service.close.assert_called_once()


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_user_not_found_fails(mock_create_session, mock_publish):
    """When user lookup returns None the task must fail with 'entitlement_revoked'."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    mock_sweep = MagicMock()
    mock_sweep.user_id = uuid4()
    mock_sweep.status = "queued"

    def _get(model, uid, **kwargs):
        name = model.__name__
        if name == "SweepJob":
            return mock_sweep
        if name == "User":
            return None
        return None

    mock_create_session.return_value = _make_session(_get)

    with patch("apps.worker.app.tasks._validate_task_ownership", return_value=True):
        result = run_sweep(str(job_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"
    assert mock_sweep.status == "failed"
    assert mock_sweep.error_code == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_app_error_fails_with_code(mock_create_session, mock_publish):
    """When SweepService.run_job raises AppError the job is marked failed
    with the correct error_code from the exception."""
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
    mock_service.run_job.side_effect = AppError(
        code="data_unavailable", message="Market data feed down"
    )
    mock_service.close = MagicMock()

    policy = SimpleNamespace(forecasting_access=True, monthly_sweep_quota=None)

    with (
        patch("apps.worker.app.tasks._validate_task_ownership", return_value=True),
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.SweepService", return_value=mock_service),
    ):
        result = run_sweep(str(job_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "data_unavailable"
    mock_service.close.assert_called_once()

    fail_calls = [
        c for c in mock_publish.call_args_list
        if len(c.args) >= 3 and c.args[2] == "failed"
    ]
    assert len(fail_calls) >= 1
    assert fail_calls[-1].kwargs.get("metadata", {}).get("error_code") == "data_unavailable"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.create_worker_session")
def test_sweep_not_found_returns_failed(mock_create_session, mock_publish):
    """When the SweepJob lookup returns None the task returns status 'failed'."""
    from apps.worker.app.tasks import run_sweep

    job_id = uuid4()

    def _get(model, uid, **kwargs):
        return None

    mock_create_session.return_value = _make_session(_get)

    with patch("apps.worker.app.tasks._validate_task_ownership", return_value=True):
        result = run_sweep(str(job_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "not_found"
