"""Test _find_pipeline_run fallback behavior."""
from __future__ import annotations

from unittest.mock import MagicMock

from apps.worker.app.tasks import _find_pipeline_run


def test_find_by_run_id():
    session = MagicMock()
    session.get.return_value = "mock_run"
    result = _find_pipeline_run(session, MagicMock, None, None, run_id="test-id")
    session.get.assert_called_once()
    assert result == "mock_run"


def test_find_by_run_object():
    session = MagicMock()
    mock_run = MagicMock()
    mock_run.id = "run-obj-id"
    session.get.return_value = "mock_run"
    _find_pipeline_run(session, MagicMock, mock_run, None)
    session.get.assert_called_once()
