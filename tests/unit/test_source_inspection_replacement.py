"""Fix 68: Placeholder documenting source-inspection tests to replace.

Several existing tests use ``inspect.getsource()`` to verify implementation
details (e.g. checking that a string like "with_for_update" appears in the
source code). These should eventually be replaced with behavioral tests that
actually exercise the functionality.

This file is a tracking placeholder; the individual tests are skipped.
"""
from __future__ import annotations

import pytest


@pytest.mark.skip(reason="Placeholder: replace source-inspection tests with behavioral tests")
def test_sweep_idempotency_should_be_behavioral():
    """test_sweep_idempotency.py uses inspect.getsource — needs behavioral rewrite."""


@pytest.mark.skip(reason="Placeholder: replace source-inspection tests with behavioral tests")
def test_sweep_quota_locking_should_be_behavioral():
    """test_sweep_quota_locking.py uses inspect.getsource — needs behavioral rewrite."""


@pytest.mark.skip(reason="Placeholder: replace source-inspection tests with behavioral tests")
def test_export_terminal_states_source_inspection():
    """test_export_terminal_states.py uses inspect.getsource — needs behavioral rewrite."""
