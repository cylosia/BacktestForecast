"""Tracking test for early assignment risk model (FIXME #98).

This test documents the missing feature and will fail when the feature
is implemented — at which point it should be replaced with proper
behavioral tests.
"""
from __future__ import annotations

import inspect


def test_early_assignment_fixme_is_documented():
    """Verify the FIXME comment exists so it's not silently removed."""
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "FIXME(#98)" in source, "Early assignment risk FIXME must remain until implemented"
