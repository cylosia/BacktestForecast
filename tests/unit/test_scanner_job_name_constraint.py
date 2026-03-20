"""Test that ScannerJob.name has a CHECK constraint preventing empty strings.

NULL is allowed (unnamed scan), but empty string "" is not — it would
bypass display logic that falls back to "Unnamed scan" for NULL names.
"""
from __future__ import annotations


def test_scanner_job_has_name_not_empty_constraint() -> None:
    from backtestforecast.models import ScannerJob

    constraint_names = {
        c.name
        for c in ScannerJob.__table__.constraints
        if hasattr(c, "name") and c.name is not None
    }
    assert "ck_scanner_jobs_name_not_empty" in constraint_names, (
        "ScannerJob must have a CHECK constraint 'ck_scanner_jobs_name_not_empty' "
        "preventing empty-string names (NULL is allowed)"
    )


def test_backtest_template_has_name_not_empty_constraint() -> None:
    """Verify the pattern match: BacktestTemplate also has name_not_empty."""
    from backtestforecast.models import BacktestTemplate

    constraint_names = {
        c.name
        for c in BacktestTemplate.__table__.constraints
        if hasattr(c, "name") and c.name is not None
    }
    assert "ck_backtest_templates_name_not_empty" in constraint_names
