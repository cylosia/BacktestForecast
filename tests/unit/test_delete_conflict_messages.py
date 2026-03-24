from __future__ import annotations

from pathlib import Path


def test_delete_conflict_messages_do_not_reference_missing_cancel_workflow() -> None:
    targets = [
        "src/backtestforecast/services/backtests.py",
        "src/backtestforecast/services/exports.py",
        "src/backtestforecast/services/sweeps.py",
        "src/backtestforecast/pipeline/deep_analysis.py",
    ]

    for path in targets:
        source = Path(path).read_text()
        assert "Cancel it first." not in source, path
