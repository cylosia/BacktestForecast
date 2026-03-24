"""Verify scan handles all candidates failing gracefully."""
from __future__ import annotations

from datetime import UTC, datetime, date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from backtestforecast.schemas.scans import CreateScannerJobRequest
from backtestforecast.services.scans import ScanService


def test_empty_candidates_marks_failed():
    """When no candidates complete, the job should end in scan_empty."""
    session = MagicMock()
    service = ScanService(session, execution_service=MagicMock())

    job = SimpleNamespace(
        id=uuid4(),
        created_at=datetime.now(UTC),
        status="running",
        error_code=None,
        error_message=None,
        completed_at=None,
        candidate_count=0,
        evaluated_candidate_count=0,
        warnings_json=[],
    )

    payload = CreateScannerJobRequest(
        name="Empty scan",
        mode="basic",
        symbols=["AAPL"],
        strategy_types=["long_call"],
        rule_sets=[{"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}]}],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 3, 29),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        max_recommendations=5,
    )

    service._count_compatible_candidates = MagicMock(return_value=(0, []))
    service._prepare_bundles = MagicMock(return_value={})
    service._batch_historical_performance = MagicMock(return_value={})

    result = service._execute_scan(job, payload)

    assert result is job
    assert job.status == "failed"
    assert job.error_code == "scan_empty"
    assert job.error_message == "No scan combinations completed successfully."
    assert job.completed_at is not None
    session.commit.assert_called()
