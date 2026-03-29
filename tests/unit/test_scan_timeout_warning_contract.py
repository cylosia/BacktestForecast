from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from backtestforecast.billing.entitlements import ScannerMode
from backtestforecast.schemas.common import PlanTier
from backtestforecast.services.scan_service_helpers import scanner_job_response
from backtestforecast.version import DEFAULT_ENGINE_VERSION, DEFAULT_RANKING_VERSION


def test_scanner_job_response_accepts_timeout_warning_code() -> None:
    now = datetime.now(UTC)
    job = SimpleNamespace(
        id=uuid4(),
        name="Timeout scan",
        status="failed",
        mode=ScannerMode.BASIC,
        plan_tier_snapshot=PlanTier.FREE,
        job_kind="manual",
        candidate_count=10,
        evaluated_candidate_count=7,
        recommendation_count=0,
        refresh_daily=False,
        refresh_priority=0,
        ranking_version=DEFAULT_RANKING_VERSION,
        engine_version=DEFAULT_ENGINE_VERSION,
        warnings_json=[
            {
                "code": "timeout",
                "message": "Candidate evaluation stopped early after nearing the scan timeout.",
            }
        ],
        error_code="timeout",
        error_message="scan timed out",
        idempotency_key="timeout-scan",
        created_at=now,
        started_at=now,
        completed_at=now,
    )

    response = scanner_job_response(job)

    warning = response.model_dump()["warnings"][0]
    assert warning["code"] == "timeout"
    assert warning["message"] == "Candidate evaluation stopped early after nearing the scan timeout."
