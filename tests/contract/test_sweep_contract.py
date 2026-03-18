"""Verify sweep endpoint response shapes match frontend type expectations.

These tests guard against frontend-backend contract drift for sweep
endpoints that are not yet in the auto-generated OpenAPI schema.
"""
from __future__ import annotations

from backtestforecast.schemas.sweeps import (
    SweepJobResponse,
    SweepResultResponse,
)

EXPECTED_JOB_FIELDS = {
    "id", "status", "symbol", "candidate_count",
    "evaluated_candidate_count", "result_count",
    "prefetch_summary", "warnings", "error_code",
    "error_message", "created_at", "started_at",
    "completed_at",
}

EXPECTED_RESULT_FIELDS = {
    "id", "rank", "score", "strategy_type",
    "delta", "width_mode", "width_value",
    "entry_rule_set_name", "exit_rule_set_name",
    "profit_target_pct", "stop_loss_pct",
    "summary", "warnings", "trades_json", "equity_curve",
}


def test_sweep_job_response_fields():
    """SweepJobResponse must expose all fields the frontend expects."""
    schema_fields = set(SweepJobResponse.model_fields.keys())
    missing = EXPECTED_JOB_FIELDS - schema_fields
    assert not missing, f"SweepJobResponse is missing fields expected by frontend: {missing}"


def test_sweep_result_response_fields():
    """SweepResultResponse must expose all fields the frontend expects."""
    schema_fields = set(SweepResultResponse.model_fields.keys())
    missing = EXPECTED_RESULT_FIELDS - schema_fields
    assert not missing, f"SweepResultResponse is missing fields expected by frontend: {missing}"


def test_sweep_job_response_uses_field_names_not_aliases():
    """SweepJobResponse JSON uses field names (prefetch_summary, warnings), not aliases."""
    schema = SweepJobResponse.model_json_schema()
    props = set(schema.get("properties", {}).keys())
    assert "prefetch_summary" in props, "Expected 'prefetch_summary' in JSON schema"
    assert "warnings" in props, "Expected 'warnings' in JSON schema"
