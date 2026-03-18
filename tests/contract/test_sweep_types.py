"""Verify sweep schema fields are present in the response model."""
from __future__ import annotations

from backtestforecast.schemas.sweeps import SweepJobResponse, SweepResultResponse


def test_sweep_job_response_has_required_fields():
    fields = set(SweepJobResponse.model_fields.keys())
    expected = {
        "id", "status", "symbol", "candidate_count",
        "evaluated_candidate_count", "result_count",
        "prefetch_summary", "warnings", "error_code",
        "error_message", "created_at", "started_at", "completed_at",
    }
    assert expected.issubset(fields), f"Missing: {expected - fields}"


def test_sweep_result_response_has_required_fields():
    fields = set(SweepResultResponse.model_fields.keys())
    expected = {
        "id", "rank", "score", "strategy_type", "delta",
        "width_mode", "width_value", "entry_rule_set_name",
        "summary", "warnings", "trades_json", "equity_curve",
    }
    assert expected.issubset(fields), f"Missing: {expected - fields}"
