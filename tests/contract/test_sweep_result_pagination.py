"""Verify SweepResultListResponse includes pagination fields."""
from __future__ import annotations

from backtestforecast.schemas.sweeps import SweepResultListResponse


def test_sweep_result_list_has_pagination_fields():
    """Backend response schema must include total, offset, limit for pagination."""
    fields = set(SweepResultListResponse.model_fields.keys())
    assert "total" in fields, "SweepResultListResponse missing 'total'"
    assert "offset" in fields, "SweepResultListResponse missing 'offset'"
    assert "limit" in fields, "SweepResultListResponse missing 'limit'"
    assert "items" in fields, "SweepResultListResponse missing 'items'"
