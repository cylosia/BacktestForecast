"""Verify BacktestRunDetailResponse has end_date, not date_to."""
from __future__ import annotations

from backtestforecast.schemas.backtests import BacktestRunDetailResponse


def test_detail_response_has_end_date():
    """BacktestRunDetailResponse must have 'end_date' field."""
    fields = BacktestRunDetailResponse.model_fields
    assert "end_date" in fields, f"Missing 'end_date' in {sorted(fields.keys())}"


def test_detail_response_has_no_date_to():
    """BacktestRunDetailResponse must NOT have 'date_to' field.

    The compare page previously used run.date_to which doesn't exist.
    """
    fields = BacktestRunDetailResponse.model_fields
    assert "date_to" not in fields, "'date_to' should not exist on BacktestRunDetailResponse"
