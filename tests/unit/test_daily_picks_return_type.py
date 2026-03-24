"""Test that DailyPicksService returns DailyPicksResponse consistently.

Regression test for the inconsistency where get_latest_picks returned
a raw dict for the "no data" case but a DailyPicksResponse for the
"has data" case.
"""
from __future__ import annotations

import inspect
from typing import get_type_hints

from backtestforecast.schemas.analysis import DailyPicksResponse


def test_return_type_annotation_is_consistent():
    """get_latest_picks must declare DailyPicksResponse as its return type."""
    from backtestforecast.services.daily_picks import DailyPicksService
    hints = get_type_hints(DailyPicksService.get_latest_picks)
    assert hints.get("return") is DailyPicksResponse, (
        f"get_latest_picks return type should be DailyPicksResponse, got {hints.get('return')}"
    )


def test_no_data_path_returns_response_object():
    """The no-data path must return a DailyPicksResponse, not a raw dict."""
    from backtestforecast.services.daily_picks import DailyPicksService
    source = inspect.getsource(DailyPicksService.get_latest_picks)
    assert "DailyPicksResponse(" in source, (
        "The no-data path must construct a DailyPicksResponse, not return a dict"
    )
    assert 'return {' not in source, (
        "get_latest_picks must not return a raw dict anywhere"
    )


def test_no_data_response_has_expected_fields():
    """The no-data DailyPicksResponse must have the expected shape."""
    from datetime import date
    r = DailyPicksResponse(
        trade_date=date(2024, 1, 15),
        pipeline_run_id=None,
        status="no_data",
        items=[],
        pipeline_stats=None,
    )
    assert r.status == "no_data"
    assert r.pipeline_run_id is None
    assert r.items == []
    assert r.pipeline_stats is None
    assert r.trade_date == date(2024, 1, 15)


def test_no_data_response_none_date():
    r = DailyPicksResponse(
        trade_date=None,
        pipeline_run_id=None,
        status="no_data",
        items=[],
        pipeline_stats=None,
    )
    assert r.trade_date is None
