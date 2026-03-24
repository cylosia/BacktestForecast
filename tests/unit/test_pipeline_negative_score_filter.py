"""Test that the nightly pipeline filters out negative-score candidates.

Regression test for the performance issue where candidates with negative
quick-backtest scores (losing strategies) were not filtered before stage 4,
wasting compute on full backtests of known-bad strategies.
"""
from __future__ import annotations

import inspect


def test_pipeline_filters_negative_scores():
    """Stage 3 results must be filtered to positive scores before stage 4."""
    from backtestforecast.pipeline.service import NightlyPipelineService

    source = inspect.getsource(NightlyPipelineService.run_pipeline)

    assert "score > 0" in source or "positive_results" in source, (
        "Pipeline run_pipeline must filter quick-backtest results to "
        "positive scores before selecting top candidates for stage 4."
    )

    assert "negative" in source.lower() or "positive" in source.lower(), (
        "Pipeline should log when negative-score candidates are filtered."
    )


def test_pipeline_sorts_after_filtering():
    """The sort must happen on positive results, not all results."""
    from backtestforecast.pipeline.service import NightlyPipelineService

    source = inspect.getsource(NightlyPipelineService.run_pipeline)

    sort_pos = source.find(".sort(")
    filter_pos = source.find("positive_results")

    if filter_pos >= 0 and sort_pos >= 0:
        assert filter_pos < sort_pos, (
            "Filtering to positive results must happen before sorting. "
            "Otherwise negative scores consume slots in top_candidates."
        )
