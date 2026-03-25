"""Test the current stage-3 ranking semantics for nightly pipeline candidates."""
from __future__ import annotations

import inspect


def test_pipeline_counts_and_logs_non_positive_scores():
    """Stage 3 must count and log retained non-positive quick-backtest scores."""
    from backtestforecast.pipeline.service import NightlyPipelineService

    source = inspect.getsource(NightlyPipelineService.run_pipeline)

    assert "score <= 0" in source, (
        "Pipeline run_pipeline must count non-positive quick-backtest scores "
        "before selecting top candidates for stage 4."
    )
    assert "pipeline.stage3_negative_scores_retained" in source, (
        "Pipeline should log when non-positive quick-backtest scores are retained."
    )


def test_pipeline_sorts_before_taking_top_candidates():
    """Stage 3 must sort all ranked results before slicing top candidates."""
    from backtestforecast.pipeline.service import NightlyPipelineService

    source = inspect.getsource(NightlyPipelineService.run_pipeline)

    sort_pos = source.find(".sort(")
    slice_pos = source.find("top_candidates = ranked_results[:max_full_candidates]")

    assert sort_pos >= 0 and slice_pos >= 0, (
        "Pipeline run_pipeline must sort ranked results and then slice top candidates."
    )
    assert sort_pos < slice_pos, (
        "Sorting must happen before slicing top_candidates. "
        "Otherwise stage 4 would not receive the highest-scoring candidates."
    )
