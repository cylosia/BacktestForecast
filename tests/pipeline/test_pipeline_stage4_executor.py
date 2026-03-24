"""Verify pipeline Stage 4 passes the executor kwarg correctly."""
from __future__ import annotations

import inspect


class TestPipelineStage4ExecutorKwarg:
    """FINDING-001: _stage4_full_backtest requires executor as kwarg."""

    def test_stage4_called_with_executor_kwarg(self):
        """The call to _stage4_full_backtest must pass executor=executor."""
        from backtestforecast.pipeline.service import NightlyPipelineService

        source = inspect.getsource(NightlyPipelineService.run_pipeline)
        assert "executor=executor" in source or "_stage4_full_backtest(top_candidates, trade_date, executor=executor)" in source, \
            "_stage4_full_backtest must be called with executor=executor kwarg"

    def test_stage4_signature_requires_executor(self):
        """_stage4_full_backtest must have executor as a keyword-only parameter."""
        from backtestforecast.pipeline.service import NightlyPipelineService

        sig = inspect.signature(NightlyPipelineService._stage4_full_backtest)
        params = sig.parameters
        assert "executor" in params, "executor parameter must exist"
        param = params["executor"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY, \
            "executor must be keyword-only (after *)"
