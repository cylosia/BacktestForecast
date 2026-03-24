"""Verify pipeline Stage 4 receives the shared executor instance."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from backtestforecast.pipeline.service import NightlyPipelineService


class TestPipelineStage4ExecutorKwarg:
    """FINDING-001: _stage4_full_backtest requires executor as kwarg."""

    def test_stage4_called_with_executor_kwarg(self):
        """run_pipeline() must pass the active pool to stage 4 via executor=."""
        session = MagicMock()
        run = MagicMock()
        run.id = "run-1"
        run.status = "running"
        run.stage = "pending"
        run.symbols_screened = 0
        run.symbols_after_screen = 0
        run.pairs_generated = 0
        run.quick_backtests_run = 0
        run.full_backtests_run = 0
        run.recommendations_produced = 0
        session.scalar.return_value = None

        captured: dict[str, object] = {}

        service = NightlyPipelineService(
            session,
            market_data_fetcher=MagicMock(),
            backtest_executor=MagicMock(),
            forecaster=None,
        )

        def stage4_side_effect(candidates, trade_date, *, executor):
            captured["executor"] = executor
            captured["candidates"] = candidates
            captured["trade_date"] = trade_date
            return []

        with (
            patch("backtestforecast.pipeline.service.get_settings") as mock_settings,
            patch.object(service, "_stage1_screen", return_value=[]),
            patch.object(service, "_stage2_match", return_value=[]),
            patch.object(service, "_stage3_quick_backtest", return_value=[]),
            patch.object(service, "_stage4_full_backtest", side_effect=stage4_side_effect) as mock_stage4,
            patch.object(service, "_stage5_forecast_and_rank", return_value=[]),
        ):
            mock_settings.return_value.pipeline_max_workers = 1
            session.refresh.side_effect = lambda obj: None
            session.add.side_effect = lambda obj: None

            added_runs: list[object] = []

            def add_side_effect(obj):
                added_runs.append(obj)
                if getattr(obj, "id", None) is None:
                    obj.id = run.id

            session.add.side_effect = add_side_effect
            session.refresh.side_effect = lambda obj: None

            result = service.run_pipeline(date(2025, 6, 2), ["AAPL"])

        assert mock_stage4.call_count == 1
        assert captured["candidates"] == []
        assert captured["trade_date"] == date(2025, 6, 2)
        assert captured["executor"] is not None
        assert result is not None

    def test_stage4_signature_requires_executor(self):
        """_stage4_full_backtest must have executor as a keyword-only parameter."""
        import inspect

        sig = inspect.signature(NightlyPipelineService._stage4_full_backtest)
        params = sig.parameters
        assert "executor" in params, "executor parameter must exist"
        param = params["executor"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY, (
            "executor must be keyword-only (after *)"
        )
