"""Service for daily picks / nightly pipeline queries."""
from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from backtestforecast.models import DailyRecommendation, NightlyPipelineRun
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.repositories.daily_picks import DailyPicksRepository
from backtestforecast.schemas.analysis import DailyPicksResponse
from backtestforecast.schemas.common import sanitize_error_message


class DailyPicksService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = DailyPicksRepository(session)

    def get_latest_picks(
        self, trade_date: date | None = None, limit: int = 20, offset: int = 0,
    ) -> DailyPicksResponse:
        pipeline_run = self.repository.get_latest_succeeded_run(trade_date)
        if pipeline_run is None:
            return DailyPicksResponse(
                trade_date=trade_date,
                pipeline_run_id=None,
                status="no_data",
                items=[],
                pipeline_stats=None,
            )
        recommendations = self.repository.get_recommendations_for_run(
            pipeline_run.id, limit=limit, offset=offset,
        )
        return self._build_picks_response(pipeline_run, recommendations)

    def get_history(
        self, *, limit: int = 10, cursor: str | None = None,
    ) -> dict[str, Any]:
        effective_limit = min(max(limit, 1), 30)
        cursor_before, _ = parse_cursor_param(cursor)

        total = self.repository.count_pipeline_history()
        offset = (
            self.repository.count_pipeline_history_before_cursor(
                cursor_before=cursor_before,
            )
            if cursor_before is not None
            else 0
        )
        runs = self.repository.list_pipeline_history(
            limit=effective_limit + 1, cursor_before=cursor_before,
        )
        page = finalize_cursor_page(runs, total=total, offset=offset, limit=effective_limit)

        return {
            "items": [self._run_to_dict(r) for r in page.items],
            "total": page.total,
            "offset": page.offset,
            "limit": page.limit,
            "next_cursor": page.next_cursor,
        }

    @staticmethod
    def _build_picks_response(
        run: NightlyPipelineRun,
        recommendations: list[DailyRecommendation],
    ) -> DailyPicksResponse:
        dur = float(run.duration_seconds) if run.duration_seconds else None
        completed = run.completed_at.isoformat() if run.completed_at else None
        result = {
            "trade_date": run.trade_date.isoformat(),
            "pipeline_run_id": str(run.id),
            "status": "ok",
            "pipeline_stats": {
                "symbols_screened": run.symbols_screened,
                "symbols_after_screen": run.symbols_after_screen,
                "pairs_generated": run.pairs_generated,
                "quick_backtests_run": run.quick_backtests_run,
                "full_backtests_run": run.full_backtests_run,
                "recommendations_produced": run.recommendations_produced,
                "duration_seconds": dur,
                "completed_at": completed,
            },
            "items": [
                {
                    "rank": rec.rank,
                    "score": rec.score,
                    "symbol": rec.symbol,
                    "strategy_type": rec.strategy_type,
                    "regime_labels": rec.regime_labels or [],
                    "close_price": rec.close_price,
                    "target_dte": rec.target_dte,
                    "config_snapshot": rec.config_snapshot_json,
                    "summary": rec.summary_json,
                    "forecast": rec.forecast_json,
                }
                for rec in recommendations
            ],
        }
        return DailyPicksResponse.model_validate(result)

    @staticmethod
    def _run_to_dict(r: NightlyPipelineRun) -> dict[str, Any]:
        return {
            "id": str(r.id),
            "trade_date": r.trade_date.isoformat(),
            "status": r.status,
            "symbols_screened": r.symbols_screened,
            "recommendations_produced": r.recommendations_produced,
            "duration_seconds": (
                float(r.duration_seconds) if r.duration_seconds else None
            ),
            "completed_at": (
                r.completed_at.isoformat() if r.completed_at else None
            ),
            "error_code": r.error_code,
            "error_message": sanitize_error_message(r.error_message) if r.error_message else None,
        }
