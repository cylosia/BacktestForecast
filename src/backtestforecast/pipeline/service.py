"""Nightly scan pipeline service.

Orchestrates the five-stage funnel:
  Stage 1: Universe screening (indicators only, all symbols)
  Stage 2: Strategy-symbol matching (regime → strategy lookup)
  Stage 3: Quick backtest sampling (180-day, 3-5 configs per pair)
  Stage 4: Full backtest refinement (top candidates, full lookback)
  Stage 5: Forecast overlay and final ranking
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.backtests.strategies.registry import BEARISH_STRATEGIES
from backtestforecast.models import DailyRecommendation, NightlyPipelineRun
from backtestforecast.pipeline.regime import RegimeSnapshot, classify_regime
from backtestforecast.config import get_settings
from backtestforecast.observability.metrics import DUPLICATE_NIGHTLY_RUNS_TOTAL
from backtestforecast.pipeline.strategy_map import (
    DEFAULT_PARAM_GRID,
    strategies_for_regime,
)

logger = structlog.get_logger("pipeline")


# ---------------------------------------------------------------------------
# Internal data structures for pipeline state
# ---------------------------------------------------------------------------


@dataclass
class SymbolStrategyPair:
    symbol: str
    strategy_type: str
    regime: RegimeSnapshot
    close_price: float


@dataclass
class QuickBacktestResult:
    symbol: str
    strategy_type: str
    regime: RegimeSnapshot
    close_price: float
    target_dte: int
    config_snapshot: dict[str, Any]
    trade_count: int
    win_rate: float
    total_roi_pct: float
    net_pnl: float
    max_drawdown_pct: float
    score: float = 0.0


@dataclass
class FullBacktestResult:
    symbol: str
    strategy_type: str
    regime: RegimeSnapshot
    close_price: float
    target_dte: int
    config_snapshot: dict[str, Any]
    summary: dict[str, Any]
    trades_json: list[dict[str, Any]]
    equity_curve_json: list[dict[str, Any]]
    forecast_json: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0


# ---------------------------------------------------------------------------
# Pipeline service
# ---------------------------------------------------------------------------


class NightlyPipelineService:
    """Orchestrates the nightly scan pipeline.

    Requires two injected services:
      - A market_data_fetcher that can return daily bars for any symbol
      - A backtest_executor that can run a quick or full backtest
    """

    def __init__(
        self,
        session: Session,
        *,
        market_data_fetcher: Any,
        backtest_executor: Any,
        forecaster: Any | None = None,
    ) -> None:
        self.session = session
        self.market_data = market_data_fetcher
        self.executor = backtest_executor
        self.forecaster = forecaster

    def run_pipeline(
        self,
        trade_date: date,
        symbols: list[str],
        *,
        max_full_candidates: int = 200,
        max_recommendations: int = 20,
    ) -> NightlyPipelineRun:
        """Execute the full 5-stage pipeline and persist results."""
        started_at = time.monotonic()

        # Prevent duplicate runs for the same trade_date (retry safety)
        existing = self.session.scalar(
            select(NightlyPipelineRun).where(
                NightlyPipelineRun.trade_date == trade_date,
                NightlyPipelineRun.status.in_(["succeeded", "running"]),
            ).with_for_update()
        )
        if existing is not None:
            DUPLICATE_NIGHTLY_RUNS_TOTAL.inc()
            logger.info("pipeline.already_exists", run_id=str(existing.id), status=existing.status)
            return existing

        from sqlalchemy import or_

        stale_cutoff = datetime.now(UTC) - timedelta(hours=1)
        stale = list(self.session.scalars(
            select(NightlyPipelineRun).where(
                NightlyPipelineRun.trade_date == trade_date,
                NightlyPipelineRun.status.in_(["running", "queued"]),
                or_(
                    NightlyPipelineRun.started_at.is_(None),
                    NightlyPipelineRun.started_at < stale_cutoff,
                ),
            ).with_for_update()
        ))
        for s in stale:
            s.status = "failed"
            s.error_message = "Superseded by retry"
            s.completed_at = datetime.now(UTC)
        if stale:
            self.session.commit()

        run = NightlyPipelineRun(
            trade_date=trade_date,
            status="running",
            stage="universe_screen",
            started_at=datetime.now(UTC),
        )
        self.session.add(run)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            succeeded = self.session.scalar(
                select(NightlyPipelineRun).where(
                    NightlyPipelineRun.trade_date == trade_date,
                    NightlyPipelineRun.status == "succeeded",
                )
            )
            if succeeded is not None:
                DUPLICATE_NIGHTLY_RUNS_TOTAL.inc()
                logger.info("pipeline.already_exists_on_conflict", run_id=str(succeeded.id))
                return succeeded
            raise
        self.session.refresh(run)
        _run_id = run.id

        max_workers = max(1, get_settings().pipeline_max_workers)
        try:
            # Stage 1: Universe screening
            run.stage = "universe_screen"
            run.symbols_screened = len(symbols)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                regime_snapshots = self._stage1_screen(symbols, trade_date, executor=executor)
            run.symbols_after_screen = len(regime_snapshots)
            logger.info(
                "pipeline.stage1_complete",
                run_id=str(run.id),
                screened=len(symbols),
                passed=len(regime_snapshots),
            )

            # Stage 2: Strategy matching
            run.stage = "strategy_match"

            pairs = self._stage2_match(regime_snapshots)
            run.pairs_generated = len(pairs)
            self.session.flush()
            logger.info(
                "pipeline.stage2_complete",
                run_id=str(run.id),
                pairs=len(pairs),
            )

            # Stage 3: Quick backtests
            run.stage = "quick_backtest"

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                quick_results = self._stage3_quick_backtest(pairs, trade_date, executor=executor)
            run.quick_backtests_run = len(quick_results)

            if not quick_results and pairs:
                logger.warning(
                    "pipeline.survivorship_bias_all_filtered",
                    run_id=str(run.id),
                    pairs_evaluated=len(pairs),
                    msg="All quick-backtest candidates scored <= 0 and were filtered. "
                        "Results may reflect survivorship bias.",
                )

            quick_results.sort(key=lambda r: (-r.score, r.symbol, r.strategy_type))
            top_candidates = quick_results[:max_full_candidates]
            logger.info(
                "pipeline.stage3_complete",
                run_id=str(run.id),
                backtests_run=len(quick_results),
                top_candidates=len(top_candidates),
            )

            # Stage 4: Full backtests
            run.stage = "full_backtest"

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                full_results = self._stage4_full_backtest(top_candidates, trade_date, executor=executor)
            run.full_backtests_run = len(full_results)
            self.session.flush()
            logger.info(
                "pipeline.stage4_complete",
                run_id=str(run.id),
                full_backtests=len(full_results),
            )

            # Stage 5: Forecast + ranking
            run.stage = "forecast_rank"

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                final_ranked = self._stage5_forecast_and_rank(full_results, trade_date, executor=executor)
            final_ranked = final_ranked[:max_recommendations]

            # Persist recommendations
            for rank, result in enumerate(final_ranked, start=1):
                rec = DailyRecommendation(
                    pipeline_run_id=run.id,
                    trade_date=trade_date,
                    rank=rank,
                    score=Decimal(str(round(result.score, 6))),
                    symbol=result.symbol,
                    strategy_type=result.strategy_type,
                    regime_labels=sorted(r.value for r in result.regime.regimes),
                    close_price=Decimal(str(round(result.close_price, 4))),
                    target_dte=result.target_dte,
                    config_snapshot_json=result.config_snapshot,
                    summary_json=result.summary,
                    forecast_json=result.forecast_json,
                )
                self.session.add(rec)

            run.recommendations_produced = len(final_ranked)
            run.status = "succeeded"
            run.completed_at = datetime.now(UTC)
            run.duration_seconds = Decimal(str(round(time.monotonic() - started_at, 2)))
            self.session.commit()

            logger.info(
                "pipeline.complete",
                run_id=str(run.id),
                recommendations=len(final_ranked),
                duration_seconds=float(run.duration_seconds),
            )

        except Exception:  # Intentional broad catch: the pipeline must mark its DB row as
            # "failed" regardless of what went wrong, so that the scheduler doesn't
            # re-attempt and operators can see the failure in the dashboard.
            failing_stage = run.stage
            counters = {
                "symbols_screened": run.symbols_screened,
                "symbols_after_screen": run.symbols_after_screen,
                "pairs_generated": run.pairs_generated,
                "quick_backtests_run": run.quick_backtests_run,
                "full_backtests_run": run.full_backtests_run,
                "recommendations_produced": run.recommendations_produced,
            }
            self.session.rollback()
            run = self.session.get(NightlyPipelineRun, _run_id)
            if run is None:
                logger.error("pipeline.failure_handler_missing_run", run_id=str(_run_id))
                raise
            run.stage = failing_stage
            for attr, value in counters.items():
                setattr(run, attr, value)
            run.status = "failed"
            run.error_message = "Pipeline execution failed. See logs for details."
            run.completed_at = datetime.now(UTC)
            run.duration_seconds = Decimal(str(round(time.monotonic() - started_at, 2)))
            try:
                self.session.commit()
            except Exception:  # Intentional: if the failure-recording commit itself fails,
                # we still want to log and re-raise the original exception.
                self.session.rollback()
            logger.exception("pipeline.failed", run_id=str(run.id))
            raise

        self.session.refresh(run)
        return run

    # -------------------------------------------------------------------
    # Stage 1: Universe Screening
    # -------------------------------------------------------------------

    def _stage1_screen(
        self,
        symbols: list[str],
        trade_date: date,
        *,
        executor: ThreadPoolExecutor,
    ) -> list[RegimeSnapshot]:
        """Fetch bars and classify regime for each symbol.
        Skip symbols with insufficient data.

        Thread-safety: ``self.market_data`` wraps an ``httpx.Client`` which is
        thread-safe for concurrent reads.  No per-thread client is needed.
        """
        lookback_start = trade_date - timedelta(days=400)
        earnings_start = trade_date - timedelta(days=10)
        earnings_end = trade_date + timedelta(days=10)

        def _screen_one(symbol: str) -> RegimeSnapshot | None:
            try:
                bars = self.market_data.get_daily_bars(symbol, lookback_start, trade_date)
                earnings_dates = self.market_data.get_earnings_dates(
                    symbol, earnings_start, earnings_end,
                )
                return classify_regime(symbol, bars, earnings_dates=earnings_dates)
            except Exception as exc:  # Intentional: individual symbol screening failures
                # (API timeouts, bad data) must not abort the entire universe screen.
                logger.debug("pipeline.stage1_skip", symbol=symbol, error=str(exc))
                return None

        results: list[RegimeSnapshot] = []
        futures = {executor.submit(_screen_one, s): s for s in symbols}
        collected: set[int] = set()
        try:
            for future in as_completed(futures, timeout=300):
                collected.add(id(future))
                try:
                    snapshot = future.result(timeout=300)
                except Exception:
                    logger.warning("pipeline.screen_task_failed", exc_info=True)
                    continue
                if snapshot is not None:
                    results.append(snapshot)
        except TimeoutError:
            logger.warning("pipeline.stage1_timeout", completed=len(results), total=len(symbols))
            for f in futures:
                if id(f) in collected:
                    continue
                if f.done() and not f.cancelled():
                    try:
                        snapshot = f.result(timeout=0)
                        if snapshot is not None:
                            results.append(snapshot)
                    except Exception:
                        pass

        return results

    # -------------------------------------------------------------------
    # Stage 2: Strategy Matching
    # -------------------------------------------------------------------

    def _stage2_match(
        self,
        snapshots: list[RegimeSnapshot],
    ) -> list[SymbolStrategyPair]:
        """Map each regime snapshot to compatible strategies."""
        pairs: list[SymbolStrategyPair] = []
        for snapshot in snapshots:
            strategies = strategies_for_regime(snapshot.regimes)
            for strategy_type in strategies:
                pairs.append(
                    SymbolStrategyPair(
                        symbol=snapshot.symbol,
                        strategy_type=strategy_type,
                        regime=snapshot,
                        close_price=snapshot.close_price,
                    )
                )
        return pairs

    # -------------------------------------------------------------------
    # Stage 3: Quick Backtest Sampling
    # -------------------------------------------------------------------

    # 180 days minimum: shorter windows suffer from survivorship bias and
    # produce unreliable win-rate / drawdown estimates because they capture
    # too few regime transitions and trade samples for statistical validity.
    _QUICK_BACKTEST_DAYS = 180

    def _stage3_quick_backtest(
        self,
        pairs: list[SymbolStrategyPair],
        trade_date: date,
        *,
        executor: ThreadPoolExecutor,
    ) -> list[QuickBacktestResult]:
        """Run short-lookback backtests with a small parameter grid."""
        lookback_start = trade_date - timedelta(days=self._QUICK_BACKTEST_DAYS)

        work_items = [
            (pair, param_config)
            for pair in pairs
            for param_config in DEFAULT_PARAM_GRID
        ]

        def _run_one(item: tuple[SymbolStrategyPair, dict[str, Any]]) -> QuickBacktestResult | None:
            pair, param_config = item
            try:
                target_dte = param_config.get("target_dte", 30)
                overrides = param_config.get("strategy_overrides")

                summary = self.executor.run_quick_backtest(
                    symbol=pair.symbol,
                    strategy_type=pair.strategy_type,
                    start_date=lookback_start,
                    end_date=trade_date,
                    target_dte=target_dte,
                    strategy_overrides=overrides,
                )

                if summary is None or summary.get("trade_count", 0) == 0:
                    return None

                roi = summary.get("total_roi_pct", 0.0)
                raw_win_rate = summary.get("win_rate", 0.0)
                win_rate = raw_win_rate / 100.0
                win_rate = max(0.0, min(win_rate, 1.0))
                drawdown = min(summary.get("max_drawdown_pct", 50.0), 100.0)
                trade_count = summary.get("trade_count", 1)
                sample_factor = min(trade_count / 10.0, 1.0)
                score = roi * win_rate * (1.0 - drawdown / 100.0) * sample_factor
                if drawdown >= 100.0:
                    score = min(score, 0.0)

                return QuickBacktestResult(
                    symbol=pair.symbol,
                    strategy_type=pair.strategy_type,
                    regime=pair.regime,
                    close_price=pair.close_price,
                    target_dte=target_dte,
                    config_snapshot={
                        "target_dte": target_dte,
                        "strategy_overrides": overrides,
                    },
                    trade_count=summary["trade_count"],
                    win_rate=summary["win_rate"],
                    total_roi_pct=roi,
                    net_pnl=summary.get("total_net_pnl", 0.0),
                    max_drawdown_pct=drawdown,
                    score=score,
                )

            except Exception as exc:  # Intentional: individual quick-backtest failures must
                # not prevent other candidates from being evaluated.
                logger.warning(
                    "pipeline.stage3_skip",
                    symbol=pair.symbol,
                    strategy=pair.strategy_type,
                    error=str(exc),
                )
                return None

        results: list[QuickBacktestResult] = []
        futures = {executor.submit(_run_one, item): item for item in work_items}
        collected: set[int] = set()
        try:
            for future in as_completed(futures, timeout=300):
                collected.add(id(future))
                try:
                    result = future.result(timeout=300)
                except Exception:
                    logger.warning("pipeline.quick_backtest_task_failed", exc_info=True)
                    continue
                if result is not None:
                    results.append(result)
        except TimeoutError:
            logger.warning("pipeline.stage3_timeout", completed=len(results), total=len(work_items))
            for f in futures:
                if id(f) in collected:
                    continue
                if f.done() and not f.cancelled():
                    try:
                        result = f.result(timeout=0)
                        if result is not None:
                            results.append(result)
                    except Exception:
                        pass

        return results

    # -------------------------------------------------------------------
    # Stage 4: Full Backtest Refinement
    # -------------------------------------------------------------------

    def _stage4_full_backtest(
        self,
        candidates: list[QuickBacktestResult],
        trade_date: date,
        *,
        executor: ThreadPoolExecutor,
    ) -> list[FullBacktestResult]:
        """Run full-lookback backtests on the top candidates."""
        lookback_start = trade_date - timedelta(days=365)

        def _run_one(candidate: QuickBacktestResult) -> FullBacktestResult | None:
            try:
                full = self.executor.run_full_backtest(
                    symbol=candidate.symbol,
                    strategy_type=candidate.strategy_type,
                    start_date=lookback_start,
                    end_date=trade_date,
                    target_dte=candidate.target_dte,
                    strategy_overrides=candidate.config_snapshot.get("strategy_overrides"),
                )

                if full is None or full.get("trade_count", 0) == 0:
                    return None

                roi = full.get("total_roi_pct", 0.0)
                raw_win_rate = full.get("win_rate", 0.0)
                win_rate = raw_win_rate / 100.0
                win_rate = max(0.0, min(win_rate, 1.0))
                drawdown = min(full.get("max_drawdown_pct", 50.0), 100.0)
                trade_count = full.get("trade_count", 1)
                sample_factor = min(trade_count / 10.0, 1.0)
                score = roi * win_rate * (1.0 - drawdown / 100.0) * sample_factor
                if drawdown >= 100.0:
                    score = min(score, 0.0)

                return FullBacktestResult(
                    symbol=candidate.symbol,
                    strategy_type=candidate.strategy_type,
                    regime=candidate.regime,
                    close_price=candidate.close_price,
                    target_dte=candidate.target_dte,
                    config_snapshot=candidate.config_snapshot,
                    summary=full,
                    trades_json=full.get("trades", []),
                    equity_curve_json=full.get("equity_curve", []),
                    score=score,
                )

            except Exception as exc:  # Intentional: individual full-backtest failures must
                # not prevent other candidates from being refined.
                logger.warning(
                    "pipeline.stage4_skip",
                    symbol=candidate.symbol,
                    strategy=candidate.strategy_type,
                    error=str(exc),
                )
                return None

        results: list[FullBacktestResult] = []
        futures = {executor.submit(_run_one, c): c for c in candidates}
        collected: set[int] = set()
        try:
            for future in as_completed(futures, timeout=300):
                collected.add(id(future))
                try:
                    result = future.result(timeout=300)
                except Exception:
                    logger.warning("pipeline.full_backtest_task_failed", exc_info=True)
                    continue
                if result is not None:
                    results.append(result)
        except TimeoutError:
            logger.warning("pipeline.stage4_timeout", completed=len(results), total=len(candidates))
            for f in futures:
                if id(f) in collected:
                    continue
                if f.done() and not f.cancelled():
                    try:
                        result = f.result(timeout=0)
                        if result is not None:
                            results.append(result)
                    except Exception:
                        pass

        return results

    # -------------------------------------------------------------------
    # Stage 5: Forecast Overlay + Final Ranking
    # -------------------------------------------------------------------

    def _stage5_forecast_and_rank(
        self,
        candidates: list[FullBacktestResult],
        trade_date: date,
        *,
        executor: ThreadPoolExecutor,
    ) -> list[FullBacktestResult]:
        """Overlay forecast data and produce the final ranked list."""
        if self.forecaster is not None:
            def _fetch_forecast(candidate: FullBacktestResult) -> tuple[FullBacktestResult, dict[str, Any] | None]:
                try:
                    forecast = self.forecaster.get_forecast(
                        symbol=candidate.symbol,
                        strategy_type=candidate.strategy_type,
                        horizon_days=candidate.target_dte,
                        as_of_date=trade_date,
                    )
                    return candidate, forecast
                except Exception as exc:
                    logger.debug(
                        "pipeline.stage5_forecast_skip",
                        symbol=candidate.symbol,
                        error=str(exc),
                    )
                    return candidate, None

            futures = {executor.submit(_fetch_forecast, c): c for c in candidates}
            collected: set[int] = set()

            def _apply_forecast(candidate: FullBacktestResult, forecast: dict[str, Any]) -> None:
                candidate.forecast_json = forecast
                median_return = forecast.get("expected_return_median_pct", 0)
                positive_rate = forecast.get("positive_outcome_rate_pct", 50)
                backtest_roi = candidate.summary.get("total_roi_pct", 0)
                forecast_supports = float(median_return) > 0
                if candidate.strategy_type in BEARISH_STRATEGIES:
                    forecast_supports = float(median_return) < 0
                adjusted_score = candidate.score
                if backtest_roi > 0 and float(median_return) != 0 and forecast_supports:
                    adjusted_score *= 1.2
                effective_rate = float(positive_rate)
                if candidate.strategy_type in BEARISH_STRATEGIES:
                    effective_rate = 100.0 - effective_rate
                if effective_rate > 60:
                    adjusted_score *= 1.0 + (effective_rate - 60) / 200.0
                candidate.score = adjusted_score

            try:
                for future in as_completed(futures, timeout=300):
                    collected.add(id(future))
                    try:
                        candidate, forecast = future.result(timeout=300)
                    except Exception:
                        logger.warning("pipeline.forecast_task_failed", exc_info=True)
                        continue
                    if forecast:
                        _apply_forecast(candidate, forecast)
            except TimeoutError:
                logger.warning("pipeline.stage5_timeout", completed=len(collected), total=len(futures))
                for f in futures:
                    if id(f) in collected:
                        continue
                    if f.done() and not f.cancelled():
                        try:
                            candidate, forecast = f.result(timeout=0)
                            if forecast:
                                _apply_forecast(candidate, forecast)
                        except Exception:
                            pass

        # Final sort
        candidates.sort(key=lambda r: (-r.score, r.symbol, r.strategy_type))

        # Deduplicate: max 1 recommendation per symbol
        seen_symbols: set[str] = set()
        deduped: list[FullBacktestResult] = []
        for candidate in candidates:
            if candidate.symbol not in seen_symbols:
                seen_symbols.add(candidate.symbol)
                deduped.append(candidate)

        return deduped
