"""Single-symbol deep analysis service.

Performs an exhaustive analysis of one symbol:
  1. Regime analysis (full indicator snapshot)
  2. Strategy landscape (all strategies × dense param grid → quick backtests)
  3. Top-10 deep dive (full backtests with trades + equity curve)
  4. Forecast overlay on winning configurations
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.backtests.strategies.registry import BEARISH_STRATEGIES, STRATEGY_REGISTRY
from backtestforecast.errors import ConfigurationError, ConflictError, DataUnavailableError, NotFoundError, QuotaExceededError
from backtestforecast.schemas.json_shapes import (
    _REGIME_REQUIRED_KEYS,
    validate_json_shape,
)
from backtestforecast.models import SymbolAnalysis, User
from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository
from backtestforecast.pipeline.regime import classify_regime

logger = structlog.get_logger("deep_analysis")

_STAGE_ORDER = ["pending", "regime", "landscape", "deep_dive", "forecast"]


def _validate_stage_transition(current: str, target: str) -> bool:
    try:
        return _STAGE_ORDER.index(target) >= _STAGE_ORDER.index(current)
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Dense parameter grid for exhaustive single-symbol scanning
# ---------------------------------------------------------------------------

DEEP_PARAM_GRID: list[dict[str, Any]] = []

_DTES = [21, 30, 45]
_DELTA_TARGETS = [
    None,  # default (nearest OTM)
    {"mode": "delta_target", "value": 16},
    {"mode": "delta_target", "value": 30},
    {"mode": "delta_target", "value": 45},
]
_WIDTHS = [
    None,  # default (1 strike)
    {"mode": "strike_steps", "value": 2},
    {"mode": "dollar_width", "value": 5},
]

for _dte in _DTES:
    for _delta in _DELTA_TARGETS:
        for _width in _WIDTHS:
            overrides: dict[str, Any] | None = None
            if _delta or _width:
                overrides = {}
                if _delta:
                    overrides["short_call_strike"] = _delta
                    overrides["short_put_strike"] = _delta
                if _width:
                    overrides["spread_width"] = _width
            DEEP_PARAM_GRID.append(
                {
                    "target_dte": _dte,
                    "strategy_overrides": overrides,
                }
            )


_DEEP_PARAM_GRID_MAX = 500
if len(DEEP_PARAM_GRID) > _DEEP_PARAM_GRID_MAX:
    logger.warning(
        "deep_analysis.param_grid_large",
        size=len(DEEP_PARAM_GRID),
        max_recommended=_DEEP_PARAM_GRID_MAX,
        msg="Large parameter grid will increase deep analysis runtime significantly.",
    )

# Strategies to skip in landscape (custom strategies need user-defined legs)
_SKIP_STRATEGIES = {
    "custom_2_leg",
    "custom_3_leg",
    "custom_4_leg",
    "custom_5_leg",
    "custom_6_leg",
    "custom_8_leg",
    "wheel_strategy",  # multi-cycle, own execution path
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class LandscapeCell:
    """One cell in the strategy × config grid."""

    strategy_type: str
    strategy_label: str
    target_dte: int
    config_snapshot: dict[str, Any]
    trade_count: int = 0
    win_rate: float = 0.0
    total_roi_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    score: float = 0.0
    stable_order: int = 0


@dataclass
class TopResult:
    """A fully backtested top candidate."""

    rank: int
    strategy_type: str
    strategy_label: str
    target_dte: int
    config_snapshot: dict[str, Any]
    summary: dict[str, Any]
    trades: list[dict[str, Any]]
    equity_curve: list[dict[str, Any]]
    forecast: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SymbolDeepAnalysisService:
    """Run an exhaustive single-symbol analysis."""

    def __init__(
        self,
        session: Session,
        *,
        market_data_fetcher: Any,
        backtest_executor: Any,
        forecaster: Any | None = None,
    ) -> None:
        self.session = session
        self._market_data = market_data_fetcher
        self._executor = backtest_executor
        self.forecaster = forecaster
        self._repo = SymbolAnalysisRepository(session)

    @property
    def market_data(self) -> Any:
        if self._market_data is None:
            raise ConfigurationError("market_data_fetcher is required for analysis execution")
        return self._market_data

    @property
    def executor(self) -> Any:
        if self._executor is None:
            raise ConfigurationError("backtest_executor is required for analysis execution")
        return self._executor

    def create_analysis(
        self,
        user: User,
        symbol: str,
        *,
        idempotency_key: str | None = None,
    ) -> SymbolAnalysis:
        """Create a queued analysis record. Caller dispatches to Celery."""
        if idempotency_key:
            existing = self._repo.get_by_idempotency_key(user.id, idempotency_key)
            if existing is not None:
                return existing

        self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        )
        from backtestforecast.billing.entitlements import resolve_feature_policy
        from backtestforecast.config import get_settings
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        _settings = get_settings()
        max_concurrent = (
            _settings.max_concurrent_analyses_premium
            if policy.tier.value == "premium"
            else _settings.max_concurrent_analyses_default
        )

        active_count = self.session.scalar(
            select(func.count()).select_from(SymbolAnalysis).where(
                SymbolAnalysis.user_id == user.id,
                SymbolAnalysis.status.in_(["queued", "running"]),
            )
        )
        if active_count is not None and active_count >= max_concurrent:
            raise QuotaExceededError(
                f"You already have {active_count} analyses in progress (limit: {max_concurrent}). "
                "Please wait for them to complete."
            )

        analysis = SymbolAnalysis(
            user_id=user.id,
            symbol=symbol.strip().upper(),
            status="queued",
            stage="pending",
            idempotency_key=idempotency_key,
        )
        self.session.add(analysis)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            if idempotency_key:
                stmt = select(SymbolAnalysis).where(
                    SymbolAnalysis.user_id == user.id,
                    SymbolAnalysis.idempotency_key == idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        self.session.refresh(analysis)
        return analysis

    def execute_analysis(self, analysis_id: UUID) -> SymbolAnalysis:
        """Execute the deep analysis (called by Celery worker)."""
        analysis = self.session.scalar(
            select(SymbolAnalysis).where(SymbolAnalysis.id == analysis_id).with_for_update()
        )
        if analysis is None:
            raise NotFoundError("Symbol analysis not found.")
        if analysis.status not in ("queued", "running"):
            logger.info("deep_analysis.execute_skipped", analysis_id=str(analysis_id), status=analysis.status)
            return analysis

        # Re-entitlement check: verify user still has access before executing
        user = self.session.get(User, analysis.user_id)
        if user is None:
            analysis.status = "failed"
            analysis.error_code = "entitlement_revoked"
            analysis.error_message = "User account not found."
            self.session.commit()
            return analysis
        from backtestforecast.billing.entitlements import resolve_feature_policy
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        if not policy.forecasting_access:
            analysis.status = "failed"
            analysis.error_code = "entitlement_revoked"
            analysis.error_message = "Your plan no longer supports this operation."
            self.session.commit()
            return analysis

        from sqlalchemy import func as sa_func
        concurrent = self.session.scalar(
            select(sa_func.count()).select_from(SymbolAnalysis).where(
                SymbolAnalysis.user_id == analysis.user_id,
                SymbolAnalysis.status == "running",
                SymbolAnalysis.id != analysis.id,
            )
        ) or 0
        from backtestforecast.config import get_settings as _get_settings
        _settings = _get_settings()
        _max_concurrent = (
            _settings.max_concurrent_analyses_premium
            if policy.tier.value == "premium"
            else _settings.max_concurrent_analyses_default
        )
        if concurrent >= _max_concurrent:
            analysis.status = "failed"
            analysis.error_code = "concurrent_limit"
            analysis.error_message = (
                f"Too many analyses running concurrently ({concurrent}, limit: {_max_concurrent}). "
                "Wait for existing analyses to complete."
            )
            self.session.commit()
            return analysis

        started_at = time.monotonic()
        analysis.status = "running"
        if not _validate_stage_transition(analysis.stage, "regime"):
            logger.warning("deep_analysis.backward_stage_transition", current=analysis.stage, target="regime", analysis_id=str(analysis_id))
        analysis.stage = "regime"
        analysis.started_at = datetime.now(UTC)
        self.session.commit()

        try:
            symbol = analysis.symbol
            from backtestforecast.utils.dates import market_date_today
            trade_date = market_date_today()

            # --- Stage 1: Regime analysis ---
            bars = self.market_data.get_daily_bars(
                symbol,
                trade_date - timedelta(days=400),
                trade_date,
            )
            earnings_dates = self.market_data.get_earnings_dates(
                symbol,
                trade_date - timedelta(days=10),
                trade_date + timedelta(days=10),
            )
            regime = classify_regime(symbol, bars, earnings_dates=earnings_dates)
            if regime is None:
                raise DataUnavailableError(f"Insufficient data to classify regime for {symbol}.")

            analysis.close_price = Decimal(str(round(regime.close_price, 4)))
            regime_dict: dict[str, Any] = {
                "regimes": sorted(r.value for r in regime.regimes),
                "rsi_14": regime.rsi_14,
                "ema_8": regime.ema_8,
                "ema_21": regime.ema_21,
                "sma_50": regime.sma_50,
                "sma_200": regime.sma_200,
                "realized_vol_20": regime.realized_vol_20,
                "iv_rank_proxy": regime.iv_rank_proxy,
                "volume_ratio": regime.volume_ratio,
                "close_price": regime.close_price,
            }
            validate_json_shape(regime_dict, "SymbolAnalysis.regime_json", required_keys=_REGIME_REQUIRED_KEYS)
            analysis.regime_json = regime_dict
            if not _validate_stage_transition(analysis.stage, "landscape"):
                logger.warning("deep_analysis.backward_stage_transition", current=analysis.stage, target="landscape", analysis_id=str(analysis_id))
            analysis.stage = "landscape"
            self.session.commit()
            logger.info("deep_analysis.regime_done", analysis_id=str(analysis_id), symbol=symbol)

            # --- Stage 2: Strategy landscape ---
            landscape = self._build_landscape(symbol, trade_date)
            analysis.landscape_json = [
                {
                    "strategy_type": cell.strategy_type,
                    "strategy_label": cell.strategy_label,
                    "target_dte": cell.target_dte,
                    "config": cell.config_snapshot,
                    "trade_count": cell.trade_count,
                    "win_rate": cell.win_rate,
                    "total_roi_pct": cell.total_roi_pct,
                    "max_drawdown_pct": cell.max_drawdown_pct,
                    "score": round(cell.score, 4),
                }
                for cell in landscape
            ]
            analysis.strategies_tested = len({c.strategy_type for c in landscape})
            analysis.configs_tested = len(landscape)
            if not _validate_stage_transition(analysis.stage, "deep_dive"):
                logger.warning("deep_analysis.backward_stage_transition", current=analysis.stage, target="deep_dive", analysis_id=str(analysis_id))
            analysis.stage = "deep_dive"
            self.session.commit()
            logger.info(
                "deep_analysis.landscape_done",
                analysis_id=str(analysis_id),
                cells=len(landscape),
            )

            # --- Stage 3: Top-10 deep dive ---
            landscape.sort(key=lambda c: (-c.score, c.strategy_type, c.target_dte))
            # Deduplicate: best config per strategy, then top 10
            seen_strategies: set[str] = set()
            top_candidates: list[LandscapeCell] = []
            for cell in landscape:
                if cell.strategy_type not in seen_strategies and cell.score > 0:
                    seen_strategies.add(cell.strategy_type)
                    top_candidates.append(cell)
                if len(top_candidates) >= 10:
                    break

            top_results = self._deep_dive(symbol, trade_date, top_candidates)
            analysis.top_results_json = [
                {
                    "rank": r.rank,
                    "strategy_type": r.strategy_type,
                    "strategy_label": r.strategy_label,
                    "target_dte": r.target_dte,
                    "config": r.config_snapshot,
                    "summary": r.summary,
                    "trades": r.trades,
                    "equity_curve": r.equity_curve,
                    "forecast": r.forecast,
                    "score": round(r.score, 4),
                }
                for r in top_results
            ]
            analysis.top_results_count = len(top_results)
            if not _validate_stage_transition(analysis.stage, "forecast"):
                logger.warning("deep_analysis.backward_stage_transition", current=analysis.stage, target="forecast", analysis_id=str(analysis_id))
            analysis.stage = "forecast"
            self.session.commit()

            # --- Stage 4: Forecast on best result ---
            if top_results and top_results[0].forecast:
                analysis.forecast_json = top_results[0].forecast

            from sqlalchemy import update as sa_update
            success_values: dict[str, Any] = {
                "status": "succeeded",
                "completed_at": datetime.now(UTC),
                "duration_seconds": Decimal(str(round(time.monotonic() - started_at, 2))),
                "updated_at": datetime.now(UTC),
            }
            if not top_results:
                forecast_json = {
                    **(analysis.forecast_json or {}),
                    "no_results_message": "Analysis completed but no profitable strategy configurations were found for this symbol and date range.",
                }
                success_values["forecast_json"] = forecast_json

            success_rows = self.session.execute(
                sa_update(SymbolAnalysis)
                .where(SymbolAnalysis.id == analysis.id, SymbolAnalysis.status == "running")
                .values(**success_values)
            )
            self.session.commit()
            if success_rows.rowcount == 0:
                logger.warning("analysis.success_overwrite_prevented", analysis_id=str(analysis.id))

            logger.info(
                "deep_analysis.complete",
                analysis_id=str(analysis_id),
                symbol=symbol,
                top_results=len(top_results),
                duration=float(success_values["duration_seconds"]),
            )

        except Exception:
            failing_stage = analysis.stage
            counters = {
                "strategies_tested": analysis.strategies_tested,
                "configs_tested": analysis.configs_tested,
                "top_results_count": analysis.top_results_count,
            }
            self.session.rollback()
            analysis = self.session.get(SymbolAnalysis, analysis_id)
            if analysis is not None:
                analysis.stage = failing_stage
                for attr, value in counters.items():
                    setattr(analysis, attr, value)
                analysis.error_code = "analysis_execution_failed"
                analysis.status = "failed"
                analysis.error_message = "Analysis failed. Please try again."
                analysis.completed_at = datetime.now(UTC)
                analysis.duration_seconds = Decimal(str(round(time.monotonic() - started_at, 2)))
                try:
                    self.session.commit()
                except Exception:
                    self.session.rollback()
            logger.exception("deep_analysis.failed", analysis_id=str(analysis_id))
            raise

        self.session.refresh(analysis)
        return analysis

    def get_analysis(self, user: User, analysis_id: UUID) -> SymbolAnalysis:
        analysis = self.session.scalar(
            select(SymbolAnalysis).where(
                SymbolAnalysis.id == analysis_id,
                SymbolAnalysis.user_id == user.id,
            )
        )
        if analysis is None:
            raise NotFoundError("Symbol analysis not found.")
        return analysis

    def list_for_user(
        self,
        user: User,
        limit: int = 20,
        offset: int = 0,
        cursor_before: datetime | None = None,
    ) -> list[SymbolAnalysis]:
        from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository

        repo = SymbolAnalysisRepository(self.session)
        return repo.list_for_user(
            user.id, limit=limit, offset=offset, cursor_before=cursor_before,
        )

    def count_for_user(self, user: User) -> int:
        from sqlalchemy import func as sa_func
        stmt = select(sa_func.count(SymbolAnalysis.id)).where(SymbolAnalysis.user_id == user.id)
        return int(self.session.scalar(stmt) or 0)

    def delete_for_user(self, analysis_id: UUID, user_id: UUID) -> None:
        """Delete an analysis. Raises ConflictError if the analysis is queued or running."""
        analysis = self._repo.get_for_user(analysis_id, user_id)
        if analysis is None:
            raise NotFoundError("Symbol analysis not found.")
        if analysis.status in ("queued", "running"):
            raise ConflictError(
                "Cannot delete an analysis that is currently queued or running. Cancel it first."
            )
        self.session.delete(analysis)
        self.session.commit()

    # -------------------------------------------------------------------
    # Internal stages
    # -------------------------------------------------------------------

    def _build_landscape(self, symbol: str, trade_date: date) -> list[LandscapeCell]:
        """Test all strategies × dense param grid with 90-day quick backtests."""
        lookback_start = trade_date - timedelta(days=90)
        strategy_types = [st for st in STRATEGY_REGISTRY.keys() if st not in _SKIP_STRATEGIES]

        work_items: list[tuple[str, str, dict[str, Any]]] = []
        for strategy_type in strategy_types:
            label = _strategy_label(strategy_type)
            for param_config in DEEP_PARAM_GRID:
                work_items.append((strategy_type, label, param_config))

        cells: list[LandscapeCell] = []

        def _run_cell(item: tuple[str, str, dict[str, Any]]) -> LandscapeCell | None:
            strategy_type, label, param_config = item
            structlog.contextvars.bind_contextvars(
                symbol=symbol,
                stage="landscape",
            )
            try:
                target_dte = param_config["target_dte"]
                overrides = param_config.get("strategy_overrides")
                summary = self.executor.run_quick_backtest(
                    symbol=symbol,
                    strategy_type=strategy_type,
                    start_date=lookback_start,
                    end_date=trade_date,
                    target_dte=target_dte,
                    strategy_overrides=overrides,
                )
                if summary is None:
                    return None
                trade_count = summary.get("trade_count", 0)
                win_rate = summary.get("win_rate", 0.0)
                roi = summary.get("total_roi_pct", 0.0)
                drawdown = min(summary.get("max_drawdown_pct", 50.0), 100.0)
                score = 0.0
                if trade_count > 0:
                    sample_factor = min(trade_count / 10.0, 1.0)
                    score = roi * (win_rate / 100.0) * (1.0 - drawdown / 100.0) * sample_factor
                    if drawdown >= 100.0:
                        score = min(score, 0.0)
                return LandscapeCell(
                    strategy_type=strategy_type,
                    strategy_label=label,
                    target_dte=target_dte,
                    config_snapshot={"target_dte": target_dte, "strategy_overrides": overrides},
                    trade_count=trade_count,
                    win_rate=win_rate,
                    total_roi_pct=roi,
                    max_drawdown_pct=drawdown,
                    score=score,
                )
            except Exception:
                logger.warning(
                    "deep_analysis.landscape_cell_failed",
                    strategy_type=strategy_type,
                    exc_info=True,
                )
                return None

        max_workers = max(1, min(8, len(work_items)))
        landscape_timeout = max(300, min(len(work_items) * 2, 900))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            collected_futures: set = set()
            futures = {pool.submit(_run_cell, item): item for item in work_items}
            try:
                for future in as_completed(futures, timeout=landscape_timeout):
                    collected_futures.add(future)
                    try:
                        cell = future.result(timeout=300)
                    except Exception:
                        logger.warning("deep_analysis.landscape_future_failed", exc_info=True)
                        continue
                    if cell is not None:
                        cells.append(cell)
            except TimeoutError:
                logger.warning("deep_analysis.landscape_timeout", total_items=len(work_items), collected=len(cells))
                for f in futures:
                    if f not in collected_futures and f.done() and not f.cancelled():
                        try:
                            cell = f.result(timeout=0)
                            if cell is not None:
                                cells.append(cell)
                        except Exception:
                            pass

        return cells

    def _deep_dive(
        self,
        symbol: str,
        trade_date: date,
        candidates: list[LandscapeCell],
    ) -> list[TopResult]:
        """Full 1-year backtests on the top candidates (parallelized)."""
        lookback_start = trade_date - timedelta(days=365)

        def _run_candidate(cell: LandscapeCell) -> tuple[LandscapeCell, dict[str, Any] | None]:
            structlog.contextvars.bind_contextvars(
                symbol=symbol,
                stage="deep_dive",
            )
            try:
                return cell, self.executor.run_full_backtest(
                    symbol=symbol,
                    strategy_type=cell.strategy_type,
                    start_date=lookback_start,
                    end_date=trade_date,
                    target_dte=cell.target_dte,
                    strategy_overrides=cell.config_snapshot.get("strategy_overrides"),
                )
            except Exception:
                logger.warning(
                    "deep_analysis.deep_dive_candidate_failed",
                    strategy_type=cell.strategy_type,
                    exc_info=True,
                )
                return cell, None

        max_workers = max(1, min(4, len(candidates)))
        backtest_results: list[tuple[LandscapeCell, dict[str, Any] | None]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            collected_futures: set = set()
            futures = {pool.submit(_run_candidate, c): c for c in candidates}
            try:
                for future in as_completed(futures, timeout=300):
                    collected_futures.add(future)
                    try:
                        backtest_results.append(future.result(timeout=300))
                    except Exception:
                        logger.warning("deep_analysis.deep_dive_future_failed", exc_info=True)
            except TimeoutError:
                logger.warning("deep_analysis.deep_dive_timeout", total_candidates=len(candidates), collected=len(backtest_results))
                for f in futures:
                    if f not in collected_futures and f.done() and not f.cancelled():
                        try:
                            backtest_results.append(f.result(timeout=0))
                        except Exception:
                            pass

        for idx, c in enumerate(candidates):
            c.stable_order = idx
        backtest_results.sort(key=lambda pair: pair[0].stable_order)

        results: list[TopResult] = []
        for rank_idx, (cell, full) in enumerate(backtest_results, start=1):
            if full is None or full.get("trade_count", 0) == 0:
                continue

            roi = full.get("total_roi_pct", 0.0)
            win_rate = full.get("win_rate", 0.0) / 100.0
            drawdown = min(full.get("max_drawdown_pct", 50.0), 100.0)
            trade_count = full.get("trade_count", 1)
            sample_factor = min(trade_count / 10.0, 1.0)
            score = roi * win_rate * (1.0 - drawdown / 100.0) * sample_factor
            if drawdown >= 100.0:
                score = min(score, 0.0)

            forecast: dict[str, Any] = {}
            if self.forecaster:
                try:
                    f = self.forecaster.get_forecast(
                        symbol=symbol,
                        strategy_type=cell.strategy_type,
                        horizon_days=cell.target_dte,
                        as_of_date=trade_date,
                    )
                    if f:
                        forecast = f
                        median_return = f.get("expected_return_median_pct", 0)
                        forecast_supports = float(median_return) > 0
                        if cell.strategy_type in BEARISH_STRATEGIES:
                            forecast_supports = float(median_return) < 0
                        if roi > 0 and float(median_return) != 0 and forecast_supports:
                            score *= 1.2
                        positive_rate = f.get("positive_outcome_rate_pct") or 50
                        effective_rate = float(positive_rate)
                        if cell.strategy_type in BEARISH_STRATEGIES:
                            effective_rate = 100.0 - effective_rate
                        if effective_rate > 60:
                            score *= 1.0 + (effective_rate - 60) / 200.0
                except Exception:
                    logger.warning(
                        "deep_analysis.candidate_forecast_failed",
                        strategy_type=cell.strategy_type,
                        exc_info=True,
                    )

            results.append(
                TopResult(
                    rank=rank_idx,
                    strategy_type=cell.strategy_type,
                    strategy_label=cell.strategy_label,
                    target_dte=cell.target_dte,
                    config_snapshot=cell.config_snapshot,
                    summary=full,
                    trades=full.get("trades", [])[:50],
                    equity_curve=full.get("equity_curve", []),
                    forecast=forecast,
                    score=score,
                )
            )

        results.sort(key=lambda r: (-r.score, r.strategy_type, r.target_dte))
        for i, r in enumerate(results, start=1):
            r.rank = i
        return results


def _strategy_label(strategy_type: str) -> str:
    """Human-readable label for a strategy type."""
    return strategy_type.replace("_", " ").title()
