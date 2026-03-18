from __future__ import annotations

import time as _time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

import structlog
from sqlalchemy.orm import Session

from backtestforecast.config import get_settings
from backtestforecast.errors import AppError, NotFoundError, ValidationError
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import SweepJob, SweepResult, User
from backtestforecast.repositories.sweep_jobs import SweepJobRepository
from backtestforecast.schemas.backtests import (
    BacktestSummaryResponse,
    CreateBacktestRunRequest,
    EquityCurvePointResponse,
    SpreadWidthConfig,
    SpreadWidthMode,
    StrikeSelection,
    StrikeSelectionMode,
    StrategyOverrides,
)
from backtestforecast.schemas.sweeps import (
    CreateSweepRequest,
    SweepJobListResponse,
    SweepJobResponse,
    SweepResultListResponse,
    SweepResultResponse,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.backtests import to_decimal
from backtestforecast.services.serialization import (
    downsample_equity_curve,
    serialize_equity_point,
    serialize_summary,
    serialize_trade,
)

logger = structlog.get_logger("services.sweeps")

_CANDIDATE_TIMEOUT_SECONDS = 120
_SWEEP_TIMEOUT_SECONDS = 3600
_MAX_EQUITY_POINTS = 500

_SWEEP_SCORE_WIN_RATE_WEIGHT = 0.25
_SWEEP_SCORE_ROI_WEIGHT = 0.30
_SWEEP_SCORE_SHARPE_WEIGHT = 0.25
_SWEEP_SCORE_DRAWDOWN_WEIGHT = 0.20
_SWEEP_SCORE_SHARPE_MULTIPLIER = 10.0
_SWEEP_SCORE_MIN_TRADES = 3


class SweepService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self.repository = SweepJobRepository(session)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- public API ----------------------------------------------------------

    def create_job(self, user: User, payload: CreateSweepRequest) -> SweepJob:
        if payload.idempotency_key:
            existing = self.repository.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing is not None:
                return existing

        candidate_count = self._compute_candidate_count(payload)
        if candidate_count == 0:
            raise ValidationError("The sweep grid produces zero candidates.")

        job = SweepJob(
            user_id=user.id,
            symbol=payload.symbol,
            status="queued",
            candidate_count=candidate_count,
            request_snapshot_json=payload.model_dump(mode="json"),
            idempotency_key=payload.idempotency_key,
        )
        self.repository.add(job)
        self.session.commit()
        self.session.refresh(job)
        return job

    def run_job(self, job_id: UUID) -> SweepJob:
        job = self.repository.get(job_id, for_update=True)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        if job.status not in ("queued", "running"):
            logger.warning("sweep.run_job_skip", job_id=str(job_id), status=job.status)
            return job

        job.status = "running"
        job.started_at = datetime.now(UTC)
        self.session.commit()

        try:
            payload = CreateSweepRequest.model_validate(job.request_snapshot_json)
            from backtestforecast.schemas.sweeps import SweepMode
            if payload.mode == SweepMode.GENETIC:
                self._execute_genetic(job, payload)
            else:
                self._execute_sweep(job, payload)
            self.session.commit()
            self.session.refresh(job)
            return job
        except Exception:
            self.session.rollback()
            try:
                job = self.repository.get(job_id, for_update=True)
                if job is not None and job.status == "running":
                    job.status = "failed"
                    job.error_code = "sweep_execution_error"
                    job.error_message = "The sweep failed with an unexpected error."
                    job.completed_at = datetime.now(UTC)
                    self.session.commit()
            except Exception:
                logger.exception("sweep.run_job_failed.recovery_failed", job_id=str(job_id))
                self.session.rollback()
            raise

    def list_jobs(self, user: User, limit: int = 50, offset: int = 0) -> SweepJobListResponse:
        jobs = self.repository.list_for_user(user.id, limit=limit, offset=offset)
        total = self.repository.count_for_user(user.id)
        return SweepJobListResponse(
            items=[self._to_job_response(j) for j in jobs],
            total=total,
            offset=offset,
            limit=limit,
        )

    def get_job(self, user: User, job_id: UUID) -> SweepJobResponse:
        job = self.repository.get_for_user(job_id, user.id)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        return self._to_job_response(job)

    def get_results(
        self, user: User, job_id: UUID, *, limit: int = 100, offset: int = 0,
    ) -> SweepResultListResponse:
        job = self.repository.get_for_user(job_id, user.id, include_results=False)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        from sqlalchemy import select
        stmt = (
            select(SweepResult)
            .where(SweepResult.sweep_job_id == job.id)
            .order_by(SweepResult.rank)
            .offset(offset)
            .limit(limit)
        )
        results = list(self.session.scalars(stmt))
        return SweepResultListResponse(
            items=[self._to_result_response(r) for r in results]
        )

    # -- execution -----------------------------------------------------------

    def _execute_sweep(self, job: SweepJob, payload: CreateSweepRequest) -> None:
        warnings: list[dict[str, Any]] = []

        # Phase 1: prepare bundle and prefetch
        representative = CreateBacktestRunRequest(
            symbol=payload.symbol,
            strategy_type=payload.strategy_types[0],
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else [],
        )
        bundle = self.execution_service.market_data_service.prepare_backtest(representative)
        prefetcher = OptionDataPrefetcher()
        prefetch_summary = prefetcher.prefetch_for_symbol(
            symbol=payload.symbol,
            bars=bundle.bars,
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            option_gateway=bundle.option_gateway,
        )
        job.prefetch_summary_json = prefetch_summary.to_dict()

        # Phase 2: execute grid
        candidates: list[dict[str, Any]] = []
        sweep_start = _time.monotonic()
        timed_out = False

        delta_values = [item.value for item in payload.delta_grid] if payload.delta_grid else [None]
        width_values = [(item.mode, item.value) for item in payload.width_grid] if payload.width_grid else [None]
        exit_sets = payload.exit_rule_sets if payload.exit_rule_sets else [None]

        for strategy_type in payload.strategy_types:
            if timed_out:
                break
            for entry_rule_set in payload.entry_rule_sets:
                if timed_out:
                    break
                for delta_val in delta_values:
                    if timed_out:
                        break
                    for width_val in width_values:
                        if timed_out:
                            break
                        for exit_set in exit_sets:
                            if timed_out:
                                break

                            elapsed = _time.monotonic() - sweep_start
                            if elapsed > _SWEEP_TIMEOUT_SECONDS - _CANDIDATE_TIMEOUT_SECONDS:
                                timed_out = True
                                warnings.append({
                                    "code": "timeout",
                                    "message": "Sweep time limit approaching; remaining candidates were skipped.",
                                })
                                break

                            overrides = self._build_overrides(delta_val, width_val)
                            request = CreateBacktestRunRequest(
                                symbol=payload.symbol,
                                strategy_type=strategy_type,
                                start_date=payload.start_date,
                                end_date=payload.end_date,
                                target_dte=payload.target_dte,
                                dte_tolerance_days=payload.dte_tolerance_days,
                                max_holding_days=payload.max_holding_days,
                                account_size=payload.account_size,
                                risk_per_trade_pct=payload.risk_per_trade_pct,
                                commission_per_contract=payload.commission_per_contract,
                                entry_rules=entry_rule_set.entry_rules,
                                slippage_pct=payload.slippage_pct,
                                profit_target_pct=exit_set.profit_target_pct if exit_set else None,
                                stop_loss_pct=exit_set.stop_loss_pct if exit_set else None,
                                strategy_overrides=overrides,
                            )

                            try:
                                result = self.execution_service.execute_request(request, bundle=bundle)
                                candidates.append(self._build_candidate(
                                    result=result,
                                    strategy_type=strategy_type.value,
                                    delta_val=delta_val,
                                    width_val=width_val,
                                    entry_rule_set_name=entry_rule_set.name,
                                    exit_set=exit_set,
                                ))
                                job.evaluated_candidate_count += 1
                            except AppError as exc:
                                warnings.append({
                                    "code": "candidate_failed",
                                    "message": (
                                        f"{strategy_type.value} / delta={delta_val} / "
                                        f"{entry_rule_set.name}: {exc.code}"
                                    ),
                                    "error_code": exc.code,
                                })
                            except Exception:
                                logger.warning(
                                    "sweep.candidate_failed",
                                    strategy=strategy_type.value,
                                    delta=delta_val,
                                    exc_info=True,
                                )
                                warnings.append({
                                    "code": "candidate_failed_internal",
                                    "message": f"{strategy_type.value} / delta={delta_val} / {entry_rule_set.name} failed",
                                })

        # Phase 3: rank and store
        if not candidates:
            job.status = "succeeded"
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
            job.result_count = 0
            return

        sorted_candidates = sorted(candidates, key=self._score_candidate, reverse=True)
        selected = sorted_candidates[:payload.max_results]

        for idx, candidate in enumerate(selected, 1):
            job.results.append(
                SweepResult(
                    rank=idx,
                    score=Decimal(str(round(candidate["score"], 6))),
                    strategy_type=candidate["strategy_type"],
                    parameter_snapshot_json=candidate["parameters"],
                    summary_json=candidate["summary"],
                    warnings_json=candidate.get("warnings", []),
                    trades_json=candidate["trades"],
                    equity_curve_json=candidate["equity_curve"],
                )
            )

        job.result_count = len(selected)
        job.status = "succeeded"
        job.completed_at = datetime.now(UTC)
        job.warnings_json = warnings

    # -- genetic mode --------------------------------------------------------

    def _execute_genetic(self, job: SweepJob, payload: CreateSweepRequest) -> None:
        from backtestforecast.schemas.backtests import (
            CUSTOM_LEG_COUNT,
            CustomLegDefinition,
            StrategyType,
        )
        from backtestforecast.sweeps.constraints import Individual
        from backtestforecast.sweeps.genetic import GAResult, GeneticConfig, GeneticOptimizer

        gc = payload.genetic_config
        if gc is None:
            raise ValidationError("genetic_config is required for genetic mode.")

        num_legs = gc.num_legs
        leg_count_map = {v: k for k, v in CUSTOM_LEG_COUNT.items()}
        strategy_type = leg_count_map.get(num_legs)
        if strategy_type is None:
            raise ValidationError(f"No custom strategy type for {num_legs} legs.")

        warnings: list[dict[str, Any]] = []

        representative = CreateBacktestRunRequest(
            symbol=payload.symbol,
            strategy_type=strategy_type,
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else [],
            custom_legs=[
                CustomLegDefinition(contract_type="call", side="long", strike_offset=0)
                for _ in range(num_legs)
            ],
        )
        bundle = self.execution_service.market_data_service.prepare_backtest(representative)
        prefetcher = OptionDataPrefetcher()
        prefetch_summary = prefetcher.prefetch_for_symbol(
            symbol=payload.symbol,
            bars=bundle.bars,
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            option_gateway=bundle.option_gateway,
        )
        job.prefetch_summary_json = prefetch_summary.to_dict()

        entry_rules = payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else []
        exit_set = payload.exit_rule_sets[0] if payload.exit_rule_sets else None
        exec_service = self.execution_service

        def fitness_fn(individual: Individual) -> float:
            legs = [
                CustomLegDefinition(
                    asset_type=leg.get("asset_type", "option"),
                    contract_type=leg.get("contract_type"),
                    side=leg["side"],
                    strike_offset=leg.get("strike_offset", 0),
                    expiration_offset=leg.get("expiration_offset", 0),
                    quantity_ratio=leg.get("quantity_ratio", Decimal("1")),
                )
                for leg in individual
            ]
            request = CreateBacktestRunRequest(
                symbol=payload.symbol,
                strategy_type=strategy_type,
                start_date=payload.start_date,
                end_date=payload.end_date,
                target_dte=payload.target_dte,
                dte_tolerance_days=payload.dte_tolerance_days,
                max_holding_days=payload.max_holding_days,
                account_size=payload.account_size,
                risk_per_trade_pct=payload.risk_per_trade_pct,
                commission_per_contract=payload.commission_per_contract,
                entry_rules=entry_rules,
                slippage_pct=payload.slippage_pct,
                profit_target_pct=exit_set.profit_target_pct if exit_set else None,
                stop_loss_pct=exit_set.stop_loss_pct if exit_set else None,
                custom_legs=legs,
            )
            try:
                result = exec_service.execute_request(request, bundle=bundle)
                summary = self._serialize_summary(result.summary)
                return self._score_candidate_from_summary(summary)
            except Exception:
                return 0.0

        ga_config = GeneticConfig(
            num_legs=gc.num_legs,
            population_size=gc.population_size,
            max_generations=gc.max_generations,
            tournament_size=gc.tournament_size,
            crossover_rate=gc.crossover_rate,
            mutation_rate=gc.mutation_rate,
            elitism_count=gc.elitism_count,
            max_workers=gc.max_workers,
            max_stale_generations=gc.max_stale_generations,
        )
        optimizer = GeneticOptimizer(ga_config)
        ga_result = optimizer.run(fitness_fn)

        job.evaluated_candidate_count = ga_result.total_evaluations

        max_results = min(payload.max_results, len(ga_result.top_individuals))
        for rank_idx, (ind, fit_val) in enumerate(ga_result.top_individuals[:max_results], 1):
            legs = [
                CustomLegDefinition(
                    asset_type=leg.get("asset_type", "option"),
                    contract_type=leg.get("contract_type"),
                    side=leg["side"],
                    strike_offset=leg.get("strike_offset", 0),
                    expiration_offset=leg.get("expiration_offset", 0),
                    quantity_ratio=leg.get("quantity_ratio", Decimal("1")),
                )
                for leg in ind
            ]
            request = CreateBacktestRunRequest(
                symbol=payload.symbol,
                strategy_type=strategy_type,
                start_date=payload.start_date,
                end_date=payload.end_date,
                target_dte=payload.target_dte,
                dte_tolerance_days=payload.dte_tolerance_days,
                max_holding_days=payload.max_holding_days,
                account_size=payload.account_size,
                risk_per_trade_pct=payload.risk_per_trade_pct,
                commission_per_contract=payload.commission_per_contract,
                entry_rules=entry_rules,
                slippage_pct=payload.slippage_pct,
                profit_target_pct=exit_set.profit_target_pct if exit_set else None,
                stop_loss_pct=exit_set.stop_loss_pct if exit_set else None,
                custom_legs=legs,
            )
            try:
                result = exec_service.execute_request(request, bundle=bundle)
            except Exception:
                logger.debug("ga.result_backtest_failed", rank=rank_idx, exc_info=True)
                continue

            summary = self._serialize_summary(result.summary)
            trades = [self._serialize_trade(t) for t in result.trades[:50]]
            equity_curve = self._downsample_equity_curve(result.equity_curve)

            parameters: dict[str, Any] = {
                "strategy_type": strategy_type.value,
                "mode": "genetic",
                "num_legs": num_legs,
                "generations_run": ga_result.generations_run,
                "total_evaluations": ga_result.total_evaluations,
                "custom_legs": [leg.model_dump(mode="json") for leg in legs],
                "entry_rule_set_name": payload.entry_rule_sets[0].name if payload.entry_rule_sets else None,
            }
            if exit_set is not None:
                parameters["exit_rule_set_name"] = exit_set.name
                parameters["profit_target_pct"] = exit_set.profit_target_pct
                parameters["stop_loss_pct"] = exit_set.stop_loss_pct

            job.results.append(
                SweepResult(
                    rank=rank_idx,
                    score=Decimal(str(round(fit_val, 6))),
                    strategy_type=strategy_type.value,
                    parameter_snapshot_json=parameters,
                    summary_json=summary,
                    warnings_json=result.warnings or [],
                    trades_json=trades,
                    equity_curve_json=equity_curve,
                )
            )

        job.result_count = len(job.results)
        job.status = "succeeded"
        job.completed_at = datetime.now(UTC)
        job.warnings_json = warnings

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _compute_candidate_count(payload: CreateSweepRequest) -> int:
        from backtestforecast.schemas.sweeps import SweepMode
        if payload.mode == SweepMode.GENETIC and payload.genetic_config:
            return payload.genetic_config.population_size * payload.genetic_config.max_generations
        strategies = len(payload.strategy_types)
        entry_sets = len(payload.entry_rule_sets)
        deltas = max(len(payload.delta_grid), 1)
        widths = max(len(payload.width_grid), 1)
        exits = max(len(payload.exit_rule_sets), 1)
        return strategies * entry_sets * deltas * widths * exits

    @staticmethod
    def _build_overrides(
        delta_val: int | None,
        width_val: tuple[SpreadWidthMode, Decimal] | None,
    ) -> StrategyOverrides | None:
        if delta_val is None and width_val is None:
            return None

        strike_sel = None
        if delta_val is not None:
            strike_sel = StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_val)),
            )

        spread_width = None
        if width_val is not None:
            from backtestforecast.schemas.backtests import SpreadWidthMode
            spread_width = SpreadWidthConfig(
                mode=SpreadWidthMode(width_val[0]),
                value=width_val[1],
            )

        return StrategyOverrides(
            short_put_strike=strike_sel,
            short_call_strike=strike_sel,
            spread_width=spread_width,
        )

    def _build_candidate(
        self,
        result,
        strategy_type: str,
        delta_val: int | None,
        width_val: tuple[str, Decimal] | None,
        entry_rule_set_name: str,
        exit_set,
    ) -> dict[str, Any]:
        summary = self._serialize_summary(result.summary)
        score = self._score_candidate_from_summary(summary)
        parameters: dict[str, Any] = {
            "strategy_type": strategy_type,
            "delta": delta_val,
            "entry_rule_set_name": entry_rule_set_name,
        }
        if width_val is not None:
            parameters["width_mode"] = width_val[0]
            parameters["width_value"] = float(width_val[1])
        if exit_set is not None:
            parameters["exit_rule_set_name"] = exit_set.name
            parameters["profit_target_pct"] = exit_set.profit_target_pct
            parameters["stop_loss_pct"] = exit_set.stop_loss_pct

        trades = [self._serialize_trade(t) for t in result.trades[:50]]
        equity_curve = self._downsample_equity_curve(result.equity_curve)

        return {
            "score": score,
            "strategy_type": strategy_type,
            "parameters": parameters,
            "summary": summary,
            "warnings": result.warnings or [],
            "trades": trades,
            "equity_curve": equity_curve,
        }

    @staticmethod
    def _score_candidate(candidate: dict[str, Any]) -> float:
        return candidate.get("score", 0.0)

    @staticmethod
    def _score_candidate_from_summary(summary: dict[str, Any]) -> float:
        win_rate = float(summary.get("win_rate", 0))
        roi = float(summary.get("total_roi_pct", 0))
        drawdown = float(summary.get("max_drawdown_pct", 0))
        sharpe = float(summary.get("sharpe_ratio") or 0)
        trade_count = int(summary.get("trade_count", 0))

        if trade_count < _SWEEP_SCORE_MIN_TRADES:
            return 0.0

        score = (
            win_rate * _SWEEP_SCORE_WIN_RATE_WEIGHT
            + roi * _SWEEP_SCORE_ROI_WEIGHT
            + sharpe * _SWEEP_SCORE_SHARPE_MULTIPLIER * _SWEEP_SCORE_SHARPE_WEIGHT
            - drawdown * _SWEEP_SCORE_DRAWDOWN_WEIGHT
        )
        return score

    _serialize_summary = staticmethod(serialize_summary)
    _serialize_trade = staticmethod(serialize_trade)
    _serialize_equity_point = staticmethod(serialize_equity_point)

    @classmethod
    def _downsample_equity_curve(cls, equity_curve: list) -> list[dict[str, Any]]:
        return downsample_equity_curve(equity_curve, max_points=_MAX_EQUITY_POINTS)

    @staticmethod
    def _to_job_response(job: SweepJob) -> SweepJobResponse:
        return SweepJobResponse(
            id=job.id,
            status=job.status,
            symbol=job.symbol,
            candidate_count=job.candidate_count,
            evaluated_candidate_count=job.evaluated_candidate_count,
            result_count=job.result_count,
            prefetch_summary=job.prefetch_summary_json,
            warnings=job.warnings_json,
            error_code=job.error_code,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    @staticmethod
    def _to_result_response(result: SweepResult) -> SweepResultResponse:
        params = result.parameter_snapshot_json or {}
        return SweepResultResponse(
            id=result.id,
            rank=result.rank,
            score=result.score,
            strategy_type=result.strategy_type,
            delta=params.get("delta"),
            width_mode=params.get("width_mode"),
            width_value=params.get("width_value"),
            entry_rule_set_name=params.get("entry_rule_set_name", ""),
            exit_rule_set_name=params.get("exit_rule_set_name"),
            profit_target_pct=params.get("profit_target_pct"),
            stop_loss_pct=params.get("stop_loss_pct"),
            summary=BacktestSummaryResponse.model_validate(result.summary_json),
            warnings=result.warnings_json,
            trades_json=result.trades_json,
            equity_curve=[
                EquityCurvePointResponse.model_validate(item)
                for item in result.equity_curve_json
            ],
        )
