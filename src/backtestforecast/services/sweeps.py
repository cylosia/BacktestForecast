from __future__ import annotations

import contextlib
import time as _time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.config import get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.errors import AppError, AppValidationError, ConflictError, NotFoundError
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import SweepJob, SweepResult, User
from backtestforecast.observability.metrics import (
    SWEEP_CANDIDATE_FAILURES_TOTAL,
    SWEEP_EXECUTION_DURATION_SECONDS,
)
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.repositories.sweep_jobs import SweepJobRepository
from backtestforecast.schemas.backtests import (
    CreateBacktestRunRequest,
    SpreadWidthConfig,
    SpreadWidthMode,
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
)
from backtestforecast.schemas.json_shapes import _SUMMARY_REQUIRED_KEYS, validate_json_shape
from backtestforecast.schemas.sweeps import (
    CreateSweepRequest,
    SweepJobListResponse,
    SweepJobResponse,
    SweepJobStatusResponse,
    SweepResultListResponse,
    SweepResultResponse,
)
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtest_execution import (
    BacktestExecutionService,
    get_thread_local_shared_execution_service,
)
from backtestforecast.services.dispatch_recovery import (
    observe_job_create_to_running_latency,
    redispatch_if_stale_queued,
)
from backtestforecast.services.job_cancellation import (
    mark_job_cancelled,
    publish_cancellation_event,
    revoke_celery_task,
)
from backtestforecast.services.job_transitions import (
    cancellation_blocked_message,
    deletion_blocked_message,
    fail_job,
    fail_job_if_active,
    running_transition_values,
)
from backtestforecast.services.serialization import (
    downsample_equity_curve,
    safe_validate_summary as _safe_validate_summary,
    serialize_equity_point,
    serialize_summary,
    serialize_trade,
)
from backtestforecast.services.sweep_service_helpers import (
    score_candidate_from_summary,
    sweep_job_response,
    sweep_result_response,
)
from backtestforecast.services.sweep_service_helpers import (
    update_sweep_heartbeat as _update_heartbeat,
)

logger = structlog.get_logger("services.sweeps")
_SWEEP_QUEUE = "sweeps"

_CANDIDATE_TIMEOUT_SECONDS = 120
_MAX_EQUITY_POINTS = 500


@dataclass(frozen=True, slots=True)
class _SweepGridWorkItem:
    strategy_type: Any
    entry_rule_set_name: str
    entry_rules: list[Any]
    delta_val: int | None
    width_val: tuple[SpreadWidthMode, Decimal] | None
    exit_group: list[Any]

class SweepService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self._owns_execution_service = False
        self.repository = SweepJobRepository(session)
        self.audit = AuditService(session)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = get_thread_local_shared_execution_service()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None and self._owns_execution_service:
            self._execution_service.close()

    def __enter__(self) -> SweepService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- public API ----------------------------------------------------------

    @property
    def _max_concurrent_sweeps(self) -> int:
        return get_settings().max_concurrent_sweeps

    def create_job(self, user: User, payload: CreateSweepRequest) -> SweepJob:
        from backtestforecast.billing.entitlements import ensure_sweep_access
        ensure_sweep_access(
            user.plan_tier, user.subscription_status,
            user.subscription_current_period_end,
        )

        self._enforce_sweep_quota(user)

        if payload.idempotency_key:
            existing = self.repository.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing,
                    model_name="SweepJob",
                    task_name="sweeps.run",
                    task_kwargs={"job_id": str(existing.id)},
                    queue=_SWEEP_QUEUE,
                    log_event="sweep",
                    logger=logger,
                )

        recent = self.repository.find_recent_duplicate(
            user.id,
            payload.symbol,
            payload.model_dump(mode="json"),
            since=datetime.now(UTC) - timedelta(minutes=10),
        )
        if recent is not None:
            return redispatch_if_stale_queued(
                self.session,
                recent,
                model_name="SweepJob",
                task_name="sweeps.run",
                task_kwargs={"job_id": str(recent.id)},
                queue=_SWEEP_QUEUE,
                log_event="sweep",
                logger=logger,
            )

        candidate_count = self._compute_candidate_count(payload)
        if candidate_count == 0:
            raise AppValidationError("The sweep grid produces zero candidates.")

        _MAX_CANDIDATES = 50_000
        if candidate_count > _MAX_CANDIDATES:
            raise AppValidationError(
                f"Sweep grid produces {candidate_count:,} candidates, exceeding the {_MAX_CANDIDATES:,} limit. "
                "Reduce the number of parameter combinations."
            )

        snapshot = payload.model_dump(mode="json")
        import hashlib
        import json
        request_hash = hashlib.sha256(
            json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        job = SweepJob(
            user_id=user.id,
            symbol=payload.symbol,
            status="queued",
            plan_tier_snapshot=user.plan_tier,
            candidate_count=candidate_count,
            request_snapshot_json=snapshot,
            request_hash=request_hash,
            idempotency_key=payload.idempotency_key,
        )
        self.repository.add(job)
        self.audit.record(
            event_type="sweep.created",
            subject_type="sweep_job",
            subject_id=job.id,
            user_id=user.id,
            metadata={"symbol": job.symbol},
        )
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if payload.idempotency_key:
                from sqlalchemy import select
                stmt = select(SweepJob).where(
                    SweepJob.user_id == user.id,
                    SweepJob.idempotency_key == payload.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        return job

    def create_and_dispatch_job(
        self,
        user: User,
        payload: CreateSweepRequest,
        *,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> SweepJob:
        """Create a sweep job and persist dispatch state transactionally."""
        from apps.api.app.dispatch import dispatch_celery_task

        job = self.create_job(user, payload)
        dispatch_celery_task(
            db=self.session,
            job=job,
            task_name="sweeps.run",
            task_kwargs={"job_id": str(job.id)},
            queue=_SWEEP_QUEUE,
            log_event="sweep",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(job)
        return job

    def run_job(self, job_id: UUID) -> SweepJob:
        job = self.repository.get(job_id, for_update=True)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        if job.status not in ("queued", "running"):
            logger.warning("sweep.run_job_skip", job_id=str(job_id), status=job.status)
            return job

        user = self.session.get(User, job.user_id)
        if user is None:
            fail_job(job, error_code="user_not_found", error_message="User account not found.")
            self.session.commit()
            return job

        try:
            from backtestforecast.billing.entitlements import ensure_sweep_access
            ensure_sweep_access(
                user.plan_tier, user.subscription_status,
                user.subscription_current_period_end,
            )
        except AppError:
            fail_job(job, error_code="entitlement_revoked", error_message="Subscription no longer active.")
            self.session.commit()
            return job

        from sqlalchemy import update
        now = datetime.now(UTC)
        cas_result = self.session.execute(
            update(SweepJob)
            .where(SweepJob.id == job_id, SweepJob.status == "queued")
            .values(**running_transition_values(now=now))
        )
        self.session.commit()
        if cas_result.rowcount == 0:
            self.session.refresh(job)
            logger.warning("sweep.cas_transition_failed", job_id=str(job_id), status=job.status)
            return job
        self.session.refresh(job)
        observe_job_create_to_running_latency(job)

        _run_start = _time.monotonic()
        try:
            payload = CreateSweepRequest.model_validate(job.request_snapshot_json)
            from backtestforecast.schemas.sweeps import SweepMode

            self.repository.delete_results(job_id)
            self.session.commit()

            if payload.mode == SweepMode.GENETIC:
                self._execute_genetic(job, payload)
            else:
                self._execute_sweep(job, payload)
            self.session.commit()
            self.session.refresh(job)
            if job.status == "succeeded":
                self.audit.record_always(
                    event_type="sweep.completed",
                    subject_type="sweep_job",
                    subject_id=job.id,
                    user_id=job.user_id,
                    metadata={"symbol": job.symbol, "result_count": job.result_count},
                )
                self.session.commit()
            else:
                logger.warning(
                    "sweep.post_execution_status_mismatch",
                    job_id=str(job_id),
                    expected="succeeded",
                    actual=job.status,
                )
            return job
        except Exception:
            self.session.rollback()
            try:
                job = self.repository.get(job_id, for_update=True)
                if job is not None:
                    fail_job_if_active(
                        job,
                        error_code="sweep_execution_error",
                        error_message="The sweep failed with an unexpected error.",
                        active_statuses=frozenset({"running"}),
                    )
                    self.session.commit()
            except Exception:
                logger.exception("sweep.run_job_failed.recovery_failed", job_id=str(job_id))
                self.session.rollback()
            raise
        finally:
            SWEEP_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - _run_start)

    def list_jobs(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> SweepJobListResponse:
        effective_limit = min(limit, 200)
        cursor_before, offset = parse_cursor_param(cursor) if cursor else (None, offset)
        jobs, total = self.repository.list_for_user_with_count(
            user.id, limit=effective_limit + 1, offset=offset, cursor_before=cursor_before,
        )
        page = finalize_cursor_page(jobs, total=total, offset=offset, limit=effective_limit)
        return SweepJobListResponse(
            items=[self._to_job_response(j) for j in page.items],
            total=page.total,
            offset=page.offset,
            limit=page.limit,
            next_cursor=page.next_cursor,
        )

    def get_job(self, user: User, job_id: UUID) -> SweepJobResponse:
        job = self.repository.get_for_user(job_id, user.id)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        return self._to_job_response(job)

    def delete_for_user(self, job_id: UUID, user_id: UUID) -> None:
        job = self.repository.get_for_user(job_id, user_id)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        if job.status in ("queued", "running"):
            raise ConflictError(deletion_blocked_message("sweep job"))
        self.audit.record(
            event_type="sweep.deleted",
            subject_type="sweep_job",
            subject_id=job.id,
            user_id=user_id,
            metadata={"symbol": job.symbol, "mode": job.mode},
        )
        self.session.delete(job)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def cancel_for_user(self, job_id: UUID, user_id: UUID) -> SweepJobStatusResponse:
        job = self.repository.get_for_user(job_id, user_id)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        if job.status not in ("queued", "running"):
            raise ConflictError(cancellation_blocked_message("sweep job"))
        task_id = mark_job_cancelled(job)
        self.audit.record_always(
            event_type="sweep.cancelled",
            subject_type="sweep_job",
            subject_id=job.id,
            user_id=user_id,
            metadata={"symbol": job.symbol, "mode": job.mode, "reason": "user_cancelled"},
        )
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        revoke_celery_task(task_id, job_type="sweep", job_id=job.id)
        publish_cancellation_event(job_type="sweep", job_id=job.id)
        return SweepJobStatusResponse(
            id=job.id,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_code=job.error_code,
            error_message=job.error_message,
        )

    def get_results(
        self, user: User, job_id: UUID, *, limit: int = 100, offset: int = 0,
    ) -> SweepResultListResponse:
        job = self.repository.get_for_user(job_id, user.id, include_results=False)
        if job is None:
            raise NotFoundError("Sweep job not found.")
        total = self.repository.count_results(job.id)
        results = self.repository.list_results(job.id, limit=limit, offset=offset)
        return SweepResultListResponse(
            items=[self._to_result_response(r) for r in results],
            total=total,
            offset=offset,
            limit=limit,
        )

    def _enforce_sweep_quota(self, user: User) -> None:
        from sqlalchemy import func, select

        from backtestforecast.billing.entitlements import resolve_feature_policy
        from backtestforecast.errors import QuotaExceededError

        locked_user = self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        ).scalar_one_or_none()
        if locked_user is None:
            raise NotFoundError("User not found.")

        policy = resolve_feature_policy(
            locked_user.plan_tier, locked_user.subscription_status, locked_user.subscription_current_period_end,
        )
        quota = policy.monthly_sweep_quota
        if quota is not None and quota <= 0:
            raise QuotaExceededError(
                "Your plan does not include sweep access.",
                current_tier=policy.tier.value,
            )
        if quota is None:
            return

        now = datetime.now(UTC)
        month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        if now.month == 12:
            next_month = datetime(now.year + 1, 1, 1, tzinfo=UTC)
        else:
            next_month = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
        used = self.session.scalar(
            select(func.count()).select_from(SweepJob).where(
                SweepJob.user_id == user.id,
                SweepJob.created_at >= month_start,
                SweepJob.created_at < next_month,
                SweepJob.status.notin_(("failed", "cancelled")),
            )
        ) or 0
        if used >= quota:
            raise QuotaExceededError(
                f"Monthly sweep quota ({quota}) reached. Used: {used}.",
                current_tier=policy.tier.value,
            )

        concurrent = self.session.scalar(
            select(func.count()).select_from(SweepJob).where(
                SweepJob.user_id == user.id,
                SweepJob.status.in_(["queued", "running"]),
            )
        ) or 0
        if concurrent >= self._max_concurrent_sweeps:
            raise QuotaExceededError(
                f"Maximum concurrent sweeps ({self._max_concurrent_sweeps}) reached. "
                "Wait for existing sweeps to complete.",
                current_tier=policy.tier.value,
            )

    # -- execution -----------------------------------------------------------

    def _execute_sweep(self, job: SweepJob, payload: CreateSweepRequest) -> None:
        warnings: list[dict[str, Any]] = []

        # Phase 1: prepare one shared bundle and warm the shared gateway once
        bundle_stage_start = _time.monotonic()
        service_init_start = _time.monotonic()
        market_data_service = self.execution_service.market_data_service
        market_data_service_ms = round((_time.monotonic() - service_init_start) * 1000, 3)
        representative = self._make_bundle_request(payload)
        bundle_prepare_start = _time.monotonic()
        bundle = market_data_service.prepare_backtest(representative)
        bundle_prepare_ms = round((_time.monotonic() - bundle_prepare_start) * 1000, 3)
        prefetch_requests = self._make_prefetch_requests(payload)
        bundle_prefetch_start = _time.monotonic()
        prefetch_summary = self.execution_service.prefetch_requests_with_shared_bundle(
            prefetch_requests,
            bundle=bundle,
        )
        bundle_prefetch_ms = round((_time.monotonic() - bundle_prefetch_start) * 1000, 3)
        bundle_total_ms = round((_time.monotonic() - bundle_stage_start) * 1000, 3)
        job.prefetch_summary_json = {
            **prefetch_summary,
            "market_data_service_ms": market_data_service_ms,
            "bundle_prepare_ms": bundle_prepare_ms,
            "bundle_prefetch_ms": bundle_prefetch_ms,
            "bundle_total_ms": bundle_total_ms,
            "bundle_request_count": len(prefetch_requests),
        }
        logger.info(
            "sweep.shared_bundle_ready",
            job_id=str(job.id),
            symbol=payload.symbol,
            strategy_count=len(payload.strategy_types),
            bundle_request_count=len(prefetch_requests),
            market_data_service_ms=market_data_service_ms,
            bundle_prepare_ms=bundle_prepare_ms,
            bundle_prefetch_ms=bundle_prefetch_ms,
            bundle_total_ms=bundle_total_ms,
            prefetch_count=prefetch_summary.get("prefetch_count", 0),
            skipped_count=prefetch_summary.get("skipped_count", 0),
            dates_processed=prefetch_summary.get("dates_processed", 0),
            contracts_fetched=prefetch_summary.get("contracts_fetched", 0),
            quotes_fetched=prefetch_summary.get("quotes_fetched", 0),
        )
        resolved_parameters, risk_free_rate_curve = self.execution_service.resolve_execution_inputs(
            representative,
        )

        # Phase 2: execute grid
        candidates: list[dict[str, Any]] = []
        _MAX_CANDIDATES_IN_MEMORY = 2_000
        sweep_start = _time.monotonic()
        sweep_timeout = max(get_settings().sweep_timeout_seconds, _CANDIDATE_TIMEOUT_SECONDS * 2)
        timed_out = False
        local_evaluated_count = 0

        delta_values = [item.value for item in payload.delta_grid] if payload.delta_grid else [None]
        width_values = [(item.mode, item.value) for item in payload.width_grid] if payload.width_grid else [None]
        exit_sets = payload.exit_rule_sets if payload.exit_rule_sets else [None]
        grouped_exit_sets = (
            [exit_sets]
            if len(payload.exit_rule_sets) > 1
            else [[exit_set] for exit_set in exit_sets]
        )
        work_items = self._build_grid_work_items(
            payload,
            delta_values=delta_values,
            width_values=width_values,
            grouped_exit_sets=grouped_exit_sets,
        )
        parallel_workers = self._grid_parallel_worker_count(bundle, len(work_items))
        if parallel_workers > 1:
            local_evaluated_count, timed_out = self._execute_grid_work_items_parallel(
                job=job,
                payload=payload,
                work_items=work_items,
                bundle=bundle,
                resolved_parameters=resolved_parameters,
                risk_free_rate_curve=risk_free_rate_curve,
                candidates=candidates,
                warnings=warnings,
                sweep_start=sweep_start,
                sweep_timeout=sweep_timeout,
                max_candidates_in_memory=_MAX_CANDIDATES_IN_MEMORY,
                max_workers=parallel_workers,
            )
        else:
            local_evaluated_count, timed_out = self._execute_grid_work_items_serial(
                job=job,
                payload=payload,
                work_items=work_items,
                bundle=bundle,
                resolved_parameters=resolved_parameters,
                risk_free_rate_curve=risk_free_rate_curve,
                candidates=candidates,
                warnings=warnings,
                sweep_start=sweep_start,
                sweep_timeout=sweep_timeout,
                max_candidates_in_memory=_MAX_CANDIDATES_IN_MEMORY,
            )

        # Phase 3: rank and store
        job.evaluated_candidate_count = local_evaluated_count
        if not candidates:
            job.status = "failed"
            job.error_code = "sweep_empty"
            job.error_message = "No sweep combinations completed successfully."
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
            job.result_count = 0
            return

        sorted_candidates = sorted(candidates, key=self._score_candidate, reverse=True)
        selected = sorted_candidates[:payload.max_results]

        from sqlalchemy import update as sa_update
        with self.session.no_autoflush:
            for idx, candidate in enumerate(selected, 1):
                params = dict(candidate["parameters"])
                params["trade_count"] = candidate.get("trade_count", len(candidate.get("trades", [])))
                params["serialized_trade_count"] = candidate.get("serialized_trade_count", len(candidate.get("trades", [])))
                validate_json_shape(
                    candidate["summary"],
                    f"SweepResult[{idx}].summary_json",
                    required_keys=_SUMMARY_REQUIRED_KEYS,
                )
                self._persist_result(
                    job_id=job.id,
                    rank=idx,
                    score=candidate["score"],
                    strategy_type=candidate["strategy_type"],
                    parameter_snapshot_json=params,
                    summary_json=candidate["summary"],
                    warnings_json=candidate.get("warnings", []),
                    trades_json=candidate["trades"],
                    equity_curve_json=candidate["equity_curve"],
                )

            success_rows = self.session.execute(
                sa_update(SweepJob)
                .where(SweepJob.id == job.id, SweepJob.status == "running")
                .values(
                    status="succeeded",
                    result_count=len(selected),
                    completed_at=datetime.now(UTC),
                    warnings_json=warnings,
                    updated_at=datetime.now(UTC),
                )
            )
        if success_rows.rowcount == 0:
            self.session.rollback()
            logger.warning("sweep.success_overwrite_prevented", job_id=str(job.id))

    # -- genetic mode --------------------------------------------------------

    def _execute_genetic(self, job: SweepJob, payload: CreateSweepRequest) -> None:
        from backtestforecast.schemas.backtests import (
            CUSTOM_LEG_COUNT,
            CustomLegDefinition,
        )
        from backtestforecast.sweeps.genetic import (
            GeneticConfig,
            GeneticOptimizer,
            SerializableFitnessEvaluator,
        )

        gc = payload.genetic_config
        if gc is None:
            raise AppValidationError("genetic_config is required for genetic mode.")

        num_legs = gc.num_legs
        leg_count_map = {v: k for k, v in CUSTOM_LEG_COUNT.items()}
        strategy_type = leg_count_map.get(num_legs)
        if strategy_type is None:
            raise AppValidationError(f"No custom strategy type for {num_legs} legs.")

        warnings: list[dict[str, Any]] = []

        bundle_stage_start = _time.monotonic()
        service_init_start = _time.monotonic()
        market_data_service = self.execution_service.market_data_service
        market_data_service_ms = round((_time.monotonic() - service_init_start) * 1000, 3)
        representative = self._make_bundle_request(
            payload,
            strategy_type=strategy_type,
            custom_legs=[
                CustomLegDefinition(contract_type="call", side="long", strike_offset=0)
                for _ in range(num_legs)
            ],
        )
        bundle_prepare_start = _time.monotonic()
        bundle = market_data_service.prepare_backtest(representative)
        bundle_prepare_ms = round((_time.monotonic() - bundle_prepare_start) * 1000, 3)
        bundle_prefetch_start = _time.monotonic()
        prefetch_summary = self.execution_service.prefetch_requests_with_shared_bundle(
            [representative],
            bundle=bundle,
        )
        bundle_prefetch_ms = round((_time.monotonic() - bundle_prefetch_start) * 1000, 3)
        bundle_total_ms = round((_time.monotonic() - bundle_stage_start) * 1000, 3)
        job.prefetch_summary_json = {
            **prefetch_summary,
            "market_data_service_ms": market_data_service_ms,
            "bundle_prepare_ms": bundle_prepare_ms,
            "bundle_prefetch_ms": bundle_prefetch_ms,
            "bundle_total_ms": bundle_total_ms,
            "bundle_request_count": 1,
        }
        logger.info(
            "sweep.shared_bundle_ready",
            job_id=str(job.id),
            symbol=payload.symbol,
            strategy_count=1,
            bundle_request_count=1,
            market_data_service_ms=market_data_service_ms,
            bundle_prepare_ms=bundle_prepare_ms,
            bundle_prefetch_ms=bundle_prefetch_ms,
            bundle_total_ms=bundle_total_ms,
            prefetch_count=prefetch_summary.get("prefetch_count", 0),
            skipped_count=prefetch_summary.get("skipped_count", 0),
            dates_processed=prefetch_summary.get("dates_processed", 0),
            contracts_fetched=prefetch_summary.get("contracts_fetched", 0),
            quotes_fetched=prefetch_summary.get("quotes_fetched", 0),
        )

        genetic_start = _time.monotonic()
        safe_genetic_timeout = max(get_settings().sweep_genetic_timeout_seconds, _CANDIDATE_TIMEOUT_SECONDS * 2)

        fitness_fn = SerializableFitnessEvaluator(
            initializer_module="backtestforecast.services.sweep_genetic_runtime",
            initializer_name="init_sweep_genetic_runtime",
            evaluator_module="backtestforecast.services.sweep_genetic_runtime",
            evaluator_name="evaluate_sweep_individual",
            context={
                "payload": payload.model_dump(mode="json"),
                "strategy_type": strategy_type,
                "timeout_seconds": safe_genetic_timeout - _CANDIDATE_TIMEOUT_SECONDS,
                "score_summary": True,
            },
        )

        effective_max_workers = min(gc.max_workers, get_settings().pipeline_max_workers)
        ga_config = GeneticConfig(
            num_legs=gc.num_legs,
            population_size=gc.population_size,
            max_generations=gc.max_generations,
            tournament_size=gc.tournament_size,
            crossover_rate=gc.crossover_rate,
            mutation_rate=gc.mutation_rate,
            elitism_count=gc.elitism_count,
            max_workers=effective_max_workers,
            max_stale_generations=gc.max_stale_generations,
        )
        optimizer = GeneticOptimizer(ga_config)
        ga_result = optimizer.run(fitness_fn)

        _update_heartbeat(self.session, SweepJob, job.id)

        genetic_elapsed = _time.monotonic() - genetic_start
        genetic_limit = get_settings().sweep_genetic_timeout_seconds
        if genetic_elapsed > genetic_limit:
            logger.warning(
                "sweep.genetic_timeout_exceeded",
                elapsed_seconds=round(genetic_elapsed, 1),
                limit_seconds=genetic_limit,
            )
            warnings.append({
                "code": "genetic_timeout",
                "message": f"Genetic optimizer took {genetic_elapsed:.0f}s, exceeding the {genetic_limit}s limit.",
            })

        job.evaluated_candidate_count = ga_result.total_evaluations
        entry_rules = payload.entry_rule_sets[0].entry_rules if payload.entry_rule_sets else []
        exit_set = payload.exit_rule_sets[0] if payload.exit_rule_sets else None
        exec_service = self.execution_service

        max_results = min(payload.max_results, len(ga_result.top_individuals))
        actual_rank = 0
        for _enum_idx, (ind, fit_val) in enumerate(ga_result.top_individuals[:max_results]):
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
                logger.debug("ga.result_backtest_failed", rank=_enum_idx, exc_info=True)
                continue

            actual_rank += 1
            summary = self._serialize_summary(result.summary)
            trades = [self._serialize_trade(t) for t in result.trades[:50]]
            serialized_trade_count = len(trades)
            full_trade_count = max(int(summary.get("trade_count") or 0), serialized_trade_count)
            equity_curve = self._downsample_equity_curve(result.equity_curve)
            serialized_equity_point_count = len(equity_curve)
            full_equity_point_count = len(result.equity_curve)

            parameters: dict[str, Any] = {
                "strategy_type": strategy_type.value,
                "mode": "genetic",
                "num_legs": num_legs,
                "generations_run": ga_result.generations_run,
                "total_evaluations": ga_result.total_evaluations,
                "custom_legs": [leg.model_dump(mode="json") for leg in legs],
                "entry_rule_set_name": payload.entry_rule_sets[0].name if payload.entry_rule_sets else None,
                "trade_count": full_trade_count,
                "serialized_trade_count": serialized_trade_count,
                "equity_point_count": full_equity_point_count,
                "serialized_equity_point_count": serialized_equity_point_count,
            }
            if exit_set is not None:
                parameters["exit_rule_set_name"] = exit_set.name
                parameters["profit_target_pct"] = self._json_scalar(exit_set.profit_target_pct)
                parameters["stop_loss_pct"] = self._json_scalar(exit_set.stop_loss_pct)

            validate_json_shape(
                summary,
                f"GeneticSweepResult[{actual_rank}].summary_json",
                required_keys=_SUMMARY_REQUIRED_KEYS,
            )
            self._persist_result(
                job_id=job.id,
                rank=actual_rank,
                score=fit_val,
                strategy_type=strategy_type.value,
                parameter_snapshot_json=parameters,
                summary_json=summary,
                warnings_json=result.warnings or [],
                trades_json=trades,
                equity_curve_json=equity_curve,
            )

        if actual_rank == 0:
            job.result_count = 0
            job.status = "failed"
            job.error_code = "sweep_empty"
            job.error_message = "No sweep combinations completed successfully."
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
        else:
            from sqlalchemy import update as sa_update
            with self.session.no_autoflush:
                success_rows = self.session.execute(
                    sa_update(SweepJob)
                    .where(SweepJob.id == job.id, SweepJob.status == "running")
                    .values(
                        status="succeeded",
                        result_count=actual_rank,
                        completed_at=datetime.now(UTC),
                        warnings_json=warnings,
                        updated_at=datetime.now(UTC),
                    )
                )
            if success_rows.rowcount == 0:
                self.session.rollback()
                logger.warning("sweep.success_overwrite_prevented", job_id=str(job.id))

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
    def _combined_entry_rules(payload: CreateSweepRequest) -> list[Any]:
        combined: list[Any] = []
        for rule_set in payload.entry_rule_sets:
            combined.extend(rule_set.entry_rules)
        return combined

    def _build_grid_work_items(
        self,
        payload: CreateSweepRequest,
        *,
        delta_values: list[int | None],
        width_values: list[tuple[SpreadWidthMode, Decimal] | None],
        grouped_exit_sets: list[list[Any]],
    ) -> list[_SweepGridWorkItem]:
        work_items: list[_SweepGridWorkItem] = []
        for strategy_type in payload.strategy_types:
            for entry_rule_set in payload.entry_rule_sets:
                for delta_val in delta_values:
                    for width_val in width_values:
                        for exit_group in grouped_exit_sets:
                            work_items.append(
                                _SweepGridWorkItem(
                                    strategy_type=strategy_type,
                                    entry_rule_set_name=entry_rule_set.name,
                                    entry_rules=list(entry_rule_set.entry_rules),
                                    delta_val=delta_val,
                                    width_val=width_val,
                                    exit_group=list(exit_group),
                                )
                            )
        return work_items

    def _grid_parallel_worker_count(
        self,
        bundle: HistoricalDataBundle,
        work_item_count: int,
    ) -> int:
        if work_item_count < 6:
            return 1
        settings = get_settings()
        if isinstance(bundle.option_gateway, HistoricalOptionGateway):
            max_workers = min(settings.pipeline_max_workers, 2)
        else:
            max_workers = min(settings.pipeline_max_workers, 4)
        return max(1, min(work_item_count, max_workers))

    @staticmethod
    def _remaining_grid_timeout(
        *,
        sweep_start: float,
        sweep_timeout: float,
    ) -> float:
        return sweep_timeout - (_time.monotonic() - sweep_start) - _CANDIDATE_TIMEOUT_SECONDS

    def _execute_grid_work_items_serial(
        self,
        *,
        job: SweepJob,
        payload: CreateSweepRequest,
        work_items: list[_SweepGridWorkItem],
        bundle: HistoricalDataBundle,
        resolved_parameters: ResolvedExecutionParameters,
        risk_free_rate_curve: object | None,
        candidates: list[dict[str, Any]],
        warnings: list[dict[str, Any]],
        sweep_start: float,
        sweep_timeout: float,
        max_candidates_in_memory: int,
    ) -> tuple[int, bool]:
        local_evaluated_count = 0
        timed_out = False
        for work_item in work_items:
            if self._remaining_grid_timeout(sweep_start=sweep_start, sweep_timeout=sweep_timeout) <= 0:
                timed_out = True
                warnings.append({
                    "code": "timeout",
                    "message": "Sweep time limit approaching; remaining candidates were skipped.",
                })
                break
            try:
                completed_candidates = self._execute_grid_work_item(
                    payload=payload,
                    work_item=work_item,
                    bundle=bundle,
                    resolved_parameters=resolved_parameters,
                    risk_free_rate_curve=risk_free_rate_curve,
                    execution_service=self.execution_service,
                    clone_bundle=False,
                )
            except AppError as exc:
                SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason=exc.code).inc()
                warnings.append({
                    "code": "candidate_failed",
                    "message": (
                        f"{work_item.strategy_type.value} / delta={work_item.delta_val} / "
                        f"{work_item.entry_rule_set_name}: {exc.code}"
                    ),
                    "error_code": exc.code,
                })
                continue
            except Exception:
                SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason="internal").inc()
                logger.warning(
                    "sweep.candidate_failed",
                    strategy=work_item.strategy_type.value,
                    delta=work_item.delta_val,
                    exc_info=True,
                )
                warnings.append({
                    "code": "candidate_failed_internal",
                    "message": (
                        f"{work_item.strategy_type.value} / delta={work_item.delta_val} / "
                        f"{work_item.entry_rule_set_name} failed"
                    ),
                })
                continue
            local_evaluated_count, hit_cap = self._record_grid_candidates(
                job=job,
                payload=payload,
                candidates=candidates,
                completed_candidates=completed_candidates,
                local_evaluated_count=local_evaluated_count,
                max_candidates_in_memory=max_candidates_in_memory,
            )
            if hit_cap:
                timed_out = True
                warnings.append({
                    "code": "candidate_cap",
                    "message": f"In-memory candidate cap of {max_candidates_in_memory:,} reached; remaining candidates were skipped.",
                })
                break
        return local_evaluated_count, timed_out

    def _execute_grid_work_items_parallel(
        self,
        *,
        job: SweepJob,
        payload: CreateSweepRequest,
        work_items: list[_SweepGridWorkItem],
        bundle: HistoricalDataBundle,
        resolved_parameters: ResolvedExecutionParameters,
        risk_free_rate_curve: object | None,
        candidates: list[dict[str, Any]],
        warnings: list[dict[str, Any]],
        sweep_start: float,
        sweep_timeout: float,
        max_candidates_in_memory: int,
        max_workers: int,
    ) -> tuple[int, bool]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        local_evaluated_count = 0
        timed_out = False
        pending_items = iter(work_items)
        queued_limit = max_workers * 2

        def _submit_next(pool: ThreadPoolExecutor, futures: dict[Any, _SweepGridWorkItem]) -> None:
            nonlocal timed_out
            while len(futures) < queued_limit and not timed_out:
                remaining = self._remaining_grid_timeout(sweep_start=sweep_start, sweep_timeout=sweep_timeout)
                if remaining <= 0:
                    timed_out = True
                    warnings.append({
                        "code": "timeout",
                        "message": "Sweep time limit approaching; remaining candidates were skipped.",
                    })
                    return
                try:
                    work_item = next(pending_items)
                except StopIteration:
                    return
                future = pool.submit(
                    self._execute_grid_work_item,
                    payload=payload,
                    work_item=work_item,
                    bundle=bundle,
                    resolved_parameters=resolved_parameters,
                    risk_free_rate_curve=risk_free_rate_curve,
                    execution_service=None,
                    clone_bundle=True,
                )
                futures[future] = work_item

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures: dict[Any, _SweepGridWorkItem] = {}
            _submit_next(pool, futures)
            while futures:
                remaining = self._remaining_grid_timeout(sweep_start=sweep_start, sweep_timeout=sweep_timeout)
                if remaining <= 0:
                    timed_out = True
                    warnings.append({
                        "code": "timeout",
                        "message": "Sweep time limit approaching; remaining candidates were skipped.",
                    })
                    for future in futures:
                        future.cancel()
                    break
                try:
                    completed_future = next(as_completed(list(futures), timeout=remaining))
                except TimeoutError:
                    timed_out = True
                    warnings.append({
                        "code": "timeout",
                        "message": "Sweep time limit approaching; remaining candidates were skipped.",
                    })
                    for future in futures:
                        future.cancel()
                    break
                work_item = futures.pop(completed_future)
                try:
                    completed_candidates = completed_future.result()
                except AppError as exc:
                    SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason=exc.code).inc()
                    warnings.append({
                        "code": "candidate_failed",
                        "message": (
                            f"{work_item.strategy_type.value} / delta={work_item.delta_val} / "
                            f"{work_item.entry_rule_set_name}: {exc.code}"
                        ),
                        "error_code": exc.code,
                    })
                    _submit_next(pool, futures)
                    continue
                except Exception:
                    SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason="internal").inc()
                    logger.warning(
                        "sweep.candidate_failed",
                        strategy=work_item.strategy_type.value,
                        delta=work_item.delta_val,
                        exc_info=True,
                    )
                    warnings.append({
                        "code": "candidate_failed_internal",
                        "message": (
                            f"{work_item.strategy_type.value} / delta={work_item.delta_val} / "
                            f"{work_item.entry_rule_set_name} failed"
                        ),
                    })
                    _submit_next(pool, futures)
                    continue
                local_evaluated_count, hit_cap = self._record_grid_candidates(
                    job=job,
                    payload=payload,
                    candidates=candidates,
                    completed_candidates=completed_candidates,
                    local_evaluated_count=local_evaluated_count,
                    max_candidates_in_memory=max_candidates_in_memory,
                )
                if hit_cap:
                    timed_out = True
                    warnings.append({
                        "code": "candidate_cap",
                        "message": f"In-memory candidate cap of {max_candidates_in_memory:,} reached; remaining candidates were skipped.",
                    })
                    for future in futures:
                        future.cancel()
                    break
                _submit_next(pool, futures)
        return local_evaluated_count, timed_out

    def _execute_grid_work_item(
        self,
        *,
        payload: CreateSweepRequest,
        work_item: _SweepGridWorkItem,
        bundle: HistoricalDataBundle,
        resolved_parameters: ResolvedExecutionParameters,
        risk_free_rate_curve: object | None,
        execution_service: BacktestExecutionService | None,
        clone_bundle: bool,
    ) -> list[dict[str, Any]]:
        overrides = self._build_overrides(work_item.delta_val, work_item.width_val)
        request = CreateBacktestRunRequest(
            symbol=payload.symbol,
            strategy_type=work_item.strategy_type,
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=work_item.entry_rules,
            slippage_pct=payload.slippage_pct,
            strategy_overrides=overrides,
        )
        executor = execution_service or get_thread_local_shared_execution_service()
        execution_bundle = bundle.clone_for_execution() if clone_bundle else bundle
        if len(work_item.exit_group) > 1:
            results = executor.execute_exit_policy_variants(
                request,
                exit_policies=[
                    (exit_set.profit_target_pct, exit_set.stop_loss_pct)
                    for exit_set in work_item.exit_group
                    if exit_set is not None
                ],
                bundle=execution_bundle,
                resolved_parameters=resolved_parameters,
                risk_free_rate_curve=risk_free_rate_curve,
            )
            exit_result_pairs = list(zip(work_item.exit_group, results, strict=False))
        else:
            exit_set = work_item.exit_group[0]
            result = executor.execute_request(
                request.model_copy(
                    update={
                        "profit_target_pct": exit_set.profit_target_pct if exit_set else None,
                        "stop_loss_pct": exit_set.stop_loss_pct if exit_set else None,
                    }
                ),
                bundle=execution_bundle,
                resolved_parameters=resolved_parameters,
                risk_free_rate_curve=risk_free_rate_curve,
            )
            exit_result_pairs = [(exit_set, result)]

        return [
            self._build_candidate(
                result=result,
                strategy_type=work_item.strategy_type.value,
                delta_val=work_item.delta_val,
                width_val=work_item.width_val,
                entry_rule_set_name=work_item.entry_rule_set_name,
                exit_set=exit_set,
            )
            for exit_set, result in exit_result_pairs
        ]

    def _record_grid_candidates(
        self,
        *,
        job: SweepJob,
        payload: CreateSweepRequest,
        candidates: list[dict[str, Any]],
        completed_candidates: list[dict[str, Any]],
        local_evaluated_count: int,
        max_candidates_in_memory: int,
    ) -> tuple[int, bool]:
        previous_evaluated_count = local_evaluated_count
        for candidate in completed_candidates:
            candidates.append(candidate)
            local_evaluated_count += 1
            _TRIM_INTERVAL = 200
            max_results = payload.max_results or 20
            if (
                len(candidates) > 0
                and len(candidates) % _TRIM_INTERVAL == 0
                and len(candidates) > max_results * 2
            ):
                candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
                keep = max(max_results * 3, _TRIM_INTERVAL)
                for stale_candidate in candidates[keep:]:
                    stale_candidate["trades_json"] = []
                    stale_candidate["equity_curve"] = []

            if len(candidates) >= max_candidates_in_memory:
                return local_evaluated_count, True

        if (local_evaluated_count // 50) > (previous_evaluated_count // 50):
            _update_heartbeat(self.session, SweepJob, job.id)
            nested_progress = None
            try:
                nested_progress = self.session.begin_nested()
                from sqlalchemy import update as _progress_update
                self.session.execute(
                    _progress_update(SweepJob)
                    .where(SweepJob.id == job.id)
                    .values(evaluated_candidate_count=local_evaluated_count)
                )
                nested_progress.commit()
            except Exception:
                if nested_progress is not None:
                    with contextlib.suppress(Exception):
                        nested_progress.rollback()
                logger.warning(
                    "sweep.progress_commit_failed",
                    evaluated=local_evaluated_count,
                    candidates_in_memory=len(candidates),
                )
        return local_evaluated_count, False

    def _make_bundle_request(
        self,
        payload: CreateSweepRequest,
        *,
        strategy_type: Any | None = None,
        custom_legs: list[Any] | None = None,
    ) -> CreateBacktestRunRequest:
        return CreateBacktestRunRequest(
            symbol=payload.symbol,
            strategy_type=strategy_type or payload.strategy_types[0],
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=self._combined_entry_rules(payload),
            custom_legs=custom_legs,
        )

    def _make_prefetch_requests(self, payload: CreateSweepRequest) -> list[CreateBacktestRunRequest]:
        return [
            CreateBacktestRunRequest(
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
                entry_rules=[],
            )
            for strategy_type in dict.fromkeys(payload.strategy_types)
        ]

    @staticmethod
    def _build_overrides(
        delta_val: int | None,
        width_val: tuple[SpreadWidthMode, Decimal] | None,
    ) -> StrategyOverrides | None:
        if delta_val is None and width_val is None:
            return None

        spread_width = None
        if width_val is not None:
            spread_width = SpreadWidthConfig(
                mode=SpreadWidthMode(width_val[0]),
                value=width_val[1],
            )

        put_strike_sel = None
        call_strike_sel = None
        if delta_val is not None:
            put_strike_sel = StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_val)),
            )
            call_strike_sel = StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(delta_val)),
            )

        return StrategyOverrides(
            short_put_strike=put_strike_sel,
            short_call_strike=call_strike_sel,
            spread_width=spread_width,
        )

    def _build_candidate(
        self,
        result,
        strategy_type: str,
        delta_val: int | None,
        width_val: tuple[SpreadWidthMode, Decimal] | None,
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
            parameters["profit_target_pct"] = self._json_scalar(exit_set.profit_target_pct)
            parameters["stop_loss_pct"] = self._json_scalar(exit_set.stop_loss_pct)

        trades = [self._serialize_trade(t) for t in result.trades[:50]]
        equity_curve = self._downsample_equity_curve(result.equity_curve)

        serialized_trade_count = len(trades)
        full_trade_count = max(int(summary.get("trade_count") or 0), serialized_trade_count)
        serialized_equity_point_count = len(equity_curve)
        full_equity_point_count = len(result.equity_curve)

        return {
            "score": score,
            "strategy_type": strategy_type,
            "parameters": parameters,
            "summary": summary,
            "warnings": result.warnings or [],
            "trades": trades,
            "trade_count": full_trade_count,
            "serialized_trade_count": serialized_trade_count,
            "equity_curve": equity_curve,
            "equity_point_count": full_equity_point_count,
            "serialized_equity_point_count": serialized_equity_point_count,
        }

    @staticmethod
    def _score_candidate(candidate: dict[str, Any]) -> float:
        return candidate.get("score", 0.0)

    @staticmethod
    def _json_scalar(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        return value

    def _persist_result(
        self,
        *,
        job_id: UUID,
        rank: int,
        score: float | Decimal,
        strategy_type: str,
        parameter_snapshot_json: dict[str, Any],
        summary_json: dict[str, Any],
        warnings_json: list[dict[str, Any]],
        trades_json: list[dict[str, Any]],
        equity_curve_json: list[dict[str, Any]],
    ) -> None:
        self.session.add(
            SweepResult(
                sweep_job_id=job_id,
                rank=rank,
                score=Decimal(str(round(score, 6))),
                strategy_type=strategy_type,
                parameter_snapshot_json=parameter_snapshot_json,
                summary_json=summary_json,
                warnings_json=warnings_json,
                trades_json=trades_json,
                equity_curve_json=equity_curve_json,
            )
        )

    @staticmethod
    def _score_candidate_from_summary(summary: dict[str, Any], cfg: dict[str, float] | None = None) -> float:
        return score_candidate_from_summary(summary, cfg)

    _serialize_summary = staticmethod(serialize_summary)
    _serialize_trade = staticmethod(serialize_trade)
    _serialize_equity_point = staticmethod(serialize_equity_point)

    @classmethod
    def _downsample_equity_curve(cls, equity_curve: list) -> list[dict[str, Any]]:
        return downsample_equity_curve(equity_curve, max_points=_MAX_EQUITY_POINTS)

    @staticmethod
    def _to_job_response(job: SweepJob) -> SweepJobResponse:
        return sweep_job_response(job)

    @staticmethod
    def _to_result_response(result: SweepResult) -> SweepResultResponse:
        return sweep_result_response(result)
