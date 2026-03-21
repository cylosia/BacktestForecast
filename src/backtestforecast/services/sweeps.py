from __future__ import annotations

import time as _time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from pydantic import ValidationError as _PydanticValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.config import get_settings
from backtestforecast.errors import AppError, AppValidationError, ConflictError, NotFoundError
from backtestforecast.schemas.json_shapes import _SUMMARY_REQUIRED_KEYS, validate_json_shape
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import SweepJob, SweepResult, User
from backtestforecast.repositories.sweep_jobs import SweepJobRepository

UTC = timezone.utc
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
from backtestforecast.observability.metrics import (
    SWEEP_CANDIDATE_FAILURES_TOTAL,
    SWEEP_EXECUTION_DURATION_SECONDS,
)
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.dispatch_recovery import redispatch_if_stale_queued
from backtestforecast.utils import to_decimal
from backtestforecast.services.serialization import (
    downsample_equity_curve,
    serialize_equity_point,
    serialize_summary,
    serialize_trade,
)

logger = structlog.get_logger("services.sweeps")


from backtestforecast.services.serialization import (
    safe_validate_equity_curve as _safe_validate_equity_curve,
    safe_validate_summary as _safe_validate_summary,
)


_CANDIDATE_TIMEOUT_SECONDS = 120
_MAX_EQUITY_POINTS = 500

def _sweep_scoring_config() -> dict[str, float]:
    """Return sweep scoring weights from settings."""
    settings = get_settings()
    return {
        "win_rate_weight": settings.sweep_score_win_rate_weight,
        "roi_weight": settings.sweep_score_roi_weight,
        "sharpe_weight": settings.sweep_score_sharpe_weight,
        "drawdown_weight": settings.sweep_score_drawdown_weight,
        "sharpe_multiplier": settings.sweep_score_sharpe_multiplier,
        "min_trades": settings.sweep_score_min_trades,
    }


_heartbeat_failures: int = 0


def _update_heartbeat(session: Session, job_id: UUID) -> None:
    """Best-effort heartbeat update so the reaper knows the job is alive.

    Uses a savepoint to avoid committing dirty ORM state from the parent
    transaction (e.g. in-progress candidate counts).
    """
    global _heartbeat_failures
    from sqlalchemy import update as _hb_update
    nested = None
    try:
        nested = session.begin_nested()
        session.execute(
            _hb_update(SweepJob)
            .where(SweepJob.id == job_id)
            .values(last_heartbeat_at=datetime.now(UTC))
        )
        nested.commit()
        _heartbeat_failures = 0
    except Exception:
        _heartbeat_failures += 1
        if nested is not None:
            try:
                nested.rollback()
            except Exception:
                pass
        if _heartbeat_failures >= 3:
            logger.error(
                "sweep.heartbeat_consecutive_failures",
                job_id=str(job_id),
                consecutive_failures=_heartbeat_failures,
                hint="Reaper may kill this job if heartbeat stays stale.",
            )


class SweepService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self.repository = SweepJobRepository(session)
        self.audit = AuditService(session)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> "SweepService":
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
                    queue="research",
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
                queue="research",
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
        import hashlib, json
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
            queue="research",
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
            job.status = "failed"
            job.error_code = "user_not_found"
            job.error_message = "User account not found."
            job.completed_at = datetime.now(UTC)
            self.session.commit()
            return job

        try:
            from backtestforecast.billing.entitlements import ensure_sweep_access
            ensure_sweep_access(
                user.plan_tier, user.subscription_status,
                user.subscription_current_period_end,
            )
        except AppError:
            job.status = "failed"
            job.error_code = "entitlement_revoked"
            job.error_message = "Subscription no longer active."
            job.completed_at = datetime.now(UTC)
            self.session.commit()
            return job

        from sqlalchemy import update
        now = datetime.now(UTC)
        cas_result = self.session.execute(
            update(SweepJob)
            .where(SweepJob.id == job_id, SweepJob.status == "queued")
            .values(status="running", started_at=now, updated_at=now)
        )
        self.session.commit()
        if cas_result.rowcount == 0:
            self.session.refresh(job)
            logger.warning("sweep.cas_transition_failed", job_id=str(job_id), status=job.status)
            return job
        self.session.refresh(job)

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
        finally:
            SWEEP_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - _run_start)

    def list_jobs(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> SweepJobListResponse:
        from backtestforecast.utils import decode_cursor, encode_cursor

        effective_limit = min(limit, 200)
        cursor_before = None
        if cursor:
            cursor_before = decode_cursor(cursor)
            if cursor_before is None:
                from backtestforecast.errors import ValidationError
                raise ValidationError("Invalid pagination cursor.")
            offset = 0
        jobs = self.repository.list_for_user(
            user.id, limit=effective_limit + 1, offset=offset, cursor_before=cursor_before,
        )
        has_next = len(jobs) > effective_limit
        if has_next:
            jobs = jobs[:effective_limit]
        total = self.repository.count_for_user(user.id)
        next_cursor = encode_cursor(jobs[-1].created_at, jobs[-1].id) if has_next and jobs else None
        return SweepJobListResponse(
            items=[self._to_job_response(j) for j in jobs],
            total=total,
            offset=offset,
            limit=effective_limit,
            next_cursor=next_cursor,
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
            raise ConflictError(
                "Cannot delete a job that is currently queued or running. Cancel it first."
            )
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
        from backtestforecast.billing.entitlements import resolve_feature_policy
        from backtestforecast.errors import QuotaExceededError
        from sqlalchemy import select, func

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
        _MAX_CANDIDATES_IN_MEMORY = 2_000
        sweep_start = _time.monotonic()
        sweep_timeout = max(get_settings().sweep_timeout_seconds, _CANDIDATE_TIMEOUT_SECONDS * 2)
        timed_out = False
        local_evaluated_count = 0

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
                            if elapsed > sweep_timeout - _CANDIDATE_TIMEOUT_SECONDS:
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
                                local_evaluated_count += 1
                                if local_evaluated_count % 50 == 0:
                                    _update_heartbeat(self.session, job.id)
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
                                            try:
                                                nested_progress.rollback()
                                            except Exception:
                                                pass
                                        logger.warning(
                                            "sweep.progress_commit_failed",
                                            evaluated=local_evaluated_count,
                                            candidates_in_memory=len(candidates),
                                        )
                                _TRIM_INTERVAL = 200
                                max_results = payload.max_results or 20
                                if (
                                    len(candidates) > 0
                                    and len(candidates) % _TRIM_INTERVAL == 0
                                    and len(candidates) > max_results * 2
                                ):
                                    candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
                                    keep = max(max_results * 3, _TRIM_INTERVAL)
                                    for c in candidates[keep:]:
                                        c["trades_json"] = []
                                        c["equity_curve"] = []

                                if len(candidates) >= _MAX_CANDIDATES_IN_MEMORY:
                                    timed_out = True
                                    warnings.append({
                                        "code": "candidate_cap",
                                        "message": f"In-memory candidate cap of {_MAX_CANDIDATES_IN_MEMORY:,} reached; remaining candidates were skipped.",
                                    })
                                    break
                            except AppError as exc:
                                SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason=exc.code).inc()
                                warnings.append({
                                    "code": "candidate_failed",
                                    "message": (
                                        f"{strategy_type.value} / delta={delta_val} / "
                                        f"{entry_rule_set.name}: {exc.code}"
                                    ),
                                    "error_code": exc.code,
                                })
                            except Exception:
                                SWEEP_CANDIDATE_FAILURES_TOTAL.labels(reason="internal").inc()
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
                params["trades_truncated"] = candidate.get("trades_truncated", False)
                validate_json_shape(
                    candidate["summary"],
                    f"SweepResult[{idx}].summary_json",
                    required_keys=_SUMMARY_REQUIRED_KEYS,
                )
                job.results.append(
                    SweepResult(
                        rank=idx,
                        score=Decimal(str(round(candidate["score"], 6))),
                        strategy_type=candidate["strategy_type"],
                        parameter_snapshot_json=params,
                        summary_json=candidate["summary"],
                        warnings_json=candidate.get("warnings", []),
                        trades_json=candidate["trades"],
                        equity_curve_json=candidate["equity_curve"],
                    )
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
            StrategyType,
        )
        from backtestforecast.sweeps.constraints import Individual
        from backtestforecast.sweeps.genetic import GAResult, GeneticConfig, GeneticOptimizer

        gc = payload.genetic_config
        if gc is None:
            raise AppValidationError("genetic_config is required for genetic mode.")

        num_legs = gc.num_legs
        leg_count_map = {v: k for k, v in CUSTOM_LEG_COUNT.items()}
        strategy_type = leg_count_map.get(num_legs)
        if strategy_type is None:
            raise AppValidationError(f"No custom strategy type for {num_legs} legs.")

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

        genetic_start = _time.monotonic()
        safe_genetic_timeout = max(get_settings().sweep_genetic_timeout_seconds, _CANDIDATE_TIMEOUT_SECONDS * 2)
        _genetic_timed_out = False

        def fitness_fn(individual: Individual) -> float:
            nonlocal _genetic_timed_out
            if _genetic_timed_out:
                return 0.0
            elapsed = _time.monotonic() - genetic_start
            if elapsed > safe_genetic_timeout - _CANDIDATE_TIMEOUT_SECONDS:
                _genetic_timed_out = True
                warnings.append({
                    "code": "timeout",
                    "message": "Genetic sweep time limit approaching; remaining evaluations returned 0.",
                })
                return 0.0
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
            except (AppError, AppValidationError):
                return 0.0
            except _PydanticValidationError:
                return 0.0
            except Exception:
                logger.warning("sweep.genetic_fitness_unexpected_error", exc_info=True)
                return 0.0

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

        _update_heartbeat(self.session, job.id)

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
            trades_truncated = len(result.trades) > 50
            equity_curve = self._downsample_equity_curve(result.equity_curve)

            parameters: dict[str, Any] = {
                "strategy_type": strategy_type.value,
                "mode": "genetic",
                "num_legs": num_legs,
                "generations_run": ga_result.generations_run,
                "total_evaluations": ga_result.total_evaluations,
                "custom_legs": [leg.model_dump(mode="json") for leg in legs],
                "entry_rule_set_name": payload.entry_rule_sets[0].name if payload.entry_rule_sets else None,
                "trades_truncated": trades_truncated,
            }
            if exit_set is not None:
                parameters["exit_rule_set_name"] = exit_set.name
                parameters["profit_target_pct"] = exit_set.profit_target_pct
                parameters["stop_loss_pct"] = exit_set.stop_loss_pct

            validate_json_shape(
                summary,
                f"GeneticSweepResult[{actual_rank}].summary_json",
                required_keys=_SUMMARY_REQUIRED_KEYS,
            )
            job.results.append(
                SweepResult(
                    rank=actual_rank,
                    score=Decimal(str(round(fit_val, 6))),
                    strategy_type=strategy_type.value,
                    parameter_snapshot_json=parameters,
                    summary_json=summary,
                    warnings_json=result.warnings or [],
                    trades_json=trades,
                    equity_curve_json=equity_curve,
                )
            )

        if len(job.results) == 0:
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
                        result_count=len(job.results),
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
            "trades_truncated": len(result.trades) > 50,
            "equity_curve": equity_curve,
        }

    @staticmethod
    def _score_candidate(candidate: dict[str, Any]) -> float:
        return candidate.get("score", 0.0)

    @staticmethod
    def _score_candidate_from_summary(summary: dict[str, Any], cfg: dict[str, float] | None = None) -> float:
        """Score a candidate based on backtest summary metrics.

        **Formula**::

            raw = win_rate * W_wr + roi * W_roi + sharpe * (M * W_sharpe) - drawdown * W_dd
            score = raw * norm

        Where:

        - ``M`` is the ``sharpe_multiplier`` (default 2.0), which amplifies
          Sharpe's *relative* contribution vs other factors.
        - ``norm`` is a normalization factor that keeps the total effective
          weight proportional to the configured sum (default 1.0). Without
          normalization, the multiplier would inflate absolute scores.

        **Default weights** (effective after normalization):

        ========== ========== ========= ===========
        Factor     Configured Effective Proportion
        ========== ========== ========= ===========
        Win rate   0.25       0.25      ~21%
        ROI        0.35       0.35      ~29%
        Sharpe     0.20×2.0   0.40      ~33%
        Drawdown   0.20       0.20      ~17%
        ========== ========== ========= ===========

        The multiplier makes Sharpe the highest-weighted factor, favouring
        risk-adjusted returns over raw ROI in sweep ranking.
        """
        if cfg is None:
            cfg = _sweep_scoring_config()
        win_rate = Decimal(str(summary.get("win_rate", 0)))
        roi = Decimal(str(summary.get("total_roi_pct", 0)))
        drawdown = max(Decimal(str(summary.get("max_drawdown_pct", 0))), Decimal("0"))
        sharpe = Decimal(str(summary.get("sharpe_ratio") or 0))
        trade_count = int(summary.get("trade_count", 0))

        import math
        if any(math.isnan(float(v)) for v in [win_rate, roi, drawdown, sharpe]):
            return 0.0

        min_trades = int(cfg["min_trades"])
        if trade_count < min_trades:
            return 0.0

        win_rate_w = Decimal(str(round(cfg["win_rate_weight"], 10)))
        roi_w = Decimal(str(round(cfg["roi_weight"], 10)))
        sharpe_w = Decimal(str(round(cfg["sharpe_weight"], 10)))
        sharpe_m = Decimal(str(round(cfg["sharpe_multiplier"], 10)))
        drawdown_w = Decimal(str(round(cfg["drawdown_weight"], 10)))

        effective_sharpe_w = sharpe_w * sharpe_m
        total_effective = win_rate_w + roi_w + effective_sharpe_w + drawdown_w
        if total_effective > 0:
            norm = (win_rate_w + roi_w + sharpe_w + drawdown_w) / total_effective
        else:
            norm = Decimal("1")

        score = (
            win_rate * win_rate_w
            + roi * roi_w
            + sharpe * effective_sharpe_w
            - drawdown * drawdown_w
        ) * norm
        return float(score)

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
            mode=job.mode,
            plan_tier_snapshot=job.plan_tier_snapshot,
            candidate_count=job.candidate_count,
            evaluated_candidate_count=job.evaluated_candidate_count,
            result_count=job.result_count,
            prefetch_summary=job.prefetch_summary_json,
            warnings=job.warnings_json,
            request_snapshot=job.request_snapshot_json,
            error_code=job.error_code,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    @staticmethod
    def _to_result_response(result: SweepResult) -> SweepResultResponse:
        params = result.parameter_snapshot_json or {}
        stored_truncated = params.get("trades_truncated")
        trades_truncated = stored_truncated if stored_truncated is not None else len(result.trades_json or []) >= 50
        return SweepResultResponse(
            id=result.id,
            rank=result.rank,
            score=result.score,
            strategy_type=result.strategy_type,
            delta=params.get("delta"),
            width_mode=params.get("width_mode"),
            width_value=params.get("width_value"),
            entry_rule_set_name=params.get("entry_rule_set_name") or "default",
            exit_rule_set_name=params.get("exit_rule_set_name"),
            profit_target_pct=params.get("profit_target_pct"),
            stop_loss_pct=params.get("stop_loss_pct"),
            parameter_snapshot_json=params,
            summary=_safe_validate_summary(result.summary_json),
            warnings=result.warnings_json,
            trades_json=result.trades_json,
            equity_curve=_safe_validate_equity_curve(result.equity_curve_json),
            trades_truncated=trades_truncated,
        )
