from __future__ import annotations

import heapq
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import (
    ScannerAccessPolicy,
    ensure_forecasting_access,
    resolve_scanner_policy,
    validate_strategy_access,
)
from backtestforecast.config import get_settings
from backtestforecast.errors import AppError, AppValidationError, ConflictError, NotFoundError, QuotaExceededError
from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import ScannerJob, ScannerRecommendation, User
from backtestforecast.observability.metrics import (
    SCAN_CANDIDATE_FAILURES_TOTAL,
    SCAN_EXECUTION_DURATION_SECONDS,
    _normalize_scan_failure_reason,
)
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.repositories.scanner_jobs import ScannerJobRepository
from backtestforecast.scans.ranking import (
    HistoricalObservation,
    aggregate_historical_performance,
    build_ranking_breakdown,
    is_strategy_rule_set_compatible,
    recommendation_sort_key,
    rule_set_hash,
)
from backtestforecast.schemas.backtests import (
    CreateBacktestRunRequest,
    EquityCurvePointResponse,
    RsiRule,
    TradeJsonResponse,
)
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.schemas.json_shapes import _FORECAST_REQUIRED_KEYS, validate_json_shape
from backtestforecast.schemas.scans import (
    CreateScannerJobRequest,
    HistoricalAnalogForecastResponse,
    HistoricalPerformanceResponse,
    RankingBreakdownResponse,
    ScannerJobListResponse,
    ScannerJobResponse,
    ScannerJobStatusResponse,
    ScannerRecommendationListResponse,
    ScannerRecommendationResponse,
)
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtest_execution import BacktestExecutionService
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
from backtestforecast.services.scan_components import ScanExecutor, ScanJobFactory, ScanPresenter
from backtestforecast.services.scan_service_helpers import (
    RankedCandidate as _RankedCandidate,
)
from backtestforecast.services.scan_service_helpers import (
    get_fallback_entry_rules as _get_fallback_entry_rules,
)
from backtestforecast.services.scan_service_helpers import (
    historical_observation_from_summary as _historical_observation_from_summary,
)
from backtestforecast.services.scan_service_helpers import (
    request_hash as _request_hash,
)
from backtestforecast.services.scan_service_helpers import (
    scanner_job_response,
)
from backtestforecast.services.serialization import (
    downsample_equity_curve,
    serialize_equity_point,
    serialize_summary,
    serialize_trade,
)
from backtestforecast.services.serialization import (
    safe_validate_json as _safe_validate_json,
)
from backtestforecast.services.serialization import (
    safe_validate_list as _safe_validate_list,
)
from backtestforecast.services.serialization import (
    safe_validate_model as _safe_validate_model,
)
from backtestforecast.services.serialization import (
    safe_validate_summary as _safe_validate_summary,
)
from backtestforecast.services.serialization import (
    safe_validate_warning_list as _safe_validate_warning_list,
)
from backtestforecast.utils import to_decimal
from backtestforecast.utils.dates import market_date_today
from backtestforecast.version import DEFAULT_ENGINE_VERSION, DEFAULT_RANKING_VERSION

logger = structlog.get_logger("services.scans")
_SCAN_QUEUE = "scans"


class ScanService:
    _MAX_CONCURRENT_SCANS = 5

    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
        forecaster: HistoricalAnalogForecaster | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self._owns_execution_service = execution_service is None
        self._forecaster = forecaster
        self.repository = ScannerJobRepository(session)
        self.audit = AuditService(session)
        self.job_factory = ScanJobFactory(self)
        self.executor = ScanExecutor(self)
        self.presenter = ScanPresenter(self)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None and self._owns_execution_service:
            self._execution_service.close()

    def __enter__(self) -> ScanService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


    def create_job(self, user: User, payload: CreateScannerJobRequest) -> ScannerJob:
        return self.job_factory.create_job(user, payload)

    def run_job(self, job_id: UUID) -> ScannerJob:
        return self.executor.run_job(job_id)

    def list_jobs(self, user: User, limit: int = 50, offset: int = 0, cursor: str | None = None) -> ScannerJobListResponse:
        return self.presenter.list_jobs(user, limit=limit, offset=offset, cursor=cursor)

    def get_job(self, user: User, job_id: UUID) -> ScannerJobResponse:
        return self.presenter.get_job(user, job_id)

    def get_recommendations(self, user: User, job_id: UUID, *, limit: int = 100, offset: int = 0) -> ScannerRecommendationListResponse:
        return self.presenter.get_recommendations(user, job_id, limit=limit, offset=offset)

    def build_forecast(
        self,
        *,
        user: User,
        symbol: str,
        strategy_type: str | None,
        horizon_days: int,
    ) -> ForecastEnvelopeResponse:
        return self.executor.build_forecast(
            user=user,
            symbol=symbol,
            strategy_type=strategy_type,
            horizon_days=horizon_days,
        )

    @property
    def forecaster(self) -> HistoricalAnalogForecaster:
        if self._forecaster is None:
            self._forecaster = HistoricalAnalogForecaster()
        return self._forecaster

    def _create_job_impl(self, user: User, payload: CreateScannerJobRequest) -> ScannerJob:
        from sqlalchemy import select
        self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        )
        policy = resolve_scanner_policy(
            user.plan_tier, payload.mode.value, user.subscription_status,
            user.subscription_current_period_end,
        )
        validate_strategy_access(policy, [strategy.value for strategy in payload.strategy_types])
        effective_max_recommendations = payload.max_recommendations
        if policy.max_recommendations:
            effective_max_recommendations = min(payload.max_recommendations, policy.max_recommendations)
        self._validate_limits(policy, payload)
        self._enforce_concurrent_scan_limit(user)

        candidate_count, compatibility_warnings = self._count_compatible_candidates(payload)
        if candidate_count <= 0:
            raise AppValidationError("No compatible symbol/strategy/rule-set combinations were left after validation.")

        request_hash = self._request_hash(payload)
        if payload.idempotency_key:
            existing_by_key = self.repository.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing_by_key is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing_by_key,
                    model_name="ScannerJob",
                    task_name="scans.run_job",
                    task_kwargs={"job_id": str(existing_by_key.id)},
                    queue=_SCAN_QUEUE,
                    log_event="scan",
                    logger=logger,
                )

        recent_duplicate = self.repository.find_recent_duplicate(
            user.id,
            request_hash,
            payload.mode.value,
            since=datetime.now(UTC) - timedelta(minutes=10),
        )
        if recent_duplicate is not None:
            return redispatch_if_stale_queued(
                self.session,
                recent_duplicate,
                model_name="ScannerJob",
                task_name="scans.run_job",
                task_kwargs={"job_id": str(recent_duplicate.id)},
                queue=_SCAN_QUEUE,
                log_event="scan",
                logger=logger,
            )

        job = ScannerJob(
            user_id=user.id,
            name=payload.name,
            status="queued",
            mode=payload.mode.value,
            plan_tier_snapshot=user.plan_tier,
            job_kind="manual",
            request_hash=request_hash,
            idempotency_key=payload.idempotency_key,
            refresh_daily=payload.refresh_daily,
            refresh_priority=payload.refresh_priority,
            candidate_count=candidate_count,
            evaluated_candidate_count=0,
            recommendation_count=0,
            request_snapshot_json={**payload.model_dump(mode="json"), "max_recommendations": effective_max_recommendations},
            warnings_json=compatibility_warnings,
            ranking_version=DEFAULT_RANKING_VERSION,
            engine_version=DEFAULT_ENGINE_VERSION,
        )
        self.repository.add(job)
        self.audit.record(
            event_type="scan.created",
            subject_type="scanner_job",
            subject_id=job.id,
            user_id=user.id,
            metadata={"mode": job.mode, "candidate_count": job.candidate_count},
        )
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if payload.idempotency_key:
                from sqlalchemy import select as sa_select
                stmt = sa_select(ScannerJob).where(
                    ScannerJob.user_id == user.id,
                    ScannerJob.idempotency_key == payload.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            recent = self.repository.find_recent_duplicate(
                user.id, request_hash, payload.mode.value,
                since=datetime.now(UTC) - timedelta(minutes=10),
            )
            if recent is not None:
                return recent
            raise
        return job

    def create_and_dispatch_job(
        self,
        user: User,
        payload: CreateScannerJobRequest,
        *,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> ScannerJob:
        """Create a scan job and persist dispatch state transactionally."""
        from apps.api.app.dispatch import dispatch_celery_task

        job = self.create_job(user, payload)
        dispatch_celery_task(
            db=self.session,
            job=job,
            task_name="scans.run_job",
            task_kwargs={"job_id": str(job.id)},
            queue=_SCAN_QUEUE,
            log_event="scan",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(job)
        return job

    def _run_job_impl(self, job_id: UUID) -> ScannerJob:
        job = self.repository.get(job_id, for_update=True)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status not in ("queued", "running"):
            logger.info("scan.run_job_skipped", job_id=str(job_id), status=job.status)
            return job

        user = self.session.get(User, job.user_id)
        if user is None:
            fail_job(job, error_code="user_not_found", error_message="User account not found.")
            self.session.commit()
            return job

        try:
            policy = resolve_scanner_policy(
                user.plan_tier, job.mode,
                subscription_status=user.subscription_status,
                subscription_current_period_end=user.subscription_current_period_end,
            )
        except AppError:
            fail_job(job, error_code="entitlement_revoked", error_message="Subscription no longer active.")
            self.session.commit()
            return job

        payload = CreateScannerJobRequest.model_validate(job.request_snapshot_json)
        self._validate_limits(policy, payload)

        from sqlalchemy import update as sa_update
        rows_updated = self.session.execute(
            sa_update(ScannerJob)
            .where(ScannerJob.id == job.id, ScannerJob.status == "queued")
            .values(**running_transition_values(
                recommendation_count=0,
                evaluated_candidate_count=0,
            ))
        ).rowcount
        self.session.commit()
        if rows_updated == 0:
            logger.warning("scan.run_job_already_running", job_id=str(job.id))
            return job
        self.session.refresh(job)
        observe_job_create_to_running_latency(job)

        try:
            self.repository.delete_recommendations(job.id)
            return self._execute_scan(job, payload)
        except Exception:  # Intentional broad catch: any failure during scan execution must
            # mark the job as failed in the DB so the client sees a terminal state
            # rather than a perpetually "running" job. The exception is re-raised.
            logger.exception("scan.run_job_failed", job_id=str(job_id))
            try:
                self.session.rollback()
                self.session.expire_all()
                job = self.repository.get(job_id, for_update=True)
                if job is not None:
                    fail_job_if_active(
                        job,
                        error_code="internal_error",
                        error_message="An unexpected error occurred during scan execution.",
                    )
                    self.session.commit()
            except Exception:  # Intentional: the recovery handler itself must not raise,
                # otherwise the original exception would be masked.
                logger.exception("scan.run_job_failed.recovery_failed", job_id=str(job_id))
                self.session.rollback()
            raise

    # _CANDIDATE_TIMEOUT_SECONDS must be shorter than the DB statement_timeout
    # configured for worker sessions (currently 300s via create_worker_session).
    # If this value exceeds the statement_timeout, individual candidate backtests
    # will fail with StatementTimeout before the scan-level timeout fires.
    _CANDIDATE_TIMEOUT_SECONDS = 120
    # Keep only a bounded pool of top-ranked candidates in memory. We retain a
    # small buffer above max_recommendations to preserve deterministic ranking
    # after ties while avoiding O(universe size) memory growth.
    _MIN_TOP_CANDIDATE_BUFFER = 50

    def _execute_scan(
        self,
        job: ScannerJob,
        payload: CreateScannerJobRequest,
    ) -> ScannerJob:
        import time as _time

        compatibility_candidate_count, compatibility_warnings = self._count_compatible_candidates(payload)
        job.candidate_count = compatibility_candidate_count
        warnings: list[dict[str, Any]] = list(compatibility_warnings)
        candidates_heap: list[_RankedCandidate] = []
        forecast_cache: dict[tuple[str, str], HistoricalAnalogForecastResponse] = {}

        bundle_cache = self._prepare_bundles(payload, warnings)
        historical_cache = self._batch_historical_performance(payload, job.created_at)
        scan_start = _time.monotonic()
        _scan_timed_out = False
        _scan_timeout = get_settings().scan_timeout_seconds
        keep_limit = max(payload.max_recommendations * 3, self._MIN_TOP_CANDIDATE_BUFFER)

        if _scan_timeout <= self._CANDIDATE_TIMEOUT_SECONDS:
            safe_minimum = self._CANDIDATE_TIMEOUT_SECONDS * 2
            logger.warning(
                "scan.timeout_too_low",
                configured=_scan_timeout,
                candidate_timeout=self._CANDIDATE_TIMEOUT_SECONDS,
                using=safe_minimum,
            )
            _scan_timeout = safe_minimum

        import random
        symbols = list(payload.symbols)
        random.Random(job.id.int).shuffle(symbols)

        for symbol in symbols:
            if _scan_timed_out:
                break
            for strategy in payload.strategy_types:
                if _scan_timed_out:
                    break
                for rule_set in payload.rule_sets:
                    if not is_strategy_rule_set_compatible(strategy.value, rule_set.entry_rules):
                        continue

                    request = CreateBacktestRunRequest(
                        symbol=symbol,
                        strategy_type=strategy,
                        start_date=payload.start_date,
                        end_date=payload.end_date,
                        target_dte=payload.target_dte,
                        dte_tolerance_days=payload.dte_tolerance_days,
                        max_holding_days=payload.max_holding_days,
                        account_size=payload.account_size,
                        risk_per_trade_pct=payload.risk_per_trade_pct,
                        commission_per_contract=payload.commission_per_contract,
                        entry_rules=rule_set.entry_rules,
                    )
                    candidate_rule_set_hash = rule_set_hash(rule_set.entry_rules)
                    try:
                        elapsed_so_far = _time.monotonic() - scan_start
                        if elapsed_so_far > _scan_timeout - self._CANDIDATE_TIMEOUT_SECONDS:
                            warnings.append({
                                "code": "timeout",
                                "message": "Scan time limit approaching; remaining candidates were skipped.",
                            })
                            _scan_timed_out = True
                            break
                        bundle = bundle_cache.get(symbol)
                        if bundle is None:
                            continue
                        job.evaluated_candidate_count += 1
                        execution_result = self.execution_service.execute_request(request, bundle=bundle)
                        forecast = forecast_cache.get((symbol, strategy.value))
                        if forecast is None:
                            forecast = self._forecast_for_bundle(
                                symbol=symbol,
                                strategy_type=strategy.value,
                                bars=bundle.bars,
                                horizon_days=min(payload.max_holding_days, payload.target_dte),
                            )
                            forecast_cache[(symbol, strategy.value)] = forecast
                        hist_key = (symbol, strategy.value, candidate_rule_set_hash)
                        historical = historical_cache.get(hist_key)
                        if historical is None:
                            historical = self._historical_performance(
                                symbol=symbol,
                                strategy_type=strategy.value,
                                candidate_rule_set_hash=candidate_rule_set_hash,
                                before=job.created_at,
                            )
                        ranking = build_ranking_breakdown(
                            execution_result=execution_result,
                            historical_performance=historical,
                            forecast=forecast,
                            strategy_type=strategy.value,
                            account_size=float(payload.account_size),
                        )
                        sort_key = recommendation_sort_key(
                            (symbol, strategy.value, rule_set.name, ranking),
                        )
                        if len(candidates_heap) >= keep_limit and sort_key >= candidates_heap[0].sort_key:
                            continue

                        serialized_trades = [
                            self._serialize_trade(trade)
                            for trade in execution_result.trades[:50]
                        ]
                        summary = self._serialize_summary(execution_result.summary)
                        serialized_trade_count = len(serialized_trades)
                        full_trade_count = max(
                            int(summary.get("trade_count") or 0),
                            serialized_trade_count,
                        )
                        candidate = {
                            "symbol": symbol,
                            "strategy_type": strategy.value,
                            "rule_set_name": rule_set.name,
                            "rule_set_hash": candidate_rule_set_hash,
                            "request_snapshot": request.model_dump(mode="json"),
                            "summary": summary,
                            "warnings": execution_result.warnings,
                            "trades": serialized_trades,
                            "trade_count": full_trade_count,
                            "serialized_trade_count": serialized_trade_count,
                            "equity_curve": self._downsample_equity_curve(execution_result.equity_curve),
                            "equity_point_count": len(execution_result.equity_curve),
                            "serialized_equity_point_count": min(
                                len(execution_result.equity_curve),
                                get_settings().max_scan_equity_points,
                            ),
                            "historical": historical.model_dump(mode="json"),
                            "forecast": forecast.model_dump(mode="json"),
                            "ranking": ranking.model_dump(mode="json"),
                        }
                        ranked_candidate = _RankedCandidate(sort_key=sort_key, candidate=candidate)
                        if len(candidates_heap) < keep_limit:
                            heapq.heappush(candidates_heap, ranked_candidate)
                        else:
                            heapq.heapreplace(candidates_heap, ranked_candidate)
                    except AppError as exc:
                        SCAN_CANDIDATE_FAILURES_TOTAL.labels(reason=_normalize_scan_failure_reason(exc.code if hasattr(exc, 'code') else "internal")).inc()
                        warnings.append(
                            {
                                "code": "candidate_failed",
                                "message": (
                                    f"{symbol} / {strategy.value} / {rule_set.name} "
                                    f"could not be evaluated ({exc.code})"
                                ),
                                "error_code": exc.code,
                            }
                        )
                    except Exception:  # Intentional broad catch: individual candidate failures
                        # must not abort the entire scan. Logged and appended as a warning.
                        SCAN_CANDIDATE_FAILURES_TOTAL.labels(
                            reason=_normalize_scan_failure_reason("internal")
                        ).inc()
                        logger.warning(
                            "scan.candidate_failed",
                            symbol=symbol,
                            strategy=strategy.value,
                            rule_set=rule_set.name,
                            exc_info=True,
                        )
                        warnings.append(
                            {
                                "code": "candidate_failed_internal",
                                "message": (
                                    f"{symbol} / {strategy.value} / {rule_set.name} "
                                    f"failed with an unexpected error"
                                ),
                            }
                        )

        if not candidates_heap:
            job.status = "failed"
            job.error_code = "scan_empty"
            job.error_message = "No scan combinations completed successfully."
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
            self.session.commit()
            return job

        sorted_candidates = [
            entry.candidate
            for entry in sorted(
                candidates_heap,
                key=lambda entry: entry.sort_key,
            )
        ]
        rank_lookup = {
            (c["symbol"], c["strategy_type"], c["rule_set_name"]): idx + 1
            for idx, c in enumerate(sorted_candidates)
        }
        selected = sorted_candidates[: payload.max_recommendations]

        from sqlalchemy import update as sa_update
        SCAN_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - scan_start)

        with self.session.no_autoflush:
            for candidate in selected:
                validate_json_shape(candidate["summary"], "ScannerRecommendation.summary_json", required_keys=frozenset({"trade_count"}))
                validate_json_shape(candidate["forecast"], "ScannerRecommendation.forecast_json", required_keys=_FORECAST_REQUIRED_KEYS)
                rank = rank_lookup[(candidate["symbol"], candidate["strategy_type"], candidate["rule_set_name"])]
                ranking_with_meta = dict(candidate["ranking"])
                ranking_with_meta["trade_count"] = candidate.get("trade_count", 0)
                ranking_with_meta["serialized_trade_count"] = candidate.get("serialized_trade_count", 0)
                ranking_with_meta["equity_point_count"] = candidate.get("equity_point_count", 0)
                ranking_with_meta["serialized_equity_point_count"] = candidate.get("serialized_equity_point_count", 0)
                self.session.add(
                    ScannerRecommendation(
                        scanner_job_id=job.id,
                        rank=rank,
                        score=to_decimal(float(candidate["ranking"]["final_score"])),
                        symbol=candidate["symbol"],
                        strategy_type=candidate["strategy_type"],
                        rule_set_name=candidate["rule_set_name"],
                        rule_set_hash=candidate["rule_set_hash"],
                        request_snapshot_json=candidate["request_snapshot"],
                        summary_json=candidate["summary"],
                        warnings_json=candidate["warnings"],
                        trades_json=candidate["trades"],
                        equity_curve_json=candidate["equity_curve"],
                        historical_performance_json=candidate["historical"],
                        forecast_json=candidate["forecast"],
                        ranking_features_json=ranking_with_meta,
                    )
                )

            success_rows = self.session.execute(
                sa_update(ScannerJob)
                .where(ScannerJob.id == job.id, ScannerJob.status == "running")
                .values(
                    status="succeeded",
                    recommendation_count=len(selected),
                    completed_at=datetime.now(UTC),
                    warnings_json=warnings,
                    updated_at=datetime.now(UTC),
                )
            )
        if success_rows.rowcount == 0:
            self.session.rollback()
            logger.warning("scan.success_overwrite_prevented", job_id=str(job.id))
        else:
            self.session.commit()
        self.session.refresh(job)
        if job.status == "succeeded":
            try:
                self.audit.record_always(
                    event_type="scan.completed",
                    subject_type="scanner_job",
                    subject_id=job.id,
                    user_id=job.user_id,
                    metadata={"mode": job.mode, "recommendation_count": job.recommendation_count},
                )
                self.session.commit()
            except Exception:
                self.session.rollback()
                logger.warning(
                    "scan.audit_commit_failed",
                    job_id=str(job.id),
                    exc_info=True,
                )
        return job

    def _list_jobs_impl(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> ScannerJobListResponse:
        effective_limit = min(limit, 200)
        cursor_before, offset = parse_cursor_param(cursor) if cursor else (None, offset)
        jobs, total = self.repository.list_for_user_with_count(
            user.id, limit=effective_limit + 1, offset=offset, cursor_before=cursor_before,
        )
        page = finalize_cursor_page(jobs, total=total, offset=offset, limit=effective_limit)
        return ScannerJobListResponse(
            items=[self._to_job_response(job) for job in page.items],
            total=page.total,
            offset=page.offset,
            limit=page.limit,
            next_cursor=page.next_cursor,
        )

    def _get_job_impl(self, user: User, job_id: UUID) -> ScannerJobResponse:
        job = self.repository.get_for_user(job_id, user.id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        return self._to_job_response(job)

    def delete_for_user(self, job_id: UUID, user_id: UUID) -> None:
        job = self.repository.get_for_user(job_id, user_id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status in ("queued", "running"):
            raise ConflictError(deletion_blocked_message("scanner job"))
        snapshot = job.request_snapshot_json or {}
        symbols = snapshot.get("symbols", [])
        self.audit.record(
            event_type="scan.deleted",
            subject_type="scanner_job",
            subject_id=job.id,
            user_id=user_id,
            metadata={"symbols": symbols[:5], "mode": job.mode},
        )
        self.session.delete(job)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def cancel_for_user(self, job_id: UUID, user_id: UUID) -> ScannerJobStatusResponse:
        job = self.repository.get_for_user(job_id, user_id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status not in ("queued", "running"):
            raise ConflictError(cancellation_blocked_message("scanner job"))
        task_id = mark_job_cancelled(job)
        snapshot = job.request_snapshot_json or {}
        symbols = snapshot.get("symbols", [])
        self.audit.record_always(
            event_type="scan.cancelled",
            subject_type="scanner_job",
            subject_id=job.id,
            user_id=user_id,
            metadata={"symbols": symbols[:5], "mode": job.mode, "reason": "user_cancelled"},
        )
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        revoke_celery_task(task_id, job_type="scan", job_id=job.id)
        publish_cancellation_event(job_type="scan", job_id=job.id)
        return ScannerJobStatusResponse(
            id=job.id,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_code=job.error_code,
            error_message=job.error_message,
        )

    def _get_recommendations_impl(
        self, user: User, job_id: UUID, *, limit: int = 100, offset: int = 0,
    ) -> ScannerRecommendationListResponse:
        import time as _time
        _query_start = _time.monotonic()
        effective_limit = min(limit, 200)
        job = self.repository.get_for_user(job_id, user.id, include_recommendations=False)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        total = job.recommendation_count
        from sqlalchemy import select
        stmt = (
            select(ScannerRecommendation)
            .where(ScannerRecommendation.scanner_job_id == job.id)
            .order_by(ScannerRecommendation.rank)
            .offset(offset)
            .limit(effective_limit)
        )
        recs = list(self.session.scalars(stmt))
        _query_elapsed = _time.monotonic() - _query_start
        _SLOW_QUERY_THRESHOLD = 5.0
        if _query_elapsed > _SLOW_QUERY_THRESHOLD:
            from backtestforecast.observability.metrics import API_SLOW_QUERIES_TOTAL
            API_SLOW_QUERIES_TOTAL.labels(endpoint="scan_recommendations").inc()
            logger.warning(
                "scan.slow_recommendation_query",
                job_id=str(job_id),
                elapsed_seconds=round(_query_elapsed, 2),
                total_recs=total,
                threshold=_SLOW_QUERY_THRESHOLD,
            )
        return ScannerRecommendationListResponse(
            items=[self._to_recommendation_response(r) for r in recs],
            total=total,
            offset=offset,
            limit=effective_limit,
        )

    def _list_scheduled_refresh_specs(self, limit: int = 25) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
        latest_sources: dict[tuple[UUID, str, str], ScannerJob] = {}
        for source in self.repository.list_refresh_sources(limit=200):
            key = (source.user_id, source.request_hash, source.mode)
            latest_sources.setdefault(key, source)

        from sqlalchemy import select

        sources_to_process = list(latest_sources.values())[:limit]
        user_ids = {source.user_id for source in sources_to_process}
        users = self.session.scalars(select(User).where(User.id.in_(user_ids))).all()
        user_cache = {u.id: u for u in users}

        _MAX_REFRESH_PER_USER = 5
        refresh_day = market_date_today().isoformat()
        user_refresh_counts: dict[UUID, int] = {}
        for source in sources_to_process:
            owner = user_cache.get(source.user_id)
            if owner is None:
                continue
            try:
                policy = resolve_scanner_policy(
                    owner.plan_tier,
                    source.mode,
                    subscription_status=owner.subscription_status,
                    subscription_current_period_end=owner.subscription_current_period_end,
                )
                payload = CreateScannerJobRequest.model_validate(source.request_snapshot_json)
                validate_strategy_access(policy, [s.value for s in payload.strategy_types])
            except AppError:
                logger.info(
                    "refresh.skipped_entitlement",
                    user_id=str(source.user_id),
                    mode=source.mode,
                )
                continue
            except Exception:  # Intentional: entitlement/validation errors for individual
                # refresh sources must not prevent processing of remaining sources.
                logger.exception(
                    "refresh.skipped_unexpected_error",
                    user_id=str(source.user_id),
                    mode=source.mode,
                )
                continue
            user_count = user_refresh_counts.get(source.user_id, 0)
            if user_count >= _MAX_REFRESH_PER_USER:
                logger.info(
                    "refresh.skipped_per_user_limit",
                    user_id=str(source.user_id),
                    limit=_MAX_REFRESH_PER_USER,
                )
                continue
            specs.append(
                {
                    "user_id": source.user_id,
                    "parent_job_id": source.id,
                    "name": source.name,
                    "mode": source.mode,
                    "plan_tier_snapshot": owner.plan_tier,
                    "job_kind": "refresh",
                    "request_hash": source.request_hash,
                    "refresh_key": f"{source.user_id}:{source.request_hash}:{refresh_day}:{source.mode}",
                    "refresh_daily": source.refresh_daily,
                    "refresh_priority": source.refresh_priority,
                    "candidate_count": source.candidate_count,
                    "request_snapshot_json": source.request_snapshot_json,
                    "warnings_json": [],
                    "ranking_version": source.ranking_version,
                    "engine_version": source.engine_version,
                }
            )
            user_refresh_counts[source.user_id] = user_count + 1
        return specs

    def create_scheduled_refresh_jobs(self, limit: int = 25) -> list[ScannerJob]:
        created_jobs: list[ScannerJob] = []
        for spec in self._list_scheduled_refresh_specs(limit=limit):
            job = ScannerJob(
                user_id=spec["user_id"],
                parent_job_id=spec["parent_job_id"],
                name=spec["name"],
                status="queued",
                mode=spec["mode"],
                plan_tier_snapshot=spec["plan_tier_snapshot"],
                job_kind=spec["job_kind"],
                request_hash=spec["request_hash"],
                refresh_key=spec["refresh_key"],
                refresh_daily=spec["refresh_daily"],
                refresh_priority=spec["refresh_priority"],
                candidate_count=spec["candidate_count"],
                evaluated_candidate_count=0,
                recommendation_count=0,
                request_snapshot_json=spec["request_snapshot_json"],
                warnings_json=spec["warnings_json"],
                ranking_version=spec["ranking_version"],
                engine_version=spec["engine_version"],
            )
            try:
                nested = self.session.begin_nested()
                self.repository.add(job)
                nested.commit()
                self.session.refresh(job)
                created_jobs.append(job)
            except IntegrityError:
                nested.rollback()
                continue
        return created_jobs

    def create_and_dispatch_scheduled_refresh_jobs(
        self,
        *,
        limit: int = 25,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> tuple[int, int]:
        from apps.api.app.dispatch import DispatchResult, dispatch_celery_task

        dispatched = 0
        pending_recovery = 0
        for spec in self._list_scheduled_refresh_specs(limit=limit):
            job = ScannerJob(
                user_id=spec["user_id"],
                parent_job_id=spec["parent_job_id"],
                name=spec["name"],
                status="queued",
                mode=spec["mode"],
                plan_tier_snapshot=spec["plan_tier_snapshot"],
                job_kind=spec["job_kind"],
                request_hash=spec["request_hash"],
                refresh_key=spec["refresh_key"],
                refresh_daily=spec["refresh_daily"],
                refresh_priority=spec["refresh_priority"],
                candidate_count=spec["candidate_count"],
                evaluated_candidate_count=0,
                recommendation_count=0,
                request_snapshot_json=spec["request_snapshot_json"],
                warnings_json=spec["warnings_json"],
                ranking_version=spec["ranking_version"],
                engine_version=spec["engine_version"],
            )
            try:
                self.repository.add(job)
                result = dispatch_celery_task(
                    db=self.session,
                    job=job,
                    task_name="scans.run_job",
                    task_kwargs={"job_id": str(job.id)},
                    queue=_SCAN_QUEUE,
                    log_event="scan.refresh",
                    logger=dispatch_logger or logger,
                    request_id=request_id,
                    traceparent=traceparent,
                )
                if result == DispatchResult.SENT:
                    dispatched += 1
                elif result == DispatchResult.ENQUEUE_FAILED:
                    pending_recovery += 1
                self.session.refresh(job)
            except IntegrityError:
                self.session.rollback()
                continue
            except Exception:
                logger.exception("refresh.dispatch_failed", parent_job_id=str(spec["parent_job_id"]))
                self.session.rollback()
        return dispatched, pending_recovery

    def _build_forecast_impl(
        self,
        *,
        user: User,
        symbol: str,
        strategy_type: str | None,
        horizon_days: int,
    ) -> ForecastEnvelopeResponse:
        ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        from backtestforecast.schemas.backtests import StrategyType
        effective_strategy = strategy_type or "long_call"
        try:
            StrategyType(effective_strategy)
        except ValueError as exc:
            raise AppValidationError(f"Unknown strategy_type: {effective_strategy}") from exc
        today = market_date_today()
        request = CreateBacktestRunRequest(
            symbol=symbol,
            strategy_type=effective_strategy,
            start_date=today - timedelta(days=365),
            end_date=today - timedelta(days=1),
            target_dte=max(horizon_days, 1),
            dte_tolerance_days=min(5, max(horizon_days, 1) - 1),
            max_holding_days=horizon_days,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            entry_rules=[RsiRule(type="rsi", operator="lte", threshold=Decimal("40"), period=14)],
        )
        bundle = self.execution_service.market_data_service.prepare_backtest(request)
        forecast = self._forecast_for_bundle(
            symbol=symbol,
            strategy_type=effective_strategy,
            bars=bundle.bars,
            horizon_days=horizon_days,
        )
        expected_move_abs_pct = max(
            abs(forecast.expected_return_low_pct),
            abs(forecast.expected_return_high_pct),
        )
        return ForecastEnvelopeResponse(
            forecast=forecast,
            expected_move_abs_pct=expected_move_abs_pct,
            probabilistic_note=(
                "This range is probabilistic, derived from historical analog setups, "
                "and is not financial advice or a certainty of future results."
            ),
        )

    def _enforce_concurrent_scan_limit(self, user: User) -> None:
        from sqlalchemy import func, select
        concurrent = self.session.scalar(
            select(func.count()).select_from(ScannerJob).where(
                ScannerJob.user_id == user.id,
                ScannerJob.status.in_(["queued", "running"]),
            )
        ) or 0
        if concurrent >= self._MAX_CONCURRENT_SCANS:
            raise QuotaExceededError(
                f"Maximum concurrent scans ({self._MAX_CONCURRENT_SCANS}) reached. "
                "Wait for existing scans to complete.",
                current_tier=user.plan_tier or "free",
            )

    def _validate_limits(
        self,
        policy: ScannerAccessPolicy,
        payload: CreateScannerJobRequest,
    ) -> None:
        if len(payload.symbols) > policy.max_symbols:
            raise AppValidationError(f"The selected scanner mode allows at most {policy.max_symbols} symbols.")
        if len(payload.strategy_types) > policy.max_strategies:
            raise AppValidationError(f"The selected scanner mode allows at most {policy.max_strategies} strategies.")
        if len(payload.rule_sets) > policy.max_rule_sets:
            raise AppValidationError(f"The selected scanner mode allows at most {policy.max_rule_sets} rule sets.")

    def _count_compatible_candidates(self, payload: CreateScannerJobRequest) -> tuple[int, list[dict[str, Any]]]:
        count = 0
        warnings: list[dict[str, Any]] = []
        for strategy in payload.strategy_types:
            for rule_set in payload.rule_sets:
                if not is_strategy_rule_set_compatible(strategy.value, rule_set.entry_rules):
                    warnings.append(
                        {
                            "code": "incompatible_combination",
                            "message": (
                                f"{strategy.value} is incompatible with the directional bias "
                                f"of rule set '{rule_set.name}'."
                            ),
                        }
                    )
                    continue
                count += len(payload.symbols)
        unique_warnings = {warning["message"]: warning for warning in warnings}
        return count, list(unique_warnings.values())

    def _prepare_bundles(
        self,
        payload: CreateScannerJobRequest,
        warnings: list[dict[str, Any]],
    ) -> dict[str, HistoricalDataBundle]:
        if not payload.symbols:
            raise AppValidationError("At least one symbol is required.")
        if not payload.rule_sets:
            raise AppValidationError("At least one rule set is required.")
        all_rules = [rule for rule_set in payload.rule_sets for rule in rule_set.entry_rules]
        if not all_rules:
            logger.warning(
                "prepare_bundles_fallback_rules",
                msg="No entry rules aggregated from rule_sets; using fallback RSI rule for warmup calculation",
                fallback_threshold=get_settings().fallback_entry_rule_rsi_threshold,
            )
            warnings.append({
                "code": "fallback_entry_rules",
                "message": "No entry rules were provided in any rule set. A default RSI rule was used for indicator warmup only.",
            })
        representative = CreateBacktestRunRequest(
            symbol=payload.symbols[0],
            strategy_type=payload.strategy_types[0],
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=all_rules or _get_fallback_entry_rules(),
        )
        bundles: dict[str, HistoricalDataBundle] = {}
        mds = self.execution_service.market_data_service

        def _fetch_one(symbol: str) -> tuple[str, HistoricalDataBundle | None, dict[str, Any] | None]:
            try:
                req = representative.model_copy(update={"symbol": symbol})
                return symbol, mds.prepare_backtest(req), None
            except AppError as exc:
                return symbol, None, {
                    "code": "symbol_data_unavailable",
                    "message": f"{symbol} could not be loaded ({exc.code})",
                    "error_code": exc.code,
                }

        from concurrent.futures import ThreadPoolExecutor, as_completed

        settings = get_settings()
        # Cap concurrency to avoid overwhelming upstream APIs with parallel requests.
        # If the upstream returns 429s, individual _fetch_one calls will fail and
        # the warning is surfaced to the user.
        max_workers = min(len(payload.symbols), settings.prefetch_max_workers)
        pool = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures = {pool.submit(_fetch_one, sym): sym for sym in payload.symbols}
            scan_timeout = settings.scan_timeout_seconds
            try:
                for future in as_completed(futures, timeout=min(300, scan_timeout)):
                    try:
                        sym, bundle, warning = future.result()
                    except Exception:
                        sym = futures[future]
                        logger.warning("scan.bundle_fetch_failed", symbol=sym, exc_info=True)
                        warnings.append({
                            "code": "symbol_data_unavailable",
                            "message": f"{sym} could not be loaded (unexpected error)",
                        })
                        continue
                    if bundle is not None:
                        bundles[sym] = bundle
                    if warning is not None:
                        warnings.append(warning)
            except TimeoutError:
                logger.warning("scan.bundle_prefetch_timeout", timeout=scan_timeout)
                warnings.append({
                    "code": "prefetch_timeout",
                    "message": f"Bundle prefetch timed out after {scan_timeout}s; some symbols may be missing.",
                })
        finally:
            try:
                pool.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pool.shutdown(wait=False, cancel_futures=True)
                logger.warning("scan.threadpool_graceful_shutdown_failed", exc_info=True)
        return bundles

    def _batch_historical_performance(
        self,
        payload: CreateScannerJobRequest,
        before: datetime,
    ) -> dict[tuple[str, str, str], Any]:
        keys: list[tuple[str, str, str]] = []
        for symbol in payload.symbols:
            for strategy in payload.strategy_types:
                for rs in payload.rule_sets:
                    if not is_strategy_rule_set_compatible(strategy.value, rs.entry_rules):
                        continue
                    keys.append((symbol, strategy.value, rule_set_hash(rs.entry_rules)))

        if not keys:
            return {}

        _BATCH_CHUNK_SIZE = 500
        raw: dict = {}
        for i in range(0, len(keys), _BATCH_CHUNK_SIZE):
            chunk = keys[i:i + _BATCH_CHUNK_SIZE]
            raw.update(self.repository.batch_list_historical_recommendations(keys=chunk, before=before))
        result: dict[tuple[str, str, str], Any] = {}
        for key, rows in raw.items():
            observations: list[HistoricalObservation] = []
            for recommendation, completed_at in rows:
                summary = recommendation.summary_json or {}
                observation = _historical_observation_from_summary(
                    completed_at=completed_at,
                    summary=summary,
                )
                if observation is not None:
                    observations.append(observation)
            result[key] = aggregate_historical_performance(observations, reference_time=before)
        return result

    def _historical_performance(
        self,
        *,
        symbol: str,
        strategy_type: str,
        candidate_rule_set_hash: str,
        before: datetime,
    ):
        observations: list[HistoricalObservation] = []
        for recommendation, completed_at in self.repository.list_historical_recommendations(
            symbol=symbol,
            strategy_type=strategy_type,
            rule_set_hash=candidate_rule_set_hash,
            before=before,
        ):
            summary = recommendation.summary_json or {}
            observation = _historical_observation_from_summary(
                completed_at=completed_at,
                summary=summary,
            )
            if observation is not None:
                observations.append(observation)
        return aggregate_historical_performance(observations, reference_time=before)

    def _forecast_for_bundle(
        self,
        *,
        symbol: str,
        strategy_type: str | None,
        bars,
        horizon_days: int,
    ) -> HistoricalAnalogForecastResponse:
        try:
            return self.forecaster.forecast(
                symbol=symbol,
                bars=bars,
                horizon_days=horizon_days,
                strategy_type=strategy_type,
            )
        except (ValueError, LookupError) as exc:
            from backtestforecast.observability.metrics import FORECAST_FALLBACK_TOTAL
            FORECAST_FALLBACK_TOTAL.inc()
            logger.warning("scan.forecast_fallback", symbol=symbol, error=str(exc)[:200], exc_info=True)
            fallback_date = bars[-1].trade_date if bars else market_date_today()
            return HistoricalAnalogForecastResponse(
                symbol=symbol,
                strategy_type=strategy_type,
                as_of_date=fallback_date,
                horizon_days=horizon_days,
                analog_count=0,
                expected_return_low_pct=Decimal("0"),
                expected_return_median_pct=Decimal("0"),
                expected_return_high_pct=Decimal("0"),
                positive_outcome_rate_pct=None,
                summary="Not enough analog history was available to build a bounded expected range for this symbol.",
                disclaimer=(
                    "This is a bounded probability range based on historical analogs "
                    "under similar daily-bar conditions. "
                    "It is not a prediction, certainty, or financial advice."
                ),
                analog_dates=[],
            )

    _serialize_summary = staticmethod(serialize_summary)
    _serialize_trade = staticmethod(serialize_trade)
    _serialize_equity_point = staticmethod(serialize_equity_point)

    @classmethod
    def _downsample_equity_curve(cls, equity_curve: list) -> list[dict[str, Any]]:
        return downsample_equity_curve(equity_curve, max_points=get_settings().max_scan_equity_points)

    @staticmethod
    def _ranking_response_model(payload: dict[str, Any]):
        from backtestforecast.schemas.scans import RankingBreakdownResponse

        return RankingBreakdownResponse.model_validate(payload)

    @staticmethod
    def _request_hash(payload: CreateScannerJobRequest) -> str:
        return _request_hash(payload)

    @staticmethod
    def _to_job_response(job: ScannerJob) -> ScannerJobResponse:
        return scanner_job_response(job)

    @staticmethod
    def _to_recommendation_response(recommendation: ScannerRecommendation) -> ScannerRecommendationResponse:
        warnings = _safe_validate_warning_list(recommendation.warnings_json)
        trades = _safe_validate_list(
            TradeJsonResponse,
            recommendation.trades_json,
            "trades",
            response_warnings=warnings,
        )
        equity_curve = _safe_validate_list(
            EquityCurvePointResponse,
            recommendation.equity_curve_json,
            "equity_curve",
            response_warnings=warnings,
        )
        persisted_trade_count = max(
            int((recommendation.ranking_features_json or {}).get("trade_count") or 0),
            int((recommendation.summary_json or {}).get("trade_count") or 0),
            len(recommendation.trades_json or []),
        )
        serialized_trade_count = max(
            int((recommendation.ranking_features_json or {}).get("serialized_trade_count") or 0),
            len(recommendation.trades_json or []),
        )
        persisted_equity_point_count = max(
            int((recommendation.ranking_features_json or {}).get("equity_point_count") or 0),
            len(recommendation.equity_curve_json or []),
        )
        serialized_equity_point_count = max(
            int((recommendation.ranking_features_json or {}).get("serialized_equity_point_count") or 0),
            len(recommendation.equity_curve_json or []),
        )
        return ScannerRecommendationResponse(
            id=recommendation.id,
            rank=recommendation.rank,
            score=recommendation.score,
            symbol=recommendation.symbol,
            strategy_type=recommendation.strategy_type,
            rule_set_name=recommendation.rule_set_name,
            request_snapshot=_safe_validate_json(
                recommendation.request_snapshot_json,
                "request_snapshot_json",
                default={},
                response_warnings=warnings,
            ),
            summary=_safe_validate_summary(
                recommendation.summary_json,
                field_name="summary_json",
                response_warnings=warnings,
            ),
            warnings=warnings,
            historical_performance=_safe_validate_model(
                HistoricalPerformanceResponse,
                recommendation.historical_performance_json,
                "historical_performance_json",
                default=None,
                response_warnings=warnings,
                required_keys={
                    "sample_count",
                    "weighted_win_rate",
                    "weighted_total_roi_pct",
                    "weighted_max_drawdown_pct",
                },
            ),
            forecast=_safe_validate_model(
                HistoricalAnalogForecastResponse,
                recommendation.forecast_json,
                "forecast_json",
                default=None,
                response_warnings=warnings,
            ),
            ranking_breakdown=_safe_validate_model(
                RankingBreakdownResponse,
                recommendation.ranking_features_json,
                "ranking_features_json",
                default=None,
                response_warnings=warnings,
                required_keys={
                    "current_performance_score",
                    "historical_performance_score",
                    "forecast_alignment_score",
                    "final_score",
                },
            ),
            trades=trades,
            equity_curve=equity_curve,
            trades_truncated=persisted_trade_count > serialized_trade_count,
            trade_items_omitted=max(persisted_trade_count - serialized_trade_count, 0),
            equity_curve_points_omitted=max(
                persisted_equity_point_count - serialized_equity_point_count,
                0,
            ),
        )


if ScanService._CANDIDATE_TIMEOUT_SECONDS >= 300:
    raise RuntimeError(
        "_CANDIDATE_TIMEOUT_SECONDS must be shorter than the worker statement_timeout (300s)"
    )
