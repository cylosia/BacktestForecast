from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.observability.metrics import (
    SCAN_CANDIDATE_FAILURES_TOTAL,
    SCAN_EXECUTION_DURATION_SECONDS,
    _normalize_scan_failure_reason,
)
from backtestforecast.billing.entitlements import (
    ScannerAccessPolicy,
    ensure_forecasting_access,
    resolve_scanner_policy,
    validate_strategy_access,
)

UTC = timezone.utc
from backtestforecast.config import get_settings
from backtestforecast.errors import AppError, AppValidationError, ConflictError, NotFoundError, QuotaExceededError
from backtestforecast.schemas.json_shapes import _FORECAST_REQUIRED_KEYS, validate_json_shape
from backtestforecast.utils.dates import market_date_today
from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import ScannerJob, ScannerRecommendation, User
from backtestforecast.repositories.scanner_jobs import ScannerJobRepository
from backtestforecast.scans.ranking import (
    HistoricalObservation,
    aggregate_historical_performance,
    build_ranking_breakdown,
    hash_payload,
    is_strategy_rule_set_compatible,
    recommendation_sort_key,
    rule_set_hash,
)
from backtestforecast.schemas.backtests import (
    BacktestSummaryResponse,
    CreateBacktestRunRequest,
    EquityCurvePointResponse,
    TradeJsonResponse,
    RsiRule,
)
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.schemas.scans import (
    CreateScannerJobRequest,
    HistoricalAnalogForecastResponse,
    ScannerJobListResponse,
    ScannerJobResponse,
    ScannerRecommendationListResponse,
    ScannerRecommendationResponse,
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

logger = structlog.get_logger("services.scans")


from backtestforecast.services.serialization import (
    safe_validate_json as _safe_validate_json,
    safe_validate_list as _safe_validate_list,
    safe_validate_summary as _safe_validate_summary,
)


def _get_fallback_entry_rules() -> list[RsiRule]:
    """Return a default RSI entry rule for warmup-period calculation.

    This fallback is used when no entry rules are provided by the user's
    rule sets. It affects the indicator warmup window (RSI-14 needs ~14
    bars) but does NOT affect which trades the engine takes — the engine
    still requires the user's configured entry rules to fire.

    If you change the default period here, update
    ``fallback_entry_rule_rsi_threshold`` in config.py to keep docs in sync.
    """
    threshold = get_settings().fallback_entry_rule_rsi_threshold
    return [RsiRule(type="rsi", operator="lte", threshold=Decimal(str(threshold)), period=14)]


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
        self._forecaster = forecaster
        self.repository = ScannerJobRepository(session)
        self.audit = AuditService(session)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> "ScanService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def forecaster(self) -> HistoricalAnalogForecaster:
        if self._forecaster is None:
            self._forecaster = HistoricalAnalogForecaster()
        return self._forecaster

    def create_job(self, user: User, payload: CreateScannerJobRequest) -> ScannerJob:
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
                    queue="research",
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
                queue="research",
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
            ranking_version="scanner-ranking-v1",
            engine_version="options-multileg-v2",
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
            queue="research",
            log_event="scan",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(job)
        return job

    def run_job(self, job_id: UUID) -> ScannerJob:
        job = self.repository.get(job_id, for_update=True)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status not in ("queued", "running"):
            logger.info("scan.run_job_skipped", job_id=str(job_id), status=job.status)
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
            policy = resolve_scanner_policy(
                user.plan_tier, job.mode,
                subscription_status=user.subscription_status,
                subscription_current_period_end=user.subscription_current_period_end,
            )
        except AppError:
            job.status = "failed"
            job.error_code = "entitlement_revoked"
            job.error_message = "Subscription no longer active."
            job.completed_at = datetime.now(UTC)
            self.session.commit()
            return job

        payload = CreateScannerJobRequest.model_validate(job.request_snapshot_json)
        self._validate_limits(policy, payload)

        from sqlalchemy import update as sa_update
        rows_updated = self.session.execute(
            sa_update(ScannerJob)
            .where(ScannerJob.id == job.id, ScannerJob.status == "queued")
            .values(
                status="running",
                started_at=datetime.now(UTC),
                completed_at=None,
                error_code=None,
                error_message=None,
                recommendation_count=0,
                evaluated_candidate_count=0,
                updated_at=datetime.now(UTC),
            )
        ).rowcount
        self.session.commit()
        if rows_updated == 0:
            logger.warning("scan.run_job_already_running", job_id=str(job.id))
            return job
        self.session.refresh(job)

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
                if job is not None and job.status not in ("succeeded", "cancelled"):
                    job.status = "failed"
                    job.error_code = "internal_error"
                    job.error_message = "An unexpected error occurred during scan execution."
                    job.completed_at = datetime.now(UTC)
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
    # Candidates are accumulated in memory and periodically trimmed (every 200)
    # to keep peak memory bounded. Low-ranked candidates have their heavy
    # fields (trades, equity_curve) cleared during trimming.
    _MAX_CANDIDATES_IN_MEMORY = 1000

    def _execute_scan(
        self,
        job: ScannerJob,
        payload: CreateScannerJobRequest,
    ) -> ScannerJob:
        import time as _time

        compatibility_candidate_count, compatibility_warnings = self._count_compatible_candidates(payload)
        job.candidate_count = compatibility_candidate_count
        warnings: list[dict[str, Any]] = list(compatibility_warnings)
        candidates: list[dict[str, Any]] = []
        forecast_cache: dict[tuple[str, str], HistoricalAnalogForecastResponse] = {}

        bundle_cache = self._prepare_bundles(payload, warnings)
        historical_cache = self._batch_historical_performance(payload, job.created_at)
        scan_start = _time.monotonic()
        _scan_timed_out = False
        _candidate_cap_hit = False
        _scan_timeout = get_settings().scan_timeout_seconds

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
            if _scan_timed_out or _candidate_cap_hit:
                break
            for strategy in payload.strategy_types:
                if _scan_timed_out or _candidate_cap_hit:
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
                            warnings.append({"type": "timeout", "message": "Scan time limit approaching; remaining candidates were skipped."})
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
                        _TRIM_INTERVAL = 200
                        if (
                            len(candidates) > 0
                            and len(candidates) % _TRIM_INTERVAL == 0
                            and len(candidates) > payload.max_recommendations * 2
                        ):
                            candidates.sort(
                                key=lambda c: recommendation_sort_key((
                                    c["symbol"], c["strategy_type"], c["rule_set_name"],
                                    self._ranking_response_model(c["ranking"]),
                                )),
                            )
                            keep = max(payload.max_recommendations * 3, _TRIM_INTERVAL)
                            for c in candidates[keep:]:
                                c["trades"] = []
                                c["equity_curve"] = []

                        if len(candidates) >= self._MAX_CANDIDATES_IN_MEMORY:
                            logger.warning("scan.candidate_cap_reached", max=self._MAX_CANDIDATES_IN_MEMORY)
                            warnings.append({"type": "candidate_cap", "message": f"Candidate cap of {self._MAX_CANDIDATES_IN_MEMORY} reached; remaining candidates were skipped."})
                            _candidate_cap_hit = True
                            break
                        candidates.append(
                            {
                                "symbol": symbol,
                                "strategy_type": strategy.value,
                                "rule_set_name": rule_set.name,
                                "rule_set_hash": candidate_rule_set_hash,
                                "request_snapshot": request.model_dump(mode="json"),
                                "summary": self._serialize_summary(execution_result.summary),
                                "warnings": execution_result.warnings,
                                "trades": [
                                    self._serialize_trade(trade)
                                    for trade in execution_result.trades[:50]
                                ],
                                "trades_truncated": len(execution_result.trades) > 50,
                                "equity_curve": self._downsample_equity_curve(
                                    execution_result.equity_curve
                                ),
                                "historical": historical.model_dump(mode="json"),
                                "forecast": forecast.model_dump(mode="json"),
                                "ranking": ranking.model_dump(mode="json"),
                            }
                        )
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

        if not candidates:
            job.status = "failed"
            job.error_code = "scan_empty"
            job.error_message = "No scan combinations completed successfully."
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
            self.session.commit()
            return job

        sorted_candidates = sorted(
            candidates,
            key=lambda c: recommendation_sort_key(
                (
                    c["symbol"],
                    c["strategy_type"],
                    c["rule_set_name"],
                    self._ranking_response_model(c["ranking"]),
                )
            ),
        )
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
                ranking_with_meta["trades_truncated"] = candidate.get("trades_truncated", False)
                job.recommendations.append(
                    ScannerRecommendation(
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

    def list_jobs(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> ScannerJobListResponse:
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
        return ScannerJobListResponse(
            items=[self._to_job_response(job) for job in jobs],
            total=total,
            offset=offset,
            limit=effective_limit,
            next_cursor=next_cursor,
        )

    def get_job(self, user: User, job_id: UUID) -> ScannerJobResponse:
        job = self.repository.get_for_user(job_id, user.id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        return self._to_job_response(job)

    def delete_for_user(self, job_id: UUID, user_id: UUID) -> None:
        job = self.repository.get_for_user(job_id, user_id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status in ("queued", "running"):
            raise ConflictError(
                "Cannot delete a job that is currently queued or running. Cancel it first."
            )
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

    def get_recommendations(
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

    def create_scheduled_refresh_jobs(self, limit: int = 25) -> list[ScannerJob]:
        created_jobs: list[ScannerJob] = []
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
            refresh_key = f"{source.user_id}:{source.request_hash}:{refresh_day}:{source.mode}"
            job = ScannerJob(
                user_id=source.user_id,
                parent_job_id=source.id,
                name=source.name,
                status="queued",
                mode=source.mode,
                plan_tier_snapshot=owner.plan_tier,
                job_kind="refresh",
                request_hash=source.request_hash,
                refresh_key=refresh_key,
                refresh_daily=source.refresh_daily,
                refresh_priority=source.refresh_priority,
                candidate_count=source.candidate_count,
                evaluated_candidate_count=0,
                recommendation_count=0,
                request_snapshot_json=source.request_snapshot_json,
                warnings_json=[],
                ranking_version=source.ranking_version,
                engine_version=source.engine_version,
            )
            try:
                nested = self.session.begin_nested()
                self.repository.add(job)
                nested.commit()
                self.session.refresh(job)
                created_jobs.append(job)
                user_refresh_counts[source.user_id] = user_count + 1
            except IntegrityError:
                nested.rollback()
                continue
        if created_jobs:
            self.session.commit()
        return created_jobs

    def build_forecast(
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
        except ValueError:
            raise AppValidationError(f"Unknown strategy_type: {effective_strategy}")
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
        from sqlalchemy import select, func
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
                if completed_at is None:
                    continue
                summary = recommendation.summary_json or {}
                observations.append(
                    HistoricalObservation(
                        completed_at=completed_at,
                        win_rate=float(summary.get("win_rate", 0.0)),
                        total_roi_pct=float(summary.get("total_roi_pct", 0.0)),
                        total_net_pnl=float(summary.get("total_net_pnl", 0.0)),
                        max_drawdown_pct=float(summary.get("max_drawdown_pct", 0.0)),
                    )
                )
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
            if completed_at is None:
                continue
            summary = recommendation.summary_json or {}
            observations.append(
                HistoricalObservation(
                    completed_at=completed_at,
                    win_rate=float(summary.get("win_rate", 0.0)),
                    total_roi_pct=float(summary.get("total_roi_pct", 0.0)),
                    total_net_pnl=float(summary.get("total_net_pnl", 0.0)),
                    max_drawdown_pct=float(summary.get("max_drawdown_pct", 0.0)),
                )
            )
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
        base_payload = payload.model_dump(mode="json")
        base_payload.pop("name", None)
        base_payload.pop("idempotency_key", None)
        return hash_payload(base_payload)

    @staticmethod
    def _to_job_response(job: ScannerJob) -> ScannerJobResponse:
        return ScannerJobResponse(
            id=job.id,
            name=job.name,
            status=job.status,
            mode=job.mode,
            plan_tier_snapshot=job.plan_tier_snapshot,
            job_kind=job.job_kind,
            candidate_count=job.candidate_count,
            evaluated_candidate_count=job.evaluated_candidate_count,
            recommendation_count=job.recommendation_count,
            refresh_daily=job.refresh_daily,
            refresh_priority=job.refresh_priority,
            ranking_version=job.ranking_version,
            engine_version=job.engine_version,
            warnings=job.warnings_json,
            error_code=job.error_code,
            error_message=job.error_message,
            idempotency_key=job.idempotency_key,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    @staticmethod
    def _to_recommendation_response(recommendation: ScannerRecommendation) -> ScannerRecommendationResponse:
        return ScannerRecommendationResponse(
            id=recommendation.id,
            rank=recommendation.rank,
            score=recommendation.score,
            symbol=recommendation.symbol,
            strategy_type=recommendation.strategy_type,
            rule_set_name=recommendation.rule_set_name,
            request_snapshot=recommendation.request_snapshot_json,
            summary=_safe_validate_summary(recommendation.summary_json),
            warnings=recommendation.warnings_json,
            historical_performance=_safe_validate_json(recommendation.historical_performance_json, "historical_performance", default={}),
            forecast=_safe_validate_json(recommendation.forecast_json, "forecast"),
            ranking_breakdown=_safe_validate_json(recommendation.ranking_features_json, "ranking_breakdown", default={}),
            trades=_safe_validate_list(TradeJsonResponse, recommendation.trades_json, "trades"),
            equity_curve=_safe_validate_list(EquityCurvePointResponse, recommendation.equity_curve_json, "equity_curve"),
            trades_truncated=bool(
                (recommendation.ranking_features_json or {}).get(
                    "trades_truncated",
                    len(recommendation.trades_json or []) >= 50,
                )
            ),
        )


assert ScanService._CANDIDATE_TIMEOUT_SECONDS < 300, (
    "_CANDIDATE_TIMEOUT_SECONDS must be shorter than the worker statement_timeout (300s)"
)
