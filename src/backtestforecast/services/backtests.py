from __future__ import annotations

import time as _time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import structlog
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.backtests.run_warnings import build_user_warnings, make_warning, merge_warnings
from backtestforecast.backtests.types import BacktestExecutionResult
from backtestforecast.billing.entitlements import POLICIES, ScannerMode, resolve_feature_policy
from backtestforecast.config import get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.errors import (
    AppError,
    AppValidationError,
    ConflictError,
    FeatureLockedError,
    NotFoundError,
    QuotaExceededError,
)
from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade, User
from backtestforecast.observability.metrics import (
    BACKTEST_EXECUTION_DURATION_SECONDS,
    DERIVED_RESPONSE_PARTIAL_DATA_TOTAL,
    TRUNCATED_PAYLOAD_ITEMS_TOTAL,
    TRUNCATED_PAYLOADS_TOTAL,
)
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    BacktestRunHistoryItemResponse,
    BacktestRunListResponse,
    BacktestRunStatusResponse,
    BacktestSummaryResponse,
    BacktestTradeResponse,
    CompareBacktestsRequest,
    CompareBacktestsResponse,
    CreateBacktestRunRequest,
    CurrentUserResponse,
    EquityCurvePointResponse,
    FeatureAccessResponse,
    UsageSummaryResponse,
)
from backtestforecast.schemas.json_shapes import _TRADE_DETAIL_REQUIRED_KEYS, validate_json_shape
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.dispatch_recovery import (
    get_dispatch_diagnostic,
    observe_job_create_to_running_latency,
    redispatch_if_stale_queued,
)
from backtestforecast.services.job_cancellation import (
    mark_job_cancelled,
    publish_cancellation_event,
    revoke_celery_task,
)
from backtestforecast.services.job_transitions import cancellation_blocked_message, deletion_blocked_message
from backtestforecast.services.risk_free_rate import (
    build_backtest_risk_free_rate_curve,
    resolve_backtest_risk_free_rate,
)
from backtestforecast.utils import to_decimal
from backtestforecast.version import DEFAULT_ENGINE_VERSION

logger = structlog.get_logger("services.backtests")

EQUITY_CURVE_LIMIT = 10_000
_RUNNING_DELETE_CONFLICT = deletion_blocked_message("backtest run")
_SUMMARY_PROVENANCE = "persisted_run_aggregates"
_BACKTEST_QUEUE = "backtests"

class BacktestService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self.run_repository = BacktestRunRepository(session)
        self.audit = AuditService(session)
        self._execution_service = execution_service

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> BacktestService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _merge_warnings(*warning_sets: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for warning_set in warning_sets:
            for warning in warning_set or []:
                key = (str(warning.get("code", "")), str(warning.get("message", "")))
                if key in seen:
                    continue
                seen.add(key)
                merged.append(warning)
        return merged

    def _build_initial_run(
        self,
        user_id: UUID,
        request: CreateBacktestRunRequest,
        *,
        resolved_parameters: ResolvedExecutionParameters,
        status: str = "queued",
        started_at: datetime | None = None,
    ) -> BacktestRun:
        return BacktestRun(
            user_id=user_id,
            status=status,
            started_at=started_at,
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            date_from=request.start_date,
            date_to=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=to_decimal(request.account_size),
            risk_per_trade_pct=to_decimal(request.risk_per_trade_pct),
            commission_per_contract=to_decimal(request.commission_per_contract),
            risk_free_rate=to_decimal(resolved_parameters.risk_free_rate),
            input_snapshot_json={
                **request.model_dump(mode="json"),
                **resolved_parameters.to_snapshot_fields(),
            },
            idempotency_key=request.idempotency_key,
            warnings_json=build_user_warnings(
                request,
                resolved_risk_free_rate=resolved_parameters.risk_free_rate,
                risk_free_rate_source=resolved_parameters.risk_free_rate_source,
            ),
            engine_version=DEFAULT_ENGINE_VERSION,
            data_source="massive",
            trade_count=0,
            win_rate=Decimal("0"),
            total_roi_pct=Decimal("0"),
            average_win_amount=Decimal("0"),
            average_loss_amount=Decimal("0"),
            average_holding_period_days=Decimal("0"),
            average_dte_at_open=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            total_commissions=Decimal("0"),
            total_net_pnl=Decimal("0"),
            starting_equity=to_decimal(request.account_size),
            ending_equity=to_decimal(request.account_size),
        )

    @staticmethod
    def _request_payload_from_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
        payload = snapshot or {}
        allowed_fields = set(CreateBacktestRunRequest.model_fields)
        return {key: value for key, value in payload.items() if key in allowed_fields}

    def _audit_execution_parameter_resolution(
        self,
        *,
        run: BacktestRun,
        resolved_parameters: ResolvedExecutionParameters,
        user_id: UUID,
    ) -> None:
        self.audit.record_always(
            event_type="backtest.execution_parameters_resolved",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user_id,
            metadata={
                "risk_free_rate": resolved_parameters.risk_free_rate,
                "risk_free_rate_source": resolved_parameters.risk_free_rate_source,
                "risk_free_rate_field_name": resolved_parameters.risk_free_rate_field_name,
                "dividend_yield": resolved_parameters.dividend_yield,
                "source_of_truth": resolved_parameters.source_of_truth,
            },
        )

    @staticmethod
    def _resolve_enqueue_parameters(request: CreateBacktestRunRequest) -> ResolvedExecutionParameters:
        if request.risk_free_rate is not None:
            return ResolvedExecutionParameters.from_request_resolution(
                request,
                resolve_backtest_risk_free_rate(request, client=None),
            )
        settings = get_settings()
        return ResolvedExecutionParameters(
            risk_free_rate=float(settings.risk_free_rate),
            risk_free_rate_source="configured_fallback",
            risk_free_rate_field_name=None,
            risk_free_rate_model="curve_default",
            dividend_yield=float(request.dividend_yield) if request.dividend_yield is not None else 0.0,
            source_of_truth="enqueue_fallback_only",
        )

    def _refresh_worker_execution_parameters(
        self,
        *,
        run: BacktestRun,
        request: CreateBacktestRunRequest,
        resolved_parameters: ResolvedExecutionParameters,
    ) -> ResolvedExecutionParameters:
        if request.risk_free_rate is not None and resolved_parameters.risk_free_rate is not None:
            return resolved_parameters
        if resolved_parameters.risk_free_rate_source not in {None, "configured_fallback"}:
            return resolved_parameters

        refreshed_rate = resolve_backtest_risk_free_rate(
            request,
            client=self.execution_service.market_data_service.client,
        )
        refreshed_parameters = ResolvedExecutionParameters.from_request_resolution(
            request,
            refreshed_rate,
        )
        curve_points = self._snapshot_risk_free_rate_curve_points(
            request=request,
            resolved_parameters=refreshed_parameters,
        )
        run.risk_free_rate = to_decimal(refreshed_parameters.risk_free_rate)
        run.input_snapshot_json = {
            **(run.input_snapshot_json or {}),
            **refreshed_parameters.to_snapshot_fields(),
            "resolved_risk_free_rate_curve_points": curve_points,
        }
        run.warnings_json = build_user_warnings(
            request,
            resolved_risk_free_rate=refreshed_parameters.risk_free_rate,
            risk_free_rate_source=refreshed_parameters.risk_free_rate_source,
        )
        self._audit_execution_parameter_resolution(
            run=run,
            resolved_parameters=refreshed_parameters,
            user_id=run.user_id,
        )
        self.session.commit()
        self.session.refresh(run)
        return refreshed_parameters

    def enqueue(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Create a queued backtest run. The caller is responsible for dispatching to Celery."""
        if request.idempotency_key:
            existing = self.run_repository.get_by_idempotency_key(user.id, request.idempotency_key)
            if existing is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing,
                    model_name="BacktestRun",
                    task_name="backtests.run",
                    task_kwargs={"run_id": str(existing.id)},
                    queue=_BACKTEST_QUEUE,
                    log_event="backtest",
                    logger=logger,
                )

        self._enforce_backtest_quota(user)

        resolved_parameters = self._resolve_enqueue_parameters(request)
        run = self._build_initial_run(
            user.id,
            request,
            resolved_parameters=resolved_parameters,
            status="queued",
        )
        if request.risk_free_rate is None:
            run.input_snapshot_json = {
                key: value for key, value in (run.input_snapshot_json or {}).items() if key != "risk_free_rate"
            }
        self.run_repository.add(run)
        self.audit.record(
            event_type="backtest.created",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user.id,
            metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
        )
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if request.idempotency_key:
                stmt = select(BacktestRun).where(
                    BacktestRun.user_id == user.id,
                    BacktestRun.idempotency_key == request.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        self._audit_execution_parameter_resolution(
            run=run,
            resolved_parameters=resolved_parameters,
            user_id=user.id,
        )
        return run

    def create_and_dispatch(
        self,
        user: User,
        request: CreateBacktestRunRequest,
        *,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> BacktestRun:
        """Create a backtest run and persist its dispatch state transactionally."""
        from apps.api.app.dispatch import dispatch_celery_task

        run = self.enqueue(user, request)
        dispatch_celery_task(
            db=self.session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue=_BACKTEST_QUEUE,
            log_event="backtest",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(run)
        return run

    def execute_run_by_id(self, run_id: UUID) -> BacktestRun:
        """Execute the backtest for a previously enqueued run. Called by the Celery worker."""
        run = self.run_repository.get_by_id_unfiltered(run_id, for_update=True)
        if run is None:
            raise NotFoundError("Backtest run not found.")

        if run.status not in ("queued", "running"):
            return run

        user = self.session.get(User, run.user_id)
        if user is None:
            run.status = "failed"
            run.error_code = "user_not_found"
            run.error_message = "User account not found."
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            return run

        _transition_ts = datetime.now(UTC)
        rows = self.session.execute(
            update(BacktestRun)
            .where(BacktestRun.id == run_id, BacktestRun.status == "queued")
            .values(status="running", updated_at=_transition_ts, started_at=_transition_ts)
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(run)
            return run
        self.session.refresh(run)
        observe_job_create_to_running_latency(run)

        request = CreateBacktestRunRequest.model_validate(
            self._request_payload_from_snapshot(run.input_snapshot_json)
        )
        resolved_parameters = ResolvedExecutionParameters.from_snapshot(run.input_snapshot_json)
        resolved_parameters = self._refresh_worker_execution_parameters(
            run=run,
            request=request,
            resolved_parameters=resolved_parameters,
        )

        _exec_start = _time.monotonic()
        try:
            execution_result = self.execution_service.execute_request(
                request,
                resolved_parameters=resolved_parameters,
            )
            with self.session.no_autoflush:
                self._apply_execution_result(run, execution_result)
                completed_at = datetime.now(UTC)
                # CAS guard: only set status to "succeeded" if the reaper has not
                # concurrently marked this run as "failed".  The FOR UPDATE lock
                # was released by the commit on the queued->running transition, so
                # the reaper can legitimately change status while execution is in
                # progress.  no_autoflush prevents the ORM dirty state from being
                # flushed (and acquiring a row lock) before the CAS UPDATE, ensuring
                # the reaper can win the race if it changed status first.
                success_rows = self.session.execute(
                    update(BacktestRun)
                    .where(BacktestRun.id == run.id, BacktestRun.status == "running")
                    .values(
                        status="succeeded",
                        completed_at=completed_at,
                        updated_at=datetime.now(UTC),
                    )
                )
            if success_rows.rowcount == 0:
                self.session.rollback()
                logger.warning(
                    "backtest.success_overwrite_prevented",
                    run_id=str(run.id),
                    msg="Concurrent status change detected; success commit skipped.",
                )
            else:
                run.status = "succeeded"
                run.completed_at = completed_at
                self.audit.record_always(
                    event_type="backtest.completed",
                    subject_type="backtest_run",
                    subject_id=run.id,
                    user_id=run.user_id,
                    metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
                )
                self.session.commit()
        except AppError as exc:
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status.notin_(["succeeded", "cancelled"]))
                .values(
                    status="failed",
                    error_code=exc.code,
                    error_message="Backtest execution failed. Please try again.",
                    updated_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except Exception:
            logger.exception("backtest.execution_failed", run_id=str(run.id))
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status.notin_(["succeeded", "cancelled"]))
                .values(
                    status="failed",
                    error_code="internal_error",
                    error_message="An internal error occurred during backtest execution.",
                    updated_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        finally:
            BACKTEST_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - _exec_start)
            self.session.expire_all()

        stored = self.session.get(BacktestRun, run.id)
        if stored is None:
            raise NotFoundError("Backtest run was executed but could not be reloaded.")
        return stored

    def create_and_run(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Synchronous create-and-run for tests only.

        WARNING: Do not call from production code paths. Use ``enqueue``
        followed by the Celery task instead. This method bypasses the
        dispatch layer and holds a DB connection for the entire execution.
        """
        settings = get_settings()
        if settings.app_env not in ("test", "development"):
            raise RuntimeError(
                "create_and_run is for tests only; use enqueue + Celery in production"
            )
        if request.idempotency_key:
            existing = self.run_repository.get_by_idempotency_key(user.id, request.idempotency_key)
            if existing is not None:
                return existing

        self._enforce_backtest_quota(user)

        resolved_risk_free_rate = resolve_backtest_risk_free_rate(
            request,
            client=self.execution_service.market_data_service.client,
        )
        resolved_parameters = ResolvedExecutionParameters.from_request_resolution(
            request,
            resolved_risk_free_rate,
        )
        run = self._build_initial_run(
            user.id,
            request,
            resolved_parameters=resolved_parameters,
            status="running",
            started_at=datetime.now(UTC),
        )
        run.input_snapshot_json = {
            **(run.input_snapshot_json or {}),
            "resolved_risk_free_rate_curve_points": self._snapshot_risk_free_rate_curve_points(
                request=request,
                resolved_parameters=resolved_parameters,
            ),
        }
        self.run_repository.add(run)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            if request.idempotency_key:
                stmt = select(BacktestRun).where(
                    BacktestRun.user_id == user.id,
                    BacktestRun.idempotency_key == request.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        self._audit_execution_parameter_resolution(
            run=run,
            resolved_parameters=resolved_parameters,
            user_id=user.id,
        )

        try:
            execution_result = self.execution_service.execute_request(
                request,
                resolved_parameters=resolved_parameters,
            )
            self._apply_execution_result(run, execution_result)
            run.status = "succeeded"
            run.completed_at = datetime.now(UTC)
            self.session.commit()
        except AppError as exc:
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
                .values(
                    status="failed",
                    error_code=exc.code,
                    error_message="Backtest execution failed. Please try again.",
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except Exception:
            logger.exception("backtest.execution_failed", run_id=str(run.id))
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
                .values(
                    status="failed",
                    error_code="internal_error",
                    error_message="An internal error occurred during backtest execution.",
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        finally:
            self.session.expire_all()

        stored = self.run_repository.get_for_user(run.id, user.id)
        if stored is None:
            raise NotFoundError("Backtest run was created but could not be reloaded.")
        return stored

    def list_runs(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> BacktestRunListResponse:
        """List backtest runs for the user. Returns BacktestRunHistoryItemResponse items
        without equity curve data; equity curves are only included in detail/compare responses.

        Supports both offset-based and cursor-based pagination.  When
        ``cursor`` is provided, ``offset`` is ignored and keyset pagination
        is used (no scan-and-discard overhead at high page numbers).
        """
        if limit < 1:
            raise AppValidationError("limit must be >= 1")
        if offset < 0:
            raise AppValidationError("offset must be >= 0")
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
        created_since = None
        if feature_policy.history_days is not None:
            created_since = datetime.now(UTC) - timedelta(days=feature_policy.history_days)
        effective_limit = min(limit, feature_policy.history_item_limit, 200)

        try:
            cursor_before, offset = parse_cursor_param(cursor) if cursor else (None, offset)
        except Exception as exc:
            if isinstance(exc, AppError):
                raise
            raise

        runs, total = self.run_repository.list_for_user_with_capped_count(
            user.id,
            max_items=feature_policy.history_item_limit,
            limit=effective_limit + 1,
            offset=offset,
            created_since=created_since,
            cursor_before=cursor_before,
        )
        page = finalize_cursor_page(runs, total=total, offset=offset, limit=effective_limit)

        return BacktestRunListResponse(
            items=[self._to_history_item(run) for run in page.items],
            total=page.total,
            offset=page.offset,
            limit=page.limit,
            next_cursor=page.next_cursor,
        )

    def get_run_status(self, user: User, run_id: UUID) -> BacktestRunStatusResponse:
        """Lightweight status check without loading trades/equity."""
        run = self.run_repository.get_lightweight_for_user(run_id, user.id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        diagnostic = get_dispatch_diagnostic(run)
        return BacktestRunStatusResponse(
            id=run.id,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_code=run.error_code or (diagnostic[0] if diagnostic else None),
            error_message=run.error_message or (diagnostic[1] if diagnostic else None),
        )

    def get_run_for_owner(self, *, user_id: UUID, run_id: UUID, trade_limit: int = 10_000) -> BacktestRunDetailResponse:
        run = self.run_repository.get_lightweight_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        trades = self.run_repository.get_trades_for_run(run_id, limit=trade_limit, user_id=user_id)
        equity = self.run_repository.get_equity_points_for_run(run_id, limit=EQUITY_CURVE_LIMIT + 1, user_id=user_id)
        payload_counts = self.run_repository.get_payload_counts_for_run(run_id, user_id=user_id)
        persisted_trade_count = payload_counts.trade_count
        persisted_equity_point_count = payload_counts.equity_point_count
        decided_trades = payload_counts.decided_trade_count
        equity_curve_truncated = len(equity) > EQUITY_CURVE_LIMIT
        if equity_curve_truncated:
            equity_curve_points_omitted = max(persisted_equity_point_count - EQUITY_CURVE_LIMIT, 0)
            TRUNCATED_PAYLOADS_TOTAL.labels(surface="backtest_detail", kind="equity_curve").inc()
            TRUNCATED_PAYLOAD_ITEMS_TOTAL.labels(surface="backtest_detail", kind="equity_curve").inc(
                equity_curve_points_omitted
            )
        else:
            equity_curve_points_omitted = 0
        trade_items_omitted = max(persisted_trade_count - len(trades), 0)
        if persisted_trade_count > len(trades):
            TRUNCATED_PAYLOADS_TOTAL.labels(surface="backtest_detail", kind="trades").inc()
            TRUNCATED_PAYLOAD_ITEMS_TOTAL.labels(surface="backtest_detail", kind="trades").inc(
                trade_items_omitted
            )
        return self._to_detail_response(
            run,
            trades=trades,
            equity_points=equity[:EQUITY_CURVE_LIMIT],
            equity_curve_truncated=equity_curve_truncated,
            trade_items_omitted=trade_items_omitted,
            equity_curve_points_omitted=equity_curve_points_omitted,
            decided_trades=decided_trades,
            additional_warnings=self._build_response_integrity_warnings(
                surface="backtest_detail",
                run=run,
                persisted_trade_count=persisted_trade_count,
                decided_trades=decided_trades,
                returned_trade_count=len(trades),
                trade_payload_truncated=persisted_trade_count > len(trades),
                equity_curve_truncated=equity_curve_truncated,
            ),
        )

    def delete_for_user(self, run_id: UUID, user_id: UUID) -> None:
        run = self.run_repository.get_lightweight_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        if run.status in ("queued", "running"):
            raise ConflictError(_RUNNING_DELETE_CONFLICT)
        self.audit.record(
            event_type="backtest.deleted",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user_id,
            metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
        )
        self.session.delete(run)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def cancel_for_user(self, run_id: UUID, user_id: UUID) -> BacktestRunStatusResponse:
        run = self.run_repository.get_lightweight_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        if run.status not in ("queued", "running"):
            raise ConflictError(cancellation_blocked_message("backtest run"))
        task_id = mark_job_cancelled(run)
        self.audit.record_always(
            event_type="backtest.cancelled",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user_id,
            metadata={"symbol": run.symbol, "strategy_type": run.strategy_type, "reason": "user_cancelled"},
        )
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        revoke_celery_task(task_id, job_type="backtest", job_id=run.id)
        publish_cancellation_event(job_type="backtest", job_id=run.id)
        return self.get_run_status_for_owner(user_id=user_id, run_id=run_id)

    def compare_runs(self, user: User, request: CompareBacktestsRequest) -> CompareBacktestsResponse:
        if len(request.run_ids) != len(set(request.run_ids)):
            raise AppValidationError("Duplicate run IDs are not allowed in comparison requests.")
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
        limit = feature_policy.side_by_side_comparison_limit

        if len(request.run_ids) > limit:
            tier_name = feature_policy.tier.value
            if tier_name == "premium":
                raise QuotaExceededError(
                    f"You can compare up to {limit} runs at a time. "
                    f"You requested {len(request.run_ids)}.",
                    current_tier="premium",
                )
            required = "pro" if tier_name == "free" else "premium"
            raise FeatureLockedError(
                f"Your {tier_name} plan allows comparing up to {limit} runs at a time. "
                f"You requested {len(request.run_ids)}. "
                f"Upgrade to the {required} plan to compare more runs side-by-side.",
                required_tier=required,
            )

        runs = self.run_repository.get_many_for_user(request.run_ids, user.id)

        # Preserve the requested order and report missing IDs
        run_map = {run.id: run for run in runs}
        found_ids = set(run_map.keys())
        missing_ids = [rid for rid in request.run_ids if rid not in found_ids]
        if missing_ids:
            raise NotFoundError(f"One or more runs could not be found: {', '.join(str(rid) for rid in missing_ids)}")

        non_succeeded = [rid for rid in request.run_ids if run_map[rid].status != "succeeded"]
        if non_succeeded:
            raise AppValidationError(
                f"All runs must have status 'succeeded' to compare. "
                f"Non-succeeded: {', '.join(str(rid) for rid in non_succeeded)}"
            )

        ordered = [run_map[rid] for rid in request.run_ids]
        _MAX_TOTAL_COMPARE_TRADES = 8_000
        _DEFAULT_COMPARE_TRADE_LIMIT = 2_000
        num_runs = len(ordered)
        trade_limit = min(_DEFAULT_COMPARE_TRADE_LIMIT, _MAX_TOTAL_COMPARE_TRADES // max(num_runs, 1))
        all_run_ids = [r.id for r in ordered]
        trade_batches_by_run = self.run_repository.get_trades_for_runs(
            all_run_ids, limit_per_run=trade_limit, user_id=user.id,
        )
        equity_by_run = self.run_repository.get_equity_points_for_runs(
            all_run_ids,
            limit_per_run=EQUITY_CURVE_LIMIT + 1,
            user_id=user.id,
        )
        payload_counts_by_run = self.run_repository.get_payload_counts_for_runs(all_run_ids, user_id=user.id)
        truncated = any(
            trade_batches_by_run.get(run.id) is not None
            and trade_batches_by_run[run.id].exceeded_limit
        for run in ordered
        )
        if truncated:
            TRUNCATED_PAYLOADS_TOTAL.labels(surface="backtest_compare", kind="trades").inc()
            TRUNCATED_PAYLOAD_ITEMS_TOTAL.labels(surface="backtest_compare", kind="trades").inc(
                sum(
                    max(batch.total_count - len(batch.trades), 0)
                    for batch in trade_batches_by_run.values()
                )
            )
        if any(len(equity_by_run.get(run.id, [])) > EQUITY_CURVE_LIMIT for run in ordered):
            TRUNCATED_PAYLOADS_TOTAL.labels(surface="backtest_compare", kind="equity_curve").inc()
            TRUNCATED_PAYLOAD_ITEMS_TOTAL.labels(surface="backtest_compare", kind="equity_curve").inc(
                sum(
                    max(payload_counts_by_run.get(run.id).equity_point_count - EQUITY_CURVE_LIMIT, 0)
                    for run in ordered
                )
            )
        return CompareBacktestsResponse(
            items=[
                self._to_detail_response(
                    run,
                    trades=(trade_batches_by_run[run.id].trades if run.id in trade_batches_by_run else []),
                    equity_points=equity_by_run.get(run.id, [])[:EQUITY_CURVE_LIMIT],
                    equity_curve_truncated=len(equity_by_run.get(run.id, [])) > EQUITY_CURVE_LIMIT,
                    trade_items_omitted=max(
                        payload_counts_by_run.get(run.id).trade_count
                        - (len(trade_batches_by_run[run.id].trades) if run.id in trade_batches_by_run else 0),
                        0,
                    ),
                    equity_curve_points_omitted=max(
                        payload_counts_by_run.get(run.id).equity_point_count - EQUITY_CURVE_LIMIT,
                        0,
                    ) if len(equity_by_run.get(run.id, [])) > EQUITY_CURVE_LIMIT else 0,
                    decided_trades=payload_counts_by_run.get(run.id).decided_trade_count,
                    additional_warnings=self._build_response_integrity_warnings(
                        surface="backtest_compare",
                        run=run,
                        persisted_trade_count=(
                            trade_batches_by_run[run.id].total_count if run.id in trade_batches_by_run else 0
                        ),
                        decided_trades=payload_counts_by_run.get(run.id).decided_trade_count,
                        returned_trade_count=(
                            len(trade_batches_by_run[run.id].trades) if run.id in trade_batches_by_run else 0
                        ),
                        trade_payload_truncated=(
                            trade_batches_by_run[run.id].exceeded_limit if run.id in trade_batches_by_run else False
                        ),
                        equity_curve_truncated=len(equity_by_run.get(run.id, [])) > EQUITY_CURVE_LIMIT,
                    ),
                )
                for run in ordered
            ],
            comparison_limit=limit,
            trade_limit_per_run=trade_limit,
            trades_truncated=truncated,
        )

    def to_current_user_response(self, user: User) -> CurrentUserResponse:
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
        used_this_month = self._current_month_backtest_count(user)
        remaining = None
        if feature_policy.monthly_backtest_quota is not None:
            remaining = max(feature_policy.monthly_backtest_quota - used_this_month, 0)
        scanner_modes: list[str] = []
        basic_scanner_strategies: list[str] = []
        advanced_scanner_strategies: list[str] = []
        if feature_policy.basic_scanner_access:
            scanner_modes.append("basic")
            basic_scanner_strategies = sorted(POLICIES[(feature_policy.tier, ScannerMode.BASIC)].allowed_strategies)
        if feature_policy.advanced_scanner_access:
            scanner_modes.append("advanced")
            advanced_scanner_strategies = sorted(POLICIES[(feature_policy.tier, ScannerMode.ADVANCED)].allowed_strategies)
        settings = get_settings()
        return CurrentUserResponse(
            id=user.id,
            clerk_user_id=user.clerk_user_id,
            email=user.email,
            plan_tier=feature_policy.tier.value,
            subscription_status=user.subscription_status,
            subscription_billing_interval=user.subscription_billing_interval,
            subscription_current_period_end=user.subscription_current_period_end,
            cancel_at_period_end=user.cancel_at_period_end,
            created_at=user.created_at,
            features=FeatureAccessResponse(
                plan_tier=feature_policy.tier.value,
                monthly_backtest_quota=feature_policy.monthly_backtest_quota,
                history_days=feature_policy.history_days,
                history_item_limit=feature_policy.history_item_limit,
                side_by_side_comparison_limit=feature_policy.side_by_side_comparison_limit,
                forecasting_access=feature_policy.forecasting_access,
                export_formats=[
                    fmt.value for fmt in sorted(feature_policy.export_formats, key=lambda item: item.value)
                ],
                scanner_modes=scanner_modes,
                scanner_basic_allowed_strategy_types=basic_scanner_strategies,
                scanner_advanced_allowed_strategy_types=advanced_scanner_strategies,
                max_scanner_window_days=settings.max_scanner_window_days,
                max_sweep_window_days=settings.max_sweep_window_days,
                cancel_at_period_end=user.cancel_at_period_end,
            ),
            usage=UsageSummaryResponse(
                backtests_used_this_month=used_this_month,
                backtests_remaining_this_month=remaining,
            ),
        )

    def _enforce_backtest_quota(self, user: User) -> None:
        locked_user = self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        ).scalar_one_or_none()
        if locked_user is None:
            raise NotFoundError("User not found.")

        feature_policy = resolve_feature_policy(
            locked_user.plan_tier, locked_user.subscription_status, locked_user.subscription_current_period_end,
        )
        if feature_policy.monthly_backtest_quota is None:
            return
        used_this_month = self._current_month_backtest_count(locked_user)
        if used_this_month >= feature_policy.monthly_backtest_quota:
            raise QuotaExceededError(
                f"The {feature_policy.tier.value} plan allows "
                f"{feature_policy.monthly_backtest_quota} backtests per month. "
                f"You have used {used_this_month}. Upgrade your plan for more.",
                current_tier=feature_policy.tier.value,
            )

    def _current_month_backtest_count(self, user: User) -> int:
        now = datetime.now(UTC)
        month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        if now.month == 12:
            next_month_start = datetime(now.year + 1, 1, 1, tzinfo=UTC)
        else:
            next_month_start = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
        return self.run_repository.count_for_user_created_between(
            user.id,
            start_inclusive=month_start,
            end_exclusive=next_month_start,
        )

    def _apply_execution_result(self, run: BacktestRun, execution_result: BacktestExecutionResult) -> None:
        summary = execution_result.summary

        run.warnings_json = merge_warnings(run.warnings_json, execution_result.warnings)
        run.trade_count = summary.trade_count
        run.win_rate = to_decimal(summary.win_rate) or Decimal("0")
        run.total_roi_pct = to_decimal(summary.total_roi_pct) or Decimal("0")
        run.average_win_amount = to_decimal(summary.average_win_amount) or Decimal("0")
        run.average_loss_amount = to_decimal(summary.average_loss_amount) or Decimal("0")
        run.average_holding_period_days = to_decimal(summary.average_holding_period_days) or Decimal("0")
        run.average_dte_at_open = to_decimal(summary.average_dte_at_open) or Decimal("0")
        run.max_drawdown_pct = to_decimal(summary.max_drawdown_pct) or Decimal("0")
        run.total_commissions = to_decimal(summary.total_commissions) or Decimal("0")
        run.total_net_pnl = to_decimal(summary.total_net_pnl) or Decimal("0")
        run.starting_equity = to_decimal(summary.starting_equity) or Decimal("0")
        run.ending_equity = to_decimal(summary.ending_equity) or Decimal("0")
        run.profit_factor = to_decimal(summary.profit_factor, allow_infinite=True) if summary.profit_factor is not None else None
        run.payoff_ratio = to_decimal(summary.payoff_ratio, allow_infinite=True) if summary.payoff_ratio is not None else None
        run.expectancy = to_decimal(summary.expectancy) or Decimal("0")
        run.sharpe_ratio = to_decimal(summary.sharpe_ratio, allow_infinite=True) if summary.sharpe_ratio is not None else None
        run.sortino_ratio = to_decimal(summary.sortino_ratio, allow_infinite=True) if summary.sortino_ratio is not None else None
        run.cagr_pct = to_decimal(summary.cagr_pct, allow_infinite=True) if summary.cagr_pct is not None else None
        run.calmar_ratio = to_decimal(summary.calmar_ratio, allow_infinite=True) if summary.calmar_ratio is not None else None
        run.max_consecutive_wins = summary.max_consecutive_wins
        run.max_consecutive_losses = summary.max_consecutive_losses
        run.recovery_factor = to_decimal(summary.recovery_factor, allow_infinite=True) if summary.recovery_factor is not None else None

        _MAX_TRADES = 10_000
        _MAX_EQUITY_POINTS = 10_000
        _BATCH_SIZE = 2_000

        with self.session.no_autoflush:
            if execution_result.trades:
                trades_to_insert = execution_result.trades[:_MAX_TRADES]
                if len(execution_result.trades) > _MAX_TRADES:
                    logger.warning(
                        "backtests.trades_capped",
                        run_id=str(run.id),
                        total=len(execution_result.trades),
                        cap=_MAX_TRADES,
                    )
                trade_dicts: list[dict] = []
                for trade in trades_to_insert:
                    if not validate_json_shape(trade.detail_json, "BacktestTrade.detail_json", required_keys=_TRADE_DETAIL_REQUIRED_KEYS):
                        logger.warning("backtests.malformed_trade_detail_json", option_ticker=trade.option_ticker, keys=list(trade.detail_json.keys()) if trade.detail_json else [])
                    trade_dicts.append({
                        "id": uuid4(),
                        "run_id": run.id,
                        "option_ticker": trade.option_ticker,
                        "strategy_type": trade.strategy_type,
                        "underlying_symbol": trade.underlying_symbol,
                        "entry_date": trade.entry_date,
                        "exit_date": trade.exit_date,
                        "expiration_date": trade.expiration_date,
                        "quantity": trade.quantity,
                        "dte_at_open": trade.dte_at_open,
                        "holding_period_days": trade.holding_period_days,
                        "holding_period_trading_days": trade.holding_period_trading_days,
                        "entry_underlying_close": to_decimal(trade.entry_underlying_close),
                        "exit_underlying_close": to_decimal(trade.exit_underlying_close),
                        "entry_mid": to_decimal(trade.entry_mid),
                        "exit_mid": to_decimal(trade.exit_mid),
                        "gross_pnl": to_decimal(trade.gross_pnl),
                        "net_pnl": to_decimal(trade.net_pnl),
                        "total_commissions": to_decimal(trade.total_commissions),
                        "entry_reason": trade.entry_reason,
                        "exit_reason": trade.exit_reason,
                        "detail_json": trade.detail_json,
                    })
                for batch_start in range(0, len(trade_dicts), _BATCH_SIZE):
                    self.session.execute(insert(BacktestTrade).values(trade_dicts[batch_start:batch_start + _BATCH_SIZE]))

            if execution_result.equity_curve:
                curve_to_insert = execution_result.equity_curve[:_MAX_EQUITY_POINTS]
                if len(execution_result.equity_curve) > _MAX_EQUITY_POINTS:
                    logger.warning(
                        "backtests.equity_points_capped",
                        run_id=str(run.id),
                        total=len(execution_result.equity_curve),
                        cap=_MAX_EQUITY_POINTS,
                    )
                equity_dicts: list[dict] = []
                for point in curve_to_insert:
                    equity_dicts.append({
                        "id": uuid4(),
                        "run_id": run.id,
                        "trade_date": point.trade_date,
                        "equity": to_decimal(point.equity),
                        "cash": to_decimal(point.cash),
                        "position_value": to_decimal(point.position_value),
                        "drawdown_pct": to_decimal(point.drawdown_pct),
                    })
                for batch_start in range(0, len(equity_dicts), _BATCH_SIZE):
                    self.session.execute(insert(BacktestEquityPoint).values(equity_dicts[batch_start:batch_start + _BATCH_SIZE]))

        self.session.expire(run, ["trades", "equity_points"])

    @staticmethod
    def _summary_response(
        run: BacktestRun,
        *,
        decided_trades: int | None = None,
    ) -> BacktestSummaryResponse:
        """Build summary payloads from persisted run truth.

        Transport-layer slices like truncated trade lists or trimmed equity
        curves must never be used to recompute summary fields here.
        ``decided_trades`` is passed separately because it is queried as an
        aggregate; every other field comes directly from the stored run row.
        """
        return BacktestSummaryResponse(
            trade_count=run.trade_count,
            decided_trades=decided_trades,
            win_rate=run.win_rate,
            total_roi_pct=run.total_roi_pct,
            average_win_amount=run.average_win_amount,
            average_loss_amount=run.average_loss_amount,
            average_holding_period_days=run.average_holding_period_days,
            average_dte_at_open=run.average_dte_at_open,
            max_drawdown_pct=run.max_drawdown_pct,
            total_commissions=run.total_commissions,
            total_net_pnl=run.total_net_pnl,
            starting_equity=run.starting_equity,
            ending_equity=run.ending_equity,
            profit_factor=run.profit_factor,
            payoff_ratio=run.payoff_ratio,
            expectancy=run.expectancy,
            sharpe_ratio=run.sharpe_ratio,
            sortino_ratio=run.sortino_ratio,
            cagr_pct=run.cagr_pct,
            calmar_ratio=run.calmar_ratio,
            max_consecutive_wins=run.max_consecutive_wins,
            max_consecutive_losses=run.max_consecutive_losses,
            recovery_factor=run.recovery_factor,
        )

    @staticmethod
    def _build_response_integrity_warnings(
        *,
        surface: str,
        run: BacktestRun,
        persisted_trade_count: int,
        decided_trades: int | None,
        returned_trade_count: int,
        trade_payload_truncated: bool,
        equity_curve_truncated: bool,
    ) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []

        if persisted_trade_count != int(run.trade_count or 0):
            DERIVED_RESPONSE_PARTIAL_DATA_TOTAL.labels(
                surface=surface,
                reason="summary_trade_count_mismatch",
            ).inc()
            warnings.append(
                make_warning(
                    "summary_trade_count_mismatch",
                    "Persisted trade rows do not match the stored summary trade_count for this run. Summary metrics may reflect an execution-time aggregate that differs from the currently persisted trade dataset.",
                    metadata={
                        "summary_trade_count": int(run.trade_count or 0),
                        "persisted_trade_count": persisted_trade_count,
                    },
                )
            )

        if decided_trades is not None and decided_trades > persisted_trade_count:
            DERIVED_RESPONSE_PARTIAL_DATA_TOTAL.labels(
                surface=surface,
                reason="decided_trade_count_mismatch",
            ).inc()
            warnings.append(
                make_warning(
                    "decided_trade_count_mismatch",
                    "The persisted decided-trade aggregate exceeds the number of persisted trade rows. Treat win-rate context as potentially inconsistent until the run is repaired.",
                    metadata={
                        "decided_trades": decided_trades,
                        "persisted_trade_count": persisted_trade_count,
                    },
                )
            )

        if trade_payload_truncated:
            DERIVED_RESPONSE_PARTIAL_DATA_TOTAL.labels(
                surface=surface,
                reason="partial_trade_payload",
            ).inc()
            warnings.append(
                make_warning(
                    "partial_trade_payload",
                    "The returned trade list is truncated. Summary metrics come from persisted run aggregates, not from the trade slice in this response.",
                    metadata={
                        "persisted_trade_count": persisted_trade_count,
                        "returned_trade_count": returned_trade_count,
                    },
                )
            )

        if equity_curve_truncated:
            DERIVED_RESPONSE_PARTIAL_DATA_TOTAL.labels(
                surface=surface,
                reason="partial_equity_curve_payload",
            ).inc()
            warnings.append(
                make_warning(
                    "partial_equity_curve_payload",
                    "The returned equity curve is truncated to the transport limit. Use exports or narrower date windows for the full curve.",
                )
            )

        return warnings

    @staticmethod
    def _resolve_risk_free_rate(run: BacktestRun) -> float | None:
        """Return the persisted risk-free rate for this run when available."""
        snapshot = run.input_snapshot_json or {}
        return ResolvedExecutionParameters.from_snapshot(
            {
                **snapshot,
                "risk_free_rate": float(run.risk_free_rate) if run.risk_free_rate is not None else snapshot.get("risk_free_rate"),
            }
        ).risk_free_rate

    def _snapshot_risk_free_rate_curve_points(
        self,
        *,
        request: CreateBacktestRunRequest,
        resolved_parameters: ResolvedExecutionParameters,
    ) -> list[dict[str, Any]]:
        curve = build_backtest_risk_free_rate_curve(
            request,
            default_rate=resolved_parameters.risk_free_rate or 0.0,
            client=self.execution_service.market_data_service.client,
        )
        if not curve.dates or not curve.rates:
            return []
        return [
            {
                "trade_date": trade_date.isoformat(),
                "rate": rate,
            }
            for trade_date, rate in zip(curve.dates, curve.rates, strict=False)
        ]

    @staticmethod
    def _resolve_risk_free_rate_curve_points(run: BacktestRun) -> list[dict[str, Any]]:
        snapshot = run.input_snapshot_json or {}
        raw_points = snapshot.get("resolved_risk_free_rate_curve_points")
        if not isinstance(raw_points, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in raw_points:
            if not isinstance(item, dict):
                continue
            trade_date = item.get("trade_date")
            rate = item.get("rate")
            if not isinstance(trade_date, str):
                continue
            try:
                normalized_rate = float(rate)
            except (TypeError, ValueError):
                continue
            normalized.append({"trade_date": trade_date, "rate": normalized_rate})
        return normalized

    @staticmethod
    def _risk_free_rate_curve_payload_warning(run: BacktestRun) -> dict[str, Any] | None:
        snapshot = run.input_snapshot_json or {}
        raw_points = snapshot.get("resolved_risk_free_rate_curve_points")
        if not isinstance(raw_points, list):
            return None
        normalized_count = len(BacktestService._resolve_risk_free_rate_curve_points(run))
        malformed_count = len(raw_points) - normalized_count
        if malformed_count <= 0:
            return None
        return make_warning(
            "risk_free_rate_curve_partial",
            "Some persisted risk-free-rate curve points were malformed and have been omitted from this response.",
            metadata={
                "persisted_points": len(raw_points),
                "returned_points": normalized_count,
                "omitted_points": malformed_count,
            },
        )

    def _to_history_item(self, run: BacktestRun) -> BacktestRunHistoryItemResponse:
        return BacktestRunHistoryItemResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            start_date=run.date_from,
            end_date=run.date_to,
            target_dte=run.target_dte,
            max_holding_days=run.max_holding_days,
            created_at=run.created_at,
            completed_at=run.completed_at,
            summary=self._summary_response(run),
            summary_provenance=_SUMMARY_PROVENANCE,
        )

    def _to_detail_response(
        self,
        run: BacktestRun,
        *,
        trade_limit: int = 10_000,
        trades: list[BacktestTrade] | None = None,
        equity_points: list[BacktestEquityPoint] | None = None,
        equity_curve_truncated: bool | None = None,
        trade_items_omitted: int | None = None,
        equity_curve_points_omitted: int | None = None,
        decided_trades: int | None = None,
        additional_warnings: list[dict[str, Any]] | None = None,
    ) -> BacktestRunDetailResponse:
        if trades is None:
            trades = self.run_repository.get_trades_for_run(run.id, limit=trade_limit, user_id=run.user_id)
        if equity_points is None:
            equity_points = self.run_repository.get_equity_points_for_run(
                run.id,
                limit=EQUITY_CURVE_LIMIT + 1,
                user_id=run.user_id,
            )
        if equity_curve_truncated is None:
            equity_curve_truncated = len(equity_points) > EQUITY_CURVE_LIMIT
        if trade_items_omitted is None:
            trade_items_omitted = 0
        if equity_curve_points_omitted is None:
            equity_curve_points_omitted = max(len(equity_points) - EQUITY_CURVE_LIMIT, 0)
        trimmed_equity_points = equity_points[:EQUITY_CURVE_LIMIT]
        curve_warning = self._risk_free_rate_curve_payload_warning(run)
        merged_warnings = merge_warnings(
            run.warnings_json,
            additional_warnings,
            [curve_warning] if curve_warning is not None else None,
        )
        return BacktestRunDetailResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            start_date=run.date_from,
            end_date=run.date_to,
            target_dte=run.target_dte,
            dte_tolerance_days=run.dte_tolerance_days,
            max_holding_days=run.max_holding_days,
            account_size=run.account_size,
            risk_per_trade_pct=run.risk_per_trade_pct,
            commission_per_contract=run.commission_per_contract,
            engine_version=run.engine_version,
            data_source=run.data_source,
            created_at=run.created_at,
            started_at=run.started_at,
            completed_at=run.completed_at,
            warnings=merged_warnings,
            error_code=run.error_code,
            error_message=run.error_message,
            summary=self._summary_response(run, decided_trades=decided_trades),
            summary_provenance=_SUMMARY_PROVENANCE,
            trades=[BacktestTradeResponse.model_validate(trade) for trade in trades],
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in trimmed_equity_points],
            equity_curve_truncated=equity_curve_truncated,
            trade_items_omitted=trade_items_omitted,
            equity_curve_points_omitted=equity_curve_points_omitted,
            risk_free_rate=self._resolve_risk_free_rate(run),
            risk_free_rate_model=ResolvedExecutionParameters.from_snapshot(run.input_snapshot_json).risk_free_rate_model,
            risk_free_rate_curve_points=self._resolve_risk_free_rate_curve_points(run),
        )
