from __future__ import annotations

import csv
import hashlib
import io
import time as _time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import structlog
from sqlalchemy import select
from sqlalchemy import update as sa_update_top
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import ExportFormat, ensure_export_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.errors import AppError, AppValidationError, ConflictError, NotFoundError
from backtestforecast.exports.storage import DatabaseStorage, ExportStorage, get_storage
from backtestforecast.models import (
    ExportJob,
    MultiStepEquityPoint,
    MultiStepRun,
    MultiStepTrade,
    MultiSymbolEquityPoint,
    MultiSymbolRun,
    MultiSymbolTrade,
    User,
)
from backtestforecast.observability.metrics import EXPORT_EXECUTION_DURATION_SECONDS
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.repositories.export_jobs import ExportJobRepository
from backtestforecast.schemas.backtests import BacktestTradeResponse, EquityCurvePointResponse
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobResponse
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.dispatch_recovery import (
    get_dispatch_diagnostic,
    observe_job_create_to_running_latency,
    redispatch_if_stale_queued,
)
from backtestforecast.services.export_service_helpers import (
    ExportBacktestSnapshot,
    _LOOKS_NUMERIC,
    build_export_file_name,
    export_mime_type,
    format_metric_value,
    normalize_utc,
    sanitize_csv_cell,
)
from backtestforecast.services.job_cancellation import (
    mark_job_cancelled,
    publish_cancellation_event,
    revoke_celery_task,
)
from backtestforecast.services.job_transitions import (
    cancellation_blocked_message,
    deletion_blocked_message,
    running_transition_values,
)
from backtestforecast.services.serialization import safe_validate_warning_list

logger = structlog.get_logger("services.exports")
_MAX_CSV_TRADES = 10_000
_MAX_CSV_EQUITY_POINTS = 50_000
_MAX_PDF_PAGES = 50
MAX_EXPORT_BYTES = 10 * 1024 * 1024  # 10 MB
MAX_DATABASE_DOWNLOADABLE_EXPORT_BYTES = 5 * 1024 * 1024  # 5 MB
_MAX_EXPORT_BYTES = MAX_EXPORT_BYTES


def _curve_point_field(point: Any, field: str) -> Any:
    if isinstance(point, dict):
        return point.get(field)
    return getattr(point, field, None)


def _generic_summary_from_run(run: Any) -> Any:
    from backtestforecast.schemas.backtests import BacktestSummaryResponse

    return BacktestSummaryResponse(
        trade_count=run.trade_count,
        decided_trades=run.trade_count,
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

class ExportService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
        storage: ExportStorage | None = None,
    ) -> None:
        self.session = session
        self.exports = ExportJobRepository(session)
        self.backtests = BacktestRunRepository(session)
        self.audit = AuditService(session)
        self.backtest_service = BacktestService(session)
        self._storage = storage or get_storage(settings or get_settings())

    def close(self) -> None:
        try:
            self.backtest_service.close()
        except Exception:
            logger.debug("export_service.close_failed", exc_info=True)

    def __enter__(self) -> ExportService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _max_allowed_export_bytes(self) -> int:
        if isinstance(self._storage, DatabaseStorage):
            return min(MAX_EXPORT_BYTES, MAX_DATABASE_DOWNLOADABLE_EXPORT_BYTES)
        return MAX_EXPORT_BYTES

    def _resolve_export_target(self, user_id: UUID, run_id: UUID) -> tuple[str, Any]:
        backtest_run = self.backtests.get_lightweight_for_user(run_id, user_id)
        if backtest_run is not None:
            return ("backtest", backtest_run)

        multi_symbol_run = self.session.get(MultiSymbolRun, run_id)
        if multi_symbol_run is not None and multi_symbol_run.user_id == user_id:
            return ("multi_symbol", multi_symbol_run)

        multi_step_run = self.session.get(MultiStepRun, run_id)
        if multi_step_run is not None and multi_step_run.user_id == user_id:
            return ("multi_step", multi_step_run)

        raise NotFoundError("Backtest run not found.")

    @staticmethod
    def _target_run_id(job: ExportJob) -> UUID:
        return job.backtest_run_id or job.multi_symbol_run_id or job.multi_step_run_id  # type: ignore[return-value]

    @staticmethod
    def _target_label(kind: str, run: Any) -> tuple[str, str]:
        if kind == "backtest":
            return run.symbol, run.strategy_type
        if kind == "multi_symbol":
            snapshot = run.input_snapshot_json or {}
            strategy_groups = snapshot.get("strategy_groups") or []
            group_name = strategy_groups[0].get("name") if strategy_groups and isinstance(strategy_groups[0], dict) else "multi_symbol"
            symbols = [item.get("symbol") for item in (snapshot.get("symbols") or []) if isinstance(item, dict)]
            return "+".join(filter(None, symbols)) or "multi-symbol", str(group_name)
        return run.symbol, run.workflow_type

    def enqueue_export(
        self,
        user: User,
        payload: CreateExportRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJob:
        """Create a queued export job. Caller dispatches to Celery."""
        ensure_export_access(
            user.plan_tier, user.subscription_status, payload.export_format,
            user.subscription_current_period_end,
        )
        if payload.idempotency_key:
            existing = self.exports.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing,
                    model_name="ExportJob",
                    task_name="exports.generate",
                    task_kwargs={"export_job_id": str(existing.id)},
                    queue="exports",
                    log_event="export",
                    logger=logger,
                    request_id=request_id,
                )

        target_kind, run = self._resolve_export_target(user.id, payload.run_id)
        if run.status != "succeeded":
            raise AppValidationError(
                f"Cannot export a run with status \"{run.status}\". "
                "Only succeeded runs can be exported."
            )
        if isinstance(self._storage, DatabaseStorage):
            estimated_db_size = (run.trade_count or 0) * 500
            if estimated_db_size > MAX_DATABASE_DOWNLOADABLE_EXPORT_BYTES:
                raise AppValidationError(
                    "This export is too large for database-backed delivery. Enable object storage for larger exports."
                )

        label_symbol, label_strategy = self._target_label(target_kind, run)
        export_job = ExportJob(
            user_id=user.id,
            backtest_run_id=run.id if target_kind == "backtest" else None,
            multi_symbol_run_id=run.id if target_kind == "multi_symbol" else None,
            multi_step_run_id=run.id if target_kind == "multi_step" else None,
            export_target_kind=target_kind,
            export_format=payload.export_format.value,
            status="queued",
            file_name=self._build_file_name(label_symbol, label_strategy, payload.export_format),
            mime_type=self._mime_type(payload.export_format),
            idempotency_key=payload.idempotency_key,
            expires_at=datetime.now(UTC) + timedelta(days=30),
        )
        self.exports.add(export_job)
        self.audit.record(
            event_type="export.enqueued",
            subject_type="export_job",
            subject_id=export_job.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "run_id": str(run.id),
                "run_kind": target_kind,
                "format": payload.export_format.value,
            },
        )
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if payload.idempotency_key:
                stmt = select(ExportJob).where(
                    ExportJob.user_id == user.id,
                    ExportJob.idempotency_key == payload.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        return export_job

    def create_and_dispatch_export(
        self,
        user: User,
        payload: CreateExportRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> ExportJob:
        """Create an export job and persist dispatch state transactionally."""
        from apps.api.app.dispatch import dispatch_celery_task

        export_job = self.enqueue_export(
            user,
            payload,
            request_id=request_id,
            ip_address=ip_address,
        )
        dispatch_celery_task(
            db=self.session,
            job=export_job,
            task_name="exports.generate",
            task_kwargs={"export_job_id": str(export_job.id)},
            queue="exports",
            log_event="export",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(export_job)
        return export_job

    def regenerate_failed_export(
        self,
        user: User,
        export_job_id: UUID,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> ExportJob:
        original = self.exports.get_for_user(export_job_id, user.id)
        if original is None:
            raise NotFoundError("Export job not found.")
        if original.status != "failed":
            raise ConflictError("Only failed exports can be regenerated.")

        payload = CreateExportRequest.model_validate(
            {
                "run_id": self._target_run_id(original),
                "format": original.export_format,
                "idempotency_key": f"regen-{original.id}-{uuid4().hex[:12]}",
            }
        )
        regenerated = self.create_and_dispatch_export(
            user,
            payload,
            request_id=request_id,
            ip_address=ip_address,
            traceparent=traceparent,
            dispatch_logger=dispatch_logger,
        )
        self.audit.record_always(
            event_type="export.regenerated",
            subject_type="export_job",
            subject_id=regenerated.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "previous_export_job_id": str(original.id),
                "run_id": str(self._target_run_id(original)),
                "run_kind": original.export_target_kind,
                "format": original.export_format,
            },
        )
        self.session.commit()
        self.session.refresh(regenerated)
        return regenerated

    def execute_export_by_id(self, export_job_id: UUID) -> ExportJob:
        """Generate the export content. Called by the Celery worker."""
        from sqlalchemy import update as sa_update

        export_job = self.exports.get(export_job_id, for_update=True)
        if export_job is None:
            raise NotFoundError("Export job not found.")

        if export_job.status != "queued":
            logger.info("export.execute_skipped", export_job_id=str(export_job_id), status=export_job.status)
            return export_job

        user = self.session.get(User, export_job.user_id)
        if user is None:
            now = datetime.now(UTC)
            self.session.execute(
                sa_update(ExportJob)
                .where(ExportJob.id == export_job_id)
                .values(
                    status="failed",
                    error_code="user_not_found",
                    error_message="User account not found.",
                    started_at=now,
                    completed_at=now,
                    updated_at=now,
                )
            )
            self.session.commit()
            self.session.refresh(export_job)
            return export_job

        try:
            ensure_export_access(
                user.plan_tier, user.subscription_status,
                ExportFormat(export_job.export_format),
                user.subscription_current_period_end,
            )
        except AppError:
            now = datetime.now(UTC)
            self.session.execute(
                sa_update(ExportJob)
                .where(ExportJob.id == export_job_id)
                .values(
                    status="failed",
                    error_code="entitlement_revoked",
                    error_message="Subscription no longer active.",
                    started_at=now,
                    completed_at=now,
                    updated_at=now,
                )
            )
            self.session.commit()
            self.session.refresh(export_job)
            return export_job

        rows = self.session.execute(
            sa_update(ExportJob)
            .where(ExportJob.id == export_job_id, ExportJob.status == "queued")
            .values(**running_transition_values())
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(export_job)
            return export_job
        self.session.refresh(export_job)
        observe_job_create_to_running_latency(export_job)

        target_run_id = self._target_run_id(export_job)
        target_kind = export_job.export_target_kind or "backtest"
        _exec_start = _time.monotonic()
        try:
            target_kind, run = self._resolve_export_target(export_job.user_id, target_run_id)
            if run is not None:
                trade_count = run.trade_count or 0
                _bytes_per_trade = 500 if export_job.export_format == "csv" else 200
                estimated_size = trade_count * _bytes_per_trade
                if estimated_size > self._max_allowed_export_bytes():
                    self.session.execute(
                        sa_update(ExportJob)
                        .where(ExportJob.id == export_job_id, ExportJob.status == "running")
                        .values(
                            status="failed",
                            error_code="export_too_large",
                            error_message=(
                                f"Export would exceed size limit (~{estimated_size // (1024 * 1024)} MB estimated for {trade_count} trades)."
                            ),
                            completed_at=datetime.now(UTC),
                            updated_at=datetime.now(UTC),
                        )
                    )
                    self.session.commit()
                    self.session.refresh(export_job)
                    raise AppError(
                        code="export_too_large",
                        message=f"Export would exceed size limit (~{estimated_size // (1024 * 1024)} MB estimated for {trade_count} trades).",
                    )
            detail = self._build_export_snapshot(
                user_id=export_job.user_id,
                run_id=target_run_id,
                run_kind=target_kind,
                export_format=ExportFormat(export_job.export_format),
            )
            fmt = ExportFormat(export_job.export_format)
            content = self._build_csv(detail) if fmt == ExportFormat.CSV else self._build_pdf(detail)
            if len(content) > self._max_allowed_export_bytes():
                raise ValueError(
                    f"Generated export exceeds the {self._max_allowed_export_bytes() // (1024 * 1024)} MB size limit."
                )
            # ORPHAN RISK: The storage write below happens outside the DB
            # transaction.  If the subsequent commit fails, the uploaded object
            # will remain in storage with no matching DB record pointing to it.
            # A periodic cleanup job should reconcile storage keys against the
            # export_jobs table and remove orphans.
            storage_key = self._storage.put(export_job.id, content, export_job.file_name)
            success_rows = self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id == export_job.id, ExportJob.status == "running")
                .values(
                    status="succeeded",
                    storage_key=storage_key,
                    size_bytes=len(content),
                    sha256_hex=hashlib.sha256(content).hexdigest(),
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            if success_rows.rowcount == 0:
                self.session.rollback()
                logger.warning(
                    "export.success_overwrite_prevented",
                    export_job_id=str(export_job.id),
                    msg="Concurrent status change detected; success commit skipped.",
                )
                if not isinstance(self._storage, DatabaseStorage):
                    try:
                        self._storage.delete(storage_key)
                        logger.info("export.orphan_cleaned_after_cas_fail", storage_key=storage_key)
                    except Exception:
                        logger.warning("export.orphan_cleanup_failed_after_cas", storage_key=storage_key, exc_info=True)
                self.session.refresh(export_job)
                return export_job
            if isinstance(self._storage, DatabaseStorage):
                export_job.content_bytes = content
            try:
                self.session.commit()
            except Exception:
                logger.warning(
                    "export.commit_failed_after_storage_write",
                    export_job_id=str(export_job.id),
                    storage_key=storage_key,
                    exc_info=True,
                )
                if not isinstance(self._storage, DatabaseStorage):
                    try:
                        self._storage.delete(storage_key)
                        logger.info("export.orphan_cleaned", storage_key=storage_key)
                    except Exception:
                        logger.warning(
                            "export.orphan_cleanup_failed",
                            storage_key=storage_key,
                            exc_info=True,
                        )
                raise
        except AppError:
            self._mark_export_failed(
                export_job.id,
                error_message="Export generation failed. Please try again.",
                log_event="export.execution_failed",
            )
            raise
        except (ValueError, RuntimeError) as exc:
            self._mark_export_failed(
                export_job.id,
                error_message="Export generation failed due to a data or configuration error.",
                log_event="export.terminal_failure",
                log_extra={"error": str(exc)},
            )
            raise
        except Exception:
            self._mark_export_failed(
                export_job.id,
                error_message="Export generation failed due to an unexpected error.",
                log_event="export.execution_failed",
            )
            raise
        finally:
            EXPORT_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - _exec_start)

        self.session.refresh(export_job)
        if export_job.status == "succeeded":
            try:
                self.audit.record_always(
                    event_type="export.created",
                    subject_type="export_job",
                    subject_id=export_job.id,
                    user_id=export_job.user_id,
                    metadata={
                        "run_id": str(target_run_id),
                        "run_kind": target_kind,
                        "format": export_job.export_format,
                        "size_bytes": export_job.size_bytes,
                    },
                )
                self.session.commit()
            except Exception:
                self.session.rollback()
                logger.warning(
                    "export.audit_commit_failed",
                    export_job_id=str(export_job.id),
                    exc_info=True,
                )
        elif export_job.status == "failed":
            try:
                self.audit.record(
                    event_type="export.failed",
                    subject_type="export_job",
                    subject_id=export_job.id,
                    user_id=export_job.user_id,
                    metadata={
                        "run_id": str(target_run_id),
                        "run_kind": target_kind,
                        "format": export_job.export_format,
                        "error_code": export_job.error_code or "unknown",
                    },
                )
                self.session.commit()
            except Exception:
                self.session.rollback()
                logger.warning(
                    "export.failure_audit_commit_failed",
                    export_job_id=str(export_job.id),
                    exc_info=True,
                )
        return export_job

    def _mark_export_failed(
        self,
        export_job_id: UUID,
        *,
        error_message: str,
        log_event: str,
        log_extra: dict | None = None,
    ) -> None:
        """Best-effort mark an export as failed after an exception.

        Rolls back first, then attempts a fresh UPDATE+COMMIT.  If the
        commit itself fails the job stays in "running" and the stale-job
        reaper will eventually catch it - but we log loudly so operators
        notice quickly.
        """
        self.session.rollback()
        log_kw = {"export_job_id": str(export_job_id), **(log_extra or {})}
        logger.exception(log_event, **log_kw)
        try:
            self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id == export_job_id, ExportJob.status != "succeeded")
                .values(
                    status="failed",
                    error_code="export_generation_failed",
                    error_message=error_message,
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            self.session.commit()
        except Exception:
            self.session.rollback()
            logger.error(
                "export.mark_failed_commit_failed",
                export_job_id=str(export_job_id),
                exc_info=True,
                hint="Job will remain in 'running' status until the stale-job reaper picks it up.",
            )

    def cleanup_expired_exports(self, *, batch_size: int = 100, max_batches: int = 100) -> int:
        """Delete storage content for expired exports. Returns count cleaned.

        Commits DB status changes first, then deletes storage objects. This
        ordering ensures a commit failure never leaves orphaned storage
        deletions; the worst case is an orphaned storage object that the
        periodic reconciliation job will clean up.
        """
        cleaned = 0
        now = datetime.now(UTC)
        batch_count = 0

        while batch_count < max_batches:
            batch_count += 1
            jobs = self.exports.list_expired_for_cleanup(now, batch_size)
            if not jobs:
                break

            storage_keys_to_delete: list[str] = []
            job_ids: list[object] = []
            for job in jobs:
                if job.storage_key:
                    storage_keys_to_delete.append(job.storage_key)
                job_ids.append(job.id)

            self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id.in_(job_ids))
                .values(
                    content_bytes=None,
                    storage_key=None,
                    status="expired",
                    size_bytes=0,
                    sha256_hex=None,
                    updated_at=datetime.now(UTC),
                )
            )

            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                logger.warning("cleanup.batch_commit_failed", batch=batch_count, count=len(jobs), exc_info=True)
                continue

            storage_failures = 0
            orphan_keys: list[str] = []
            _batch_start = _time.monotonic()
            _BATCH_TIMEOUT = 120.0
            for key in storage_keys_to_delete:
                if _time.monotonic() - _batch_start > _BATCH_TIMEOUT:
                    remaining = len(storage_keys_to_delete) - (storage_failures + len(storage_keys_to_delete) - len(orphan_keys))
                    logger.warning("cleanup.batch_storage_timeout", batch=batch_count, remaining_keys=remaining)
                    orphan_keys.extend(
                        k for k in storage_keys_to_delete
                        if k not in orphan_keys and k != key
                    )
                    break
                try:
                    self._storage.delete(key)
                except Exception:
                    storage_failures += 1
                    orphan_keys.append(key)
                    logger.warning("cleanup.storage_delete_failed", storage_key=key, exc_info=True)
            if orphan_keys:
                logger.error(
                    "cleanup.orphan_storage_objects",
                    orphan_count=len(orphan_keys),
                    orphan_keys=orphan_keys[:10],
                    hint="These storage keys have been cleared from the DB but "
                         "not from external storage. The maintenance.reconcile_s3_orphans "
                         "task will remove them on the next run.",
                )

            db_cleaned = len(jobs)
            storage_cleaned = len(storage_keys_to_delete) - storage_failures
            cleaned += db_cleaned
            logger.info("cleanup.batch_completed", batch=batch_count, db_cleaned=db_cleaned, storage_cleaned=storage_cleaned, storage_failures=storage_failures)

            if len(jobs) < batch_size:
                break

        return cleaned

    def get_export_status(self, user: User, export_job_id: UUID) -> ExportJobResponse:
        """Return current status of an export job (for polling)."""
        export_job = self.exports.get_for_user(export_job_id, user.id)
        if export_job is None:
            raise NotFoundError("Export not found.")
        return self.to_response(export_job, **self._resolved_execution_fields_for_export(export_job))

    def delete_for_user(self, export_job_id: UUID, user_id: UUID) -> None:
        export_job = self.exports.get_for_user(export_job_id, user_id)
        if export_job is None:
            raise NotFoundError("Export not found.")
        if export_job.status in ("queued", "running"):
            raise ConflictError(deletion_blocked_message("export job"))
        storage_key = export_job.storage_key
        # Delete DB record first, then external storage.  If the DB commit
        # fails the storage object remains (recoverable orphan).  The reverse
        # order would leave a ghost DB record pointing at deleted storage -
        # an unrecoverable state.  The reconcile_s3_orphans task cleans up
        # any storage objects left behind after successful DB deletes.
        self.session.delete(export_job)
        self.session.commit()
        if storage_key and not isinstance(self._storage, DatabaseStorage):
            try:
                self._storage.delete(storage_key)
            except Exception:
                logger.warning(
                    "export.delete_storage_cleanup_failed",
                    export_job_id=str(export_job_id),
                    storage_key=storage_key,
                    exc_info=True,
                )

    def cancel_for_user(self, export_job_id: UUID, user_id: UUID) -> ExportJobResponse:
        export_job = self.exports.get_for_user(export_job_id, user_id)
        if export_job is None:
            raise NotFoundError("Export not found.")
        if export_job.status not in ("queued", "running"):
            raise ConflictError(cancellation_blocked_message("export job"))
        task_id = mark_job_cancelled(export_job)
        self.audit.record_always(
            event_type="export.cancelled",
            subject_type="export_job",
            subject_id=export_job.id,
            user_id=user_id,
            metadata={"run_id": str(self._target_run_id(export_job)), "run_kind": export_job.export_target_kind, "reason": "user_cancelled"},
        )
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        revoke_celery_task(task_id, job_type="export", job_id=export_job.id)
        publish_cancellation_event(job_type="export", job_id=export_job.id)
        return self.to_response(export_job, **self._resolved_execution_fields_for_export(export_job))

    def create_export(
        self,
        user: User,
        payload: CreateExportRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJobResponse:
        """Synchronous create-and-execute for tests only.

        WARNING: Do not call from production code paths. Use
        ``enqueue_export`` followed by the Celery task instead.
        """
        settings = get_settings()
        if settings.app_env not in ("test", "development"):
            raise RuntimeError(
                "create_export is for tests only; use enqueue_export + Celery in production"
            )
        enqueued = self.enqueue_export(user, payload, request_id=request_id, ip_address=ip_address)
        self.session.commit()
        export_job = self.execute_export_by_id(enqueued.id)
        return self.to_response(export_job, **self._resolved_execution_fields_for_export(export_job))

    def get_export_for_download(
        self,
        user: User,
        export_job_id: UUID,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJob:
        use_db_content = isinstance(self._storage, DatabaseStorage)
        export_job = self.exports.get_for_user(export_job_id, user.id, include_content=False)
        if export_job is None:
            raise NotFoundError("Export not found.")
        if export_job.status != "succeeded":
            raise NotFoundError("Export content is not available.")
        if (
            export_job.expires_at is not None
            and self._normalize_utc(export_job.expires_at) < datetime.now(UTC)
        ):
            raise NotFoundError("Export has expired.")
        if not use_db_content and not export_job.storage_key:
            raise NotFoundError("Export content is not available.")
        # Audit event is recorded before the response streams.  This is
        # intentionally optimistic: if the response fails mid-stream, the
        # audit event is still recorded.  Deferring the commit would require
        # holding the DB session open during streaming, which is worse.
        self.audit.record_always(
            event_type="export.downloaded",
            subject_type="export_job",
            subject_id=export_job.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "run_id": str(self._target_run_id(export_job)),
                "run_kind": export_job.export_target_kind,
                "format": export_job.export_format,
            },
        )
        self.session.commit()
        return export_job

    @staticmethod
    def _build_file_name(symbol: str, strategy_type: str, export_format: ExportFormat) -> str:
        return build_export_file_name(symbol, strategy_type, export_format)

    @staticmethod
    def _mime_type(export_format: ExportFormat) -> str:
        return export_mime_type(export_format)

    def _build_export_snapshot(
        self,
        *,
        user_id: UUID,
        run_id: UUID,
        run_kind: str,
        export_format: ExportFormat,
    ) -> ExportBacktestSnapshot:
        if run_kind == "backtest":
            run = self.backtests.get_lightweight_for_user(run_id, user_id)
            if run is None:
                raise NotFoundError("Backtest run not found.")
            trade_limit = _MAX_CSV_TRADES if export_format == ExportFormat.CSV else get_settings().max_pdf_trades
            equity_limit = _MAX_CSV_EQUITY_POINTS if export_format == ExportFormat.CSV else 10_000
            trades = self.backtests.get_trades_for_run(run.id, limit=trade_limit, user_id=user_id)
            equity_points = self.backtests.get_equity_points_for_run(run.id, limit=equity_limit, user_id=user_id)
            payload_counts = self.backtests.get_payload_counts_for_run(run.id, user_id=user_id)
            snapshot = run.input_snapshot_json or {}
            resolved_parameters = ResolvedExecutionParameters.from_snapshot(
                {
                    **snapshot,
                    "risk_free_rate": float(run.risk_free_rate) if run.risk_free_rate is not None else snapshot.get("risk_free_rate"),
                }
            )
            return ExportBacktestSnapshot(
                symbol=run.symbol,
                strategy_type=run.strategy_type,
                status=run.status,
                start_date=run.date_from,
                end_date=run.date_to,
                created_at=run.created_at,
                summary=self.backtest_service._summary_response(run, decided_trades=payload_counts.decided_trade_count),
                trades=[BacktestTradeResponse.model_validate(trade) for trade in trades],
                equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity_points],
                warnings=safe_validate_warning_list(run.warnings_json),
                risk_free_rate=resolved_parameters.risk_free_rate,
                risk_free_rate_source=resolved_parameters.risk_free_rate_source,
                risk_free_rate_model=resolved_parameters.risk_free_rate_model,
                risk_free_rate_curve_points=self.backtest_service._resolve_risk_free_rate_curve_points(run),
                risk_free_rate_curve_warning=(
                    self.backtest_service._risk_free_rate_curve_payload_warning(run).get("message")
                    if self.backtest_service._risk_free_rate_curve_payload_warning(run) is not None
                    else None
                ),
            )

        trade_limit = _MAX_CSV_TRADES if export_format == ExportFormat.CSV else get_settings().max_pdf_trades
        equity_limit = _MAX_CSV_EQUITY_POINTS if export_format == ExportFormat.CSV else 10_000

        if run_kind == "multi_symbol":
            run = self.session.get(MultiSymbolRun, run_id)
            if run is None or run.user_id != user_id:
                raise NotFoundError("Multi-symbol run not found.")
            trades = list(
                self.session.scalars(
                    select(MultiSymbolTrade)
                    .where(MultiSymbolTrade.run_id == run.id)
                    .order_by(MultiSymbolTrade.entry_date.desc())
                    .limit(trade_limit)
                )
            )
            trades.reverse()
            equity_points = list(
                self.session.scalars(
                    select(MultiSymbolEquityPoint)
                    .where(MultiSymbolEquityPoint.run_id == run.id)
                    .order_by(MultiSymbolEquityPoint.trade_date.asc())
                    .limit(equity_limit)
                )
            )
            snapshot = run.input_snapshot_json or {}
            symbols = [item.get("symbol") for item in (snapshot.get("symbols") or []) if isinstance(item, dict)]
            strategy_groups = snapshot.get("strategy_groups") or []
            group_name = strategy_groups[0].get("name") if strategy_groups and isinstance(strategy_groups[0], dict) else "multi_symbol"
            return ExportBacktestSnapshot(
                symbol="+".join(filter(None, symbols)) or "multi-symbol",
                strategy_type=str(group_name),
                status=run.status,
                start_date=run.start_date,
                end_date=run.end_date,
                created_at=run.created_at,
                summary=_generic_summary_from_run(run),
                trades=[
                    BacktestTradeResponse.model_validate(
                        {
                            "id": trade.id,
                            "option_ticker": trade.option_ticker,
                            "strategy_type": trade.strategy_type,
                            "underlying_symbol": trade.symbol,
                            "entry_date": trade.entry_date,
                            "exit_date": trade.exit_date,
                            "expiration_date": trade.expiration_date or trade.exit_date,
                            "quantity": trade.quantity,
                            "dte_at_open": trade.dte_at_open or 0,
                            "holding_period_days": trade.holding_period_days or 0,
                            "holding_period_trading_days": None,
                            "entry_underlying_close": trade.entry_underlying_close or Decimal("0"),
                            "exit_underlying_close": trade.exit_underlying_close or Decimal("0"),
                            "entry_mid": trade.entry_mid or Decimal("0"),
                            "exit_mid": trade.exit_mid or Decimal("0"),
                            "gross_pnl": trade.gross_pnl,
                            "net_pnl": trade.net_pnl,
                            "total_commissions": trade.total_commissions,
                            "entry_reason": trade.entry_reason,
                            "exit_reason": trade.exit_reason,
                            "detail_json": trade.detail_json,
                        }
                    )
                    for trade in trades
                ],
                equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity_points],
                warnings=safe_validate_warning_list(run.warnings_json),
                risk_free_rate=None,
                risk_free_rate_source=None,
                risk_free_rate_model=None,
                risk_free_rate_curve_points=[],
                risk_free_rate_curve_warning=None,
            )

        run = self.session.get(MultiStepRun, run_id)
        if run is None or run.user_id != user_id:
            raise NotFoundError("Multi-step run not found.")
        trades = list(
            self.session.scalars(
                select(MultiStepTrade)
                .where(MultiStepTrade.run_id == run.id)
                .order_by(MultiStepTrade.entry_date.desc())
                .limit(trade_limit)
            )
        )
        trades.reverse()
        equity_points = list(
            self.session.scalars(
                select(MultiStepEquityPoint)
                .where(MultiStepEquityPoint.run_id == run.id)
                .order_by(MultiStepEquityPoint.trade_date.asc())
                .limit(equity_limit)
            )
        )
        return ExportBacktestSnapshot(
            symbol=run.symbol,
            strategy_type=run.workflow_type,
            status=run.status,
            start_date=run.start_date,
            end_date=run.end_date,
            created_at=run.created_at,
            summary=_generic_summary_from_run(run),
            trades=[
                BacktestTradeResponse.model_validate(
                    {
                        "id": trade.id,
                        "option_ticker": trade.option_ticker,
                        "strategy_type": trade.strategy_type,
                        "underlying_symbol": run.symbol,
                        "entry_date": trade.entry_date,
                        "exit_date": trade.exit_date,
                        "expiration_date": trade.expiration_date or trade.exit_date,
                        "quantity": trade.quantity,
                        "dte_at_open": trade.dte_at_open or 0,
                        "holding_period_days": trade.holding_period_days or 0,
                        "holding_period_trading_days": None,
                        "entry_underlying_close": trade.entry_underlying_close or Decimal("0"),
                        "exit_underlying_close": trade.exit_underlying_close or Decimal("0"),
                        "entry_mid": trade.entry_mid or Decimal("0"),
                        "exit_mid": trade.exit_mid or Decimal("0"),
                        "gross_pnl": trade.gross_pnl,
                        "net_pnl": trade.net_pnl,
                        "total_commissions": trade.total_commissions,
                        "entry_reason": trade.entry_reason,
                        "exit_reason": trade.exit_reason,
                        "detail_json": trade.detail_json,
                    }
                )
                for trade in trades
            ],
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity_points],
            warnings=safe_validate_warning_list(run.warnings_json),
            risk_free_rate=None,
            risk_free_rate_source=None,
            risk_free_rate_model=None,
            risk_free_rate_curve_points=[],
            risk_free_rate_curve_warning=None,
        )

    def _build_csv(self, detail: ExportBacktestSnapshot) -> bytes:
        estimated_rows = len(detail.trades) + len(detail.equity_curve) + 30
        estimated_bytes = estimated_rows * 200
        if estimated_bytes > self._max_allowed_export_bytes():
            raise ValueError(
                f"Estimated CSV size ({estimated_bytes // (1024 * 1024)} MB) exceeds "
                f"the {self._max_allowed_export_bytes() // (1024 * 1024)} MB limit. "
                f"Trades: {len(detail.trades)}, equity points: {len(detail.equity_curve)}."
            )
        buf = io.BytesIO()
        text_wrapper = io.TextIOWrapper(buf, encoding="utf-8", newline="")
        writer = csv.writer(text_wrapper)

        def safe_row(values: list[object]) -> list[object]:
            return [sanitize_csv_cell(value) for value in values]

        def _check_size() -> None:
            if buf.tell() > MAX_EXPORT_BYTES:
                raise ValueError(
                    f"CSV export exceeded {MAX_EXPORT_BYTES // (1024 * 1024)} MB during generation."
                )
            if buf.tell() > self._max_allowed_export_bytes():
                raise ValueError(
                    f"CSV export exceeded {self._max_allowed_export_bytes() // (1024 * 1024)} MB during generation."
                )

        writer.writerow(safe_row(["section", "field", "value"]))
        writer.writerow(safe_row(["run", "symbol", detail.symbol]))
        writer.writerow(safe_row(["run", "strategy_type", detail.strategy_type]))
        writer.writerow(safe_row(["run", "status", detail.status]))
        writer.writerow(safe_row(["run", "date_from", detail.start_date.isoformat()]))
        writer.writerow(safe_row(["run", "date_to", detail.end_date.isoformat()]))
        writer.writerow(safe_row(["run", "risk_free_rate", detail.risk_free_rate]))
        writer.writerow(safe_row(["run", "risk_free_rate_source", detail.risk_free_rate_source]))
        writer.writerow(safe_row(["run", "risk_free_rate_model", detail.risk_free_rate_model]))
        for point in detail.risk_free_rate_curve_points:
            writer.writerow(
                safe_row(
                    [
                        "run",
                        "risk_free_rate_curve_point",
                        f'{_curve_point_field(point, "trade_date")}:{_curve_point_field(point, "rate")}',
                    ]
                )
            )
        if detail.risk_free_rate_curve_warning:
            writer.writerow(safe_row(["note", detail.risk_free_rate_curve_warning]))
        writer.writerow(safe_row(["summary", "trade_count", detail.summary.trade_count]))
        writer.writerow(safe_row(["summary", "win_rate", detail.summary.win_rate]))
        writer.writerow(safe_row(["summary", "total_roi_pct", detail.summary.total_roi_pct]))
        writer.writerow(safe_row(["summary", "total_net_pnl", detail.summary.total_net_pnl]))
        writer.writerow(safe_row(["summary", "max_drawdown_pct", detail.summary.max_drawdown_pct]))
        writer.writerow(safe_row(["summary", "profit_factor", detail.summary.profit_factor]))
        writer.writerow(safe_row(["summary", "sharpe_ratio", detail.summary.sharpe_ratio]))
        writer.writerow(safe_row(["summary", "sortino_ratio", detail.summary.sortino_ratio]))
        writer.writerow(safe_row(["summary", "expectancy", detail.summary.expectancy]))
        writer.writerow(safe_row(["summary", "cagr_pct", detail.summary.cagr_pct]))
        if hasattr(detail.summary, "decided_trades") and detail.summary.decided_trades is not None:
            writer.writerow(safe_row(["summary", "decided_trades", detail.summary.decided_trades]))
        writer.writerow([])
        writer.writerow(safe_row([
            "note", "entry_value_per_share and exit_value_per_share represent the per-unit position value "
            "divided by 100 (the contract multiplier). To reconstruct trade cost: value * 100 * quantity. "
            "These are NOT raw option mid-prices from the exchange.",
        ]))
        writer.writerow(safe_row([
            "note", "Sharpe ratio uses sample standard deviation (N-1 denominator). "
            "Sortino ratio uses sample downside deviation (N-1 denominator). "
            "Win rate excludes break-even trades (net_pnl == 0) from the denominator. "
            "Values may differ from other platforms.",
        ]))
        writer.writerow([])
        writer.writerow(
            safe_row(
                [
                    "trades",
                    "option_ticker",
                    "entry_date",
                    "exit_date",
                    "quantity",
                    "entry_value_per_share",
                    "exit_value_per_share",
                    "gross_pnl",
                    "net_pnl",
                    "holding_period_days",
                    "entry_reason",
                    "exit_reason",
                ]
            )
        )
        exported_trades = detail.trades[:_MAX_CSV_TRADES]
        for trade in exported_trades:
            writer.writerow(
                safe_row(
                    [
                        "trade",
                        trade.option_ticker,
                        trade.entry_date.isoformat(),
                        trade.exit_date.isoformat(),
                        trade.quantity,
                        trade.entry_mid,
                        trade.exit_mid,
                        trade.gross_pnl,
                        trade.net_pnl,
                        trade.holding_period_days,
                        trade.entry_reason,
                        trade.exit_reason,
                    ]
                )
            )
        if len(detail.trades) > _MAX_CSV_TRADES:
            omitted_count = len(detail.trades) - _MAX_CSV_TRADES
            writer.writerow(
                safe_row([
                    "trade",
                    f"WARNING: {omitted_count} additional trades omitted. "
                    f"This export contains {_MAX_CSV_TRADES} of {len(detail.trades)} total trades. "
                    f"The full dataset is available via the API.",
                ])
            )
            logger.info(
                "export.csv_trades_truncated",
                total_trades=len(detail.trades),
                exported_trades=_MAX_CSV_TRADES,
                omitted=omitted_count,
            )
        _check_size()

        writer.writerow([])
        writer.writerow(safe_row(["equity_curve", "trade_date", "equity", "cash", "position_value", "drawdown_pct"]))
        exported_points = detail.equity_curve[:_MAX_CSV_EQUITY_POINTS]
        for point in exported_points:
            writer.writerow(
                safe_row(
                    [
                        "equity_point",
                        point.trade_date.isoformat(),
                        point.equity,
                        point.cash,
                        point.position_value,
                        point.drawdown_pct,
                    ]
                )
            )
        if len(detail.equity_curve) > _MAX_CSV_EQUITY_POINTS:
            eq_omitted = len(detail.equity_curve) - _MAX_CSV_EQUITY_POINTS
            writer.writerow(
                safe_row(
                    [
                        "equity_point",
                        f"WARNING: {eq_omitted} additional equity points omitted. "
                        f"This export contains {_MAX_CSV_EQUITY_POINTS} of "
                        f"{len(detail.equity_curve)} total points.",
                    ]
                )
            )

        _check_size()
        text_wrapper.flush()
        text_wrapper.detach()
        return buf.getvalue()

    def _build_pdf(self, detail: ExportBacktestSnapshot) -> bytes:
        try:
            from reportlab.lib.pagesizes import letter  # type: ignore
            from reportlab.lib.units import inch  # type: ignore
            from reportlab.pdfgen import canvas  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("The reportlab dependency is required for PDF export.") from exc

        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        y = height - 0.75 * inch

        _BASE_FONT = "Helvetica"
        _BOLD_FONT = "Helvetica-Bold"
        try:
            pdf.setFont(_BASE_FONT, 10)
        except KeyError:
            from reportlab.pdfbase import pdfmetrics  # type: ignore
            available = pdfmetrics.getRegisteredFontNames()
            _BASE_FONT = available[0] if available else "Courier"
            _BOLD_FONT = _BASE_FONT

        _page_number = 1

        def _draw_page_footer() -> None:
            pdf.setFont(_BASE_FONT, 8)
            pdf.drawRightString(width - 0.75 * inch, 0.5 * inch, f"Page {_page_number}")

        _truncated_at_page_limit = False
        _page_has_content = False

        def line(text: str, *, bold: bool = False, step: float = 16.0) -> bool:
            """Write a line to the PDF. Returns False if page limit was reached."""
            nonlocal y, _page_number, _truncated_at_page_limit, _page_has_content
            if _page_number > _MAX_PDF_PAGES:
                _truncated_at_page_limit = True
                return False
            pdf.setFont(_BOLD_FONT if bold else _BASE_FONT, 10 if not bold else 12)
            pdf.drawString(0.75 * inch, y, text)
            _page_has_content = True
            y -= step
            if y < 0.75 * inch:
                _draw_page_footer()
                pdf.showPage()
                _page_number += 1
                y = height - 0.75 * inch
                _page_has_content = False
            return True

        def _fmt(val: object) -> str:
            return format_metric_value(val)

        def _fmt_pct(val: object) -> str:
            return format_metric_value(val, percent=True)

        def _fmt_usd(val: object) -> str:
            return format_metric_value(val, usd=True)

        line("BacktestForecast.com Export", bold=True, step=22.0)
        line(f"Symbol: {detail.symbol}")
        line(f"Strategy: {detail.strategy_type}")
        line(f"Status: {detail.status}")
        line(f"Date range: {detail.start_date.isoformat()} to {detail.end_date.isoformat()}")
        line(f"Created: {detail.created_at.isoformat()}")
        if detail.risk_free_rate is not None:
            line(f"Risk-free rate: {_fmt(detail.risk_free_rate)}")
        if detail.risk_free_rate_source:
            line(f"Risk-free rate source: {detail.risk_free_rate_source}")
        if detail.risk_free_rate_model:
            line(f"Risk-free rate model: {detail.risk_free_rate_model}")
        if detail.risk_free_rate_curve_points:
            line(f"Risk-free rate curve points: {len(detail.risk_free_rate_curve_points)}")
            line("Risk-Free Rate Curve", bold=True, step=20.0)
            for point in detail.risk_free_rate_curve_points:
                if not line(
                    f'{_curve_point_field(point, "trade_date")}: {_fmt(_curve_point_field(point, "rate"))}',
                    step=14.0,
                ):
                    break
        if detail.risk_free_rate_curve_warning:
            line(detail.risk_free_rate_curve_warning, step=14.0)
        line("")
        line("Summary", bold=True, step=20.0)
        line(f"Trades: {detail.summary.trade_count}")
        if detail.summary.decided_trades is not None:
            line(f"Decided trades: {detail.summary.decided_trades}")
            line(f"Win rate (decided trades only): {_fmt_pct(detail.summary.win_rate)}")
        else:
            line(f"Win rate: {_fmt_pct(detail.summary.win_rate)}")
        line(f"ROI: {_fmt_pct(detail.summary.total_roi_pct)}")
        line(f"Net P&L: {_fmt_usd(detail.summary.total_net_pnl)}")
        line(f"Max drawdown: {_fmt_pct(detail.summary.max_drawdown_pct)}")
        s = detail.summary
        if s.profit_factor is not None:
            line(f"Profit factor: {_fmt(s.profit_factor)}")
        if s.sharpe_ratio is not None:
            line(f"Sharpe ratio: {_fmt(s.sharpe_ratio)}")
        if s.sortino_ratio is not None:
            line(f"Sortino ratio: {_fmt(s.sortino_ratio)}")
        line(f"Expectancy: {_fmt_usd(s.expectancy)}")
        if s.cagr_pct is not None:
            line(f"CAGR: {_fmt_pct(s.cagr_pct)}")
        line("")
        line("Trades", bold=True, step=20.0)
        max_pdf_trades = get_settings().max_pdf_trades
        trades_written = 0
        for trade in detail.trades[:max_pdf_trades]:
            ok = line(
                f"{trade.entry_date.isoformat()} -> {trade.exit_date.isoformat()} | "
                f"{trade.option_ticker} | qty {trade.quantity} | net {_fmt_usd(trade.net_pnl)}",
                step=14.0,
            )
            if not ok:
                break
            trades_written += 1
        omitted = len(detail.trades) - trades_written
        if omitted > 0:
            line(f"... {omitted} additional trades omitted (page or count limit reached) ...")

        if detail.equity_curve and not _truncated_at_page_limit:
            line("")
            line("Equity Curve Summary", bold=True, step=20.0)
            equities = [p.equity for p in detail.equity_curve]
            peak = max(equities)
            trough = min(equities)
            line(f"Starting equity: {_fmt_usd(detail.equity_curve[0].equity)}")
            line(f"Ending equity: {_fmt_usd(detail.equity_curve[-1].equity)}")
            line(f"Peak equity: {_fmt_usd(peak)}")
            line(f"Trough equity: {_fmt_usd(trough)}")
            line(f"Max drawdown: {_fmt_pct(detail.summary.max_drawdown_pct)}")
            line(f"Data points: {len(detail.equity_curve)}")

        line("")
        line("Notes", bold=True, step=20.0)
        line(
            "Entry/exit values shown per trade are per-unit position values "
            "divided by 100 (the contract multiplier).",
            step=14.0,
        )
        line(
            "To reconstruct trade cost: value x 100 x quantity. "
            "These are NOT raw option mid-prices.",
            step=14.0,
        )
        line(
            "Sharpe ratio uses sample std-dev (N-1). Sortino uses sample "
            "downside deviation (N-1). Win rate excludes break-even trades.",
            step=14.0,
        )

        if _truncated_at_page_limit:
            line(
                f"[PDF truncated at {_MAX_PDF_PAGES} pages. "
                f"Use CSV export for the full dataset.]",
                bold=True,
            )
            logger.info(
                "export.pdf_truncated",
                trade_count=len(detail.trades),
                trades_written=trades_written,
                pages=_page_number,
                max_pages=_MAX_PDF_PAGES,
            )

        if _page_has_content:
            _draw_page_footer()
            pdf.showPage()
        pdf.save()
        return buffer.getvalue()

    @staticmethod
    def _sanitize_csv_cell(value: object) -> object:
        return sanitize_csv_cell(value)

    @staticmethod
    def _format_metric_value(val: object, *, percent: bool = False, usd: bool = False) -> str:
        return format_metric_value(val, percent=percent, usd=usd)

    def _resolved_execution_fields_for_export(self, job: ExportJob) -> dict[str, Any]:
        if job.export_target_kind != "backtest":
            return {
                "risk_free_rate": None,
                "risk_free_rate_source": None,
                "risk_free_rate_model": None,
                "risk_free_rate_curve_points": [],
            }
        run = self.backtests.get_lightweight_for_user(job.backtest_run_id, job.user_id) if job.backtest_run_id is not None else None
        if run is None:
            return {
                "risk_free_rate": None,
                "risk_free_rate_source": None,
                "risk_free_rate_model": None,
                "risk_free_rate_curve_points": [],
            }
        snapshot = run.input_snapshot_json or {}
        resolved = ResolvedExecutionParameters.from_snapshot(
            {
                **snapshot,
                "risk_free_rate": float(run.risk_free_rate) if run.risk_free_rate is not None else snapshot.get("risk_free_rate"),
            }
        )
        return {
            "risk_free_rate": resolved.risk_free_rate,
            "risk_free_rate_source": resolved.risk_free_rate_source,
            "risk_free_rate_model": resolved.risk_free_rate_model,
            "risk_free_rate_curve_points": self.backtest_service._resolve_risk_free_rate_curve_points(run),
        }

    @staticmethod
    def to_response(
        job: ExportJob,
        *,
        risk_free_rate: float | None = None,
        risk_free_rate_source: str | None = None,
        risk_free_rate_model: str | None = None,
        risk_free_rate_curve_points: list[dict[str, Any]] | None = None,
    ) -> ExportJobResponse:
        effective_status = job.status
        diagnostic = get_dispatch_diagnostic(job)
        if (
            job.status == "succeeded"
            and job.expires_at is not None
            and normalize_utc(job.expires_at) < datetime.now(UTC)
        ):
            effective_status = "expired"
        return ExportJobResponse(
            id=job.id,
            run_id=job.backtest_run_id or job.multi_symbol_run_id or job.multi_step_run_id,
            export_format=job.export_format,
            status=effective_status,
            file_name=job.file_name,
            mime_type=job.mime_type,
            size_bytes=job.size_bytes,
            sha256_hex=job.sha256_hex,
            error_code=job.error_code or (diagnostic[0] if diagnostic else None),
            error_message=job.error_message or (diagnostic[1] if diagnostic else None),
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            expires_at=job.expires_at,
            risk_free_rate=risk_free_rate,
            risk_free_rate_source=risk_free_rate_source,
            risk_free_rate_model=risk_free_rate_model,
            risk_free_rate_curve_points=risk_free_rate_curve_points or [],
        )


    def get_db_content_bytes_for_download(self, user: User, export_job_id: UUID) -> bytes:
        content = self.exports.get_content_bytes_for_user(export_job_id, user.id)
        if content is None:
            raise NotFoundError("Export content is not available.")
        return content
    @staticmethod
    def _normalize_utc(dt: datetime) -> datetime:
        return normalize_utc(dt)
