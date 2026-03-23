"""Account management endpoints including GDPR data deletion and export."""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends, Header, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly, get_request_metadata
from backtestforecast.db.session import get_db, get_readonly_db
from backtestforecast.errors import AppValidationError
from backtestforecast.models import User
from backtestforecast.observability.logging import short_hash
from backtestforecast.observability.metrics import ACCOUNT_DELETIONS_TOTAL
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.audit import AuditService

if TYPE_CHECKING:
    from backtestforecast.services.billing import BillingService

router = APIRouter(prefix="/account", tags=["account"])
logger = structlog.get_logger("api.account")

_EXPORT_PAGE_SIZE = 200


# ---------------------------------------------------------------------------
# GDPR export response models
# ---------------------------------------------------------------------------

class _GdprUserSummary(BaseModel):
    id: str
    clerk_user_id: str
    email: str | None
    plan_tier: str
    created_at: str | None


class _GdprPagination(BaseModel):
    limit: int
    backtests_offset: int
    templates_offset: int
    scans_offset: int
    sweeps_offset: int
    exports_offset: int
    analyses_offset: int
    audit_offset: int


class _GdprTotals(BaseModel):
    backtests: int
    templates: int
    scanner_jobs: int
    sweep_jobs: int
    export_jobs: int
    symbol_analyses: int


class _GdprBacktest(BaseModel):
    id: str
    symbol: str
    strategy_type: str
    status: str
    date_from: str | None
    date_to: str | None
    trade_count: int
    total_net_pnl: str | None
    created_at: str | None


class _GdprTemplate(BaseModel):
    id: str
    name: str
    strategy_type: str
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None


class _GdprScannerJob(BaseModel):
    id: str
    status: str
    mode: str
    recommendation_count: int
    created_at: str | None


class _GdprSweepJob(BaseModel):
    id: str
    symbol: str
    status: str
    result_count: int
    created_at: str | None


class _GdprExportJob(BaseModel):
    id: str
    backtest_run_id: str | None
    export_format: str
    status: str
    file_name: str
    size_bytes: int
    created_at: str | None


class _GdprAnalysis(BaseModel):
    id: str
    symbol: str
    status: str
    strategies_tested: int
    top_results_count: int
    created_at: str | None


class _GdprAuditEvent(BaseModel):
    id: str
    event_type: str
    subject_type: str
    created_at: str | None


class AccountDataExportResponse(BaseModel):
    """GDPR data portability export containing all user data."""
    model_config = ConfigDict(extra="allow")

    user: _GdprUserSummary
    pagination: _GdprPagination
    totals: _GdprTotals
    backtests: list[_GdprBacktest]
    templates: list[_GdprTemplate]
    scanner_jobs: list[_GdprScannerJob]
    sweep_jobs: list[_GdprSweepJob]
    export_jobs: list[_GdprExportJob]
    symbol_analyses: list[_GdprAnalysis]
    audit_events: list[_GdprAuditEvent] = []


def _count_all_for_user(db: Session, user_id: uuid.UUID) -> dict[str, int]:
    """Fetch all entity counts for a user in a single round-trip.

    Replaces 6 separate ``SELECT COUNT(*)`` queries with one query
    that uses scalar subqueries, reducing DB round-trips from 6 to 1.
    """
    from sqlalchemy import func, select

    from backtestforecast.models import (
        BacktestRun,
        BacktestTemplate,
        ExportJob,
        ScannerJob,
        SweepJob,
        SymbolAnalysis,
    )

    counts = db.execute(
        select(
            select(func.count(BacktestRun.id)).where(BacktestRun.user_id == user_id).correlate(None).scalar_subquery().label("backtests"),
            select(func.count(BacktestTemplate.id)).where(BacktestTemplate.user_id == user_id).correlate(None).scalar_subquery().label("templates"),
            select(func.count(ScannerJob.id)).where(ScannerJob.user_id == user_id).correlate(None).scalar_subquery().label("scanner_jobs"),
            select(func.count(SweepJob.id)).where(SweepJob.user_id == user_id).correlate(None).scalar_subquery().label("sweep_jobs"),
            select(func.count(ExportJob.id)).where(ExportJob.user_id == user_id).correlate(None).scalar_subquery().label("export_jobs"),
            select(func.count(SymbolAnalysis.id)).where(SymbolAnalysis.user_id == user_id).correlate(None).scalar_subquery().label("symbol_analyses"),
        )
    ).one()

    return {
        "backtests": counts.backtests,
        "templates": counts.templates,
        "scanner_jobs": counts.scanner_jobs,
        "sweep_jobs": counts.sweep_jobs,
        "export_jobs": counts.export_jobs,
        "symbol_analyses": counts.symbol_analyses,
    }


def _cleanup_export_storage(db: Session, user_id: uuid.UUID) -> None:
    """Delete S3/external storage objects for a user's exports before cascade delete.

    When ``db.delete(user)`` triggers ``ON DELETE CASCADE``, the export_jobs
    rows are removed at the database level without going through
    ``ExportService.delete_for_user()``, which normally handles storage
    cleanup.  This function pre-collects storage keys and deletes them
    so the cascade doesn't orphan S3 objects.

    Storage deletion failures are logged but do not block account deletion.
    The ``reconcile_s3_orphans`` periodic task serves as a safety net.
    """
    from sqlalchemy import select
    from backtestforecast.exports.storage import get_storage, DatabaseStorage
    from backtestforecast.models import ExportJob
    from backtestforecast.config import get_settings

    try:
        storage = get_storage(get_settings())
        if isinstance(storage, DatabaseStorage):
            return

        keys = list(db.scalars(
            select(ExportJob.storage_key)
            .where(ExportJob.user_id == user_id, ExportJob.storage_key.isnot(None))
        ))
        if not keys:
            return

        deleted = 0
        for key in keys:
            try:
                storage.delete(key)
                deleted += 1
            except Exception:
                logger.warning("account.export_storage_cleanup_failed", storage_key=key, exc_info=True)

        logger.info(
            "account.export_storage_cleaned",
            user_id=str(user_id),
            total_keys=len(keys),
            deleted=deleted,
            failed=len(keys) - deleted,
        )
    except Exception:
        logger.warning("account.export_storage_cleanup_error", user_id=str(user_id), exc_info=True)


@router.delete("/me", status_code=status.HTTP_204_NO_CONTENT)
def delete_account(
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    x_confirm_delete: str | None = Header(default=None),
) -> None:
    """Delete the current user's account and all associated data.

    Requires the ``X-Confirm-Delete: permanently-delete-my-account`` header
    to prevent accidental deletions. This is a destructive, irreversible
    operation. Cascade deletes handle child records via ON DELETE CASCADE.

    Also cancels any active Stripe subscription and deletes the Stripe
    customer object to stop billing immediately.
    """
    if x_confirm_delete != "permanently-delete-my-account":
        raise AppValidationError(
            'Account deletion requires the header '
            'X-Confirm-Delete: permanently-delete-my-account'
        )

    get_rate_limiter().check(
        bucket="account:delete",
        actor_key=str(user.id),
        limit=1,
        window_seconds=3600,
    )

    from backtestforecast.services.billing import BillingService
    billing = BillingService(db)
    try:
        cancelled_ids: list = []
        try:
            cancelled_ids = billing.cancel_in_flight_jobs(user.id)
        except Exception:
            logger.warning("account.cancel_in_flight_failed", user_id=str(user.id), exc_info=True)

        stripe_sub_id = user.stripe_subscription_id
        stripe_cust_id = user.stripe_customer_id
        saved_user_id = user.id
        _cleanup_export_storage(db, user.id)

        try:
            AuditService(db).record_always(
                event_type="account.deleted",
                subject_type="user",
                subject_id=user.id,
                user_id=None,
                request_id=metadata.request_id,
                ip_address=metadata.ip_address,
                metadata={
                    "deleted_user_id": str(user.id),
                    "clerk_user_id_hash": short_hash(user.clerk_user_id),
                    "email_hash": short_hash(user.email),
                    "plan_tier": user.plan_tier,
                    "had_stripe_subscription": stripe_sub_id is not None,
                    "had_stripe_customer": stripe_cust_id is not None,
                },
            )
            db.delete(user)
            db.commit()
        except Exception:
            db.rollback()
            logger.exception("account.delete_failed", user_id=str(user.id))
            raise

        stripe_cleanup = _cleanup_stripe(billing, stripe_sub_id, stripe_cust_id, saved_user_id)
        ACCOUNT_DELETIONS_TOTAL.labels(stripe_cleanup_result=stripe_cleanup).inc()

        if stripe_cleanup in ("partial", "failed", "client_unavailable"):
            _dispatch_stripe_cleanup_retry(stripe_sub_id, stripe_cust_id, saved_user_id, stripe_cleanup)

        if cancelled_ids:
            BillingService.publish_cancellation_events(cancelled_ids)

        logger.warning(
            "account.deleted",
            user_id=str(saved_user_id),
            stripe_cleanup_result=stripe_cleanup,
        )
    finally:
        if hasattr(billing, 'close'):
            billing.close()


@router.get("/me/export", response_model=AccountDataExportResponse)
def export_account_data(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    limit: int = Query(default=_EXPORT_PAGE_SIZE, ge=1, le=_EXPORT_PAGE_SIZE),
    offset: int = Query(default=0, ge=0, le=100_000, description="Offset for backtests"),
    templates_offset: int = Query(default=0, ge=0, le=100_000),
    scans_offset: int = Query(default=0, ge=0, le=100_000),
    sweeps_offset: int = Query(default=0, ge=0, le=100_000),
    exports_offset: int = Query(default=0, ge=0, le=100_000),
    analyses_offset: int = Query(default=0, ge=0, le=100_000),
    audit_offset: int = Query(default=0, ge=0, le=100_000),
) -> dict[str, Any]:
    """Export all user data for GDPR data portability.

    Each entity type has its own offset parameter for independent pagination.
    Returns totals so the client can determine if additional pages exist.
    """
    get_rate_limiter().check(
        bucket="account:export",
        actor_key=str(user.id),
        limit=10,
        window_seconds=3600,
    )

    try:
        from sqlalchemy import text
        db.execute(text("SET LOCAL statement_timeout = '15s'"))
    except Exception:
        pass

    from backtestforecast.repositories.audit_events import AuditEventRepository
    from backtestforecast.repositories.backtest_runs import BacktestRunRepository
    from backtestforecast.repositories.export_jobs import ExportJobRepository
    from backtestforecast.repositories.scanner_jobs import ScannerJobRepository
    from backtestforecast.repositories.sweep_jobs import SweepJobRepository
    from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository
    from backtestforecast.repositories.templates import BacktestTemplateRepository

    backtest_repo = BacktestRunRepository(db)
    template_repo = BacktestTemplateRepository(db)
    scanner_repo = ScannerJobRepository(db)
    sweep_repo = SweepJobRepository(db)
    export_repo = ExportJobRepository(db)
    analysis_repo = SymbolAnalysisRepository(db)
    audit_repo = AuditEventRepository(db)

    _gdpr_limit = min(limit, _EXPORT_PAGE_SIZE)
    runs = backtest_repo.list_for_user(user.id, limit=_gdpr_limit, offset=offset)
    user_templates = template_repo.list_for_user(user.id, limit=_gdpr_limit, offset=templates_offset)
    scan_jobs = scanner_repo.list_for_user(user.id, limit=_gdpr_limit, offset=scans_offset)
    sweep_jobs = sweep_repo.list_for_user(user.id, limit=_gdpr_limit, offset=sweeps_offset)
    export_jobs = export_repo.list_for_user(user.id, limit=_gdpr_limit, offset=exports_offset)
    analyses = analysis_repo.list_for_user(user.id, limit=_gdpr_limit, offset=analyses_offset)
    audit_events = audit_repo.list_recent(user_id=user.id, limit=_gdpr_limit, offset=audit_offset)

    totals = _count_all_for_user(db, user.id)

    return {
        "user": {
            "id": str(user.id),
            "clerk_user_id": user.clerk_user_id,
            "email": user.email,
            "plan_tier": user.plan_tier,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        },
        "pagination": {
            "limit": _gdpr_limit,
            "backtests_offset": offset,
            "templates_offset": templates_offset,
            "scans_offset": scans_offset,
            "sweeps_offset": sweeps_offset,
            "exports_offset": exports_offset,
            "analyses_offset": analyses_offset,
            "audit_offset": audit_offset,
        },
        "totals": totals,
        "backtests": [
            {
                "id": str(r.id),
                "symbol": r.symbol,
                "strategy_type": r.strategy_type,
                "status": r.status,
                "date_from": r.date_from.isoformat() if r.date_from else None,
                "date_to": r.date_to.isoformat() if r.date_to else None,
                "trade_count": r.trade_count,
                "total_net_pnl": str(r.total_net_pnl) if r.total_net_pnl is not None else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in runs
        ],
        "templates": [
            {
                "id": str(t.id),
                "name": t.name,
                "strategy_type": t.strategy_type,
                "config": t.config_json,
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in user_templates
        ],
        "scanner_jobs": [
            {
                "id": str(j.id),
                "status": j.status,
                "mode": j.mode,
                "recommendation_count": j.recommendation_count,
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in scan_jobs
        ],
        "sweep_jobs": [
            {
                "id": str(j.id),
                "symbol": j.symbol,
                "status": j.status,
                "result_count": j.result_count,
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in sweep_jobs
        ],
        "export_jobs": [
            {
                "id": str(e.id),
                "backtest_run_id": str(e.backtest_run_id) if e.backtest_run_id is not None else None,
                "export_format": e.export_format,
                "status": e.status,
                "file_name": e.file_name,
                "size_bytes": e.size_bytes,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in export_jobs
        ],
        "symbol_analyses": [
            {
                "id": str(a.id),
                "symbol": a.symbol,
                "status": a.status,
                "strategies_tested": a.strategies_tested,
                "top_results_count": a.top_results_count,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in analyses
        ],
        "audit_events": [
            {
                "id": str(ae.id),
                "event_type": ae.event_type,
                "subject_type": ae.subject_type,
                "created_at": ae.created_at.isoformat() if ae.created_at else None,
            }
            for ae in audit_events
        ],
    }


def _cleanup_stripe(
    billing: "BillingService",
    subscription_id: str | None,
    customer_id: str | None,
    user_id: uuid.UUID,
) -> str:
    """Cancel Stripe subscription and delete customer. Returns cleanup result.

    Returns one of: "skipped", "ok", "partial", "client_unavailable", "failed".
    """
    if not subscription_id and not customer_id:
        return "skipped"

    try:
        client = billing.get_stripe_client(skip_circuit_check=True)
    except Exception:
        logger.warning("account.stripe_client_unavailable", user_id=str(user_id))
        return "client_unavailable"

    sub_ok = True
    cust_ok = True

    if subscription_id:
        try:
            client.subscriptions.cancel(subscription_id)
            logger.info("account.stripe_subscription_cancelled", subscription_id_hash=short_hash(subscription_id))
        except Exception:
            sub_ok = False
            logger.warning(
                "account.stripe_subscription_cancel_failed",
                subscription_id_hash=short_hash(subscription_id),
                exc_info=True,
            )

    if customer_id:
        try:
            client.customers.delete(customer_id)
            logger.info("account.stripe_customer_deleted", customer_id_hash=short_hash(customer_id))
        except Exception:
            cust_ok = False
            logger.warning(
                "account.stripe_customer_delete_failed",
                customer_id_hash=short_hash(customer_id),
                exc_info=True,
            )

    if sub_ok and cust_ok:
        return "ok"
    if sub_ok or cust_ok:
        return "partial"
    return "failed"


def _dispatch_stripe_cleanup_retry(
    subscription_id: str | None,
    customer_id: str | None,
    user_id: uuid.UUID,
    sync_result: str,
) -> None:
    """Dispatch an async Celery task to retry Stripe cleanup.

    Called when the synchronous cleanup in ``_cleanup_stripe`` fails
    (returns "partial", "failed", or "client_unavailable"). The async
    task retries with exponential backoff up to 5 times.
    """
    try:
        from apps.worker.app.celery_app import celery_app

        celery_app.send_task(
            "maintenance.cleanup_stripe_orphan",
            kwargs={
                "subscription_id": subscription_id,
                "customer_id": customer_id,
                "user_id_str": str(user_id),
            },
            queue="maintenance",
            countdown=30,
        )
        logger.info(
            "account.stripe_cleanup_retry_dispatched",
            user_id=str(user_id),
            sync_result=sync_result,
            subscription_id_hash=short_hash(subscription_id),
            customer_id_hash=short_hash(customer_id),
        )
    except Exception:
        logger.error(
            "account.stripe_cleanup_retry_dispatch_failed",
            user_id=str(user_id),
            sync_result=sync_result,
            subscription_id_hash=short_hash(subscription_id),
            customer_id_hash=short_hash(customer_id),
            exc_info=True,
        )
