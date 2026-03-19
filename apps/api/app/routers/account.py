"""Account management endpoints including GDPR data deletion and export."""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends, Header, Query, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.observability.metrics import ACCOUNT_DELETIONS_TOTAL
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.audit import AuditService

if TYPE_CHECKING:
    from backtestforecast.services.billing import BillingService

router = APIRouter(prefix="/account", tags=["account"])
logger = structlog.get_logger("api.account")

_EXPORT_PAGE_SIZE = 1000


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
        raise ValidationError(
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
    cancelled_ids: list = []
    try:
        cancelled_ids = billing.cancel_in_flight_jobs(user.id)
    except Exception:
        logger.warning("account.cancel_in_flight_failed", user_id=str(user.id), exc_info=True)

    stripe_sub_id = user.stripe_subscription_id
    stripe_cust_id = user.stripe_customer_id

    # DB delete FIRST, then Stripe cleanup. This ordering ensures that if
    # the DB commit fails the Stripe state is unchanged. If Stripe cleanup
    # fails after DB commit, the user is deleted but Stripe may retain a
    # customer object — acceptable since it won't be billed (no matching
    # DB user). The reverse order would leave the user's DB record intact
    # but Stripe already cancelled, which is harder to recover from.
    try:
        AuditService(db).record_always(
            event_type="account.deleted",
            subject_type="user",
            subject_id=user.id,
            user_id=user.id,
            request_id=metadata.request_id,
            ip_address=metadata.ip_address,
            metadata={
                "deleted_user_id": str(user.id),
                "clerk_user_id": user.clerk_user_id,
                "stripe_subscription_id": stripe_sub_id,
                "stripe_customer_id": stripe_cust_id,
            },
        )
        db.delete(user)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("account.delete_failed", user_id=str(user.id))
        raise

    stripe_cleanup = _cleanup_stripe(billing, stripe_sub_id, stripe_cust_id, user.id)
    ACCOUNT_DELETIONS_TOTAL.labels(stripe_cleanup_result=stripe_cleanup).inc()

    if cancelled_ids:
        BillingService.publish_cancellation_events(cancelled_ids)

    logger.warning(
        "account.deleted",
        user_id=str(user.id),
        clerk_user_id=user.clerk_user_id,
        stripe_cleanup_result=stripe_cleanup,
    )


@router.get("/me/export", response_model=dict[str, object])
def export_account_data(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=_EXPORT_PAGE_SIZE, ge=1, le=_EXPORT_PAGE_SIZE),
    offset: int = Query(default=0, ge=0, le=10_000),
) -> dict[str, Any]:
    """Export all user data for GDPR data portability.

    Returns a paginated JSON object. Each section is capped at ``limit``
    rows (default 1000). Use ``offset`` to page through large datasets.
    """
    get_rate_limiter().check(
        bucket="account:export",
        actor_key=str(user.id),
        limit=10,
        window_seconds=3600,
    )

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

    runs = backtest_repo.list_for_user(user.id, limit=limit, offset=offset)
    user_templates = template_repo.list_for_user(user.id, limit=limit, offset=offset)
    scan_jobs = scanner_repo.list_for_user(user.id, limit=limit, offset=offset)
    sweep_jobs = sweep_repo.list_for_user(user.id, limit=limit, offset=offset)
    export_jobs = export_repo.list_for_user(user.id, limit=limit, offset=offset)
    analyses = analysis_repo.list_for_user(user.id, limit=limit, offset=offset)

    return {
        "user": {
            "id": str(user.id),
            "clerk_user_id": user.clerk_user_id,
            "email": user.email,
            "plan_tier": user.plan_tier,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        },
        "pagination": {"limit": limit, "offset": offset},
        "totals": {
            "backtests": backtest_repo.count_for_user(user.id),
            "templates": template_repo.count_for_user(user.id),
            "scanner_jobs": scanner_repo.count_for_user(user.id),
            "sweep_jobs": sweep_repo.count_for_user(user.id),
            "export_jobs": export_repo.count_for_user(user.id),
            "symbol_analyses": analysis_repo.count_for_user(user.id),
        },
        "backtests": [
            {
                "id": str(r.id),
                "symbol": r.symbol,
                "strategy_type": r.strategy_type,
                "status": r.status,
                "date_from": r.date_from.isoformat() if r.date_from else None,
                "date_to": r.date_to.isoformat() if r.date_to else None,
                "trade_count": r.trade_count,
                "total_net_pnl": str(r.total_net_pnl),
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
                "backtest_run_id": str(e.backtest_run_id),
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
    }


def _cleanup_stripe(
    billing: BillingService,
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
        client = billing.get_stripe_client()
    except Exception:
        logger.warning("account.stripe_client_unavailable", user_id=str(user_id))
        return "client_unavailable"

    sub_ok = True
    cust_ok = True

    if subscription_id:
        try:
            client.subscriptions.cancel(subscription_id)
            logger.info("account.stripe_subscription_cancelled", subscription_id=subscription_id)
        except Exception:
            sub_ok = False
            logger.warning(
                "account.stripe_subscription_cancel_failed",
                subscription_id=subscription_id,
                exc_info=True,
            )

    if customer_id:
        try:
            client.customers.delete(customer_id)
            logger.info("account.stripe_customer_deleted", customer_id=customer_id)
        except Exception:
            cust_ok = False
            logger.warning(
                "account.stripe_customer_delete_failed",
                customer_id=customer_id,
                exc_info=True,
            )

    if sub_ok and cust_ok:
        return "ok"
    if sub_ok or cust_ok:
        return "partial"
    return "failed"
