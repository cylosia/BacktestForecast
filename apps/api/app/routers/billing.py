from __future__ import annotations

import structlog
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.billing import (
    CheckoutSessionResponse,
    CreateCheckoutSessionRequest,
    CreatePortalSessionRequest,
    PortalSessionResponse,
    WebhookResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.billing import BillingService

router = APIRouter(prefix="/billing", tags=["billing"])
_webhook_logger = structlog.get_logger("api.billing.webhook")


@router.post("/checkout-session", response_model=CheckoutSessionResponse)
def create_checkout_session(
    payload: CreateCheckoutSessionRequest,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> CheckoutSessionResponse:
    settings = get_settings()
    if not settings.feature_billing_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Billing is temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="billing:checkout",
        actor_key=str(user.id),
        limit=settings.billing_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BillingService(db) as service:
        return service.create_checkout_session(
            user,
            payload,
            request_id=metadata.request_id,
            ip_address=metadata.ip_address,
        )


@router.post("/portal-session", response_model=PortalSessionResponse)
def create_portal_session(
    payload: CreatePortalSessionRequest,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> PortalSessionResponse:
    settings = get_settings()
    if not settings.feature_billing_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Billing is temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="billing:portal",
        actor_key=str(user.id),
        limit=settings.billing_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BillingService(db) as service:
        return service.create_portal_session(
            user,
            payload,
            request_id=metadata.request_id,
            ip_address=metadata.ip_address,
        )


@router.post("/webhook", status_code=status.HTTP_200_OK, response_model=WebhookResponse)
def stripe_webhook(
    request: Request,
    payload: bytes = Body(..., media_type="application/json", max_length=512_000),
    signature: str | None = Header(default=None, alias="Stripe-Signature"),
    db: Session = Depends(get_db),
) -> WebhookResponse:
    request_id = getattr(request.state, "request_id", None)
    meta = get_request_metadata(request)
    ip_address = meta.ip_address
    settings = get_settings()
    get_rate_limiter().check(
        bucket="billing:webhook",
        actor_key=f"billing:webhook:{ip_address or 'unknown'}",
        limit=30,
        window_seconds=settings.rate_limit_window_seconds,
    )
    get_rate_limiter().check(
        bucket="billing:webhook",
        actor_key="stripe_webhook_global",
        limit=300,
        window_seconds=settings.rate_limit_window_seconds,
    )

    if signature is None:
        raise HTTPException(status_code=400, detail="Missing Stripe-Signature header")

    try:
        with BillingService(db) as service:
            return service.handle_webhook(
                payload,
                signature,
                request_id=request_id,
                ip_address=ip_address,
            )
    except Exception as exc:
        from backtestforecast.errors import (
            AuthenticationError as _AuthErr,
            AppError as _AppErr,
            ExternalServiceError as _ExtErr,
            NotFoundError as _NotFoundErr,
        )
        if isinstance(exc, _AuthErr):
            _webhook_logger.warning(
                "webhook.signature_verification_failed", ip=ip_address, request_id=request_id,
            )
            raise HTTPException(
                status_code=400,
                detail={"code": "signature_verification_failed", "message": "Invalid webhook signature."},
            )
        if isinstance(exc, _NotFoundErr):
            _webhook_logger.warning(
                "webhook.user_not_found", code=exc.code, ip=ip_address, request_id=request_id,
            )
            return WebhookResponse(received=True, reason="Event processed.")

        if isinstance(exc, _ExtErr):
            _webhook_logger.exception(
                "webhook.transient_error", code=exc.code, ip=ip_address, request_id=request_id,
            )
            raise HTTPException(
                status_code=500,
                detail={"code": exc.code, "message": "Transient error; Stripe should retry."},
            )
        if isinstance(exc, _AppErr):
            _webhook_logger.warning(
                "webhook.deterministic_error", code=exc.code, ip=ip_address, request_id=request_id,
            )
            return WebhookResponse(received=True, reason=f"Deterministic error ({exc.code}); will not retry.")
        _webhook_logger.exception("webhook.unhandled_error", ip=ip_address, request_id=request_id)
        return WebhookResponse(received=True, reason="Unhandled processing error; will not retry.")
