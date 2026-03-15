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
    return BillingService(db).create_checkout_session(
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
    return BillingService(db).create_portal_session(
        user,
        payload,
        request_id=metadata.request_id,
        ip_address=metadata.ip_address,
    )


@router.post("/webhook", status_code=status.HTTP_200_OK, response_model=WebhookResponse)
def stripe_webhook(
    request: Request,
    payload: bytes = Body(..., media_type="application/json", max_length=256_000),
    signature: str | None = Header(alias="Stripe-Signature"),
    db: Session = Depends(get_db),
) -> WebhookResponse | dict[str, str]:
    if signature is None:
        raise HTTPException(status_code=400, detail="Missing Stripe-Signature header")

    request_id = getattr(request.state, "request_id", None)
    meta = get_request_metadata(request)
    ip_address = meta.ip_address
    settings = get_settings()
    get_rate_limiter().check(
        bucket="billing:webhook",
        actor_key=ip_address or "unknown",
        limit=60,
        window_seconds=settings.rate_limit_window_seconds,
    )

    # Informational only — this list is incomplete and NOT a security control.
    # Webhook authenticity is verified by Stripe signature validation above.
    # These IPs are logged to aid debugging; non-matching IPs are NOT rejected.
    known_stripe_cidrs = [
        "54.187.174.169/32", "54.187.205.235/32", "54.187.216.72/32",
        "54.241.31.99/32", "54.241.31.102/32", "54.241.34.107/32",
    ]
    if ip_address:
        import ipaddress as _ipaddress
        try:
            client_addr = _ipaddress.ip_address(ip_address)
            is_known = any(client_addr in _ipaddress.ip_network(cidr) for cidr in known_stripe_cidrs)
            if not is_known:
                _webhook_logger.info("billing.webhook.unknown_source_ip", ip=ip_address)
        except ValueError:
            pass

    try:
        return BillingService(db).handle_webhook(
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
        )
        if isinstance(exc, _AuthErr):
            _webhook_logger.warning(
                "webhook.signature_verification_failed", ip=ip_address, request_id=request_id,
            )
            raise HTTPException(
                status_code=400,
                detail={"code": "signature_verification_failed", "message": "Invalid webhook signature."},
            )
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
            return {"status": "error", "code": exc.code}
        _webhook_logger.exception("webhook.unhandled_error", ip=ip_address, request_id=request_id)
        raise HTTPException(
            status_code=500,
            detail={"code": "webhook_processing_failed", "message": "Webhook could not be processed."},
        )
