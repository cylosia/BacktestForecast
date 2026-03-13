from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Header, Request, status
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
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.billing import BillingService

router = APIRouter(prefix="/billing", tags=["billing"])
settings = get_settings()


@router.post("/checkout-session", response_model=CheckoutSessionResponse)
def create_checkout_session(
    payload: CreateCheckoutSessionRequest,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> CheckoutSessionResponse:
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


@router.post("/webhook", status_code=status.HTTP_200_OK)
def stripe_webhook(
    request: Request,
    payload: bytes = Body(..., media_type="application/json"),
    signature: str = Header(alias="Stripe-Signature"),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    request_id = getattr(request.state, "request_id", None)
    ip_address = request.client.host if request.client is not None else None
    return BillingService(db).handle_webhook(
        payload,
        signature,
        request_id=request_id,
        ip_address=ip_address,
    )
