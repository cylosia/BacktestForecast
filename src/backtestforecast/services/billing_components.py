from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backtestforecast.billing.entitlements import PlanTier
from backtestforecast.errors import AppValidationError, AuthenticationError, ExternalServiceError, NotFoundError
from backtestforecast.observability import get_logger
from backtestforecast.observability.metrics import STRIPE_WEBHOOK_EVENTS_TOTAL

if TYPE_CHECKING:
    from backtestforecast.models import User
    from backtestforecast.schemas.billing import (
        CheckoutSessionResponse,
        CreateCheckoutSessionRequest,
        CreatePortalSessionRequest,
        PortalSessionResponse,
    )
    from backtestforecast.services.billing import BillingService

logger = get_logger("billing.components")


class CheckoutService:
    def __init__(self, billing: "BillingService") -> None:
        self.billing = billing

    def create_checkout_session(self, user: "User", payload: "CreateCheckoutSessionRequest", *, request_id: str | None = None, ip_address: str | None = None) -> "CheckoutSessionResponse":
        if payload.tier == PlanTier.FREE.value:
            raise AppValidationError("Free does not require a Stripe checkout session.")
        return self.billing._create_checkout_session_impl(user, payload, request_id=request_id, ip_address=ip_address)


class PortalService:
    def __init__(self, billing: "BillingService") -> None:
        self.billing = billing

    def create_portal_session(self, user: "User", payload: "CreatePortalSessionRequest", *, request_id: str | None = None, ip_address: str | None = None) -> "PortalSessionResponse":
        return self.billing._create_portal_session_impl(user, payload, request_id=request_id, ip_address=ip_address)


class WebhookHandler:
    def __init__(self, billing: "BillingService") -> None:
        self.billing = billing

    def handle_webhook(self, payload_bytes: bytes, signature_header: str | None, *, request_id: str | None = None, ip_address: str | None = None) -> dict[str, str]:
        return self.billing._handle_webhook_impl(payload_bytes, signature_header, request_id=request_id, ip_address=ip_address)


class ReconciliationService:
    def __init__(self, billing: "BillingService") -> None:
        self.billing = billing

    def reconcile_subscriptions(self, *, batch_size: int = 100) -> int:
        return self.billing._reconcile_subscriptions_impl(batch_size=batch_size)
