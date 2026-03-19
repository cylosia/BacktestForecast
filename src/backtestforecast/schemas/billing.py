from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from backtestforecast.billing.entitlements import BillingInterval
from backtestforecast.schemas.common import PlanTier


class CreateCheckoutSessionRequest(BaseModel):
    model_config = {"extra": "forbid"}
    tier: Literal[PlanTier.PRO.value, PlanTier.PREMIUM.value]
    billing_interval: BillingInterval = Field(default=BillingInterval.MONTHLY)


class CheckoutSessionResponse(BaseModel):
    session_id: str
    checkout_url: str
    tier: str
    billing_interval: str
    expires_at: datetime | None = None

    @field_validator("checkout_url")
    @classmethod
    def validate_checkout_url(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("checkout_url must be an HTTPS URL")
        return v


class CreatePortalSessionRequest(BaseModel):
    return_path: str | None = Field(default="/app/settings/billing", max_length=200, pattern=r"^/[a-zA-Z0-9/_\-\.~]*$")


class PortalSessionResponse(BaseModel):
    portal_url: str

    @field_validator("portal_url")
    @classmethod
    def validate_portal_url(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("portal_url must be an HTTPS URL")
        return v


_VALID_SUBSCRIPTION_STATUSES = frozenset({
    "incomplete", "incomplete_expired", "trialing", "active",
    "past_due", "canceled", "unpaid", "paused",
})


class BillingStateResponse(BaseModel):
    plan_tier: PlanTier
    subscription_status: str | None = None
    subscription_billing_interval: str | None = None

    @field_validator("subscription_status", mode="before")
    @classmethod
    def validate_subscription_status(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_SUBSCRIPTION_STATUSES:
            raise ValueError(f"Invalid subscription_status: {v}")
        return v
    subscription_current_period_end: datetime | None = None
    cancel_at_period_end: bool = False


class WebhookResponse(BaseModel):
    """Typed response for the Stripe webhook endpoint."""
    status: str
    event_type: str | None = None
    reason: str | None = Field(default=None, max_length=500)
    code: str | None = None
