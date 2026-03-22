from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from backtestforecast.billing.entitlements import BillingInterval
from backtestforecast.schemas.common import PlanTier, sanitize_error_message




class PricingIntervalResponse(BaseModel):
    price_id: str | None = None
    unit_amount_usd: int | None = Field(default=None, ge=0)
    display_price: str
    available: bool


class PricingPlanResponse(BaseModel):
    tier: PlanTier
    title: str
    headline: str
    description: str
    features: list[str]
    monthly: PricingIntervalResponse | None = None
    yearly: PricingIntervalResponse | None = None


class PricingContractResponse(BaseModel):
    currency: str = "USD"
    checkout_authoritative: bool = True
    plans: list[PricingPlanResponse]


class CreateCheckoutSessionRequest(BaseModel):
    model_config = {"extra": "forbid"}
    tier: Literal[PlanTier.PRO.value, PlanTier.PREMIUM.value]
    billing_interval: BillingInterval = Field(default=BillingInterval.MONTHLY)


class CheckoutSessionResponse(BaseModel):
    session_id: str
    checkout_url: str
    tier: PlanTier
    billing_interval: BillingInterval
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


from backtestforecast.schemas.common import STRIPE_SUBSCRIPTION_STATUSES as _VALID_SUBSCRIPTION_STATUSES


class BillingStateResponse(BaseModel):
    plan_tier: PlanTier
    subscription_status: str | None = None
    subscription_billing_interval: BillingInterval | None = None
    subscription_current_period_end: datetime | None = None
    cancel_at_period_end: bool = False

    @field_validator("subscription_status", mode="before")
    @classmethod
    def validate_subscription_status(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_SUBSCRIPTION_STATUSES:
            raise ValueError(f"Invalid subscription_status: {v}")
        return v


class WebhookResponse(BaseModel):
    """Typed response for the Stripe webhook endpoint."""
    status: str
    event_type: str | None = None
    reason: str | None = Field(default=None, max_length=500)
    code: str | None = None

    _sanitize_reason = field_validator("reason", mode="before")(sanitize_error_message)
