from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from backtestforecast.billing.entitlements import BillingInterval
from backtestforecast.billing.entitlements import PlanTier as EntitlementPlanTier
from backtestforecast.schemas.common import PlanTier


class CreateCheckoutSessionRequest(BaseModel):
    tier: Literal[EntitlementPlanTier.PRO.value, EntitlementPlanTier.PREMIUM.value]
    billing_interval: BillingInterval = Field(default=BillingInterval.MONTHLY)


class CheckoutSessionResponse(BaseModel):
    session_id: str
    checkout_url: str
    tier: str
    billing_interval: str
    expires_at: datetime | None = None


class CreatePortalSessionRequest(BaseModel):
    return_path: str | None = Field(default="/app/settings/billing", max_length=200)


class PortalSessionResponse(BaseModel):
    portal_url: str


class BillingStateResponse(BaseModel):
    plan_tier: PlanTier
    subscription_status: str | None = None
    subscription_billing_interval: str | None = None
    subscription_current_period_end: datetime | None = None
    cancel_at_period_end: bool = False
