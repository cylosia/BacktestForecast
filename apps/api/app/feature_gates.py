from __future__ import annotations

from backtestforecast.billing.entitlements import normalize_plan_tier
from backtestforecast.errors import FeatureLockedError
from backtestforecast.feature_flags import is_feature_enabled
from backtestforecast.models import User


def require_feature_enabled(
    *,
    feature_name: str,
    user: User,
    message: str,
    required_tier: str = "free",
) -> None:
    effective_plan_tier = normalize_plan_tier(
        user.plan_tier,
        user.subscription_status,
        user.subscription_current_period_end,
    ).value
    if not is_feature_enabled(
        feature_name,
        user_id=user.id,
        plan_tier=effective_plan_tier,
    ):
        raise FeatureLockedError(message, required_tier=required_tier)
