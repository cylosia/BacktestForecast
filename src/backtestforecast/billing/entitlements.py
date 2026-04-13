from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import structlog

from backtestforecast.config import get_settings
from backtestforecast.errors import AppValidationError, FeatureLockedError
from backtestforecast.schemas.common import PlanTier

_logger = structlog.get_logger("billing.entitlements")
UTC = UTC


class BillingInterval(StrEnum):
    MONTHLY = "monthly"
    YEARLY = "yearly"


class ExportFormat(StrEnum):
    CSV = "csv"
    PDF = "pdf"


class ScannerMode(StrEnum):
    BASIC = "basic"
    ADVANCED = "advanced"


PAID_STATUSES = {"active", "trialing"}
INACTIVE_STATUSES = {"canceled", "unpaid", "incomplete", "incomplete_expired", "paused"}


def _active_renewal_grace() -> timedelta:
    """Configurable grace period after subscription period end before downgrade."""
    return timedelta(hours=get_settings().active_renewal_grace_hours)


def _past_due_grace_days() -> int:
    """Configurable grace days for past-due subscriptions before downgrade."""
    return get_settings().past_due_grace_days


@dataclass(frozen=True, slots=True)
class FeaturePolicy:
    tier: PlanTier
    monthly_backtest_quota: int | None
    monthly_sweep_quota: int | None
    history_days: int | None
    history_item_limit: int
    side_by_side_comparison_limit: int
    forecasting_access: bool
    sweep_access: bool
    export_formats: frozenset[ExportFormat]
    basic_scanner_access: bool
    advanced_scanner_access: bool


BASIC_SCANNER_STRATEGIES: frozenset[str] = frozenset({
    "long_call",
    "long_put",
    "covered_call",
    "cash_secured_put",
    "bull_call_debit_spread",
    "bear_put_debit_spread",
})

ADVANCED_SCANNER_STRATEGIES: frozenset[str] = BASIC_SCANNER_STRATEGIES | frozenset({
    "bull_put_credit_spread",
    "bear_call_credit_spread",
    "iron_condor",
    "long_straddle",
    "long_strangle",
    "calendar_spread",
    "put_calendar_spread",
    "butterfly",
    "wheel_strategy",
    "poor_mans_covered_call",
    "ratio_call_backspread",
    "ratio_put_backspread",
    "collar",
    "diagonal_spread",
    "double_diagonal",
    "short_straddle",
    "short_strangle",
    "covered_strangle",
    "synthetic_put",
    "reverse_conversion",
    "jade_lizard",
    "iron_butterfly",
    "custom_2_leg",
    "custom_3_leg",
    "custom_4_leg",
    "custom_5_leg",
    "custom_6_leg",
    "custom_7_leg",
    "custom_8_leg",
    "naked_call",
    "naked_put",
})


FEATURE_POLICIES = {
    PlanTier.FREE: FeaturePolicy(
        tier=PlanTier.FREE,
        monthly_backtest_quota=5,
        monthly_sweep_quota=0,
        history_days=30,
        history_item_limit=25,
        side_by_side_comparison_limit=2,
        forecasting_access=False,
        sweep_access=False,
        export_formats=frozenset(),
        basic_scanner_access=False,
        advanced_scanner_access=False,
    ),
    PlanTier.PRO: FeaturePolicy(
        tier=PlanTier.PRO,
        monthly_backtest_quota=None,
        monthly_sweep_quota=10,
        history_days=365,
        history_item_limit=500,
        side_by_side_comparison_limit=3,
        forecasting_access=True,
        sweep_access=True,
        export_formats=frozenset({ExportFormat.CSV}),
        basic_scanner_access=True,
        advanced_scanner_access=False,
    ),
    PlanTier.PREMIUM: FeaturePolicy(
        tier=PlanTier.PREMIUM,
        monthly_backtest_quota=None,
        monthly_sweep_quota=50,
        history_days=None,
        history_item_limit=5000,
        side_by_side_comparison_limit=8,
        forecasting_access=True,
        sweep_access=True,
        export_formats=frozenset({ExportFormat.CSV, ExportFormat.PDF}),
        basic_scanner_access=True,
        advanced_scanner_access=True,
    ),
}


@dataclass(frozen=True, slots=True)
class ScannerAccessPolicy:
    mode: ScannerMode
    max_symbols: int
    max_strategies: int
    max_rule_sets: int
    max_recommendations: int
    allowed_strategies: frozenset[str]
    refresh_allowed: bool


POLICIES = {
    (PlanTier.PRO, ScannerMode.BASIC): ScannerAccessPolicy(
        mode=ScannerMode.BASIC,
        max_symbols=5,
        max_strategies=4,
        max_rule_sets=4,
        max_recommendations=10,
        allowed_strategies=BASIC_SCANNER_STRATEGIES,
        refresh_allowed=True,
    ),
    (PlanTier.PREMIUM, ScannerMode.BASIC): ScannerAccessPolicy(
        mode=ScannerMode.BASIC,
        max_symbols=10,
        max_strategies=6,
        max_rule_sets=6,
        max_recommendations=15,
        allowed_strategies=BASIC_SCANNER_STRATEGIES,
        refresh_allowed=True,
    ),
    (PlanTier.PREMIUM, ScannerMode.ADVANCED): ScannerAccessPolicy(
        mode=ScannerMode.ADVANCED,
        max_symbols=25,
        max_strategies=14,
        max_rule_sets=10,
        max_recommendations=30,
        allowed_strategies=ADVANCED_SCANNER_STRATEGIES,
        refresh_allowed=True,
    ),
}


def _resolve_tier(plan_tier: str | None) -> PlanTier:
    """Map a raw plan_tier string to the corresponding PlanTier enum value."""
    tier_lower = plan_tier.lower() if isinstance(plan_tier, str) else ""
    if tier_lower == PlanTier.PREMIUM.value:
        return PlanTier.PREMIUM
    if tier_lower == PlanTier.PRO.value:
        return PlanTier.PRO
    return PlanTier.FREE


def _warn_unknown_tier(plan_tier: str | None, subscription_status: str | None) -> None:
    """Log a warning when plan_tier is not a recognized value."""
    tier_lower = plan_tier.lower() if isinstance(plan_tier, str) else ""
    if tier_lower and tier_lower not in ("free", ""):
        _logger.warning(
            "normalize_plan_tier.unknown_tier_value",
            plan_tier=plan_tier,
            subscription_status=subscription_status,
            hint="Unrecognized plan_tier defaults to FREE. User may lose features.",
        )


def normalize_plan_tier(
    plan_tier: str | None,
    subscription_status: str | None = None,
    subscription_current_period_end: datetime | None = None,
) -> PlanTier:
    if subscription_status in INACTIVE_STATUSES:
        return PlanTier.FREE
    if subscription_status == "past_due":
        if subscription_current_period_end is None:
            return PlanTier.FREE
        grace_deadline = subscription_current_period_end + timedelta(
            days=_past_due_grace_days(), minutes=5,
        )
        if datetime.now(UTC) > grace_deadline:
            _logger.info(
                "normalize_plan_tier.past_due_grace_expired",
                plan_tier=plan_tier,
                period_end=str(subscription_current_period_end),
            )
            return PlanTier.FREE
        resolved = _resolve_tier(plan_tier)
        if resolved != PlanTier.FREE:
            return resolved
        _warn_unknown_tier(plan_tier, subscription_status)
        return PlanTier.FREE
    elif subscription_status is not None and subscription_status not in PAID_STATUSES:
        _logger.error(
            "normalize_plan_tier.unknown_subscription_status",
            subscription_status=subscription_status,
            plan_tier=plan_tier,
            hint="Unrecognized Stripe status. Granting 72-hour grace period from "
                 "current_period_end, then downgrading to FREE. Add this status to "
                 "PAID_STATUSES or INACTIVE_STATUSES in billing/entitlements.py ASAP.",
        )
        try:
            from backtestforecast.observability.metrics import BILLING_UNKNOWN_STATUS_TOTAL
            BILLING_UNKNOWN_STATUS_TOTAL.labels(status=subscription_status).inc()
        except Exception:
            pass
        _UNKNOWN_STATUS_GRACE_HOURS = 72
        if subscription_current_period_end is not None:
            period_end = subscription_current_period_end
            if period_end.tzinfo is None:
                period_end = period_end.replace(tzinfo=UTC)
            if period_end + timedelta(hours=_UNKNOWN_STATUS_GRACE_HOURS) < datetime.now(UTC):
                _logger.warning(
                    "normalize_plan_tier.unknown_status_grace_expired",
                    subscription_status=subscription_status,
                    plan_tier=plan_tier,
                    period_end=str(subscription_current_period_end),
                    grace_hours=_UNKNOWN_STATUS_GRACE_HOURS,
                )
                return PlanTier.FREE
            return _resolve_tier(plan_tier)
        _logger.warning(
            "normalize_plan_tier.unknown_status_no_period_end",
            subscription_status=subscription_status,
            plan_tier=plan_tier,
            hint="Cannot compute grace deadline without current_period_end. "
                 "Downgrading to FREE as a safety measure.",
        )
        return PlanTier.FREE
    elif subscription_status is None:
        return PlanTier.FREE
    if subscription_current_period_end is not None:
        period_end = subscription_current_period_end
        if period_end.tzinfo is None:
            period_end = period_end.replace(tzinfo=UTC)
        if period_end + _active_renewal_grace() < datetime.now(UTC):
            return PlanTier.FREE
    resolved = _resolve_tier(plan_tier)
    if resolved != PlanTier.FREE:
        return resolved
    _warn_unknown_tier(plan_tier, subscription_status)
    return PlanTier.FREE


def resolve_feature_policy(
    plan_tier: str | None,
    subscription_status: str | None = None,
    subscription_current_period_end: datetime | None = None,
) -> FeaturePolicy:
    return FEATURE_POLICIES[
        normalize_plan_tier(plan_tier, subscription_status, subscription_current_period_end)
    ]


def ensure_export_access(
    plan_tier: str | None,
    subscription_status: str | None,
    export_format: ExportFormat,
    subscription_current_period_end: datetime | None = None,
) -> None:
    feature_policy = resolve_feature_policy(plan_tier, subscription_status, subscription_current_period_end)
    if export_format not in feature_policy.export_formats:
        if export_format == ExportFormat.PDF and feature_policy.tier != PlanTier.PREMIUM:
            raise FeatureLockedError("PDF export requires Premium.", required_tier="premium")
        raise FeatureLockedError(
            "Export access is not available for the current plan.",
            required_tier="pro",
        )


def ensure_forecasting_access(
    plan_tier: str | None,
    subscription_status: str | None,
    subscription_current_period_end: datetime | None = None,
) -> None:
    feature_policy = resolve_feature_policy(plan_tier, subscription_status, subscription_current_period_end)
    if not feature_policy.forecasting_access:
        raise FeatureLockedError(
            "Forecasting access requires Pro or Premium.",
            required_tier="pro",
        )


def ensure_sweep_access(
    plan_tier: str | None,
    subscription_status: str | None,
    subscription_current_period_end: datetime | None = None,
) -> None:
    """Sweeps require at least Pro tier."""
    feature_policy = resolve_feature_policy(plan_tier, subscription_status, subscription_current_period_end)
    if not feature_policy.sweep_access:
        raise FeatureLockedError(
            "Sweeps require Pro or Premium.",
            required_tier="pro",
        )


def resolve_scanner_policy(
    plan_tier: str | None,
    requested_mode: str,
    subscription_status: str | None = None,
    subscription_current_period_end: datetime | None = None,
) -> ScannerAccessPolicy:
    tier = normalize_plan_tier(plan_tier, subscription_status, subscription_current_period_end)
    try:
        mode = ScannerMode(requested_mode)
    except ValueError as exc:
        raise AppValidationError(f"Invalid scanner mode: {requested_mode}") from exc
    policy = POLICIES.get((tier, mode))
    if policy is None:
        if tier == PlanTier.FREE:
            raise FeatureLockedError("Scanner access requires Pro or Premium.", required_tier="pro")
        if tier == PlanTier.PRO and mode == ScannerMode.ADVANCED:
            raise FeatureLockedError("Advanced scanner access requires Premium.", required_tier="premium")
        raise FeatureLockedError(
            "Scanner access is not available for the current entitlement.", required_tier="premium"
        )
    return policy


def validate_strategy_access(policy: ScannerAccessPolicy, strategy_types: list[str]) -> None:
    disallowed = sorted({strategy for strategy in strategy_types if strategy not in policy.allowed_strategies})
    if not disallowed:
        return
    if policy.mode == ScannerMode.BASIC:
        raise AppValidationError("The basic scanner does not support: " + ", ".join(disallowed))
    raise AppValidationError("Unsupported strategies requested: " + ", ".join(disallowed))
