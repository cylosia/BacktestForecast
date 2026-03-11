from __future__ import annotations

import pytest

from backtestforecast.billing.entitlements import (
    ExportFormat,
    PlanTier,
    ensure_export_access,
    ensure_forecasting_access,
    resolve_feature_policy,
    resolve_scanner_policy,
)
from backtestforecast.billing.urls import resolve_return_url
from backtestforecast.errors import FeatureLockedError, ValidationError


def test_inactive_subscription_state_downgrades_to_free_policy() -> None:
    policy = resolve_feature_policy(PlanTier.PREMIUM.value, "canceled")

    assert policy.tier == PlanTier.FREE
    assert policy.monthly_backtest_quota == 5
    assert policy.forecasting_access is False
    assert policy.export_formats == frozenset()


def test_export_and_forecasting_access_respect_plan_boundaries() -> None:
    ensure_export_access(PlanTier.PRO.value, "active", ExportFormat.CSV)
    ensure_forecasting_access(PlanTier.PRO.value, "active")

    with pytest.raises(FeatureLockedError):
        ensure_export_access(PlanTier.PRO.value, "active", ExportFormat.PDF)

    with pytest.raises(FeatureLockedError):
        ensure_forecasting_access(PlanTier.FREE.value, None)


def test_scanner_policy_enforces_basic_vs_advanced_modes() -> None:
    pro_policy = resolve_scanner_policy(PlanTier.PRO.value, "basic", "active")
    premium_policy = resolve_scanner_policy(PlanTier.PREMIUM.value, "advanced", "active")

    assert pro_policy.mode.value == "basic"
    assert premium_policy.mode.value == "advanced"

    with pytest.raises(FeatureLockedError):
        resolve_scanner_policy(PlanTier.PRO.value, "advanced", "active")


def test_unknown_subscription_status_defaults_to_free() -> None:
    policy = resolve_feature_policy(PlanTier.PRO.value, None)
    assert policy.tier == PlanTier.FREE

    policy = resolve_feature_policy(PlanTier.PREMIUM.value, "some_unknown_status")
    assert policy.tier == PlanTier.FREE


def test_billing_return_url_rejects_cross_origin_targets() -> None:
    assert (
        resolve_return_url("https://app.backtestforecast.com", "/app/settings/billing")
        == "https://app.backtestforecast.com/app/settings/billing"
    )
    assert (
        resolve_return_url("https://app.backtestforecast.com", "https://app.backtestforecast.com/pricing")
        == "https://app.backtestforecast.com/pricing"
    )

    with pytest.raises(ValidationError):
        resolve_return_url("https://app.backtestforecast.com", "https://evil.example/portal")


def test_billing_return_url_rejects_protocol_relative_urls() -> None:
    with pytest.raises(ValidationError):
        resolve_return_url("https://app.backtestforecast.com", "//evil.example/phishing")
