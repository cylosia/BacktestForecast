"""Tests for billing entitlement resolution edge cases."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backtestforecast.billing.entitlements import (
    ExportFormat,
    FeatureLockedError,
    PlanTier,
    _active_renewal_grace,
    ensure_export_access,
    ensure_forecasting_access,
    normalize_plan_tier,
    resolve_feature_policy,
    resolve_scanner_policy,
)


class TestNormalizePlanTier:
    def test_free_when_no_subscription(self):
        assert normalize_plan_tier(None, None) == PlanTier.FREE

    def test_free_when_canceled(self):
        assert normalize_plan_tier("pro", "canceled") == PlanTier.FREE

    def test_pro_when_active(self):
        future = datetime.now(UTC) + timedelta(days=30)
        assert normalize_plan_tier("pro", "active", future) == PlanTier.PRO

    def test_premium_when_active(self):
        future = datetime.now(UTC) + timedelta(days=30)
        assert normalize_plan_tier("premium", "active", future) == PlanTier.PREMIUM

    def test_free_when_period_expired_beyond_grace(self):
        expired = datetime.now(UTC) - _active_renewal_grace() - timedelta(hours=1)
        assert normalize_plan_tier("pro", "active", expired) == PlanTier.FREE

    def test_pro_within_renewal_grace(self):
        almost_expired = datetime.now(UTC) - _active_renewal_grace() + timedelta(minutes=30)
        assert normalize_plan_tier("pro", "active", almost_expired) == PlanTier.PRO

    def test_past_due_within_grace_period(self):
        period_end = datetime.now(UTC) - timedelta(days=3)
        assert normalize_plan_tier("pro", "past_due", period_end) == PlanTier.PRO

    def test_past_due_beyond_grace_period(self):
        period_end = datetime.now(UTC) - timedelta(days=10)
        assert normalize_plan_tier("pro", "past_due", period_end) == PlanTier.FREE

    def test_past_due_no_period_end(self):
        assert normalize_plan_tier("pro", "past_due", None) == PlanTier.FREE

    def test_unknown_status_without_period_end_downgrades_to_free(self):
        assert normalize_plan_tier("pro", "some_unknown_status") == PlanTier.FREE
        assert normalize_plan_tier("premium", "some_unknown_status") == PlanTier.FREE
        assert normalize_plan_tier("free", "some_unknown_status") == PlanTier.FREE

    def test_unknown_status_with_future_period_end_preserves_current_tier_during_grace(self):
        future = datetime.now(UTC) + timedelta(days=7)
        assert normalize_plan_tier("pro", "some_unknown_status", future) == PlanTier.PRO
        assert normalize_plan_tier("premium", "some_unknown_status", future) == PlanTier.PREMIUM

    def test_inactive_statuses(self):
        for status in ("canceled", "unpaid", "incomplete", "incomplete_expired", "paused"):
            assert normalize_plan_tier("premium", status) == PlanTier.FREE


class TestFeaturePolicy:
    def test_free_tier_quotas(self):
        policy = resolve_feature_policy(None, None)
        assert policy.tier == PlanTier.FREE
        assert policy.monthly_backtest_quota == 5
        assert policy.forecasting_access is False
        assert len(policy.export_formats) == 0

    def test_pro_tier_has_csv_export(self):
        future = datetime.now(UTC) + timedelta(days=30)
        policy = resolve_feature_policy("pro", "active", future)
        assert ExportFormat.CSV in policy.export_formats
        assert ExportFormat.PDF not in policy.export_formats

    def test_premium_tier_has_pdf_export(self):
        future = datetime.now(UTC) + timedelta(days=30)
        policy = resolve_feature_policy("premium", "active", future)
        assert ExportFormat.CSV in policy.export_formats
        assert ExportFormat.PDF in policy.export_formats


class TestEnsureAccess:
    def test_forecasting_locked_for_free(self):
        with pytest.raises(FeatureLockedError):
            ensure_forecasting_access(None, None)

    def test_forecasting_allowed_for_pro(self):
        future = datetime.now(UTC) + timedelta(days=30)
        ensure_forecasting_access("pro", "active", future)

    def test_pdf_export_locked_for_pro(self):
        future = datetime.now(UTC) + timedelta(days=30)
        with pytest.raises(FeatureLockedError):
            ensure_export_access("pro", "active", ExportFormat.PDF, future)

    def test_csv_export_allowed_for_pro(self):
        future = datetime.now(UTC) + timedelta(days=30)
        ensure_export_access("pro", "active", ExportFormat.CSV, future)


class TestScannerPolicy:
    def test_free_tier_denied_scanner(self):
        with pytest.raises(FeatureLockedError):
            resolve_scanner_policy(None, "basic")

    def test_pro_basic_scanner_allowed(self):
        future = datetime.now(UTC) + timedelta(days=30)
        policy = resolve_scanner_policy("pro", "basic", "active", future)
        assert policy.max_symbols == 5

    def test_pro_advanced_scanner_denied(self):
        future = datetime.now(UTC) + timedelta(days=30)
        with pytest.raises(FeatureLockedError):
            resolve_scanner_policy("pro", "advanced", "active", future)

    def test_premium_advanced_scanner_allowed(self):
        future = datetime.now(UTC) + timedelta(days=30)
        policy = resolve_scanner_policy("premium", "advanced", "active", future)
        assert policy.max_symbols == 25
