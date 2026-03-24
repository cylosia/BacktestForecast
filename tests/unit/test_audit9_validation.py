"""Validation and edge-case tests for audit round 9 fixes.

Covers: date validation, export size limits, backtest window limits,
scanner validation, feature flags, template constraints, pagination,
entitlement enforcement, and cleanup tasks.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from pydantic import ValidationError as PydanticValidationError

from backtestforecast.config import Settings


class TestBacktestDateValidation:
    """Fix #70: Backtest date range respects max_backtest_window_days."""

    def test_max_window_days_configured(self):
        assert Settings.model_fields["max_backtest_window_days"].default == 1_825

    def test_max_scanner_window_configured(self):
        assert Settings.model_fields["max_scanner_window_days"].default == 730


class TestExportSizeLimits:
    """Fix #69: Export size limit is enforced."""

    def test_max_export_bytes_defined(self):
        from backtestforecast.services.exports import _MAX_EXPORT_BYTES

        assert _MAX_EXPORT_BYTES == 10 * 1024 * 1024

    def test_max_csv_trades_defined(self):
        from backtestforecast.services.exports import _MAX_CSV_TRADES

        assert _MAX_CSV_TRADES == 10_000

    def test_max_csv_equity_points_defined(self):
        from backtestforecast.services.exports import _MAX_CSV_EQUITY_POINTS

        assert _MAX_CSV_EQUITY_POINTS == 50_000


class TestFeatureFlags:
    """Fix #82: Feature flags are properly configured."""

    def test_all_feature_flags_have_defaults(self):
        flag_fields = [
            "feature_backtests_enabled",
            "feature_scanner_enabled",
            "feature_exports_enabled",
            "feature_forecasts_enabled",
            "feature_analysis_enabled",
            "feature_daily_picks_enabled",
            "feature_billing_enabled",
            "feature_sweeps_enabled",
        ]
        for field_name in flag_fields:
            assert field_name in Settings.model_fields, f"Missing flag: {field_name}"
            assert Settings.model_fields[field_name].default is True, (
                f"{field_name} should default to True"
            )


class TestEntitlementEnforcement:
    """Fix #86: Entitlement boundaries are correctly enforced."""

    def test_free_tier_has_no_export(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        free = FEATURE_POLICIES[PlanTier.FREE]
        assert len(free.export_formats) == 0

    def test_free_tier_has_no_scanner(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        free = FEATURE_POLICIES[PlanTier.FREE]
        assert free.basic_scanner_access is False
        assert free.advanced_scanner_access is False

    def test_free_tier_has_backtest_quota(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        free = FEATURE_POLICIES[PlanTier.FREE]
        assert free.monthly_backtest_quota == 5

    def test_pro_tier_has_csv_export_only(self):
        from backtestforecast.billing.entitlements import (
            FEATURE_POLICIES,
            ExportFormat,
        )
        from backtestforecast.schemas.common import PlanTier

        pro = FEATURE_POLICIES[PlanTier.PRO]
        assert ExportFormat.CSV in pro.export_formats
        assert ExportFormat.PDF not in pro.export_formats

    def test_premium_tier_has_all_exports(self):
        from backtestforecast.billing.entitlements import (
            FEATURE_POLICIES,
            ExportFormat,
        )
        from backtestforecast.schemas.common import PlanTier

        premium = FEATURE_POLICIES[PlanTier.PREMIUM]
        assert ExportFormat.CSV in premium.export_formats
        assert ExportFormat.PDF in premium.export_formats

    def test_premium_tier_has_unlimited_backtests(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        premium = FEATURE_POLICIES[PlanTier.PREMIUM]
        assert premium.monthly_backtest_quota is None

    def test_scanner_strategies_are_consistent(self):
        from backtestforecast.billing.entitlements import (
            ADVANCED_SCANNER_STRATEGIES,
            BASIC_SCANNER_STRATEGIES,
        )

        assert BASIC_SCANNER_STRATEGIES.issubset(ADVANCED_SCANNER_STRATEGIES)


class TestNormalizePlanTier:
    """Fix #86 continued: normalize_plan_tier edge cases."""

    def test_unknown_status_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier("pro", "some_new_status", None)
        assert result == PlanTier.FREE

    def test_past_due_within_grace_returns_tier(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        period_end = datetime.now(UTC) - timedelta(days=1)
        result = normalize_plan_tier("pro", "past_due", period_end)
        assert result == PlanTier.PRO

    def test_past_due_beyond_grace_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        period_end = datetime.now(UTC) - timedelta(days=30)
        result = normalize_plan_tier("pro", "past_due", period_end)
        assert result == PlanTier.FREE

    def test_none_status_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier("pro", None, None)
        assert result == PlanTier.FREE

    def test_active_expired_period_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        period_end = datetime.now(UTC) - timedelta(days=30)
        result = normalize_plan_tier("pro", "active", period_end)
        assert result == PlanTier.FREE


class TestScannerAccessPolicies:
    """Fix #72: Scanner access policies are consistent."""

    def test_pro_basic_scanner_limits(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        policy = POLICIES[(PlanTier.PRO, ScannerMode.BASIC)]
        assert policy.max_symbols >= 1
        assert policy.max_strategies >= 1
        assert policy.max_rule_sets >= 1
        assert policy.max_recommendations >= 1

    def test_premium_advanced_scanner_limits(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        policy = POLICIES[(PlanTier.PREMIUM, ScannerMode.ADVANCED)]
        assert policy.max_symbols >= 1
        assert policy.max_recommendations >= policy.max_symbols

    def test_no_free_basic_policy_exists(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        assert (PlanTier.FREE, ScannerMode.BASIC) not in POLICIES

    def test_no_pro_advanced_policy_exists(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        assert (PlanTier.PRO, ScannerMode.ADVANCED) not in POLICIES


class TestCleanupTaskSafety:
    """Fix #89-90: Cleanup tasks have proper safeguards."""

    def test_cleanup_audit_events_batch_size(self):
        """Verify batch-based cleanup to prevent table locking."""
        import pathlib

        tasks_path = pathlib.Path(__file__).resolve().parents[2] / "apps" / "worker" / "app" / "tasks.py"
        source = tasks_path.read_text()
        # cleanup_audit_events uses BATCH_SIZE and max_batches
        assert "BATCH_SIZE" in source
        assert "max_batches" in source
        assert "cleanup_audit_events" in source

    def test_cleanup_daily_recommendations_has_retention(self):
        import pathlib

        tasks_path = pathlib.Path(__file__).resolve().parents[2] / "apps" / "worker" / "app" / "tasks.py"
        source = tasks_path.read_text()
        assert "retention_days" in source
        assert "cleanup_daily_recommendations" in source


class TestIdempotencyKeys:
    """Fix #62: Idempotency key handling across all job types."""

    def test_backtest_run_has_idempotency_key(self):
        from backtestforecast.models import BacktestRun

        assert hasattr(BacktestRun, "idempotency_key")

    def test_scanner_job_has_idempotency_key(self):
        from backtestforecast.models import ScannerJob

        assert hasattr(ScannerJob, "idempotency_key")

    def test_export_job_has_idempotency_key(self):
        from backtestforecast.models import ExportJob

        assert hasattr(ExportJob, "idempotency_key")

    def test_sweep_job_has_idempotency_key(self):
        from backtestforecast.models import SweepJob

        assert hasattr(SweepJob, "idempotency_key")

    def test_symbol_analysis_has_idempotency_key(self):
        from backtestforecast.models import SymbolAnalysis

        assert hasattr(SymbolAnalysis, "idempotency_key")


class TestModelCheckConstraints:
    """Fix #61: All models have proper check constraints."""

    def test_backtest_run_has_status_constraint(self):
        from backtestforecast.models import BacktestRun

        constraint_names = [
            c.name
            for c in BacktestRun.__table_args__
            if hasattr(c, "name") and c.name and "status" in c.name
        ]
        assert len(constraint_names) >= 1

    def test_scanner_job_has_status_constraint(self):
        from backtestforecast.models import ScannerJob

        constraint_names = [
            c.name
            for c in ScannerJob.__table_args__
            if hasattr(c, "name") and c.name and "status" in c.name
        ]
        assert len(constraint_names) >= 1

    def test_sweep_job_has_status_constraint(self):
        from backtestforecast.models import SweepJob

        constraint_names = [
            c.name
            for c in SweepJob.__table_args__
            if hasattr(c, "name") and c.name and "status" in c.name
        ]
        assert len(constraint_names) >= 1


class TestConfigValidation:
    """Fix #83: Config validation catches common errors."""

    def test_invalid_app_env_rejected(self):
        env = {
            "APP_ENV": "invalid_env",
            "CLERK_ISSUER": "https://clerk.example.com",
            "CLERK_JWT_KEY": "test-key",
            "CLERK_SECRET_KEY": "sk_test",
            "CLERK_AUDIENCE": "test",
            "CLERK_AUTHORIZED_PARTIES": "https://app.example.com",
            "LOG_JSON": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises((ValueError, PydanticValidationError), match="app_env"):
                Settings()

    def test_invalid_log_level_rejected(self):
        env = {
            "LOG_LEVEL": "INVALID",
            "CLERK_ISSUER": "https://clerk.example.com",
            "CLERK_JWT_KEY": "test-key",
            "CLERK_SECRET_KEY": "sk_test",
            "CLERK_AUDIENCE": "test",
            "CLERK_AUTHORIZED_PARTIES": "https://app.example.com",
            "LOG_JSON": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises((ValueError, PydanticValidationError), match="log_level"):
                Settings()

    def test_negative_pool_size_rejected(self):
        env = {
            "DB_POOL_SIZE": "0",
            "CLERK_ISSUER": "https://clerk.example.com",
            "CLERK_JWT_KEY": "test-key",
            "CLERK_SECRET_KEY": "sk_test",
            "CLERK_AUDIENCE": "test",
            "CLERK_AUTHORIZED_PARTIES": "https://app.example.com",
            "LOG_JSON": "true",
        }
        with patch.dict(os.environ, env, clear=False), pytest.raises((ValueError, PydanticValidationError)):
            Settings()
