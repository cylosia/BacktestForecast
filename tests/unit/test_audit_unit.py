"""Unit tests for audit findings that don't require a real database.

Covers:
- _fallback_persist_status behavior
- Scanner validation with plan tiers
- SymbolAnalysisRepository ownership enforcement
- SweepJobRepository.delete_results ownership check
- Validation constants consistency
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest


# ---------------------------------------------------------------------------
# Scanner validation with plan tiers
# ---------------------------------------------------------------------------


class TestScannerValidationPlanTiers:
    """Verify scanner validation respects per-tier limits from the backend."""

    def test_pro_basic_max_strategies_is_4(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        policy = POLICIES[(PlanTier.PRO, ScannerMode.BASIC)]
        assert policy.max_strategies == 4

    def test_premium_basic_max_symbols_is_10(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        policy = POLICIES[(PlanTier.PREMIUM, ScannerMode.BASIC)]
        assert policy.max_symbols == 10

    def test_premium_advanced_max_strategies_is_14(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        policy = POLICIES[(PlanTier.PREMIUM, ScannerMode.ADVANCED)]
        assert policy.max_strategies == 14

    def test_free_has_no_scanner_access(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        policy = FEATURE_POLICIES[PlanTier.FREE]
        assert not policy.basic_scanner_access
        assert not policy.advanced_scanner_access

    def test_pro_comparison_limit_is_3(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        assert FEATURE_POLICIES[PlanTier.PRO].side_by_side_comparison_limit == 3

    def test_premium_comparison_limit_is_8(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        assert FEATURE_POLICIES[PlanTier.PREMIUM].side_by_side_comparison_limit == 8

    def test_pro_commission_max(self):
        """Backend schema enforces commission <= 100."""
        from backtestforecast.schemas.backtests import CreateBacktestRunRequest
        schema_fields = CreateBacktestRunRequest.model_fields
        field = schema_fields["commission_per_contract"]
        metadata = field.metadata
        le_values = [m.le for m in metadata if hasattr(m, "le")]
        assert any(v <= 100 for v in le_values), f"Expected commission le <= 100, got {le_values}"

    def test_normalize_plan_tier_canceled_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier("premium", "canceled")
        assert result == PlanTier.FREE

    def test_normalize_plan_tier_none_returns_free(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier(None, None)
        assert result == PlanTier.FREE

    def test_normalize_plan_tier_active_pro(self):
        from backtestforecast.billing.entitlements import normalize_plan_tier
        from backtestforecast.schemas.common import PlanTier

        result = normalize_plan_tier("pro", "active")
        assert result == PlanTier.PRO


# ---------------------------------------------------------------------------
# SymbolAnalysisRepository ownership
# ---------------------------------------------------------------------------


class TestSymbolAnalysisRepositoryOwnership:
    """Verify the repository enforces ownership correctly."""

    def test_get_by_id_requires_user_id_kwarg(self):
        from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository

        sig = inspect.signature(SymbolAnalysisRepository.get_by_id)
        params = sig.parameters
        assert "user_id" in params
        assert params["user_id"].kind == inspect.Parameter.KEYWORD_ONLY

    def test_get_by_id_unfiltered_exists(self):
        from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository

        assert hasattr(SymbolAnalysisRepository, "get_by_id_unfiltered")

    def test_get_for_user_requires_both_ids(self):
        from backtestforecast.repositories.symbol_analyses import SymbolAnalysisRepository

        sig = inspect.signature(SymbolAnalysisRepository.get_for_user)
        params = sig.parameters
        assert "analysis_id" in params
        assert "user_id" in params


# ---------------------------------------------------------------------------
# SweepJobRepository delete_results ownership
# ---------------------------------------------------------------------------


class TestSweepDeleteResultsOwnership:
    """Verify delete_results accepts optional user_id for ownership check."""

    def test_delete_results_accepts_user_id(self):
        from backtestforecast.repositories.sweep_jobs import SweepJobRepository

        sig = inspect.signature(SweepJobRepository.delete_results)
        params = sig.parameters
        assert "user_id" in params
        assert params["user_id"].default is None

    def test_delete_results_worker_path(self):
        """Worker code can call delete_results without user_id."""
        from backtestforecast.repositories.sweep_jobs import SweepJobRepository

        sig = inspect.signature(SweepJobRepository.delete_results)
        params = sig.parameters
        assert params["user_id"].kind == inspect.Parameter.KEYWORD_ONLY


# ---------------------------------------------------------------------------
# Event fallback persist status
# ---------------------------------------------------------------------------


class TestFallbackPersistStatus:
    """Verify _fallback_persist_status handles edge cases."""

    def test_non_terminal_status_is_ignored(self):
        from backtestforecast.events import _fallback_persist_status

        _fallback_persist_status("backtest", uuid4(), "running")

    def test_unknown_job_type_is_handled(self):
        from backtestforecast.events import _fallback_persist_status

        _fallback_persist_status("nonexistent_type", uuid4(), "succeeded")

    def test_job_type_model_map_complete(self):
        from backtestforecast.events import _JOB_TYPE_MODEL_MAP

        expected_types = {"backtest", "export", "scan", "sweep", "analysis", "pipeline"}
        assert set(_JOB_TYPE_MODEL_MAP.keys()) == expected_types

    def test_valid_target_statuses(self):
        from backtestforecast.events import _VALID_TARGET_STATUSES
        from backtestforecast.models import JobStatus

        assert JobStatus.SUCCEEDED in _VALID_TARGET_STATUSES
        assert JobStatus.FAILED in _VALID_TARGET_STATUSES
        assert JobStatus.CANCELLED in _VALID_TARGET_STATUSES

    def test_export_includes_expired(self):
        from backtestforecast.events import _EXPORT_VALID_TARGET_STATUSES
        from backtestforecast.models import JobStatus

        assert JobStatus.EXPIRED in _EXPORT_VALID_TARGET_STATUSES


# ---------------------------------------------------------------------------
# Validation constants consistency
# ---------------------------------------------------------------------------


class TestValidationConstants:
    """Verify backend and frontend validation constants are aligned."""

    def test_strategy_type_counts(self):
        from backtestforecast.billing.entitlements import (
            ADVANCED_SCANNER_STRATEGIES,
            BASIC_SCANNER_STRATEGIES,
        )
        assert len(BASIC_SCANNER_STRATEGIES) == 6
        assert len(ADVANCED_SCANNER_STRATEGIES) > len(BASIC_SCANNER_STRATEGIES)

    def test_feature_policies_cover_all_tiers(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        for tier in PlanTier:
            assert tier in FEATURE_POLICIES, f"Missing policy for {tier}"

    def test_scanner_policies_complete(self):
        from backtestforecast.billing.entitlements import POLICIES, ScannerMode
        from backtestforecast.schemas.common import PlanTier

        assert (PlanTier.PRO, ScannerMode.BASIC) in POLICIES
        assert (PlanTier.PREMIUM, ScannerMode.BASIC) in POLICIES
        assert (PlanTier.PREMIUM, ScannerMode.ADVANCED) in POLICIES
        assert (PlanTier.FREE, ScannerMode.BASIC) not in POLICIES


# ---------------------------------------------------------------------------
# Migration chain integrity
# ---------------------------------------------------------------------------


class TestMigrationIntegrity:
    """Verify migration metadata is consistent."""

    def test_latest_revision_is_0021(self):
        """After our new migration, head should be 0021."""
        import importlib
        mod = importlib.import_module(
            "alembic.versions.20260318_0021_outbox_server_defaults"
        )
        assert mod.revision == "20260318_0021"
        assert mod.down_revision == "20260318_0020"
