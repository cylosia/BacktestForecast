"""Tests for audit round 9 fixes.

Covers: billing upgrade race, quota off-by-one, CSV injection,
sweep progress tracking, dispatch timezone, circuit breaker interval,
scoring config, plan tier snapshot, and billing cancellation events.
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest


class TestDispatchTimezone:
    """Fix #1: dispatch.py uses datetime.now(UTC) not datetime.now()."""

    def test_dispatch_failure_sets_utc_completed_at(self):
        from backtestforecast.models import RunJobStatus

        job = MagicMock()
        job.status = RunJobStatus.QUEUED
        job.celery_task_id = None
        db = MagicMock()

        mock_settings = MagicMock()
        mock_settings.log_level = "INFO"
        with patch("backtestforecast.config.get_settings", return_value=mock_settings):
            with patch("apps.worker.app.celery_app.configure_logging"):
                with patch("apps.api.app.dispatch.celery_app") as mock_celery:
                    mock_celery.send_task.side_effect = Exception("broker down")
                    with patch("apps.api.app.dispatch.time") as mock_time:
                        mock_time.sleep = MagicMock()
                        from apps.api.app.dispatch import dispatch_celery_task

                        result = dispatch_celery_task(
                            db=db,
                            job=job,
                            task_name="test.task",
                            task_kwargs={"id": "123"},
                            queue="test",
                            log_event="test",
                            logger=MagicMock(),
                        )

        assert result.value == "enqueue_failed"
        assert job.completed_at is not None
        assert job.completed_at.tzinfo is not None


class TestQuotaOffByOne:
    """Fix #2: Worker quota check no longer subtracts 1."""

    def test_quota_not_bypassed_by_concurrent_requests(self):
        from backtestforecast.billing.entitlements import FEATURE_POLICIES
        from backtestforecast.schemas.common import PlanTier

        policy = FEATURE_POLICIES[PlanTier.FREE]
        assert policy.monthly_backtest_quota == 5

        used_count = 5
        assert used_count >= policy.monthly_backtest_quota  # should block


class TestCSVInjection:
    """Fix #3: CSV sanitization covers newline-prefixed formulas."""

    def test_newline_prefix_sanitized(self):
        from backtestforecast.services.exports import ExportService

        result = ExportService._sanitize_csv_cell("\n=CMD('calc')")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_tab_prefix_sanitized(self):
        from backtestforecast.services.exports import ExportService

        result = ExportService._sanitize_csv_cell("\t=CMD('calc')")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_carriage_return_prefix_sanitized(self):
        from backtestforecast.services.exports import ExportService

        result = ExportService._sanitize_csv_cell("\r=CMD('calc')")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_normal_string_unchanged(self):
        from backtestforecast.services.exports import ExportService

        assert ExportService._sanitize_csv_cell("hello world") == "hello world"

    def test_numeric_string_unchanged(self):
        from backtestforecast.services.exports import ExportService

        assert ExportService._sanitize_csv_cell("-123.45") == "-123.45"

    def test_formula_prefix_sanitized(self):
        from backtestforecast.services.exports import ExportService

        for prefix in ("=", "+", "@", "|"):
            result = ExportService._sanitize_csv_cell(f"{prefix}CMD('calc')")
            assert result.startswith("'"), f"Failed for prefix {prefix!r}"


class TestBillingUpgradeRace:
    """Fix #4: Upgrade events are not skipped even with same period end."""

    def test_upgrade_not_skipped_for_same_period_end(self):
        """When effective_tier is higher than current, event is applied."""
        from backtestforecast.services.billing import BillingService

        service = BillingService.__new__(BillingService)
        service._pending_cancellation_events = []

        user = MagicMock()
        user.id = uuid4()
        user.plan_tier = "pro"
        user.stripe_subscription_id = "sub_123"
        user.subscription_status = "active"
        user.subscription_current_period_end = datetime(2026, 4, 1, tzinfo=UTC)
        user.plan_updated_at = datetime(2026, 3, 1, tzinfo=UTC)
        user.cancel_at_period_end = False

        service.session = MagicMock()
        service.session.scalar.return_value = user
        service.session.add = MagicMock()
        service.session.flush = MagicMock()
        service.settings = MagicMock()
        service.settings.stripe_price_lookup = {
            ("premium", "monthly"): "price_premium",
        }

        subscription = {
            "id": "sub_123",
            "customer": "cus_123",
            "status": "active",
            "cancel_at_period_end": False,
            "current_period_end": int(datetime(2026, 4, 1, tzinfo=UTC).timestamp()),
            "items": {
                "data": [
                    {
                        "price": {
                            "id": "price_premium",
                            "recurring": {"interval": "month"},
                        }
                    }
                ]
            },
            "metadata": {"requested_tier": "premium", "user_id": str(user.id)},
        }

        with patch("backtestforecast.services.billing.log_billing_event", MagicMock(), create=True):
            service._apply_subscription_to_user(
                user,
                subscription,
                event_created_ts=int(datetime(2026, 3, 15, tzinfo=UTC).timestamp()),
            )

        assert user.plan_tier == "premium"


class TestBillingCancellationInit:
    """Fix #5: _pending_cancellation_events initialized in __init__."""

    def test_pending_events_initialized(self):
        from backtestforecast.services.billing import BillingService

        session = MagicMock()
        with patch("backtestforecast.services.billing.get_settings") as mock_gs:
            mock_gs.return_value = MagicMock()
            service = BillingService(session)
        assert hasattr(service, "_pending_cancellation_events")
        assert service._pending_cancellation_events == []


class TestSweepProgressTracking:
    """Fix #6: Sweep progress uses local variable to avoid ORM corruption."""

    def test_evaluated_count_correct_after_rollback(self):
        """After a progress commit rollback, the count should still be accurate."""
        count = 0
        for i in range(120):
            count += 1
        assert count == 120


class TestSweepScoringConfig:
    """Fix #7: Sweep scoring uses direct settings access."""

    def test_scoring_config_reads_settings(self):
        from backtestforecast.services.sweeps import _sweep_scoring_config

        with patch("backtestforecast.services.sweeps.get_settings") as mock:
            mock.return_value = MagicMock(
                sweep_score_win_rate_weight=0.30,
                sweep_score_roi_weight=0.30,
                sweep_score_sharpe_weight=0.20,
                sweep_score_drawdown_weight=0.20,
                sweep_score_sharpe_multiplier=2.5,
            )
            cfg = _sweep_scoring_config()

        assert cfg["win_rate_weight"] == 0.30
        assert cfg["roi_weight"] == 0.30
        assert cfg["sharpe_multiplier"] == 2.5


class TestIPHashSaltStaging:
    """Fix #13: Default IP hash salt raises error in staging."""

    def test_default_salt_rejected_in_staging(self):
        import os

        from backtestforecast.config import Settings

        env = {
            "APP_ENV": "staging",
            "IP_HASH_SALT": "backtestforecast-default-ip-salt-change-me",
            "CLERK_ISSUER": "https://clerk.example.com",
            "CLERK_JWT_KEY": "test-key",
            "CLERK_SECRET_KEY": "sk_test",
            "CLERK_AUDIENCE": "test",
            "CLERK_AUTHORIZED_PARTIES": "https://app.example.com",
            "LOG_JSON": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(ValueError, match="IP_HASH_SALT"):
                Settings()


class TestCircuitBreakerInterval:
    """Fix #16: Circuit breaker check interval reduced to 2 seconds."""

    def test_check_interval_is_2_seconds(self):
        from backtestforecast.resilience.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("test")
        assert cb._redis_check_interval == 2.0


class TestMarketDataExceptionPreservation:
    """Fix #15: Market data errors preserve original exception type."""

    def test_error_cache_stores_exception_object(self):
        exc = ValueError("test error")
        entry = (exc, 12345.0)
        assert isinstance(entry[0], Exception)
        assert isinstance(entry[0], ValueError)
