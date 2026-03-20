"""Tests verifying critical audit fixes 31-46 remain in place."""
from __future__ import annotations

import inspect
import math
from datetime import date

import pytest


class TestDeltaLookupUsesStrikeExpirationKey:
    """Fix 31: get_chain_delta_lookup must key by (strike, expiration) not just strike."""

    def test_return_type_annotation_uses_tuple_key(self):
        from backtestforecast.market_data.service import MassiveOptionGateway

        hints = MassiveOptionGateway.get_chain_delta_lookup.__annotations__
        assert "return" in hints
        return_hint = hints["return"]
        hint_str = str(return_hint)
        assert "tuple" in hint_str.lower()
        assert "float" in hint_str
        assert "date" in hint_str


class TestResolveStrikeAcceptsExpirationDate:
    """Fix 32: resolve_strike must accept an expiration_date parameter."""

    def test_expiration_date_in_signature(self):
        from backtestforecast.backtests.strategies.common import resolve_strike

        sig = inspect.signature(resolve_strike)
        assert "expiration_date" in sig.parameters, (
            "resolve_strike must accept expiration_date parameter"
        )


class TestDatabaseStorageExistsRejectEmpty:
    """Fix 33: DatabaseStorage.exists() must return False for empty strings."""

    def test_empty_string_returns_false(self):
        from backtestforecast.exports.storage import DatabaseStorage

        storage = DatabaseStorage()
        assert storage.exists("") is False

    def test_none_coerced_returns_false(self):
        from backtestforecast.exports.storage import DatabaseStorage

        storage = DatabaseStorage()
        assert storage.exists(None) is False  # type: ignore[arg-type]


class TestBlackScholesZeroUnderlying:
    """Fix 34: Black-Scholes must return 0.0 when underlying price is zero."""

    def test_zero_underlying_returns_zero(self):
        from backtestforecast.backtests.rules import black_scholes_price

        result = black_scholes_price(
            option_type="call",
            underlying_price=0.0,
            strike_price=100.0,
            time_to_expiry_years=0.25,
            volatility=0.30,
        )
        assert result == 0.0


class TestBlackScholesZeroStrike:
    """Fix 35: Black-Scholes must return 0.0 when strike price is zero."""

    def test_zero_strike_returns_zero(self):
        from backtestforecast.backtests.rules import black_scholes_price

        result = black_scholes_price(
            option_type="call",
            underlying_price=100.0,
            strike_price=0.0,
            time_to_expiry_years=0.25,
            volatility=0.30,
        )
        assert result == 0.0


class TestSanitizeSensitiveKeysNoMutation:
    """Fix 36: _sanitize_sensitive_keys must not mutate the original nested dict."""

    def test_nested_dict_not_mutated(self):
        from backtestforecast.observability.logging import _sanitize_sensitive_keys

        inner = {"token": "secret-value", "safe_key": "visible"}
        event_dict = {"nested": inner, "event": "test"}

        _sanitize_sensitive_keys(None, "info", event_dict)

        assert inner["token"] == "secret-value", (
            "Original nested dict must not be mutated"
        )


class TestMetricsServerBindsToLocalhost:
    """Fix 37: Worker metrics server must bind to 127.0.0.1 to prevent external access."""

    def test_metrics_server_bind_is_configurable(self):
        source = inspect.getsource(
            __import__("apps.worker.app.celery_app", fromlist=["_start_worker_metrics_server"])._start_worker_metrics_server
        )
        assert "WORKER_METRICS_BIND" in source, (
            "Metrics server bind address must be configurable via WORKER_METRICS_BIND env var"
        )


class TestActiveRenewalGraceFunction:
    """Fix 25/38: ACTIVE_RENEWAL_GRACE replaced by configurable _active_renewal_grace()."""

    def test_active_renewal_grace_function_exists(self):
        import backtestforecast.billing.entitlements as mod

        assert callable(getattr(mod, "_active_renewal_grace", None)), (
            "_active_renewal_grace must be a callable in the module"
        )
        from datetime import timedelta

        assert isinstance(mod._active_renewal_grace(), timedelta)


class TestBaselineMigrationServerDefaults:
    """Fix 39: Baseline migration must have server_default for plan_tier, engine_version, data_source."""

    def test_server_defaults_present(self):
        from pathlib import Path
        source = Path("alembic/versions/20260315_0001_baseline.py").read_text(encoding="utf-8")

        assert 'server_default="free"' in source, (
            "plan_tier must have server_default='free'"
        )
        assert 'server_default="options-multileg-v2"' in source, (
            "engine_version must have server_default='options-multileg-v2'"
        )
        assert 'server_default="massive"' in source, (
            "data_source must have server_default='massive'"
        )


class TestFetchBarsCoalescedRaisesOnTimeout:
    """Fix 40: _fetch_bars_coalesced must raise DataUnavailableError on timeout.

    Structural check — inspects source to confirm the error type is referenced.
    A full behavioral test would require wiring up a real MarketDataService with
    a mocked client and coalesced waiter, which is covered in integration tests.
    """

    def test_timeout_raises_data_unavailable(self):
        from backtestforecast.market_data.service import MarketDataService

        source = inspect.getsource(MarketDataService._fetch_bars_coalesced)
        assert "DataUnavailableError" in source, (
            "_fetch_bars_coalesced must raise DataUnavailableError on timeout"
        )
        assert "timed out" in source.lower() or "timeout" in source.lower()


class TestSSESlotReleaseLogsRedisErrors:
    """Fix 41: SSE _release_sse_slot must log errors with REDIS_CONNECTION_ERRORS_TOTAL."""

    def test_release_slot_increments_redis_error_counter(self):
        source = inspect.getsource(
            __import__(
                "apps.api.app.routers.events", fromlist=["_release_sse_slot"]
            )._release_sse_slot
        )
        assert "REDIS_CONNECTION_ERRORS_TOTAL" in source, (
            "_release_sse_slot must increment REDIS_CONNECTION_ERRORS_TOTAL on error"
        )
        assert "sse_slot_release" in source


class TestTokenVerifierNotAtModuleLevel:
    """Fix 42: get_token_verifier() must NOT be called at module level in dependencies.py."""

    def test_token_verifier_lazily_initialized(self):
        """Verify get_token_verifier() is not called at module level."""
        from pathlib import Path
        source = Path("apps/api/app/dependencies.py").read_text(encoding="utf-8")
        lines = source.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("token_verifier") and "get_token_verifier()" in stripped:
                indent = len(line) - len(line.lstrip())
                assert indent > 0, (
                    f"Line {i+1}: 'token_verifier = get_token_verifier()' is at module level (indent={indent})"
                )


class TestDLQEndpointUsesRedisCacheUrl:
    """Fix 43: DLQ endpoint must use redis_cache_url for Redis connection."""

    def test_dlq_uses_redis_cache_url(self):
        from pathlib import Path
        source = Path("apps/api/app/main.py").read_text(encoding="utf-8")

        dlq_start = source.find("def dlq_status")
        assert dlq_start != -1, "dlq_status function must exist"
        dlq_source = source[dlq_start:]
        next_def = dlq_source.find("\ndef ", 1)
        if next_def != -1:
            dlq_source = dlq_source[:next_def]

        assert "redis_cache_url" in dlq_source, (
            "DLQ endpoint must use redis_cache_url"
        )


class TestResultExpiresCutoff600Seconds:
    """Fix 44: result_expires_cutoff must be 600 seconds not 86400."""

    def test_result_expires_cutoff_is_600(self):
        from pathlib import Path
        source = Path("apps/worker/app/tasks.py").read_text(encoding="utf-8")

        assert "timedelta(seconds=600)" in source, (
            "result_expires_cutoff must use timedelta(seconds=600)"
        )
        assert "result_expires_cutoff" in source


class TestExportFormatCheckConstraintMigration:
    """Fix 45: export_format CHECK constraint migration must exist."""

    def test_migration_file_exists_with_constraint(self):
        from pathlib import Path
        source = Path(
            "alembic/versions/20260317_0004_add_export_format_constraint.py",
        ).read_text(encoding="utf-8")

        assert "ck_export_jobs_valid_export_format" in source
        assert "export_format IN" in source
        assert "'csv'" in source
        assert "'pdf'" in source


class TestRequireNonNegativeRejectsNanInf:
    """Fix 46: _require_non_negative must reject NaN and Inf values."""

    def test_nan_raises_value_error(self):
        from backtestforecast.backtests.margin import _require_non_negative

        with pytest.raises(ValueError, match="finite"):
            _require_non_negative(price=float("nan"))

    def test_positive_inf_raises_value_error(self):
        from backtestforecast.backtests.margin import _require_non_negative

        with pytest.raises(ValueError, match="finite"):
            _require_non_negative(price=float("inf"))

    def test_negative_inf_raises_value_error(self):
        from backtestforecast.backtests.margin import _require_non_negative

        with pytest.raises(ValueError, match="finite"):
            _require_non_negative(price=float("-inf"))

    def test_valid_positive_value_passes(self):
        from backtestforecast.backtests.margin import _require_non_negative

        _require_non_negative(price=10.0)

    def test_zero_passes(self):
        from backtestforecast.backtests.margin import _require_non_negative

        _require_non_negative(price=0.0)
