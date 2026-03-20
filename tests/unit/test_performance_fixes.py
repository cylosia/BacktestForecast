"""Tests for performance fixes.

Covers:
- #2: Sweep candidate periodic trimming
- #5: Redis invalidate_symbol uses per-symbol key set
- #10: get_or_create reduced sleep time
"""
from __future__ import annotations

import inspect


class TestSweepCandidateTrimming:
    """Perf #2: Sweep should trim candidates periodically like the scanner."""

    def test_sweep_execute_has_trim_logic(self) -> None:
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_sweep)
        assert "_TRIM_INTERVAL" in source, (
            "Sweep execution must have periodic candidate trimming "
            "(_TRIM_INTERVAL) to bound memory like the scanner does"
        )

    def test_sweep_trims_heavy_fields(self) -> None:
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_sweep)
        assert "trades_json" in source, (
            "Sweep trimming should clear trades_json on low-ranked candidates"
        )
        assert "equity_curve" in source


class TestRedisInvalidateByKeySet:
    """Perf #5: invalidate_symbol should use per-symbol key set, not SCAN."""

    def test_invalidate_uses_smembers(self) -> None:
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache
        source = inspect.getsource(OptionDataRedisCache.invalidate_symbol)
        assert "smembers" in source, (
            "invalidate_symbol should use smembers to get tracked keys "
            "before falling back to SCAN"
        )

    def test_track_symbol_write_accepts_cache_key(self) -> None:
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache
        sig = inspect.signature(OptionDataRedisCache.track_symbol_write)
        assert "cache_key" in sig.parameters, (
            "track_symbol_write must accept a cache_key parameter to populate "
            "the per-symbol key set"
        )

    def test_set_contracts_passes_cache_key(self) -> None:
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache
        source = inspect.getsource(OptionDataRedisCache.set_contracts)
        assert "cache_key=key" in source, (
            "set_contracts must pass cache_key to track_symbol_write"
        )


class TestUserGetOrCreateSleep:
    """Perf #10: get_or_create should minimize sleep inside transaction."""

    def test_max_sleep_is_small(self) -> None:
        from backtestforecast.repositories.users import UserRepository
        source = inspect.getsource(UserRepository.get_or_create)
        assert "0.05" not in source, (
            "get_or_create should not sleep 50ms+ (old exponential backoff)"
        )

    def test_no_exponential_backoff(self) -> None:
        from backtestforecast.repositories.users import UserRepository
        source = inspect.getsource(UserRepository.get_or_create)
        assert "2 **" not in source and "2**" not in source, (
            "get_or_create should not use exponential backoff for retries"
        )
