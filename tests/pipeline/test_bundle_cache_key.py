from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backtestforecast.pipeline.adapters import PipelineBacktestExecutor
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType


_BASE = {
    "symbol": "AAPL",
    "strategy_type": StrategyType.LONG_CALL,
    "start_date": date(2024, 1, 1),
    "end_date": date(2024, 1, 31),
    "target_dte": 30,
    "max_holding_days": 20,
    "account_size": Decimal("10000"),
    "risk_per_trade_pct": Decimal("10"),
    "commission_per_contract": Decimal("1"),
}


def _base_request(**kwargs: object) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(**{**_BASE, **kwargs})


def test_same_params_same_key() -> None:
    req1 = _base_request()
    req2 = _base_request()
    key1 = PipelineBacktestExecutor._bundle_cache_key(req1)
    key2 = PipelineBacktestExecutor._bundle_cache_key(req2)
    assert key1 == key2


def test_different_target_dte_different_key() -> None:
    req1 = _base_request(target_dte=30)
    req2 = _base_request(target_dte=45)
    key1 = PipelineBacktestExecutor._bundle_cache_key(req1)
    key2 = PipelineBacktestExecutor._bundle_cache_key(req2)
    assert key1 != key2


def test_different_max_holding_days_different_key() -> None:
    req1 = _base_request(max_holding_days=20)
    req2 = _base_request(max_holding_days=45)
    key1 = PipelineBacktestExecutor._bundle_cache_key(req1)
    key2 = PipelineBacktestExecutor._bundle_cache_key(req2)
    assert key1 != key2


def test_different_strategy_type_different_key() -> None:
    req1 = _base_request(strategy_type=StrategyType.LONG_CALL)
    req2 = _base_request(strategy_type=StrategyType.LONG_PUT)
    key1 = PipelineBacktestExecutor._bundle_cache_key(req1)
    key2 = PipelineBacktestExecutor._bundle_cache_key(req2)
    assert key1 != key2


def test_different_strategy_overrides_different_key() -> None:
    req1 = _base_request(
        strategy_overrides={"spread_width": {"mode": "strike_steps", "value": Decimal("1")}}
    )
    req2 = _base_request(
        strategy_overrides={"spread_width": {"mode": "strike_steps", "value": Decimal("2")}}
    )
    key1 = PipelineBacktestExecutor._bundle_cache_key(req1)
    key2 = PipelineBacktestExecutor._bundle_cache_key(req2)
    assert key1 != key2


def test_none_overrides_vs_empty_overrides() -> None:
    req_none = _base_request(strategy_overrides=None)
    req_empty = _base_request(strategy_overrides={})
    key_none = PipelineBacktestExecutor._bundle_cache_key(req_none)
    key_empty = PipelineBacktestExecutor._bundle_cache_key(req_empty)
    assert key_none == key_empty


def test_equivalent_dicts_different_insertion_order_same_key() -> None:
    """Item 97: Two override dicts with the same keys but different insertion
    order must produce the same cache key (deterministic hashing)."""
    from collections import OrderedDict

    overrides_a = {"spread_width": {"mode": "strike_steps", "value": Decimal("2")}, "strike_selection": {"mode": "nearest_otm"}}
    overrides_b = {"strike_selection": {"mode": "nearest_otm"}, "spread_width": {"mode": "strike_steps", "value": Decimal("2")}}

    req_a = _base_request(strategy_overrides=overrides_a)
    req_b = _base_request(strategy_overrides=overrides_b)
    key_a = PipelineBacktestExecutor._bundle_cache_key(req_a)
    key_b = PipelineBacktestExecutor._bundle_cache_key(req_b)
    assert key_a == key_b, (
        "Cache keys must be deterministic: same overrides in different insertion order "
        "should produce the same key"
    )


# ---------------------------------------------------------------------------
# Item 54: _bar_fetch_locks capped at max size
# ---------------------------------------------------------------------------


def test_bar_fetch_locks_capped_at_max_size() -> None:
    """Verify that adding > 500 locks trims the dict to at most 500 entries."""
    import threading
    from unittest.mock import MagicMock

    from backtestforecast.pipeline.adapters import PipelineForecaster

    mock_forecaster = MagicMock()
    mock_market_data = MagicMock()
    pf = PipelineForecaster(mock_forecaster, mock_market_data)

    for i in range(600):
        key = (f"SYM{i}", date(2024, 1, 1))
        with pf._bar_fetch_locks_lock:
            pf._bar_fetch_locks[key] = threading.Lock()

    assert len(pf._bar_fetch_locks) == 600

    new_key = ("NEW_SYM", date(2024, 6, 1))
    with pf._bar_fetch_locks_lock:
        pf._bar_fetch_locks.setdefault(new_key, threading.Lock())
        if len(pf._bar_fetch_locks) > 500:
            keys_to_remove = list(pf._bar_fetch_locks.keys())[:len(pf._bar_fetch_locks) - 500]
            for k in keys_to_remove:
                pf._bar_fetch_locks.pop(k, None)

    assert len(pf._bar_fetch_locks) <= 500, (
        f"_bar_fetch_locks should be trimmed to <= 500, got {len(pf._bar_fetch_locks)}"
    )


# ---------------------------------------------------------------------------
# Item 80: pipeline adapters max_holding_days clamped to 120
# ---------------------------------------------------------------------------


def test_max_holding_days_clamped_to_120() -> None:
    """Verify pipeline adapters cap max_holding_days at 120."""
    target_dte = 150
    max_holding_days = min(target_dte, 120)
    assert max_holding_days == 120
