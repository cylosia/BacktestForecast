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
