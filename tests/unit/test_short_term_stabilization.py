"""Verification tests for the Short-Term Stabilization items.

Tests all 6 items from the audit's "Short-Term Stabilization (This Week)" list:
  ST4: FOR UPDATE locking on reconcile_subscriptions
  ST5: _resolve_position_size Decimal arithmetic
  ST6: holding_period_trading_days field
  ST7: _entry_underlying_close logging
  ST8: Worker entitlement enforcement paths
  ST9: Webhook payload size limit
"""
from __future__ import annotations

import inspect
import warnings
from decimal import Decimal
from unittest.mock import MagicMock

# ============================================================================
# ST4: reconcile_subscriptions uses FOR UPDATE with skip_locked
# ============================================================================

def test_st4_reconcile_query_uses_for_update():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._reconcile_subscriptions_impl)
    assert "with_for_update(skip_locked=True)" in source


def test_st4_reconcile_query_has_limit():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._reconcile_subscriptions_impl)
    assert ".limit(100)" in source


# ============================================================================
# ST5: _resolve_position_size accepts and uses Decimal
# ============================================================================

def test_st5_position_sizing_signature_accepts_decimal():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    sig = inspect.signature(OptionsBacktestEngine._resolve_position_size)
    param = sig.parameters["available_cash"]
    assert "Decimal" in str(param.annotation)


def test_st5_position_sizing_uses_decimal_internally():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._resolve_position_size)
    assert "_D(" in source
    assert "d_cash" in source


def test_st5_position_sizing_result_is_int():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("50000"),
        account_size=50000.0,
        risk_per_trade_pct=5.0,
        capital_required_per_unit=1000.0,
        max_loss_per_unit=500.0,
    )
    assert isinstance(result, int)
    assert result > 0


# ============================================================================
# ST6: holding_period_trading_days field exists and is populated
# ============================================================================

def test_st6_trade_result_has_trading_days_field():
    from backtestforecast.backtests.types import TradeResult
    assert "holding_period_trading_days" in TradeResult.__dataclass_fields__


def test_st6_schema_has_trading_days_field():
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    assert "holding_period_trading_days" in BacktestTradeResponse.model_fields


def test_st6_engine_computes_trading_days():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._close_position)
    assert "current_bar_index - position.entry_index" in source
    assert "holding_period_trading_days" in source


# ============================================================================
# ST7: _entry_underlying_close warns instead of silently returning 0.0
# ============================================================================

def test_st7_entry_underlying_close_warns():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._entry_underlying_close)
    assert "missing_entry_underlying_close" in source
    assert "logger.warning" in source


def test_st7_entry_underlying_close_still_returns_float():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    position = MagicMock()
    position.stock_legs = [MagicMock(entry_price=150.0)]
    result = OptionsBacktestEngine._entry_underlying_close(position)
    assert isinstance(result, float)
    assert result == 150.0


# ============================================================================
# ST8: Worker entitlement enforcement paths tested
# ============================================================================

def test_st8_backtest_task_handles_missing_user():
    """run_backtest must call _commit_then_publish when user is None."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_backtest
    source = inspect.getsource(run_backtest)
    idx_user_none = source.index("user is None")
    idx_commit = source.index("_commit_then_publish", idx_user_none)
    assert idx_commit > idx_user_none
    idx_return = source.index('return {"status": "failed"', idx_commit)
    assert idx_return > idx_commit
    assert "entitlement_revoked" in source[idx_user_none:idx_return]


def test_st8_backtest_task_handles_zero_quota():
    """run_backtest must handle monthly_backtest_quota <= 0."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_backtest
    source = inspect.getsource(run_backtest)
    assert "monthly_backtest_quota <= 0" in source
    idx_zero = source.index("monthly_backtest_quota <= 0")
    idx_commit = source.index("_commit_then_publish", idx_zero)
    assert idx_commit > idx_zero


def test_st8_backtest_task_handles_quota_exceeded():
    """run_backtest must handle used >= monthly_backtest_quota."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_backtest
    source = inspect.getsource(run_backtest)
    assert "quota_exceeded" in source
    assert "used >= policy.monthly_backtest_quota" in source


def test_st8_export_task_handles_missing_user():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import generate_export
    source = inspect.getsource(generate_export)
    assert "user is None" in source
    assert "_commit_then_publish" in source


def test_st8_export_task_handles_revoked_format():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import generate_export
    source = inspect.getsource(generate_export)
    assert "unsupported_format" in source
    assert "entitlement_revoked" in source


def test_st8_analysis_task_handles_missing_user():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_deep_analysis
    source = inspect.getsource(run_deep_analysis)
    assert "user is None" in source
    assert "_commit_then_publish" in source


def test_st8_analysis_task_handles_concurrent_limit():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_deep_analysis
    source = inspect.getsource(run_deep_analysis)
    assert "concurrent_limit" in source
    assert "_commit_then_publish" in source


def test_st8_all_nine_commit_then_publish_calls_exist():
    """All 9+ _commit_then_publish call sites must exist."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks as tasks_mod
    source = inspect.getsource(tasks_mod)
    count = source.count("_commit_then_publish(")
    assert count >= 9, f"Expected >= 9 _commit_then_publish calls, found {count}"


# ============================================================================
# ST9: Webhook payload size limited
# ============================================================================

def test_st9_webhook_has_max_length():
    from apps.api.app.routers.billing import stripe_webhook
    source = inspect.getsource(stripe_webhook)
    assert "max_length=512_000" in source or "max_length=512000" in source


def test_st9_body_limit_override_for_webhook():
    from backtestforecast.security.http import BODY_LIMIT_OVERRIDES
    assert "/v1/billing/webhook" in BODY_LIMIT_OVERRIDES
    assert BODY_LIMIT_OVERRIDES["/v1/billing/webhook"] == 512_000
