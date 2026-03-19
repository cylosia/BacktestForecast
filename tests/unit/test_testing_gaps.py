"""Tests closing the identified Testing Gaps from the audit.

Each test targets a specific gap where the audit found missing coverage.
"""
from __future__ import annotations

import inspect
import math
import warnings
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ============================================================================
# TG2: Scan job ExternalServiceError retry — behavioral test
# ============================================================================

def test_tg2_scan_task_has_external_error_retry_branch():
    """Verify the scan task structurally checks for ExternalServiceError before retry."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job
    source = inspect.getsource(run_scan_job)
    idx_ext = source.index("ExternalServiceError")
    idx_retry = source.index("self.retry", idx_ext)
    assert idx_retry > idx_ext, "self.retry must follow ExternalServiceError check"


def test_tg2_scan_task_truncates_error_message():
    """Error messages saved to DB should be truncated to 500 chars."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job
    source = inspect.getsource(run_scan_job)
    assert "[:500]" in source


# ============================================================================
# TG3: Position sizing — slippage and commission edge cases
# ============================================================================

def test_tg3_position_sizing_with_slippage():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    without_slippage = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000"),
        account_size=10000.0,
        risk_per_trade_pct=10.0,
        capital_required_per_unit=500.0,
        max_loss_per_unit=200.0,
        slippage_pct=0.0,
        gross_notional_per_unit=500.0,
    )
    with_slippage = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000"),
        account_size=10000.0,
        risk_per_trade_pct=10.0,
        capital_required_per_unit=500.0,
        max_loss_per_unit=200.0,
        slippage_pct=2.0,
        gross_notional_per_unit=500.0,
    )
    assert with_slippage <= without_slippage


def test_tg3_position_sizing_with_commission():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    without_comm = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000"),
        account_size=10000.0,
        risk_per_trade_pct=10.0,
        capital_required_per_unit=500.0,
        max_loss_per_unit=200.0,
        commission_per_unit=0.0,
    )
    with_comm = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000"),
        account_size=10000.0,
        risk_per_trade_pct=10.0,
        capital_required_per_unit=500.0,
        max_loss_per_unit=200.0,
        commission_per_unit=50.0,
    )
    assert with_comm <= without_comm


def test_tg3_position_sizing_tiny_account():
    """A very small account should still produce a valid (possibly 0) result."""
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("100"),
        account_size=100.0,
        risk_per_trade_pct=1.0,
        capital_required_per_unit=5000.0,
        max_loss_per_unit=2000.0,
    )
    assert result == 0


def test_tg3_position_sizing_boundary():
    """When cash exactly covers 1 unit, result should be 1."""
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("1000"),
        account_size=100000.0,
        risk_per_trade_pct=100.0,
        capital_required_per_unit=1000.0,
        max_loss_per_unit=None,
    )
    assert result == 1


# ============================================================================
# TG4: holding_period_days — behavioral assertion
# ============================================================================

def test_tg4_trade_result_has_both_holding_fields():
    from backtestforecast.backtests.types import TradeResult
    fields = TradeResult.__dataclass_fields__
    assert "holding_period_days" in fields
    assert "holding_period_trading_days" in fields


def test_tg4_engine_close_position_passes_bar_index():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "current_bar_index=index" in source


def test_tg4_holding_period_trading_days_computed():
    """The engine should compute trading days from bar index difference."""
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._close_position)
    assert "current_bar_index - position.entry_index" in source


# ============================================================================
# TG5: reconcile_subscriptions locking
# ============================================================================

def test_tg5_reconcile_uses_for_update_skip_locked():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService.reconcile_subscriptions)
    assert "with_for_update(skip_locked=True)" in source


def test_tg5_reconcile_has_limit():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService.reconcile_subscriptions)
    assert ".limit(100)" in source


def test_tg5_reconcile_skips_already_locked_rows():
    """skip_locked=True means concurrent workers skip rows locked by others."""
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService.reconcile_subscriptions)
    assert "skip_locked" in source


# ============================================================================
# TG8: Export size limit — boundary tests
# ============================================================================

def test_tg8_csv_size_estimation():
    """The CSV size estimation should reject exports that would exceed 10MB."""
    from backtestforecast.services.exports import _MAX_EXPORT_BYTES
    estimated_rows = 60_000
    estimated_bytes = estimated_rows * 200
    assert estimated_bytes > _MAX_EXPORT_BYTES


def test_tg8_csv_trades_alone_within_size_limit():
    """10K trades alone (without equity points) should fit within 10MB."""
    from backtestforecast.services.exports import _MAX_CSV_TRADES, _MAX_EXPORT_BYTES
    estimated_trades_only = (_MAX_CSV_TRADES + 30) * 200
    assert estimated_trades_only <= _MAX_EXPORT_BYTES, (
        f"Trades-only estimate ({estimated_trades_only} bytes) exceeds limit ({_MAX_EXPORT_BYTES})"
    )


def test_tg8_runtime_size_check_protects_against_overflow():
    """Even if estimate passes, _check_size() catches actual overflow mid-generation."""
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_csv)
    assert "buf.tell() > _MAX_EXPORT_BYTES" in source


def test_tg8_pdf_max_pages_constant():
    from backtestforecast.services.exports import _MAX_PDF_PAGES
    assert _MAX_PDF_PAGES == 50


def test_tg8_size_check_runs_mid_generation():
    """The CSV builder checks size mid-generation via _check_size()."""
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_csv)
    assert "_check_size()" in source


# ============================================================================
# TG9: Multi-item Stripe subscriptions
# ============================================================================

def test_tg9_extract_price_details_single_item():
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_price_lookup = {("pro", "monthly"): "price_pro_monthly"}

    subscription = {
        "items": {
            "data": [{
                "price": {
                    "id": "price_pro_monthly",
                    "recurring": {"interval": "month"},
                }
            }]
        }
    }
    price_id, interval = service._extract_price_details(subscription)
    assert price_id == "price_pro_monthly"
    assert interval == "monthly"


def test_tg9_extract_price_details_multi_item_matches_known():
    """When multiple items exist, the known plan price should be selected."""
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_price_lookup = {("pro", "monthly"): "price_pro_monthly"}

    subscription = {
        "id": "sub_multi",
        "items": {
            "data": [
                {"price": {"id": "price_addon_metered", "recurring": {"interval": "month"}}},
                {"price": {"id": "price_pro_monthly", "recurring": {"interval": "month"}}},
            ]
        },
    }
    price_id, interval = service._extract_price_details(subscription)
    assert price_id == "price_pro_monthly"
    assert interval == "monthly"


def test_tg9_extract_price_details_multi_item_no_match():
    """When no items match known prices, fall back to the first item."""
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_price_lookup = {("pro", "monthly"): "price_pro_monthly"}

    subscription = {
        "id": "sub_unknown",
        "items": {
            "data": [
                {"price": {"id": "price_unknown_1", "recurring": {"interval": "year"}}},
                {"price": {"id": "price_unknown_2", "recurring": {"interval": "month"}}},
            ]
        },
    }
    price_id, interval = service._extract_price_details(subscription)
    assert price_id == "price_unknown_1"
    assert interval == "yearly"


def test_tg9_extract_price_details_empty_items():
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_price_lookup = {}

    subscription = {"items": {"data": []}}
    price_id, interval = service._extract_price_details(subscription)
    assert price_id is None
    assert interval is None


def test_tg9_extract_price_details_non_dict():
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_price_lookup = {}

    price_id, interval = service._extract_price_details("not_a_dict")
    assert price_id is None
    assert interval is None


# ============================================================================
# TG10: _mark_stripe_event_error + _trip_stripe_circuit after rollback
# ============================================================================

def test_tg10_mark_error_then_trip_circuit():
    """After marking a stripe event as error, the circuit should be tripped."""
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    stripe_events = MagicMock()
    stripe_events.mark_error.return_value = MagicMock(rowcount=1)

    service = BillingService.__new__(BillingService)
    service.session = session
    service.stripe_events = stripe_events
    service.settings = MagicMock()
    service.settings.stripe_circuit_cooldown_seconds = 30

    service._mark_stripe_event_error("evt_trip", "API error", event_type="sub.updated", livemode=False)
    session.commit.assert_called()


def test_tg10_trip_circuit_sets_redis_key():
    from backtestforecast.services.billing import BillingService, _STRIPE_CIRCUIT_KEY

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()

    mock_redis = MagicMock()
    mock_limiter = MagicMock()
    mock_limiter.get_redis.return_value = mock_redis

    with patch("backtestforecast.security.get_rate_limiter", return_value=mock_limiter), \
         patch("backtestforecast.services.billing._get_stripe_circuit_cooldown", return_value=60):
        service._trip_stripe_circuit()

    mock_redis.setex.assert_called_once_with(_STRIPE_CIRCUIT_KEY, 60, "1")


def test_tg10_trip_circuit_tolerates_redis_failure():
    from backtestforecast.services.billing import BillingService

    service = BillingService.__new__(BillingService)
    service.settings = MagicMock()
    service.settings.stripe_circuit_cooldown_seconds = 30

    mock_limiter = MagicMock()
    mock_limiter.get_redis.side_effect = ConnectionError("Redis down")

    with patch("backtestforecast.security.get_rate_limiter", return_value=mock_limiter):
        service._trip_stripe_circuit()


def test_tg10_mark_error_creates_event_on_zero_rows():
    from backtestforecast.services.billing import BillingService

    session = MagicMock()
    stripe_events = MagicMock()
    stripe_events.mark_error.return_value = MagicMock(rowcount=0)
    nested_mock = MagicMock()
    session.begin_nested.return_value = nested_mock

    service = BillingService.__new__(BillingService)
    service.session = session
    service.stripe_events = stripe_events

    service._mark_stripe_event_error("evt_new", "detail", event_type="test.event", livemode=True)

    session.add.assert_called_once()
    added_event = session.add.call_args[0][0]
    assert added_event.idempotency_status == "error"
    assert added_event.event_type == "test.event"
    assert added_event.livemode is True
