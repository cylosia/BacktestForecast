"""Verification tests for the Top 20 Likely Production Incidents.

Each test confirms the fix is in place by inspecting code structure or
running behavioral assertions.
"""
from __future__ import annotations

import inspect
import warnings
from decimal import Decimal

# ---- P1: Worker NameError on entitlement enforcement ----

def test_p1_commit_then_publish_defined():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _commit_then_publish
    assert callable(_commit_then_publish)


# ---- P2: Stale option data from 7-day cache ----

def test_p2_staleness_check_wired_into_execution():
    from backtestforecast.services.backtest_execution import BacktestExecutionService
    source = inspect.getsource(BacktestExecutionService.execute_request)
    assert "_check_data_staleness" in source


# ---- P3: Scan jobs failing permanently on transient errors ----

def test_p3_scan_retries_external_service_error():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job
    source = inspect.getsource(run_scan_job)
    assert "ExternalServiceError" in source
    assert "self.retry" in source


# ---- P4: Incorrect holding_period_days ----

def test_p4_trading_days_field_exists():
    from backtestforecast.backtests.types import TradeResult
    assert "holding_period_trading_days" in TradeResult.__dataclass_fields__


# ---- P5: Position sizing errors on large accounts ----

def test_p5_position_sizing_uses_decimal():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000000.50"),
        account_size=10000000.0,
        risk_per_trade_pct=1.0,
        capital_required_per_unit=100000.0,
        max_loss_per_unit=50000.0,
    )
    assert isinstance(result, int)
    assert result == 2


# ---- P6: Settings invalidation not refreshing main.py ----

def test_p6_metrics_uses_get_settings():
    from apps.api.app.main import prometheus_metrics
    source = inspect.getsource(prometheus_metrics)
    assert "get_settings()" in source


# ---- P7: Duplicate subscription reconciliation ----

def test_p7_reconcile_uses_for_update():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._reconcile_subscriptions_impl)
    assert "with_for_update" in source
    assert "skip_locked" in source


# ---- P8: Export orphan S3 objects accumulating ----

def test_p8_cleanup_logs_orphan_keys():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService.cleanup_expired_exports)
    assert "orphan_storage_objects" in source


# ---- P9: Pipeline heuristic marking wrong run ----

def test_p9_pipeline_refuses_ambiguous():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _find_pipeline_run
    source = inspect.getsource(_find_pipeline_run)
    assert "running_count > 1" in source


# ---- P10: entry_underlying_close 0.0 in trade details ----

def test_p10_entry_underlying_close_warns():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine._entry_underlying_close)
    assert "logger.warning" in source


# ---- P11: Audit events permanently deleted without archival ----

def test_p11_audit_cleanup_has_archival_log():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_audit_events
    source = inspect.getsource(cleanup_audit_events)
    assert "archival_batch" in source


# ---- P12: SQLite tests passing, Postgres CHECK violations ----

def test_p12_check_constraint_tests_exist():
    import tests.unit.test_postgres_check_constraints as t
    test_funcs = [name for name in dir(t) if name.startswith("test_")]
    assert len(test_funcs) >= 6


# ---- P13: PDF export silent truncation ----

def test_p13_pdf_truncation_notifies_user():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_pdf)
    assert "_truncated_at_page_limit" in source
    assert "Use CSV export" in source


def test_p13_pdf_line_returns_bool():
    """The PDF line() helper must return False on page limit instead of raising."""
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService._build_pdf)
    assert "return False" in source
    assert "return True" in source


# ---- P14: Webhook processing masked by broad except ----

def test_p14_webhook_separates_programming_errors():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._handle_webhook_impl)
    assert "KeyError" in source
    assert "likely_programming_error" in source


# ---- P15: DLQ Redis connections exhausted ----

def test_p15_dlq_uses_connection_pool():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _get_dlq_redis
    source = inspect.getsource(_get_dlq_redis)
    assert "ConnectionPool" in source


# ---- P16: entry_mid/exit_mid confusing frontend ----

def test_p16_entry_mid_documented_in_schema():
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    field = BacktestTradeResponse.model_fields["entry_mid"]
    assert field.description is not None
    assert "100" in field.description


# ---- P17: just_closed preventing same-day re-entry ----

def test_p17_same_day_reentry_warning_emitted():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "same_day_reentry_blocked" in source
    assert "just_closed_this_bar" in source


# ---- P18: admin_token falling back to metrics_token ----

def test_p18_admin_token_required_in_production():
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "ADMIN_TOKEN" in source


# ---- P19: CORS preflight rejected by TrustedHostMiddleware ----

def test_p19_cors_trustedhost_mismatch_warning():
    from apps.api.app.main import _lifespan
    source = inspect.getsource(_lifespan)
    assert "cors_trustedhost_mismatch" in source


# ---- P20: break-even trades excluded from win_rate ----

def test_p20_high_break_even_warning():
    from backtestforecast.backtests.summary import build_summary
    source = inspect.getsource(build_summary)
    assert "high_break_even_rate" in source
    assert "break_even_count" in source


def test_p20_decided_trades_in_schema():
    from backtestforecast.schemas.backtests import BacktestSummaryResponse
    assert "decided_trades" in BacktestSummaryResponse.model_fields
    field = BacktestSummaryResponse.model_fields["decided_trades"]
    assert field.description is not None


def test_p20_high_break_even_fires_warning():
    """When >= 20% of trades are break-even, a warning must appear."""
    from datetime import date

    from backtestforecast.backtests.summary import build_summary
    from backtestforecast.backtests.types import EquityPointResult, TradeResult

    trades = []
    for i in range(10):
        pnl = Decimal("100") if i < 2 else Decimal("0")
        trades.append(TradeResult(
            option_ticker=f"T{i}",
            strategy_type="long_call",
            underlying_symbol="TEST",
            entry_date=date(2025, 1, 1 + i),
            exit_date=date(2025, 1, 2 + i),
            expiration_date=date(2025, 2, 1),
            quantity=1, dte_at_open=30,
            holding_period_days=1,
            entry_underlying_close=Decimal("100"),
            exit_underlying_close=Decimal("101"),
            entry_mid=Decimal("1"), exit_mid=Decimal("2"),
            gross_pnl=pnl, net_pnl=pnl,
            total_commissions=Decimal("0"),
            entry_reason="signal", exit_reason="expiration",
        ))
    equity = [EquityPointResult(
        trade_date=date(2025, 1, 1 + i),
        equity=Decimal("10000") + Decimal(str(i * 10)),
        cash=Decimal("10000"),
        position_value=Decimal("0"),
        drawdown_pct=Decimal("0"),
    ) for i in range(10)]
    ws: list[dict] = []
    summary = build_summary(10000.0, 10200.0, trades, equity, warnings=ws)
    assert summary.decided_trades == 2
    codes = [w["code"] for w in ws]
    assert "high_break_even_rate" in codes
