"""Smoke tests for core happy paths: backtest, export, and scanner job creation.

These run against a real or SQLite DB and validate that the service layer
can complete the fundamental create-and-execute flows without errors.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from backtestforecast.backtests.types import (
    BacktestExecutionResult,
    BacktestSummary,
    EquityPointResult,
    TradeResult,
)
from backtestforecast.models import User

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _ensure_user(db_session: Session, *, plan_tier: str = "pro") -> User:
    """Return a test user, creating if needed."""
    from backtestforecast.repositories.users import UserRepository

    repo = UserRepository(db_session)
    user = repo.get_or_create(f"clerk_smoke_{plan_tier}", f"smoke_{plan_tier}@test.com")
    user.plan_tier = plan_tier
    user.subscription_status = "active" if plan_tier != "free" else None
    db_session.commit()
    db_session.refresh(user)
    return user


class _StubExecutionService:
    """Minimal execution service that returns valid results."""

    class market_data_service:
        @staticmethod
        def prepare_backtest(request):
            from backtestforecast.market_data.types import DailyBar

            bars = [
                DailyBar(
                    trade_date=request.start_date + timedelta(days=i),
                    open_price=100 + i,
                    high_price=101 + i,
                    low_price=99 + i,
                    close_price=100.5 + i,
                    volume=1_000_000,
                )
                for i in range(5)
            ]
            return SimpleNamespace(bars=bars, earnings_dates=set(), option_gateway=None)

    def close(self) -> None:
        pass

    def execute_request(self, request, bundle=None) -> BacktestExecutionResult:
        entry_date = request.start_date + timedelta(days=5)
        exit_date = entry_date + timedelta(days=7)
        expiration_date = exit_date + timedelta(days=23)
        trade = TradeResult(
            option_ticker=f"{request.symbol}240119C00100000",
            strategy_type=request.strategy_type.value if hasattr(request.strategy_type, "value") else request.strategy_type,
            underlying_symbol=request.symbol,
            entry_date=entry_date,
            exit_date=exit_date,
            expiration_date=expiration_date,
            quantity=1,
            dte_at_open=30,
            holding_period_days=7,
            entry_underlying_close=100.0,
            exit_underlying_close=104.0,
            entry_mid=2.0,
            exit_mid=3.0,
            gross_pnl=100.0,
            net_pnl=99.0,
            total_commissions=1.0,
            entry_reason="test",
            exit_reason="test",
            detail_json={"scenario": "smoke"},
        )
        summary = BacktestSummary(
            trade_count=1,
            decided_trades=1,
            win_rate=100.0,
            total_roi_pct=9.9,
            average_win_amount=99.0,
            average_loss_amount=0.0,
            average_holding_period_days=7.0,
            average_dte_at_open=30.0,
            max_drawdown_pct=1.0,
            total_commissions=1.0,
            total_net_pnl=99.0,
            starting_equity=float(request.account_size),
            ending_equity=float(request.account_size) + 99.0,
        )
        equity_curve = [
            EquityPointResult(
                trade_date=entry_date,
                equity=float(request.account_size),
                cash=float(request.account_size) - 200.0,
                position_value=200.0,
                drawdown_pct=0.0,
            ),
            EquityPointResult(
                trade_date=exit_date,
                equity=float(request.account_size) + 99.0,
                cash=float(request.account_size) + 99.0,
                position_value=0.0,
                drawdown_pct=0.0,
            ),
        ]
        return BacktestExecutionResult(summary=summary, trades=[trade], equity_curve=equity_curve, warnings=[])


# ---------------------------------------------------------------------------
# 1. Backtest creation and execution
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_backtest_create_and_execute(db_session: Session) -> None:
    """Happy path: create a backtest run, execute it, and verify succeeded."""
    user = _ensure_user(db_session, plan_tier="pro")
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest
    from backtestforecast.services.backtests import BacktestService

    service = BacktestService(db_session, execution_service=_StubExecutionService())
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 3, 29),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
    )
    run = service.create_and_run(user, request)
    assert run.status == "succeeded"
    assert run.trade_count == 1
    assert run.total_net_pnl > 0


# ---------------------------------------------------------------------------
# 2. Export generation
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_export_csv_generation(db_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: create a backtest, then generate a CSV export."""
    import backtestforecast.services.backtests as bs_mod

    monkeypatch.setattr(bs_mod, "BacktestExecutionService", _StubExecutionService)

    user = _ensure_user(db_session, plan_tier="pro")
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest
    from backtestforecast.schemas.exports import CreateExportRequest
    from backtestforecast.services.backtests import BacktestService
    from backtestforecast.services.exports import ExportService

    bt_service = BacktestService(db_session, execution_service=_StubExecutionService())
    request = CreateBacktestRunRequest(
        symbol="MSFT",
        strategy_type="long_call",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 3, 29),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
    )
    run = bt_service.create_and_run(user, request)
    assert run.status == "succeeded"

    export_req = CreateExportRequest(run_id=run.id, format="csv")
    export_service = ExportService(db_session)
    result = export_service.create_export(user, export_req)
    assert result.status == "succeeded"
    assert result.file_name.endswith(".csv")


# ---------------------------------------------------------------------------
# 3. Scanner job creation
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_scanner_job_creation(db_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: create a scanner job and verify it is queued."""
    user = _ensure_user(db_session, plan_tier="pro")
    from backtestforecast.schemas.scans import CreateScannerJobRequest
    from backtestforecast.services.scans import ScanService

    service = ScanService(db_session, execution_service=_StubExecutionService())
    payload = CreateScannerJobRequest(
        name="Smoke scan",
        mode="basic",
        symbols=["AAPL"],
        strategy_types=["long_call"],
        rule_sets=[{"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}]}],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 3, 29),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        max_recommendations=5,
    )
    job = service.create_job(user, payload)
    assert job.status == "queued"
    assert job.candidate_count >= 1
