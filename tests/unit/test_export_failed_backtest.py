"""Item 68: Test export of failed backtest raises AppValidationError.

Calling enqueue_export with a backtest run whose status is 'failed'
must raise a ValidationError rather than proceeding to generate content.
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.db.base import Base
from backtestforecast.errors import AppValidationError
from backtestforecast.models import BacktestRun, User
from backtestforecast.schemas.exports import CreateExportRequest
from backtestforecast.services.exports import ExportService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="export_test_user", email="export@test.com", plan_tier="pro")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_failed_backtest(session: Session, user_id) -> BacktestRun:
    from datetime import date

    run = BacktestRun(
        user_id=user_id,
        status="failed",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 3, 31),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
        error_code="test_error",
        error_message="Backtest failed for testing",
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def test_enqueue_export_raises_for_failed_backtest(db_session: Session) -> None:
    user = _create_user(db_session)
    run = _create_failed_backtest(db_session, user.id)

    user.subscription_status = "active"
    db_session.commit()
    db_session.refresh(user)

    service = ExportService(db_session)

    payload = CreateExportRequest(
        run_id=run.id,
        export_format=ExportFormat.CSV,
    )

    with pytest.raises(AppValidationError, match="failed"):
        service.enqueue_export(user, payload)


def test_enqueue_export_succeeds_for_succeeded_backtest(db_session: Session) -> None:
    user = _create_user(db_session)
    from datetime import date

    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 3, 31),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)

    user.subscription_status = "active"
    db_session.commit()
    db_session.refresh(user)

    service = ExportService(db_session)

    payload = CreateExportRequest(
        run_id=run.id,
        export_format=ExportFormat.CSV,
    )

    result = service.enqueue_export(user, payload)
    assert result.status == "queued"
