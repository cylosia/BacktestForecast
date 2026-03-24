"""Audit-round-8 operational correctness tests.

Covers:
1. Template limit raises ConfigurationError for unknown tiers
2. Billing webhook handler rolls back on unexpected exceptions
3. Unknown subscription status defaults to FREE tier
4. Export cleanup includes failed exports with storage keys
5. build_forecast uses effective_strategy (not None) for the forecaster
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.billing.entitlements import PlanTier, normalize_plan_tier
from backtestforecast.db.base import Base
from backtestforecast.errors import ConfigurationError
from backtestforecast.models import BacktestRun, ExportJob, User
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
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def free_user(db_session: Session) -> User:
    user = User(clerk_user_id="audit8_free", email="free@test.com", plan_tier="free")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture()
def pro_user(db_session: Session) -> User:
    user = User(
        clerk_user_id="audit8_pro",
        email="pro@test.com",
        plan_tier="pro",
        subscription_status="active",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


# ---------------------------------------------------------------------------
# 1. Template limit raises ConfigurationError for unknown tier
# ---------------------------------------------------------------------------

def test_template_limit_unknown_tier_raises(monkeypatch: pytest.MonkeyPatch):
    """If TEMPLATE_LIMITS is missing an entry for a tier returned by
    normalize_plan_tier, _resolve_template_limit should raise ConfigurationError."""
    from backtestforecast.services.templates import TEMPLATE_LIMITS, _resolve_template_limit

    patched = {k: v for k, v in TEMPLATE_LIMITS.items() if k != PlanTier.FREE}
    monkeypatch.setattr("backtestforecast.services.templates.TEMPLATE_LIMITS", patched)

    with pytest.raises(ConfigurationError, match="Unknown plan tier"):
        _resolve_template_limit("free", None)


# ---------------------------------------------------------------------------
# 2. Billing webhook handler rolls back on unexpected exceptions
# ---------------------------------------------------------------------------

def test_webhook_rolls_back_on_unexpected_exception(db_session: Session, free_user: User):
    """When _sync_subscription raises an unexpected exception, the session
    should be rolled back and the error re-raised."""
    from backtestforecast.services.billing import BillingService

    service = BillingService(db_session)

    mock_client = MagicMock()
    mock_client.construct_event.return_value = {
        "id": "evt_test_rollback",
        "type": "customer.subscription.updated",
        "livemode": False,
        "data": {"object": {"id": "sub_123", "customer": "cus_123", "metadata": {}}},
    }
    service._stripe_client = mock_client

    with patch.object(service, "_sync_subscription", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            service.handle_webhook(
                payload_bytes=b"{}",
                signature_header="sig",
            )


# ---------------------------------------------------------------------------
# 3. Unknown subscription status defaults to FREE tier
# ---------------------------------------------------------------------------

def test_unknown_subscription_status_defaults_to_free():
    """A subscription status not in PAID_STATUSES or INACTIVE_STATUSES
    should still resolve to FREE."""
    tier = normalize_plan_tier("pro", "some_unknown_status")
    assert tier == PlanTier.FREE


def test_none_subscription_status_defaults_to_free():
    tier = normalize_plan_tier("premium", None)
    assert tier == PlanTier.FREE


# ---------------------------------------------------------------------------
# 4. Export cleanup includes failed exports with storage keys
# ---------------------------------------------------------------------------

def _make_backtest_run(db_session: Session, user: User) -> BacktestRun:
    """Insert a minimal BacktestRun for FK satisfaction."""
    from datetime import date

    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="SPY",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
        warnings_json=[],
        trade_count=0,
        win_rate=Decimal("0"),
        total_roi_pct=Decimal("0"),
        average_win_amount=Decimal("0"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("0"),
        average_dte_at_open=Decimal("0"),
        max_drawdown_pct=Decimal("0"),
        total_commissions=Decimal("0"),
        total_net_pnl=Decimal("0"),
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10000"),
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run


def test_export_cleanup_includes_failed_with_storage_key(db_session: Session, free_user: User):
    """list_expired_for_cleanup must include failed exports that have a
    storage_key, so cleanup deletes their data."""
    from backtestforecast.repositories.export_jobs import ExportJobRepository

    run = _make_backtest_run(db_session, free_user)
    past = datetime.now(UTC) - timedelta(days=5)

    failed_with_key = ExportJob(
        user_id=free_user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="failed",
        file_name="test.csv",
        mime_type="text/csv",
        storage_key="s3://bucket/failed-export.csv",
        expires_at=past,
    )
    succeeded_with_key = ExportJob(
        user_id=free_user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name="test2.csv",
        mime_type="text/csv",
        storage_key="s3://bucket/ok-export.csv",
        expires_at=past,
    )
    queued_with_key = ExportJob(
        user_id=free_user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="queued",
        file_name="test3.csv",
        mime_type="text/csv",
        storage_key="s3://bucket/queued-export.csv",
        expires_at=past,
    )
    db_session.add_all([failed_with_key, succeeded_with_key, queued_with_key])
    db_session.commit()

    repo = ExportJobRepository(db_session)
    expired = repo.list_expired_for_cleanup(datetime.now(UTC), limit=100)
    expired_ids = {j.id for j in expired}

    assert failed_with_key.id in expired_ids, "Failed exports with storage_key should be included"
    assert succeeded_with_key.id in expired_ids, "Succeeded exports with storage_key should be included"
    assert queued_with_key.id not in expired_ids, "Queued exports should NOT be included"


# ---------------------------------------------------------------------------
# 5. build_forecast uses effective_strategy (not None)
# ---------------------------------------------------------------------------

def test_build_forecast_uses_effective_strategy_not_none(db_session: Session, pro_user: User):
    """When strategy_type is None, build_forecast should fall back to
    'long_call' and pass that to the forecaster -- never None."""
    from backtestforecast.services.scans import ScanService

    mock_execution = MagicMock()
    mock_forecaster = MagicMock()

    mock_bundle = MagicMock()
    mock_bundle.bars = []
    mock_execution.market_data_service.prepare_backtest.return_value = mock_bundle

    from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse

    mock_forecaster.forecast.return_value = HistoricalAnalogForecastResponse(
        symbol="AAPL",
        strategy_type="long_call",
        as_of_date=datetime.now(UTC).date(),
        horizon_days=30,
        analog_count=5,
        expected_return_low_pct=Decimal("-5"),
        expected_return_median_pct=Decimal("2"),
        expected_return_high_pct=Decimal("10"),
        positive_outcome_rate_pct=Decimal("60"),
        summary="Test summary",
        disclaimer="Test disclaimer",
        analog_dates=[],
    )

    service = ScanService(
        db_session,
        execution_service=mock_execution,
        forecaster=mock_forecaster,
    )

    service.build_forecast(
        user=pro_user,
        symbol="AAPL",
        strategy_type=None,
        horizon_days=30,
    )

    call_kwargs = mock_forecaster.forecast.call_args
    actual_strategy = call_kwargs.kwargs.get("strategy_type") or call_kwargs[1].get("strategy_type")
    assert actual_strategy == "long_call", (
        f"Expected 'long_call' as fallback, got {actual_strategy!r}"
    )
