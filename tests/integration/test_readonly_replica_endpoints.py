from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from apps.api.app.dependencies import get_db, get_token_verifier as _get_token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.config import get_settings, reset_settings_cache
from backtestforecast.db.base import Base
from backtestforecast.db.session import _invalidate_db_caches, get_readonly_db
from backtestforecast.models import BacktestRun, ScannerJob, SweepJob, User
from backtestforecast.security.rate_limits import get_rate_limiter


def _fake_verify(_token: str) -> AuthenticatedPrincipal:
    return AuthenticatedPrincipal(
        clerk_user_id="clerk_test_user",
        session_id="sess_test_123",
        email="test@example.com",
        claims={"sub": "clerk_test_user", "email": "test@example.com"},
    )


def _seed_replica_data(session: Session) -> dict[str, str]:
    user = User(
        clerk_user_id="clerk_test_user",
        email="test@example.com",
        plan_tier="pro",
        subscription_status="active",
        subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
    )
    session.add(user)
    session.flush()

    backtest = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2025, 1, 1),
        date_to=date(2025, 2, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("1"),
        risk_free_rate=Decimal("0.02"),
        input_snapshot_json={"symbol": "AAPL"},
        warnings_json=[],
        engine_version="test",
        data_source="test",
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
    scan = ScannerJob(
        user_id=user.id,
        name="Replica scan",
        status="succeeded",
        mode="standard",
        plan_tier_snapshot="pro",
        job_kind="manual",
        request_hash="scan-hash",
        candidate_count=1,
        evaluated_candidate_count=1,
        recommendation_count=0,
        request_snapshot_json={"symbols": ["AAPL"]},
        warnings_json=[],
        ranking_version="test",
        engine_version="test",
    )
    sweep = SweepJob(
        user_id=user.id,
        symbol="AAPL",
        status="succeeded",
        plan_tier_snapshot="pro",
        request_snapshot_json={"symbol": "AAPL"},
        request_hash="sweep-hash",
        total_configs=1,
        completed_configs=1,
        result_count=0,
        best_score=Decimal("1.0"),
        progress_pct=Decimal("100"),
    )
    session.add_all([backtest, scan, sweep])
    session.commit()
    return {"backtest_id": str(backtest.id), "scan_id": str(scan.id), "sweep_id": str(sweep.id)}


@pytest.mark.integration
def test_readonly_endpoints_use_replica_when_configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    primary_path = tmp_path / "primary.sqlite"
    replica_path = tmp_path / "replica.sqlite"
    primary_url = f"sqlite+pysqlite:///{primary_path}"
    replica_url = f"sqlite+pysqlite:///{replica_path}"

    primary_engine = create_engine(primary_url, connect_args={"check_same_thread": False})
    replica_engine = create_engine(replica_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(primary_engine)
    Base.metadata.create_all(replica_engine)
    primary_factory = sessionmaker(bind=primary_engine, autoflush=False, expire_on_commit=False)
    replica_factory = sessionmaker(bind=replica_engine, autoflush=False, expire_on_commit=False)

    ids = _seed_replica_data(replica_factory())

    monkeypatch.setenv("DATABASE_URL", primary_url)
    monkeypatch.setenv("DATABASE_READ_REPLICA_URL", replica_url)
    monkeypatch.setenv("APP_ENV", "test")
    reset_settings_cache()
    _invalidate_db_caches()

    def override_get_db():
        db = primary_factory()
        try:
            yield db
        finally:
            db.close()

    verifier = _get_token_verifier()
    monkeypatch.setattr(verifier, "verify_bearer_token", _fake_verify)
    app.dependency_overrides[get_db] = override_get_db
    get_rate_limiter().reset()

    try:
        with TestClient(app, base_url="http://localhost") as client:
            headers = {"Authorization": "Bearer test-token"}
            backtests = client.get("/v1/backtests", headers=headers)
            assert backtests.status_code == 200
            assert [item["id"] for item in backtests.json()["items"]] == [ids["backtest_id"]]

            backtest = client.get(f"/v1/backtests/{ids['backtest_id']}", headers=headers)
            assert backtest.status_code == 200
            assert backtest.json()["id"] == ids["backtest_id"]

            scans = client.get("/v1/scans", headers=headers)
            assert scans.status_code == 200
            assert [item["id"] for item in scans.json()["items"]] == [ids["scan_id"]]

            sweep = client.get(f"/v1/sweeps/{ids['sweep_id']}", headers=headers)
            assert sweep.status_code == 200
            assert sweep.json()["id"] == ids["sweep_id"]

            meta = client.get("/v1/meta", headers=headers)
            assert meta.status_code == 200
            assert meta.json()["features"]["backtests"] is get_settings().feature_backtests_enabled
    finally:
        get_rate_limiter().reset()
        app.dependency_overrides.clear()
        reset_settings_cache()
        _invalidate_db_caches()
        primary_engine.dispose()
        replica_engine.dispose()
