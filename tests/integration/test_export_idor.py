"""Test: IDOR protection on export download endpoint.

Verifies that user A cannot download an export owned by user B.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from backtestforecast.models import BacktestRun, ExportJob, User


@pytest.fixture()
def two_users(db_session: Session):
    user_a = User(
        clerk_user_id="idor_user_a",
        email="a@test.com",
        plan_tier="premium",
        subscription_status="active",
    )
    user_b = User(
        clerk_user_id="idor_user_b",
        email="b@test.com",
        plan_tier="premium",
        subscription_status="active",
    )
    db_session.add_all([user_a, user_b])
    db_session.commit()
    db_session.refresh(user_a)
    db_session.refresh(user_b)
    return user_a, user_b


@pytest.fixture()
def user_b_export(db_session: Session, two_users):
    _, user_b = two_users
    run = BacktestRun(
        user_id=user_b.id,
        symbol="AAPL",
        strategy_type="long_call",
        status="succeeded",
        date_from=datetime(2024, 1, 1).date(),
        date_to=datetime(2024, 6, 1).date(),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
    )
    db_session.add(run)
    db_session.flush()

    export = ExportJob(
        user_id=user_b.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        size_bytes=10,
        content_bytes=b"test data!",
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    db_session.add(export)
    db_session.commit()
    db_session.refresh(export)
    return export


def test_user_a_cannot_download_user_b_export(
    client: TestClient, two_users, user_b_export, monkeypatch
):
    user_a, _ = two_users
    export_id = user_b_export.id

    from apps.api.app import dependencies

    original_get_current_user = dependencies.get_current_user

    def fake_get_current_user(*args, **kwargs):
        return user_a

    monkeypatch.setattr(dependencies, "get_current_user", fake_get_current_user)

    response = client.get(
        f"/v1/exports/{export_id}",
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 404
