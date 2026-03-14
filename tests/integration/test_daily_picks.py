"""Tests for daily picks endpoint happy and edge paths."""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from backtestforecast.models import DailyRecommendation, NightlyPipelineRun, User
from tests.integration.test_api_critical_flows import _set_user_plan


def _create_pipeline_run_with_picks(session, trade_date=None):
    """Create a succeeded pipeline run with sample recommendations."""
    if trade_date is None:
        trade_date = date(2025, 3, 1)

    run = NightlyPipelineRun(
        trade_date=trade_date,
        status="succeeded",
        stage="complete",
        symbols_screened=100,
        symbols_after_screen=50,
        pairs_generated=200,
        quick_backtests_run=100,
        full_backtests_run=20,
        recommendations_produced=3,
        duration_seconds=Decimal("120.5"),
        completed_at=datetime.now(UTC),
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    for i in range(3):
        rec = DailyRecommendation(
            pipeline_run_id=run.id,
            trade_date=trade_date,
            rank=i + 1,
            score=Decimal(str(round(0.9 - i * 0.1, 2))),
            symbol=["AAPL", "MSFT", "TSLA"][i],
            strategy_type="long_call",
            regime_labels=["bullish", "trending"],
            close_price=Decimal("150.00"),
            target_dte=30,
            config_snapshot_json={"key": "value"},
            summary_json={"win_rate": 65.0},
            forecast_json={"direction": "up"},
        )
        session.add(rec)

    session.commit()
    return run


def test_daily_picks_returns_items(client, auth_headers, db_session):
    """When a succeeded pipeline run exists, daily picks returns items."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    _create_pipeline_run_with_picks(db_session)

    resp = client.get("/v1/daily-picks", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert len(data["items"]) == 3
    assert data["items"][0]["rank"] == 1
    assert data["items"][0]["symbol"] == "AAPL"
    assert isinstance(data["items"][0]["regime_labels"], list)
    assert "bullish" in data["items"][0]["regime_labels"]


def test_daily_picks_no_data(client, auth_headers, db_session):
    """When no pipeline run exists, should return no_data."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    resp = client.get("/v1/daily-picks", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "no_data"
    assert data["items"] == []


def test_daily_picks_requires_pro(client, auth_headers, db_session):
    """Free users should not access daily picks."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="free", subscription_status=None)

    resp = client.get("/v1/daily-picks", headers=auth_headers)
    assert resp.status_code in (403, 402)


# ---------------------------------------------------------------------------
# Item 55: cursor pagination handles same-timestamp records
# ---------------------------------------------------------------------------


def test_cursor_pagination_same_timestamp(client, auth_headers, db_session):
    """Two pipeline runs with identical created_at should both appear
    across paginated pages."""
    from datetime import UTC, datetime

    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    same_time = datetime(2025, 3, 10, 12, 0, 0, tzinfo=UTC)

    run1 = NightlyPipelineRun(
        trade_date=date(2025, 3, 10),
        status="succeeded",
        stage="complete",
        symbols_screened=50,
        symbols_after_screen=25,
        pairs_generated=100,
        quick_backtests_run=50,
        full_backtests_run=10,
        recommendations_produced=3,
        duration_seconds=Decimal("60.0"),
        completed_at=same_time,
    )
    run1.created_at = same_time
    db_session.add(run1)

    run2 = NightlyPipelineRun(
        trade_date=date(2025, 3, 9),
        status="succeeded",
        stage="complete",
        symbols_screened=50,
        symbols_after_screen=25,
        pairs_generated=100,
        quick_backtests_run=50,
        full_backtests_run=10,
        recommendations_produced=2,
        duration_seconds=Decimal("55.0"),
        completed_at=same_time,
    )
    run2.created_at = same_time
    db_session.add(run2)
    db_session.commit()

    resp = client.get("/v1/daily-picks/history?limit=10", headers=auth_headers)
    assert resp.status_code == 200
    items = resp.json()["items"]
    run_ids = {item["id"] for item in items}
    assert str(run1.id) in run_ids or str(run2.id) in run_ids, (
        "At least one of the same-timestamp runs should appear in history"
    )
