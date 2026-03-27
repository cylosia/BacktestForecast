from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres


class _FakeCounter:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.amounts: list[float] = []

    def labels(self, **labels):
        self.calls.append(labels)
        return self

    def inc(self, amount: float = 1.0):
        self.amounts.append(amount)
        return None


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def test_massive_pagination_limit_records_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    from backtestforecast.errors import ExternalServiceError
    from backtestforecast.integrations.massive_client import _MassiveClientCore

    counter = _FakeCounter()
    monkeypatch.setattr(
        "backtestforecast.integrations.massive_client.UPSTREAM_PAGINATION_LIMIT_EXCEEDED_TOTAL",
        counter,
    )

    core = object.__new__(_MassiveClientCore)

    with pytest.raises(ExternalServiceError, match="safety limit"):
        core._raise_pagination_limit_exceeded(
            path="/v3/reference/dividends",
            pages_fetched=100,
            rows_collected=5000,
        )

    assert counter.calls == [{"provider": "massive", "endpoint": "/v3/reference/dividends"}]


def test_invalid_massive_pagination_next_url_records_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    from backtestforecast.errors import ExternalServiceError
    from backtestforecast.integrations.massive_client import MassiveClient

    counter = _FakeCounter()
    monkeypatch.setattr(
        "backtestforecast.integrations.massive_client.UPSTREAM_PAGINATION_FAILURES_TOTAL",
        counter,
    )

    def fake_get_json(self, path, params=None):
        return {
            "results": [{"ticker": "AAPL"}],
            "next_url": "https://evil.example.com/v3/reference/dividends?page=2",
        }

    monkeypatch.setattr(MassiveClient, "_get_json", fake_get_json)
    client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
    try:
        with pytest.raises(ExternalServiceError, match="invalid pagination continuation URL"):
            client._get_paginated_json("/v3/reference/dividends", params={"limit": 1})
    finally:
        client.close()

    assert counter.calls == [
        {
            "provider": "massive",
            "endpoint": "/v3/reference/dividends",
            "reason": "invalid_next_url",
        }
    ]


def test_cleanup_stripe_failures_record_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    from apps.api.app.routers.account import _cleanup_stripe

    billing = MagicMock()
    client = MagicMock()
    client.subscriptions.cancel.side_effect = RuntimeError("boom")
    client.customers.delete.side_effect = RuntimeError("boom")
    billing.get_stripe_client.return_value = client

    counter = _FakeCounter()
    monkeypatch.setattr("apps.api.app.routers.account.EXTERNAL_CLEANUP_FAILURES_TOTAL", counter)

    result = _cleanup_stripe(billing, "sub_live", "cus_live", uuid.uuid4())

    assert result == "failed"
    assert {"resource": "stripe_subscription", "operation": "cancel", "result": "failed"} in counter.calls
    assert {"resource": "stripe_customer", "operation": "delete", "result": "failed"} in counter.calls


def test_truncated_detail_payload_records_metric(monkeypatch: pytest.MonkeyPatch, db_session) -> None:
    from datetime import date, timedelta
    from decimal import Decimal

    from backtestforecast.models import BacktestEquityPoint
    from backtestforecast.services.backtests import BacktestService
    from tests.unit.test_compare_runs import _create_run, _create_user

    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    for idx in range(10_001):
        db_session.add(
            BacktestEquityPoint(
                run_id=run.id,
                trade_date=date(2024, 1, 1) + timedelta(days=idx),
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            )
        )
    db_session.commit()

    counter = _FakeCounter()
    monkeypatch.setattr("backtestforecast.services.backtests.TRUNCATED_PAYLOADS_TOTAL", counter)

    response = BacktestService(db_session).get_run_for_owner(user_id=user.id, run_id=run.id)

    assert response.equity_curve_truncated is True
    assert counter.calls == [{"surface": "backtest_detail", "kind": "equity_curve"}]


def test_truncated_detail_payload_records_omitted_item_count(monkeypatch: pytest.MonkeyPatch, db_session) -> None:
    from datetime import date, timedelta
    from decimal import Decimal

    from backtestforecast.models import BacktestEquityPoint
    from backtestforecast.services.backtests import BacktestService
    from tests.unit.test_compare_runs import _create_run, _create_user

    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    for idx in range(10_003):
        db_session.add(
            BacktestEquityPoint(
                run_id=run.id,
                trade_date=date(2024, 1, 1) + timedelta(days=idx),
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            )
        )
    db_session.commit()

    counter = _FakeCounter()
    monkeypatch.setattr("backtestforecast.services.backtests.TRUNCATED_PAYLOAD_ITEMS_TOTAL", counter)

    BacktestService(db_session).get_run_for_owner(user_id=user.id, run_id=run.id)

    assert counter.calls == [{"surface": "backtest_detail", "kind": "equity_curve"}]
    assert counter.amounts == [3]


def test_partial_detail_payload_records_derived_data_metric(monkeypatch: pytest.MonkeyPatch, db_session) -> None:
    from backtestforecast.services.backtests import BacktestService
    from tests.unit.test_compare_runs import _create_run, _create_trade, _create_user

    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.trade_count = 3
    db_session.add(run)
    db_session.commit()

    for idx in range(3):
        _create_trade(db_session, run, idx)
    db_session.commit()

    counter = _FakeCounter()
    monkeypatch.setattr("backtestforecast.services.backtests.DERIVED_RESPONSE_PARTIAL_DATA_TOTAL", counter)

    response = BacktestService(db_session).get_run_for_owner(user_id=user.id, run_id=run.id, trade_limit=2)

    assert response.summary.trade_count == 3
    assert {"surface": "backtest_detail", "reason": "partial_trade_payload"} in counter.calls
