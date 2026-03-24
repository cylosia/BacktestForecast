"""Integration tests for safety-net scenarios: race conditions, entitlement checks, and invalid inputs."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest

from tests.integration.test_api_critical_flows import _backtest_payload, _create_backtest, _set_user_plan

# ---------------------------------------------------------------------------
# 1. User creation race condition (concurrent get_or_create)
# ---------------------------------------------------------------------------


def test_concurrent_user_creation_does_not_duplicate(
    session_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate two concurrent requests with the same clerk_user_id arriving
    before any user row exists. Both should resolve to the same user without
    raising an unhandled IntegrityError."""
    from backtestforecast.repositories.users import UserRepository

    clerk_id = "clerk_concurrent_test"
    email = "concurrent@example.com"
    results: list[UUID] = []
    errors: list[Exception] = []

    def _create_user() -> None:
        try:
            with session_factory() as session:
                repo = UserRepository(session)
                user = repo.get_or_create(clerk_id, email)
                session.commit()
                session.refresh(user)
                results.append(user.id)
        except Exception as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_create_user) for _ in range(2)]
        for f in futures:
            f.result()

    assert not errors, f"Unexpected errors during concurrent user creation: {errors}"
    assert len(results) == 2, "Both threads should have succeeded"
    assert results[0] == results[1], "Both threads should resolve to the same user ID"


# ---------------------------------------------------------------------------
# 2. Backtest entitlement check
# ---------------------------------------------------------------------------


def test_backtest_requires_strategy_entitlement(
    client, auth_headers, db_session, immediate_backtest_execution,
) -> None:
    """Free-tier users should not be able to run premium strategies."""
    payload = _backtest_payload(strategy_type="iron_butterfly")
    resp = client.post("/v1/backtests", json=payload, headers=auth_headers)
    assert resp.status_code in (403, 422), (
        f"Expected 403 or 422 for premium strategy on free tier, got {resp.status_code}"
    )


def test_backtest_entitlement_allows_free_strategy(
    client, auth_headers, immediate_backtest_execution,
) -> None:
    """Free-tier users should be able to run free strategies like long_call."""
    created = _create_backtest(client, auth_headers, symbol="AAPL", strategy_type="long_call")
    assert created["status"] == "succeeded"


def test_backtest_entitlement_pro_unlocks_more_strategies(
    client, auth_headers, db_session, immediate_backtest_execution,
) -> None:
    """Pro-tier users should be able to run strategies beyond the free set."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    created = _create_backtest(client, auth_headers, symbol="SPY", strategy_type="covered_call")
    assert created["status"] == "succeeded"


# ---------------------------------------------------------------------------
# 3. Export with invalid format
# ---------------------------------------------------------------------------


def test_export_with_invalid_format_returns_422(
    client, auth_headers, db_session, immediate_backtest_execution,
) -> None:
    """Submitting an export with an unrecognised format should return 422."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    run_id = _create_backtest(client, auth_headers)["id"]

    resp = client.post(
        "/v1/exports",
        json={"run_id": run_id, "format": "xlsx"},
        headers=auth_headers,
    )
    assert resp.status_code == 422, (
        f"Expected 422 for unsupported export format, got {resp.status_code}"
    )


def test_export_for_nonexistent_run_returns_404(
    client, auth_headers, db_session,
) -> None:
    """Requesting an export for a non-existent backtest run should return 404 or 403."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    resp = client.post(
        "/v1/exports",
        json={"run_id": "00000000-0000-0000-0000-000000000099", "format": "csv"},
        headers=auth_headers,
    )
    assert resp.status_code in (404, 403), (
        f"Expected 404 or 403 for non-existent run, got {resp.status_code}"
    )


def test_export_for_failed_run_returns_validation_error(
    client, auth_headers, db_session, stub_execution, _fake_celery,
) -> None:
    """Requesting an export for a failed backtest should fail with a validation error."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    def _boom(name: str, kwargs: dict[str, str]) -> None:
        raise ConnectionError("simulated failure")

    _fake_celery.register("backtests.run", _boom)
    created = _create_backtest(client, auth_headers)
    assert created["status"] == "failed"

    resp = client.post(
        "/v1/exports",
        json={"run_id": created["id"], "format": "csv"},
        headers=auth_headers,
    )
    assert resp.status_code in (400, 422), (
        f"Expected 400 or 422 for failed run export, got {resp.status_code}"
    )
