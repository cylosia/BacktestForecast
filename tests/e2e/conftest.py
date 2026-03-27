"""E2E test fixtures - reuse the integration test infrastructure."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from backtestforecast.config import get_settings
from tests.integration import conftest as integration_conftest
from tests.integration.conftest import *  # noqa: F403

ROOT = Path(__file__).resolve().parents[2]


def _build_real_worker_env() -> dict[str, str]:
    settings = get_settings()
    database_url = integration_conftest._resolve_database_url()
    redis_url = os.environ.get("TEST_REDIS_URL") or settings.redis_url

    try:
        from redis import Redis

        redis_client = Redis.from_url(redis_url, socket_timeout=2)
        try:
            redis_client.ping()
        finally:
            redis_client.close()
    except Exception:
        pytest.skip("Real worker lifecycle tests require a reachable Redis broker.")

    worker_env = os.environ.copy()
    worker_env["DATABASE_URL"] = database_url
    worker_env["REDIS_URL"] = redis_url
    worker_env["REDIS_CACHE_URL"] = os.environ.get("TEST_REDIS_URL") or settings.redis_cache_url or redis_url
    worker_env["CELERY_RESULT_BACKEND_URL"] = settings.celery_result_backend_url or redis_url
    worker_env["S3_BUCKET"] = os.environ.get("TEST_S3_BUCKET", "")
    return worker_env


@contextmanager
def _launch_real_worker() -> None:
    worker_env = _build_real_worker_env()
    hostname = f"pytest-real-worker-{int(time.time())}@%h"
    try:
        worker = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "celery",
                "-A",
                "apps.worker.app.celery_app:celery_app",
                "worker",
                "-P",
                "solo",
                "-Q",
                "research,exports,pipeline,maintenance",
                "--loglevel=WARNING",
                f"--hostname={hostname}",
            ],
            cwd=str(ROOT),
            env=worker_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        pytest.skip(f"Unable to start a real Celery worker: {exc}")

    from apps.worker.app.celery_app import celery_app

    deadline = time.time() + 20
    ready = False
    while time.time() < deadline:
        try:
            response = celery_app.control.inspect(timeout=1).ping()
            if response:
                ready = True
                break
        except Exception:
            pass
        time.sleep(1)

    if not ready:
        worker.terminate()
        worker.wait(timeout=10)
        pytest.skip("Timed out waiting for the real Celery worker to come online.")

    try:
        yield
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait(timeout=10)


@pytest.fixture()
def real_worker_launcher():
    @contextmanager
    def _launcher() -> None:
        with _launch_real_worker():
            yield

    return _launcher


@pytest.fixture()
def real_worker_stack(real_worker_launcher) -> None:
    with real_worker_launcher():
        yield


@pytest.fixture()
def prod_like_backtest_payload() -> Callable[..., dict[str, Any]]:
    def _build(symbol: str = "AAPL", **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": symbol,
            "strategy_type": "long_call",
            "start_date": "2024-01-02",
            "end_date": "2024-06-28",
            "target_dte": 35,
            "dte_tolerance_days": 5,
            "max_holding_days": 14,
            "account_size": "25000",
            "risk_per_trade_pct": "2.5",
            "commission_per_contract": "0.65",
            "profit_target_pct": "25",
            "stop_loss_pct": "35",
            "entry_rules": [
                {"type": "rsi", "operator": "lte", "threshold": "38", "period": 14},
            ],
        }
        payload.update(overrides)
        return payload

    return _build


@pytest.fixture()
def prod_like_backtest_run(
    client,
    auth_headers,
    immediate_backtest_execution,
    prod_like_backtest_payload,
) -> Callable[..., dict[str, Any]]:
    def _create(symbol: str = "AAPL", **overrides: Any) -> dict[str, Any]:
        response = client.post(
            "/v1/backtests",
            json=prod_like_backtest_payload(symbol=symbol, **overrides),
            headers=auth_headers,
        )
        assert response.status_code == 202
        payload = response.json()
        assert payload["status"] == "succeeded"
        return payload

    return _create


@pytest.fixture()
def prod_like_export_job(
    client,
    auth_headers,
    immediate_export_execution,
) -> Callable[[str, str], dict[str, Any]]:
    def _create(run_id: str, export_format: str = "csv") -> dict[str, Any]:
        response = client.post(
            "/v1/exports",
            json={"run_id": run_id, "format": export_format},
            headers=auth_headers,
        )
        assert response.status_code == 202
        payload = response.json()
        assert payload["status"] == "succeeded"
        return payload

    return _create


@pytest.fixture()
def prod_like_account_cleanup(monkeypatch) -> dict[str, Any]:
    calls: dict[str, Any] = {"cleanup": [], "retry": []}

    def _cleanup(_billing, subscription_id, customer_id, user_id):
        calls["cleanup"].append(
            {
                "subscription_id": subscription_id,
                "customer_id": customer_id,
                "user_id": str(user_id),
                "at": datetime.now(UTC).isoformat(),
            }
        )
        return "ok"

    def _dispatch(subscription_id, customer_id, user_id, sync_result):
        calls["retry"].append(
            {
                "subscription_id": subscription_id,
                "customer_id": customer_id,
                "user_id": str(user_id),
                "sync_result": sync_result,
            }
        )

    monkeypatch.setattr("apps.api.app.routers.account._cleanup_stripe", _cleanup)
    monkeypatch.setattr("apps.api.app.routers.account._dispatch_stripe_cleanup_retry", _dispatch)
    return calls
