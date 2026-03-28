"""E2E test fixtures - reuse the integration test infrastructure."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import contextmanager
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.config import get_settings
from backtestforecast.models import User
from tests.integration import conftest as integration_conftest
from tests.integration.conftest import _fake_celery  # noqa: F401
from tests.integration.conftest import *  # noqa: F403
from tests.integration.test_endpoint_coverage import _set_user_plan
from tests.postgres_support import reset_database

ROOT = Path(__file__).resolve().parents[2]
_LOCAL_FALLBACK_BROKER = "celery-broker-e2e.sqlite3"


@pytest.fixture(scope="session")
def session_factory(postgres_session_factory: sessionmaker[Session]):
    reset_database(postgres_session_factory)
    with postgres_session_factory() as session:
        existing_user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").first()
        if existing_user is None:
            session.add(
                User(
                    clerk_user_id="clerk_test_user",
                    email="test@example.com",
                    plan_tier="free",
                    subscription_status=None,
                )
            )
            session.commit()
    yield postgres_session_factory


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]):
    reset_database(session_factory)
    with session_factory() as session:
        session.add(
            User(
                clerk_user_id="clerk_test_user",
                email="test@example.com",
                plan_tier="free",
                subscription_status=None,
            )
        )
        session.commit()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


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
        cache_url = os.environ.get("TEST_REDIS_URL") or settings.redis_cache_url or redis_url
        backend_url = settings.celery_result_backend_url or redis_url
    except Exception:
        redis_url = f"sqla+sqlite:///{_LOCAL_FALLBACK_BROKER}"
        cache_url = "redis://localhost:0/0"
        backend_url = "cache+memory://"

    worker_env = os.environ.copy()
    repo_pythonpath = os.pathsep.join([str(ROOT), str(ROOT / "src")])
    existing_pythonpath = worker_env.get("PYTHONPATH")
    worker_env["PYTHONPATH"] = (
        repo_pythonpath
        if not existing_pythonpath
        else os.pathsep.join([repo_pythonpath, existing_pythonpath])
    )
    worker_env["DATABASE_URL"] = database_url
    worker_env["REDIS_URL"] = redis_url
    worker_env["REDIS_CACHE_URL"] = cache_url
    worker_env["CELERY_RESULT_BACKEND_URL"] = backend_url
    worker_env["S3_BUCKET"] = os.environ.get("TEST_S3_BUCKET", "")
    worker_env["BFF_TEST_FAKE_BACKTEST_EXECUTION"] = "1"
    if redis_url.startswith("sqla+sqlite:///"):
        worker_env["REDIS_PASSWORD"] = ""
    return worker_env


@contextmanager
def _launch_real_worker() -> None:
    worker_env = _build_real_worker_env()
    using_local_fallback_broker = worker_env["REDIS_URL"].startswith("sqla+sqlite:///")
    hostname = f"pytest-real-worker-{int(time.time())}@%h"
    from apps.worker.app.celery_app import celery_app
    log_path = Path(tempfile.mkstemp(prefix="bff-real-worker-", suffix=".log")[1])
    log_handle = log_path.open("w", encoding="utf-8")

    original_broker_url = celery_app.conf.broker_url
    original_result_backend = celery_app.conf.result_backend
    original_transport_options = dict(celery_app.conf.broker_transport_options or {})
    if using_local_fallback_broker:
        celery_app.conf.broker_url = worker_env["REDIS_URL"]
        celery_app.conf.result_backend = worker_env["CELERY_RESULT_BACKEND_URL"]
        celery_app.conf.broker_transport_options = {}
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
                "backtests,exports,research,pipeline,maintenance",
                "--loglevel=WARNING",
                f"--hostname={hostname}",
            ],
            cwd=str(ROOT),
            env=worker_env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
    except Exception as exc:
        log_handle.close()
        celery_app.conf.broker_url = original_broker_url
        celery_app.conf.result_backend = original_result_backend
        celery_app.conf.broker_transport_options = original_transport_options
        pytest.skip(f"Unable to start a real Celery worker: {exc}")

    deadline = time.time() + 20
    ready = False
    if using_local_fallback_broker:
        time.sleep(3)
        ready = worker.poll() is None
    else:
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
        log_handle.close()
        celery_app.conf.broker_url = original_broker_url
        celery_app.conf.result_backend = original_result_backend
        celery_app.conf.broker_transport_options = original_transport_options
        log_excerpt = ""
        with suppress(Exception):
            log_excerpt = log_path.read_text(encoding="utf-8")[-4000:]
        pytest.skip(f"Timed out waiting for the real Celery worker to come online. Worker log tail:\n{log_excerpt}")

    try:
        yield
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait(timeout=10)
        log_handle.close()
        celery_app.conf.broker_url = original_broker_url
        celery_app.conf.result_backend = original_result_backend
        celery_app.conf.broker_transport_options = original_transport_options


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
        me_response = client.get("/v1/me", headers=auth_headers)
        assert me_response.status_code == 200
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
    session_factory,
) -> Callable[[str, str], dict[str, Any]]:
    def _create(run_id: str, export_format: str = "csv") -> dict[str, Any]:
        with session_factory() as session:
            _set_user_plan(session, tier="pro", subscription_status="active")
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
