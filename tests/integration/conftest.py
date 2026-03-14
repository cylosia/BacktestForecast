from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from apps.api.app.dependencies import get_db, token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.db.base import Base
from backtestforecast.security.rate_limits import get_rate_limiter


def _make_engine():
    """Require a real Postgres DATABASE_URL — SQLite hides Postgres-specific bugs."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip(
            "DATABASE_URL is not set — integration tests require a real Postgres instance. "
            "See the postgres-integration CI job for the expected configuration."
        )
    return create_engine(url)


@pytest.fixture()
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = _make_engine()
    Base.metadata.create_all(engine)
    testing_session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield testing_session_factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


@pytest.fixture()
def client(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[TestClient, None, None]:
    def override_get_db() -> Generator[Session, None, None]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    def fake_verify(_token: str) -> AuthenticatedPrincipal:
        return AuthenticatedPrincipal(
            clerk_user_id="clerk_test_user",
            session_id="sess_test_123",
            email="test@example.com",
            claims={"sub": "clerk_test_user", "email": "test@example.com"},
        )

    monkeypatch.setattr(token_verifier, "verify_bearer_token", fake_verify)
    app.dependency_overrides[get_db] = override_get_db
    get_rate_limiter().reset()
    try:
        with TestClient(app, base_url="http://localhost") as test_client:
            yield test_client
    finally:
        get_rate_limiter().reset()
        app.dependency_overrides.clear()


class _FakeCeleryApp:
    """Composable Celery stub: each fixture registers a handler by task name."""

    def __init__(self) -> None:
        self._handlers: dict[str, object] = {}

    def register(self, task_name: str, handler: object) -> None:
        self._handlers[task_name] = handler

    def send_task(self, name: str, kwargs: dict[str, str], **extra: object):
        import types

        handler = self._handlers.get(name)
        if handler is None:
            return types.SimpleNamespace(id="noop-task-id")
        handler(name, kwargs)  # type: ignore[operator]
        return types.SimpleNamespace(id="fake-task-id")


@pytest.fixture()
def _fake_celery(monkeypatch: pytest.MonkeyPatch) -> _FakeCeleryApp:
    import apps.api.app.dispatch as dispatch_mod

    fake = _FakeCeleryApp()
    monkeypatch.setattr(dispatch_mod, "celery_app", fake)
    return fake


@pytest.fixture()
def stub_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration.test_api_critical_flows import FakeExecutionService, FakeForecaster

    import backtestforecast.services.backtests as bs
    import backtestforecast.services.scans as ss

    monkeypatch.setattr(bs, "BacktestExecutionService", FakeExecutionService)
    monkeypatch.setattr(ss, "BacktestExecutionService", FakeExecutionService)
    monkeypatch.setattr(ss, "HistoricalAnalogForecaster", FakeForecaster)


@pytest.fixture()
def immediate_backtest_execution(
    _fake_celery: _FakeCeleryApp,
    session_factory: sessionmaker[Session],
    stub_execution: None,
) -> None:
    """Patch Celery send_task so backtest runs execute inline during tests."""
    from uuid import UUID

    from backtestforecast.services.backtests import BacktestService

    def _run(name: str, kwargs: dict[str, str]) -> None:
        assert name == "backtests.run"
        with session_factory() as session:
            BacktestService(session).execute_run_by_id(UUID(kwargs["run_id"]))

    _fake_celery.register("backtests.run", _run)


@pytest.fixture()
def immediate_export_execution(
    _fake_celery: _FakeCeleryApp,
    session_factory: sessionmaker[Session],
    stub_execution: None,
) -> None:
    """Patch Celery send_task so exports generate inline during tests."""
    from uuid import UUID

    from backtestforecast.services.exports import ExportService

    def _run(name: str, kwargs: dict[str, str]) -> None:
        assert name == "exports.generate"
        with session_factory() as session:
            ExportService(session).execute_export_by_id(UUID(kwargs["export_job_id"]))

    _fake_celery.register("exports.generate", _run)
