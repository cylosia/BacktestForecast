from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from apps.api.app.dependencies import get_db, token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.db.base import Base
from backtestforecast.security.rate_limits import get_rate_limiter


def _make_engine():
    """Use Postgres when DATABASE_URL is set (CI), otherwise in-memory SQLite."""
    url = os.environ.get("DATABASE_URL")
    if url:
        return create_engine(url)
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


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


@pytest.fixture()
def immediate_backtest_execution(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: sessionmaker[Session],
    stub_execution: None,
) -> None:
    """Patch Celery send_task so backtest runs execute inline during tests."""
    from uuid import UUID

    import apps.api.app.routers.backtests as backtest_router
    from backtestforecast.services.backtests import BacktestService

    class FakeCeleryApp:
        def send_task(self, name: str, kwargs: dict[str, str], **extra):
            assert name == "backtests.run"
            with session_factory() as session:
                BacktestService(session).execute_run_by_id(UUID(kwargs["run_id"]))
            import types

            return types.SimpleNamespace(id="fake-task-id")

    monkeypatch.setattr(backtest_router, "celery_app", FakeCeleryApp(), raising=False)


@pytest.fixture()
def immediate_export_execution(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: sessionmaker[Session],
    stub_execution: None,
) -> None:
    """Patch Celery send_task so exports generate inline during tests."""
    from uuid import UUID

    import apps.api.app.routers.exports as export_router
    from backtestforecast.services.exports import ExportService

    class FakeCeleryApp:
        def send_task(self, name: str, kwargs: dict[str, str], **extra):
            assert name == "exports.generate"
            with session_factory() as session:
                ExportService(session).execute_export_by_id(UUID(kwargs["export_job_id"]))
            import types

            return types.SimpleNamespace(id="fake-task-id")

    monkeypatch.setattr(export_router, "celery_app", FakeCeleryApp(), raising=False)
