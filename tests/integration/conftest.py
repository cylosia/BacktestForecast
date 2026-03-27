from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from apps.api.app.dependencies import get_db
from apps.api.app.dependencies import get_token_verifier as _get_token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.db.base import Base
from backtestforecast.db.session import get_readonly_db
from backtestforecast.models import User
from backtestforecast.security.rate_limits import get_rate_limiter
from tests.postgres_support import apply_test_schema, assert_safe_test_database_url as _assert_safe_test_database_url


def _resolve_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Integration tests require an explicit TEST_DATABASE_URL pointing to an isolated Postgres database.")
    return _assert_safe_test_database_url(url)


def _make_engine():
    """Require an explicit isolated test database and verify it is reachable."""
    url = _resolve_database_url()
    engine = create_engine(url, connect_args={"connect_timeout": 2})
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except Exception:
        engine.dispose()
        pytest.skip(
            "Integration tests require a reachable Postgres instance. "
            "Set TEST_DATABASE_URL to an isolated test database and ensure Postgres is running."
        )
    return engine


@pytest.fixture(scope="session")
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = _make_engine()
    apply_test_schema(engine)
    testing_session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    with testing_session_factory() as session:
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
    try:
        yield testing_session_factory
    finally:
        with engine.begin() as conn:
            Base.metadata.drop_all(bind=conn)
        engine.dispose()


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    session = session_factory()
    session.begin_nested()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture(autouse=True)
def _reset_integration_state(session_factory: sessionmaker[Session]) -> Generator[None, None, None]:
    engine = session_factory.kw["bind"]
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "users" not in existing_tables:
        apply_test_schema(engine)
        existing_tables = set(inspect(engine).get_table_names())

    with session_factory() as session:
        for table in reversed(Base.metadata.sorted_tables):
            if table.name in existing_tables:
                session.execute(table.delete())
        session.commit()
        session.add(
            User(
                clerk_user_id="clerk_test_user",
                email="test@example.com",
                plan_tier="free",
                subscription_status=None,
            )
        )
        session.commit()
    get_rate_limiter().reset()
    try:
        yield
    finally:
        get_rate_limiter().reset()


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

    _verifier = _get_token_verifier()
    monkeypatch.setattr(_verifier, "verify_bearer_token", fake_verify)
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_readonly_db] = override_get_db
    # NOTE: Resetting the rate limiter here means integration tests never
    # exercise rate-limit enforcement. See test_rate_limit_enforcement.py
    # for dedicated coverage of the 429 path.
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
        self.control = self

    def register(self, task_name: str, handler: object) -> None:
        self._handlers[task_name] = handler

    def send_task(self, name: str, kwargs: dict[str, str], **extra: object):
        import types

        handler = self._handlers.get(name)
        if handler is None:
            import logging
            logging.getLogger("tests.integration.fake_celery").warning(
                "FakeCeleryApp dispatched unregistered task %s; register a handler for inline execution if needed.",
                name,
            )
            return types.SimpleNamespace(id=f"noop-{name}")
        handler(name, kwargs)  # type: ignore[operator]
        return types.SimpleNamespace(id="fake-task-id")

    def revoke(self, task_id: str, terminate: bool = False) -> None:
        return None


@pytest.fixture()
def _fake_celery(monkeypatch: pytest.MonkeyPatch) -> _FakeCeleryApp:
    import apps.worker.app.celery_app as worker_celery_mod

    fake = _FakeCeleryApp()
    monkeypatch.setattr(worker_celery_mod, "celery_app", fake)
    return fake


@pytest.fixture()
def stub_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtestforecast.services.backtests as bs
    import backtestforecast.services.scans as ss
    from tests.integration.fakes import FakeExecutionService, FakeForecaster

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
