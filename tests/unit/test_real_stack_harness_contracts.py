from __future__ import annotations

import inspect
import importlib
from contextlib import contextmanager

import pytest

integration_conftest = importlib.import_module("tests.integration.conftest")
e2e_conftest = importlib.import_module("tests.e2e.conftest")
postgres_support = importlib.import_module("tests.postgres_support")


def test_safe_test_database_url_requires_postgres_and_test_named_database() -> None:
    assert integration_conftest._assert_safe_test_database_url(
        "postgresql://user:pass@localhost/backtestforecast_test",
    ).endswith("backtestforecast_test")

    with pytest.raises(RuntimeError, match="Postgres driver"):
        integration_conftest._assert_safe_test_database_url("sqlite:///tmp/test.db")

    with pytest.raises(RuntimeError, match="isolated test database"):
        integration_conftest._assert_safe_test_database_url(
            "postgresql://user:pass@localhost/backtestforecast",
        )


def test_resolve_database_url_requires_explicit_test_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)

    with pytest.raises(pytest.skip.Exception, match="explicit TEST_DATABASE_URL"):
        integration_conftest._resolve_database_url()


def test_resolve_database_url_rejects_unsafe_database_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@localhost/backtestforecast")

    with pytest.raises(RuntimeError, match="isolated test database"):
        integration_conftest._resolve_database_url()


def test_real_worker_harness_uses_resolved_test_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@localhost/backtestforecast_e2e_test")
    monkeypatch.setenv("TEST_REDIS_URL", "redis://localhost:6379/15")

    class _FakeRedis:
        @classmethod
        def from_url(cls, _url: str, socket_timeout: int = 2):
            return cls()

        def ping(self) -> bool:
            return True

        def close(self) -> None:
            return None

    monkeypatch.setattr("redis.Redis", _FakeRedis)

    env = e2e_conftest._build_real_worker_env()

    assert env["DATABASE_URL"] == "postgresql://user:pass@localhost/backtestforecast_e2e_test"
    assert env["REDIS_URL"] == "redis://localhost:6379/15"
    assert env["BFF_TEST_FAKE_BACKTEST_EXECUTION"] == "1"


def test_real_worker_harness_falls_back_to_local_sqlite_broker_when_redis_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@localhost/backtestforecast_e2e_test")
    monkeypatch.delenv("TEST_REDIS_URL", raising=False)

    class _BrokenRedis:
        @classmethod
        def from_url(cls, _url: str, socket_timeout: int = 2):
            raise RuntimeError("redis unavailable")

    monkeypatch.setattr("redis.Redis", _BrokenRedis)

    env = e2e_conftest._build_real_worker_env()

    assert env["DATABASE_URL"] == "postgresql://user:pass@localhost/backtestforecast_e2e_test"
    assert env["REDIS_URL"].startswith("sqla+sqlite:///")
    assert env["CELERY_RESULT_BACKEND_URL"] == "cache+memory://"
    assert env["BFF_TEST_FAKE_BACKTEST_EXECUTION"] == "1"


def test_real_worker_launcher_wraps_worker_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    @contextmanager
    def _fake_launch():
        calls.append("enter")
        try:
            yield
        finally:
            calls.append("exit")

    monkeypatch.setattr(e2e_conftest, "_launch_real_worker", _fake_launch)

    launcher_factory = e2e_conftest.real_worker_launcher.__wrapped__()
    with launcher_factory():
        calls.append("inside")

    assert calls == ["enter", "inside", "exit"]


def test_apply_test_schema_requires_alembic_to_create_full_schema() -> None:
    source = inspect.getsource(postgres_support.apply_test_schema)

    assert "Base.metadata.create_all" not in source
    assert "missing tables" in source.lower()
