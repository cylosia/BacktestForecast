"""Fixtures for full-stack smoke tests.

Uses SMOKE_TEST_DATABASE_URL if set (Postgres), otherwise sqlite://.
Skips when required infra (Postgres/Redis) is unavailable.
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base


def _check_postgres_available(url: str) -> bool:
    """Return True if Postgres at url is reachable."""
    try:
        engine = create_engine(url, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


def _check_redis_available() -> bool:
    """Return True if Redis is reachable."""
    try:
        import redis

        url = os.environ.get("REDIS_URL", os.environ.get("redis_url", "redis://localhost:6379/0"))
        client = redis.from_url(url)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


def _make_engine():
    """Use SMOKE_TEST_DATABASE_URL if set, else sqlite://."""
    url = os.environ.get("SMOKE_TEST_DATABASE_URL")
    if url:
        return create_engine(url)
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _is_sqlite(engine) -> bool:
    return engine.dialect.name == "sqlite"


@pytest.fixture(scope="module")
def smoke_engine():
    """Create engine for smoke tests. Skip if infra unavailable."""
    smoke_url = os.environ.get("SMOKE_TEST_DATABASE_URL")
    if smoke_url:
        if not _check_postgres_available(smoke_url):
            pytest.skip("Postgres unavailable (SMOKE_TEST_DATABASE_URL)")
        if not _check_redis_available():
            pytest.skip("Redis unavailable")
    engine = _make_engine()
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def smoke_session_factory(smoke_engine):
    """Session factory for smoke tests."""
    Base.metadata.create_all(smoke_engine)
    return sessionmaker(bind=smoke_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def db_session(smoke_session_factory) -> Generator[Session, None, None]:
    """Provide a database session for each test."""
    session = smoke_session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def smoke_uses_sqlite(smoke_engine) -> bool:
    """True when using SQLite (for skipping Postgres-specific tests)."""
    return _is_sqlite(smoke_engine)
