"""Fixtures for full-stack smoke tests.

Uses ``SMOKE_TEST_DATABASE_URL`` when set, otherwise falls back to
``TEST_DATABASE_URL``. Smoke tests are Postgres-first and skip when the
required Postgres instance is unavailable.
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from sqlalchemy.orm import Session, sessionmaker

from tests.postgres_support import (
    apply_test_schema,
    assert_safe_test_database_url,
    make_postgres_engine,
    reset_database,
)


@pytest.fixture(scope="module")
def smoke_engine():
    """Create a Postgres engine for smoke tests or skip when unavailable."""
    smoke_url = os.environ.get("SMOKE_TEST_DATABASE_URL")
    if smoke_url:
        os.environ["TEST_DATABASE_URL"] = assert_safe_test_database_url(smoke_url)
    engine = make_postgres_engine()
    apply_test_schema(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def smoke_session_factory(smoke_engine):
    """Session factory for smoke tests."""
    return sessionmaker(bind=smoke_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def db_session(smoke_session_factory) -> Generator[Session, None, None]:
    """Provide a fresh database session for each smoke test."""
    reset_database(smoke_session_factory)
    session = smoke_session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def smoke_uses_sqlite(smoke_engine) -> bool:
    """Smoke tests are Postgres-first; SQLite fallback is no longer used."""
    return False
