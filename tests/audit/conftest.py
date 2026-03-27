"""Audit test fixtures.

Audit tests now use the shared Postgres harness so partial indexes,
constraints, and triggers are exercised under production-like semantics.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy.orm import Session, sessionmaker

from tests.postgres_support import build_postgres_session_factory, reset_database


@pytest.fixture(scope="session")
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    yield from build_postgres_session_factory()


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    reset_database(session_factory)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
