"""Audit test fixtures.

WARNING: These tests run on SQLite, not PostgreSQL.  This means:
- Partial unique indexes (e.g. uq_audit_events_dedup_null_subject) are removed.
- PostgreSQL-specific CHECK constraints, JSONB operators, and triggers are
  NOT exercised.
- Any Postgres-specific audit bugs will NOT be caught by these tests.

To run against Postgres, set TEST_DATABASE_URL in your environment.
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base


def _make_engine():
    pg_url = os.environ.get("TEST_DATABASE_URL")
    if pg_url:
        return create_engine(pg_url, echo=False)
    return create_engine(
        "sqlite://",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _prepare_metadata_for_sqlite(engine) -> None:
    """Remove postgresql_where index that SQLite renders without WHERE clause."""
    if engine.dialect.name != "sqlite":
        return
    audit_table = Base.metadata.tables.get("audit_events")
    if audit_table is not None:
        for idx in list(audit_table.indexes):
            if idx.name == "uq_audit_events_dedup_null_subject":
                audit_table.indexes.discard(idx)
                break


@pytest.fixture()
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = _make_engine()
    _prepare_metadata_for_sqlite(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield factory
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
