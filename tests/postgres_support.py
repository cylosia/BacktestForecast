from __future__ import annotations

import os
from collections.abc import Generator
from urllib.parse import urlparse

import pytest
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base


def assert_safe_test_database_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if not scheme.startswith("postgres"):
        raise RuntimeError("TEST_DATABASE_URL must use a Postgres driver.")

    database_name = parsed.path.rsplit("/", 1)[-1].lower()
    if not database_name or database_name == "/":
        raise RuntimeError("TEST_DATABASE_URL must include a concrete database name.")

    safe_markers = (
        "_test",
        "-test",
        "test_",
        "_pytest",
        "-pytest",
        "pytest_",
        "_ci",
        "-ci",
        "ci_",
        "_e2e",
        "-e2e",
        "e2e_",
    )
    if not (database_name in {"test", "pytest", "ci", "e2e"} or any(marker in database_name for marker in safe_markers)):
        raise RuntimeError(
            "TEST_DATABASE_URL must point to an isolated test database "
            "(database name should include one of: test, pytest, ci, e2e)."
        )

    return url


def resolve_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Postgres-backed tests require TEST_DATABASE_URL pointing to an isolated Postgres database.")
    return assert_safe_test_database_url(url)


def make_postgres_engine() -> Engine:
    url = resolve_test_database_url()
    engine = create_engine(
        url,
        connect_args={
            "connect_timeout": 2,
            "options": "-c timezone=UTC",
        },
    )
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except Exception:
        engine.dispose()
        pytest.skip(
            "Postgres-backed tests require a reachable Postgres instance. "
            "Set TEST_DATABASE_URL to an isolated test database and ensure Postgres is running."
        )
    return engine


def apply_test_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False))
    alembic_upgrade(alembic_cfg, "head")
    existing_tables = set(inspect(engine).get_table_names())
    expected_tables = {table.name for table in Base.metadata.sorted_tables}
    missing_tables = sorted(expected_tables - existing_tables)
    if missing_tables:
        raise RuntimeError(
            "Alembic upgrade did not materialize the full schema. Missing tables: "
            + ", ".join(missing_tables)
        )
    engine.dispose()


def reset_database(session_factory: sessionmaker[Session]) -> None:
    engine = session_factory.kw["bind"]
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if not existing_tables:
        apply_test_schema(engine)
    with session_factory() as session:
        existing_tables = set(inspect(session.connection()).get_table_names())
        for table in reversed(Base.metadata.sorted_tables):
            if table.name in existing_tables:
                session.execute(table.delete())
        session.commit()


def build_postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = make_postgres_engine()
    apply_test_schema(engine)
    testing_session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield testing_session_factory
    finally:
        engine.dispose()
