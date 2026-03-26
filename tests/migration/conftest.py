from __future__ import annotations

from collections.abc import Generator

import pytest
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig
from sqlalchemy.orm import Session, sessionmaker

from tests.integration.conftest import _make_engine


@pytest.fixture(scope="session")
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = _make_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False))
    alembic_upgrade(alembic_cfg, "head")
    testing_session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield testing_session_factory
    finally:
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
