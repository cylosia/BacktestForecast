from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy.orm import Session, sessionmaker
from tests.postgres_support import reset_database


@pytest.fixture()
def session_factory(postgres_session_factory: sessionmaker[Session]) -> Generator[sessionmaker[Session], None, None]:
    reset_database(postgres_session_factory)
    yield postgres_session_factory


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
