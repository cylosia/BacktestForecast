from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
for path in (ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file(ROOT / "apps" / "api" / ".env")
_load_env_file(ROOT / ".env")


# Keep test bootstrap resilient when optional provider credentials are absent.
os.environ.setdefault("MASSIVE_API_KEY", "test-massive-api-key")

from backtestforecast.db.base import Base
from tests.postgres_support import build_postgres_session_factory, reset_database

_TARGET_ASSERTION_REACHED = pytest.StashKey[bool]()
_CALL_REPORT = pytest.StashKey[pytest.TestReport]()


def strip_partial_indexes_for_sqlite(engine) -> None:
    """Remove PostgreSQL-specific DDL so SQLite create_all succeeds.

    Shared across test suites to avoid duplicating this workaround in every
    conftest.py that creates an in-memory SQLite engine.
    """
    if engine.dialect.name != "sqlite":
        return
    for table in Base.metadata.tables.values():
        indexes_to_remove = [
            idx for idx in table.indexes
            if idx.dialect_options.get("postgresql", {}).get("where") is not None
        ]
        for idx in indexes_to_remove:
            table.indexes.discard(idx)
        constraints_to_remove = [
            constraint
            for constraint in table.constraints
            if getattr(constraint, "sqltext", None) is not None
            and "::" in str(constraint.sqltext)
        ]
        for constraint in constraints_to_remove:
            table.constraints.discard(constraint)


@pytest.fixture
def target_assertion(request: pytest.FixtureRequest):
    """Mark that a critical regression test reached its intended business assertion."""

    def _mark_reached() -> None:
        request.node.stash[_TARGET_ASSERTION_REACHED] = True

    return _mark_reached


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[object]):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        item.stash[_CALL_REPORT] = report


def pytest_runtest_setup(item: pytest.Item) -> None:
    if item.get_closest_marker("target_assertion") is not None:
        item.stash[_TARGET_ASSERTION_REACHED] = False


def pytest_runtest_teardown(item: pytest.Item) -> None:
    marker = item.get_closest_marker("target_assertion")
    report = item.stash.get(_CALL_REPORT, None)
    if marker is None or report is None or not report.passed:
        return
    if not item.stash.get(_TARGET_ASSERTION_REACHED, False):
        raise AssertionError(
            "This regression test passed without reaching its target assertion. "
            "Call the target_assertion fixture immediately before the business-behavior assertion it is meant to protect."
        )


@pytest.fixture(scope="session")
def postgres_session_factory() -> sessionmaker[Session]:
    yield from build_postgres_session_factory()


@pytest.fixture()
def postgres_db_session(postgres_session_factory: sessionmaker[Session]) -> Session:
    reset_database(postgres_session_factory)
    session = postgres_session_factory()
    try:
        yield session
    finally:
        session.close()
