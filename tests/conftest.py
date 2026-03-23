from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
for path in (ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


# Keep test bootstrap resilient when optional provider credentials are absent.
os.environ.setdefault("MASSIVE_API_KEY", "test-massive-api-key")
os.environ.setdefault("EARNINGS_API_KEY", "test-earnings-api-key")

from backtestforecast.db.base import Base


def strip_partial_indexes_for_sqlite(engine) -> None:
    """Remove PostgreSQL-specific partial indexes so SQLite create_all succeeds.

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


def pytest_addoption(parser) -> None:
    """Register optional pytest.ini keys used by newer plugins.

    This keeps collection from failing outright when a local environment has
    an older or missing pytest-asyncio installation.
    """
    parser.addini("asyncio_mode", "pytest-asyncio execution mode", default="auto")
    parser.addini("timeout", "pytest-timeout per-test timeout", default="120")
