"""Detect drift between Alembic migrations and SQLAlchemy ORM models.

Runs ``alembic upgrade head`` against the connected database, then uses
Alembic's autogenerate comparison to find columns, indexes, or constraints
present in models.py but absent from migrations (and vice-versa).

Requires DATABASE_URL to point at a fresh, empty database (typically the
Postgres service container in CI).  Exit code 1 on drift.
"""
from __future__ import annotations

import sys
from pathlib import Path

from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine

import backtestforecast.models  # noqa: F401  — register all models
from alembic import command
from backtestforecast.config import get_settings
from backtestforecast.db.base import Base

_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    settings = get_settings()
    url = settings.database_url

    alembic_cfg = Config(str(_ROOT / "alembic.ini"))
    command.upgrade(alembic_cfg, "head")

    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            mc = MigrationContext.configure(conn)
            diffs = compare_metadata(mc, Base.metadata)
    finally:
        engine.dispose()

    if not diffs:
        print("OK — no migration drift detected.")
        return 0

    print(f"DRIFT DETECTED — {len(diffs)} difference(s):")
    for diff in diffs:
        print(f"  {diff}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
