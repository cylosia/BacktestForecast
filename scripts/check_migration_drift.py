"""Detect drift between Alembic migrations and SQLAlchemy ORM models.

Runs ``alembic upgrade head`` against the connected database, then uses
Alembic's autogenerate comparison to find columns, indexes, or constraints
present in models.py but absent from migrations (and vice-versa).

Additionally compares server defaults and check constraints between the
live DB schema and ORM metadata.

Requires DATABASE_URL to point at a fresh, empty database (typically the
Postgres service container in CI).  Exit code 1 on drift.
"""
from __future__ import annotations

import sys
from pathlib import Path

from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect, text

import backtestforecast.models  # noqa: F401  — register all models
from alembic import command
from backtestforecast.config import get_settings
from backtestforecast.db.base import Base

_ROOT = Path(__file__).resolve().parent.parent


def _check_server_defaults(engine) -> list[str]:
    """Compare ORM server_default declarations against live DB column defaults."""
    issues: list[str] = []
    inspector = inspect(engine)
    for table in Base.metadata.sorted_tables:
        db_columns = {c["name"]: c for c in inspector.get_columns(table.name)}
        for col in table.columns:
            if col.server_default is None:
                continue
            db_col = db_columns.get(col.name)
            if db_col is None:
                continue
            orm_default = str(col.server_default.arg) if hasattr(col.server_default, "arg") else str(col.server_default)
            db_default = db_col.get("default")
            if db_default is None:
                issues.append(
                    f"  {table.name}.{col.name}: ORM has server_default={orm_default!r} but DB has no default"
                )
    return issues


def _check_check_constraints(engine) -> list[str]:
    """Verify that ORM-declared CheckConstraints exist in the live DB."""
    issues: list[str] = []
    inspector = inspect(engine)
    for table in Base.metadata.sorted_tables:
        db_checks = {c["name"] for c in inspector.get_check_constraints(table.name) if c.get("name")}
        for constraint in table.constraints:
            from sqlalchemy import CheckConstraint as CC
            if isinstance(constraint, CC) and constraint.name:
                if constraint.name not in db_checks:
                    issues.append(
                        f"  {table.name}: CheckConstraint {constraint.name!r} missing from DB"
                    )
    return issues


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

        default_issues = _check_server_defaults(engine)
        constraint_issues = _check_check_constraints(engine)
    finally:
        engine.dispose()

    all_issues: list[str] = []

    if diffs:
        all_issues.append(f"Schema drift — {len(diffs)} difference(s):")
        for diff in diffs:
            all_issues.append(f"  {diff}")

    if default_issues:
        all_issues.append(f"Server-default drift — {len(default_issues)} issue(s):")
        all_issues.extend(default_issues)

    if constraint_issues:
        all_issues.append(f"CheckConstraint drift — {len(constraint_issues)} issue(s):")
        all_issues.extend(constraint_issues)

    if not all_issues:
        print("OK — no migration drift detected (schema, defaults, and constraints all match).")
        return 0

    print("DRIFT DETECTED:")
    for line in all_issues:
        print(line)
    return 1


if __name__ == "__main__":
    sys.exit(main())
