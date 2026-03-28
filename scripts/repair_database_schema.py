#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import expected_schema_tables


def _database_name(url: str) -> str:
    path = urlparse(url).path.rsplit("/", 1)[-1]
    return path or "<unknown>"


def _build_engine(database_url: str):
    return create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=30000 -c timezone=UTC",
        },
    )


def _schema_status(database_url: str) -> dict[str, object]:
    engine = _build_engine(database_url)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            existing_tables = set(inspector.get_table_names())
            missing_tables = sorted(set(expected_schema_tables()) - existing_tables)
            version_rows = connection.exec_driver_sql("SELECT version_num FROM alembic_version").fetchall() if "alembic_version" in existing_tables else []
        return {
            "database": _database_name(database_url),
            "table_count": len(existing_tables),
            "missing_tables": missing_tables,
            "alembic_versions": [row[0] for row in version_rows],
        }
    finally:
        engine.dispose()


def _rebuild_public_schema(database_url: str) -> None:
    engine = _build_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
            connection.exec_driver_sql("CREATE SCHEMA public")
    finally:
        engine.dispose()

    alembic_cfg = Config(str(ROOT / "alembic.ini"))
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect the configured PostgreSQL schema and optionally rebuild the "
            "public schema from Alembic."
        )
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="Target database URL. Defaults to DATABASE_URL from the environment.",
    )
    parser.add_argument(
        "--rebuild-public-schema",
        action="store_true",
        help="Drop and recreate the public schema, then run alembic upgrade head.",
    )
    parser.add_argument(
        "--confirm-database",
        default="",
        help="Required with --rebuild-public-schema. Must exactly match the database name in the URL.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.database_url:
        print(
            "DATABASE_URL is not set. Provide --database-url or configure apps/api/.env.",
            file=sys.stderr,
        )
        return 2

    database_name = _database_name(args.database_url)
    status = _schema_status(args.database_url)
    missing_tables = status["missing_tables"]
    print(
        f"Database: {database_name}\n"
        f"Tables: {status['table_count']}\n"
        f"Alembic revisions: {', '.join(status['alembic_versions']) or '<none>'}\n"
        f"Missing tables: {len(missing_tables)}"
    )
    if missing_tables:
        print(", ".join(missing_tables))

    if not args.rebuild_public_schema:
        return 0 if not missing_tables else 1

    if args.confirm_database != database_name:
        print(
            "Refusing to rebuild schema without --confirm-database matching the target database name.",
            file=sys.stderr,
        )
        return 2

    _rebuild_public_schema(args.database_url)
    rebuilt_status = _schema_status(args.database_url)
    rebuilt_missing_tables = rebuilt_status["missing_tables"]
    print(
        f"Rebuilt public schema for {database_name}. "
        f"Remaining missing tables: {len(rebuilt_missing_tables)}"
    )
    if rebuilt_missing_tables:
        print(", ".join(rebuilt_missing_tables), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
