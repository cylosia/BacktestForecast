"""Alembic environment configuration.

MIGRATION BEST PRACTICES FOR ZERO-DOWNTIME DEPLOYMENTS:

1. Index creation: Use ``op.execute("CREATE INDEX CONCURRENTLY ...")``
   instead of ``op.create_index()`` to avoid holding SHARE locks on
   production tables. CONCURRENTLY requires ``autocommit`` mode.

2. Constraint addition: Add constraints as NOT VALID in one migration
   (fast, no table scan), then VALIDATE in a separate subsequent
   migration. This avoids holding ACCESS EXCLUSIVE locks during the
   full-table validation scan.

3. Column additions: Always provide a server_default for new NOT NULL
   columns to avoid rewriting the entire table.
"""
from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool, text

from alembic import context
from repo_bootstrap import ensure_repo_import_paths

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)
ensure_repo_import_paths()

import backtestforecast.models  # noqa: F401
from backtestforecast.config import get_settings
from backtestforecast.db.base import Base

config = context.config


def _get_db_url() -> str:
    configured_url = config.get_main_option("sqlalchemy.url")
    placeholder = "postgresql+psycopg://placeholder:placeholder@localhost/placeholder"
    if configured_url and configured_url != placeholder:
        return configured_url
    return get_settings().database_url


config.set_main_option("sqlalchemy.url", _get_db_url())

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    # compare_server_default=True causes autogenerate to detect differences
    # between model server_default values and the actual DB column defaults.
    # This catches drifts like missing or changed DEFAULT clauses.
    context.configure(
        url=_get_db_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.begin() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True, compare_server_default=True)

        with context.begin_transaction():
            if connection.dialect.name == "postgresql":
                # Acquire a transaction-scoped advisory lock only after Alembic's
                # transaction starts. Acquiring a session lock beforehand causes
                # SQLAlchemy 2 to open an outer implicit transaction that later
                # rolls back the migration work on connection close.
                connection.execute(text("SELECT pg_advisory_xact_lock(2817513)"))
            context.run_migrations()


def run_migrations_online_autocommit() -> None:
    """Run migrations outside a transaction block (autocommit mode).

    Required for DDL that cannot run inside a transaction, such as
    ``CREATE INDEX CONCURRENTLY``.  Invoke via::

        ALEMBIC_AUTOCOMMIT=1 alembic upgrade head

    Individual migrations must be idempotent when using this mode,
    since there is no transaction to roll back on failure.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        use_pg_advisory_lock = connection.dialect.name == "postgresql"
        if use_pg_advisory_lock:
            connection.execute(text("SELECT pg_advisory_lock(2817513)"))
        try:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                compare_server_default=True,
                transaction_per_migration=True,
            )
            context.run_migrations()
        finally:
            if use_pg_advisory_lock:
                connection.execute(text("SELECT pg_advisory_unlock(2817513)"))

if context.is_offline_mode():
    run_migrations_offline()
elif os.environ.get("ALEMBIC_AUTOCOMMIT", "").strip() == "1":
    run_migrations_online_autocommit()
else:
    run_migrations_online()
