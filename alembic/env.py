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

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, text

import backtestforecast.models  # noqa: F401
from alembic import context
from backtestforecast.config import get_settings
from backtestforecast.db.base import Base

config = context.config


def _get_db_url() -> str:
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

    with connectable.connect() as connection:
        # Lock ID 2817513 derived from CRC32("backtestforecast-alembic") & 0x7FFFFFFF.
        # Must not collide with any other pg_advisory_lock in this database.
        connection.execute(text("SELECT pg_advisory_lock(2817513)"))
        try:
            context.configure(connection=connection, target_metadata=target_metadata, compare_type=True, compare_server_default=True)

            with context.begin_transaction():
                context.run_migrations()
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(2817513)"))


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
        connection.execute(text("SELECT pg_advisory_lock(2817513)"))
        try:
            connection = connection.execution_options(isolation_level="AUTOCOMMIT")
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                compare_server_default=True,
                transaction_per_migration=True,
            )
            context.run_migrations()
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(2817513)"))


import os

if context.is_offline_mode():
    run_migrations_offline()
elif os.environ.get("ALEMBIC_AUTOCOMMIT", "").strip() == "1":
    run_migrations_online_autocommit()
else:
    run_migrations_online()
