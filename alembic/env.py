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


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
