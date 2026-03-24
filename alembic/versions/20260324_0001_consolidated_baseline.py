"""Development-only consolidated baseline for the current schema.

This repository is not in production yet, so the historical Alembic chain has
been squashed into a single baseline to reduce maintenance overhead. Future
migrations should branch from this revision.

Revision ID: 20260324_0001
Revises: (root)
Create Date: 2026-03-24
"""
from __future__ import annotations

import backtestforecast.models  # noqa: F401
from alembic import op
from backtestforecast.db.base import Base

revision = "20260324_0001"
down_revision = None
branch_labels = None
depends_on = None

_TRIGGER_TABLES = [
    "users",
    "backtest_runs",
    "backtest_templates",
    "scanner_jobs",
    "scanner_recommendations",
    "export_jobs",
    "nightly_pipeline_runs",
    "daily_recommendations",
    "stripe_events",
    "symbol_analyses",
    "sweep_jobs",
    "outbox_messages",
    "sweep_results",
    "task_results",
]


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)

    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    for table in _TRIGGER_TABLES:
        op.execute(
            f"""
            CREATE OR REPLACE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )


def downgrade() -> None:
    bind = op.get_bind()

    if bind.dialect.name == "postgresql":
        for table in reversed(_TRIGGER_TABLES):
            op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
        op.execute("DROP FUNCTION IF EXISTS set_updated_at();")

    Base.metadata.drop_all(bind=bind)
