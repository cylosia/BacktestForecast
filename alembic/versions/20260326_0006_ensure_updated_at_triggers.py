"""Ensure updated_at triggers exist on mutable tables.

Revision ID: 20260326_0006
Revises: 20260326_0005
Create Date: 2026-03-26 16:10:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "20260326_0006"
down_revision = "20260326_0005"
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
    "multi_symbol_runs",
    "multi_step_runs",
]


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = clock_timestamp();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    for table in _TRIGGER_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    for table in reversed(_TRIGGER_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
