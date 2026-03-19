"""Add PostgreSQL trigger for updated_at auto-refresh.

Note: filename predates revision renumbering; revision ID is authoritative.

Revision ID: 20260318_0014
Revises: 20260318_0013
Create Date: 2026-03-18

Raw SQL UPDATE statements bypass SQLAlchemy's ORM-level onupdate=func.now().
This trigger ensures updated_at is always refreshed at the database level.
"""
from alembic import op

revision = "20260318_0014"
down_revision = "20260318_0013"
branch_labels = None
depends_on = None

_TABLES_WITH_UPDATED_AT = [
    "users",
    "backtest_runs",
    "backtest_templates",
    "scanner_jobs",
    "scanner_recommendations",
    "export_jobs",
    "nightly_pipeline_runs",
    "symbol_analyses",
    "sweep_jobs",
    "sweep_results",
    "outbox_messages",
    "stripe_events",
]

_CREATE_FUNCTION = """
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_DROP_FUNCTION = "DROP FUNCTION IF EXISTS set_updated_at();"


def upgrade() -> None:
    op.execute(_CREATE_FUNCTION)
    for table in _TABLES_WITH_UPDATED_AT:
        trigger_name = f"trg_{table}_updated_at"
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table};")
        op.execute(f"DROP TRIGGER IF EXISTS set_updated_at ON {table};")
        op.execute(
            f"CREATE TRIGGER {trigger_name} "
            f"BEFORE UPDATE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
        )


def downgrade() -> None:
    for table in _TABLES_WITH_UPDATED_AT:
        trigger_name = f"trg_{table}_updated_at"
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table};")
    op.execute(_DROP_FUNCTION)
