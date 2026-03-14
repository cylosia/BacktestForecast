"""Add PostgreSQL trigger for updated_at auto-refresh.

The ORM-level onupdate=func.now() only fires for ORM operations.
Direct SQL updates, bulk operations, and reaper queries miss it.
This trigger ensures updated_at is always accurate.

Revision ID: 20260314_0033
Revises: 20260314_0032
"""
from alembic import op

revision = "20260314_0033"
down_revision = "20260314_0032"
branch_labels = None
depends_on = None

_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_TABLES = [
    "users",
    "backtest_runs",
    "backtest_templates",
    "scanner_jobs",
    "export_jobs",
    "symbol_analyses",
    "nightly_pipeline_runs",
]


def upgrade() -> None:
    op.execute(_TRIGGER_FUNCTION)
    for table in _TABLES:
        op.execute(f"""
            CREATE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
        """)


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at();")
