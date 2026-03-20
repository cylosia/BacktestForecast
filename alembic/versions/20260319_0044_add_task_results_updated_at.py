"""Add updated_at column to task_results table.

Enables tracking of corrections to task outcome records (e.g. error
message updates, retries column adjustments during post-mortem).

Revision ID: 20260319_0044
Revises: 20260319_0043
"""
from alembic import op
import sqlalchemy as sa


revision = "20260319_0044"
down_revision = "20260319_0043"
branch_labels = None
depends_on = None


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    if not _column_exists("task_results", "updated_at"):
        op.add_column(
            "task_results",
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )
    op.execute(
        "CREATE OR REPLACE FUNCTION set_updated_at() "
        "RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;"
    )
    op.execute("DROP TRIGGER IF EXISTS trg_task_results_updated_at ON task_results;")
    op.execute(
        "CREATE TRIGGER trg_task_results_updated_at "
        "BEFORE UPDATE ON task_results "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_task_results_updated_at ON task_results;")
    if _column_exists("task_results", "updated_at"):
        op.drop_column("task_results", "updated_at")
