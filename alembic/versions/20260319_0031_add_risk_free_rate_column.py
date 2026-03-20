"""Add risk_free_rate column to backtest_runs to store the value used at computation time."""
import sqlalchemy as sa
from alembic import op

revision = "20260319_0031"
down_revision = "20260319_0030"
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
    if not _column_exists("backtest_runs", "risk_free_rate"):
        op.add_column(
            "backtest_runs",
            sa.Column("risk_free_rate", sa.Numeric(6, 4), nullable=True),
        )


def downgrade() -> None:
    if _column_exists("backtest_runs", "risk_free_rate"):
        op.drop_column("backtest_runs", "risk_free_rate")
