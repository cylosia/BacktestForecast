"""Add risk_free_rate column to backtest_runs to store the value used at computation time."""
import sqlalchemy as sa
from alembic import op

revision = "20260319_0031"
down_revision = "20260319_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_runs",
        sa.Column("risk_free_rate", sa.Numeric(6, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_runs", "risk_free_rate")
