"""Add single-column index on backtest_equity_points.run_id for efficient lookups."""
from alembic import op

revision = "20260319_0030"
down_revision = "20260319_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_backtest_equity_points_run_id", "backtest_equity_points", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_backtest_equity_points_run_id", table_name="backtest_equity_points")
