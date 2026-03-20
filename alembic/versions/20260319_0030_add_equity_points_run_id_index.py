"""Add single-column index on backtest_equity_points.run_id for efficient lookups."""
from alembic import op
import sqlalchemy as sa

revision = "20260319_0030"
down_revision = "20260319_0029"
branch_labels = None
depends_on = None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    if not _index_exists("ix_backtest_equity_points_run_id"):
        op.create_index("ix_backtest_equity_points_run_id", "backtest_equity_points", ["run_id"])


def downgrade() -> None:
    if _index_exists("ix_backtest_equity_points_run_id"):
        op.drop_index("ix_backtest_equity_points_run_id", table_name="backtest_equity_points")
