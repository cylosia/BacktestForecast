"""Add holding_period_trading_days column to backtest_trades.

Stores the number of trading days (market-open days) a position was held,
complementing the existing holding_period_days (calendar days).

Also adds a CHECK constraint to ensure non-negative values.

Revision ID: 0036
Revises: 0035
"""
from alembic import op
import sqlalchemy as sa


revision = "20260319_0036"
down_revision = "20260319_0035"
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


def _constraint_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = :name"),
        {"name": name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    if not _column_exists("backtest_trades", "holding_period_trading_days"):
        op.add_column(
            "backtest_trades",
            sa.Column("holding_period_trading_days", sa.Integer(), nullable=True),
        )
    if not _constraint_exists("ck_backtest_trades_holding_trading_days_nonneg"):
        op.create_check_constraint(
            "ck_backtest_trades_holding_trading_days_nonneg",
            "backtest_trades",
            "holding_period_trading_days IS NULL OR holding_period_trading_days >= 0",
        )


def downgrade() -> None:
    if _constraint_exists("ck_backtest_trades_holding_trading_days_nonneg"):
        op.drop_constraint("ck_backtest_trades_holding_trading_days_nonneg", "backtest_trades", type_="check")
    if _column_exists("backtest_trades", "holding_period_trading_days"):
        op.drop_column("backtest_trades", "holding_period_trading_days")
