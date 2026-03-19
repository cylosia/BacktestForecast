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


def upgrade() -> None:
    op.add_column(
        "backtest_trades",
        sa.Column("holding_period_trading_days", sa.Integer(), nullable=True),
    )
    op.create_check_constraint(
        "ck_backtest_trades_holding_trading_days_nonneg",
        "backtest_trades",
        "holding_period_trading_days IS NULL OR holding_period_trading_days >= 0",
    )


def downgrade() -> None:
    op.drop_constraint("ck_backtest_trades_holding_trading_days_nonneg", "backtest_trades", type_="check")
    op.drop_column("backtest_trades", "holding_period_trading_days")
