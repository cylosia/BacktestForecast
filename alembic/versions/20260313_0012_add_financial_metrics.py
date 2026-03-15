"""add financial metrics to backtest_runs

Revision ID: 20260313_0012
Revises: 20260312_0011
Create Date: 2026-03-13 10:00:00

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260313_0012"
down_revision = "20260312_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("backtest_runs", sa.Column("profit_factor", sa.Numeric(10, 4), nullable=True))
    op.add_column("backtest_runs", sa.Column("payoff_ratio", sa.Numeric(10, 4), nullable=True))
    op.add_column(
        "backtest_runs",
        sa.Column("expectancy", sa.Numeric(18, 4), nullable=False, server_default="0"),
    )
    op.add_column("backtest_runs", sa.Column("sharpe_ratio", sa.Numeric(10, 4), nullable=True))
    op.add_column("backtest_runs", sa.Column("sortino_ratio", sa.Numeric(10, 4), nullable=True))
    op.add_column("backtest_runs", sa.Column("cagr_pct", sa.Numeric(10, 4), nullable=True))
    op.add_column("backtest_runs", sa.Column("calmar_ratio", sa.Numeric(10, 4), nullable=True))
    op.add_column(
        "backtest_runs",
        sa.Column("max_consecutive_wins", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "backtest_runs",
        sa.Column("max_consecutive_losses", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("backtest_runs", sa.Column("recovery_factor", sa.Numeric(10, 4), nullable=True))

    for col in ["expectancy", "max_consecutive_wins", "max_consecutive_losses"]:
        op.alter_column("backtest_runs", col, server_default=None)


def downgrade() -> None:
    op.drop_column("backtest_runs", "recovery_factor")
    op.drop_column("backtest_runs", "max_consecutive_losses")
    op.drop_column("backtest_runs", "max_consecutive_wins")
    op.drop_column("backtest_runs", "calmar_ratio")
    op.drop_column("backtest_runs", "cagr_pct")
    op.drop_column("backtest_runs", "sortino_ratio")
    op.drop_column("backtest_runs", "sharpe_ratio")
    op.drop_column("backtest_runs", "expectancy")
    op.drop_column("backtest_runs", "payoff_ratio")
    op.drop_column("backtest_runs", "profit_factor")
