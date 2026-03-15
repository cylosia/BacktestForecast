"""Add unique constraint to prevent duplicate trades.

Revision ID: 20260314_0034
Revises: 20260314_0033
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0034"
down_revision = "20260314_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_backtest_trades_dedup",
        "backtest_trades",
        ["run_id", "entry_date", "option_ticker"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_backtest_trades_dedup", "backtest_trades", type_="unique")
