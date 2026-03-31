"""Add descending-date indexes for historical freshness checks.

Revision ID: 20260331_0014
Revises: 20260330_0013
Create Date: 2026-03-31 07:40:00
"""

from __future__ import annotations

from alembic import op


revision = "20260331_0014"
down_revision = "20260330_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_historical_underlying_day_bars_trade_date_desc "
        "ON historical_underlying_day_bars (trade_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_historical_option_day_bars_trade_date_desc "
        "ON historical_option_day_bars (trade_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_historical_ex_dividend_dates_date_desc "
        "ON historical_ex_dividend_dates (ex_dividend_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_historical_treasury_yields_trade_date_desc "
        "ON historical_treasury_yields (trade_date DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_historical_treasury_yields_trade_date_desc")
    op.execute("DROP INDEX IF EXISTS ix_historical_ex_dividend_dates_date_desc")
    op.execute("DROP INDEX IF EXISTS ix_historical_option_day_bars_trade_date_desc")
    op.execute("DROP INDEX IF EXISTS ix_historical_underlying_day_bars_trade_date_desc")
