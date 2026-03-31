"""use provider dividend ids for historical dividend identity

Revision ID: 20260330_0013
Revises: 20260330_0012
Create Date: 2026-03-30 19:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260330_0013"
down_revision = "20260330_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("historical_ex_dividend_dates", schema=None) as batch_op:
        batch_op.drop_constraint("uq_historical_ex_dividend_dates_symbol_date", type_="unique")
        batch_op.create_unique_constraint(
            "uq_historical_ex_dividend_dates_provider_dividend_id",
            ["provider_dividend_id"],
        )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, ex_dividend_date
                        ORDER BY
                            CASE WHEN provider_dividend_id IS NULL THEN 1 ELSE 0 END,
                            provider_dividend_id,
                            updated_at DESC,
                            id
                    ) AS rn
                FROM historical_ex_dividend_dates
            )
            DELETE FROM historical_ex_dividend_dates
            WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
            """
        )
    )
    with op.batch_alter_table("historical_ex_dividend_dates", schema=None) as batch_op:
        batch_op.drop_constraint("uq_historical_ex_dividend_dates_provider_dividend_id", type_="unique")
        batch_op.create_unique_constraint(
            "uq_historical_ex_dividend_dates_symbol_date",
            ["symbol", "ex_dividend_date"],
        )
