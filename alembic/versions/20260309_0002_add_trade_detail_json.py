"""add trade detail json for multileg backtests

Revision ID: 20260309_0002
Revises: 20260309_0001
Create Date: 2026-03-09 21:30:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260309_0002"
down_revision = "20260309_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_trades",
        sa.Column(
            "detail_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("backtest_trades", "detail_json", server_default=None)


def downgrade() -> None:
    op.drop_column("backtest_trades", "detail_json")
