"""add backtest_templates table

Revision ID: 20260310_0006
Revises: 20260310_0005
Create Date: 2026-03-10 14:00:00

"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260310_0006"
down_revision = "20260310_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backtest_templates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("strategy_type", sa.String(32), nullable=False),
        sa.Column("config_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_backtest_templates_user_created_at",
        "backtest_templates",
        ["user_id", "created_at"],
    )
    op.create_index(
        "ix_backtest_templates_user_strategy",
        "backtest_templates",
        ["user_id", "strategy_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_backtest_templates_user_strategy", table_name="backtest_templates")
    op.drop_index("ix_backtest_templates_user_created_at", table_name="backtest_templates")
    op.drop_table("backtest_templates")
