"""Add server defaults for outbox_messages.task_kwargs_json and
add CHECK constraint for strategy_type on backtest_runs.

Revision ID: 20260318_0021
Revises: 20260318_0020
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260318_0021"
down_revision = "20260318_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "outbox_messages",
        "task_kwargs_json",
        server_default=sa.text("'{}'::jsonb"),
    )


def downgrade() -> None:
    op.alter_column(
        "outbox_messages",
        "task_kwargs_json",
        server_default=None,
    )
