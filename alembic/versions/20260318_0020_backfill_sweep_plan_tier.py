"""Backfill sweep_jobs.plan_tier_snapshot from the owning user's plan_tier.

Previously the sweep service did not set this column, so all existing
sweep jobs have the server_default value 'free' regardless of the user's
actual plan tier at creation time.

Revision ID: 20260318_0020
Revises: 20260318_0019
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260318_0020"
down_revision = "20260318_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("""
        UPDATE sweep_jobs
        SET plan_tier_snapshot = users.plan_tier
        FROM users
        WHERE sweep_jobs.user_id = users.id
          AND sweep_jobs.plan_tier_snapshot = 'free'
          AND users.plan_tier != 'free'
    """))


def downgrade() -> None:
    pass
