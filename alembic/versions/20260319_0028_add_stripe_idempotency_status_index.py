"""Add missing ix_stripe_events_idempotency_status index.

The StripeEvent model declares this index but no prior migration created it.
Queries filtering by idempotency_status were doing sequential scans.

Revision ID: 20260319_0028
Revises: 20260319_0027
Create Date: 2026-03-19
"""
from __future__ import annotations

from alembic import op

revision: str = "20260319_0028"
down_revision: str = "20260319_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_stripe_events_idempotency_status",
        "stripe_events",
        ["idempotency_status"],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index("ix_stripe_events_idempotency_status", table_name="stripe_events")
