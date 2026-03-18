"""Add 'processing' to stripe_events idempotency_status CHECK constraint.

The StripeEventRepository.claim() inserts with status='processing' but the
CHECK constraint only allowed ('processed', 'ignored', 'error'), blocking
all Stripe webhook processing on PostgreSQL.

Revision ID: 20260318_0008
Revises: 20260318_0007
Create Date: 2026-03-18
"""
from __future__ import annotations

from alembic import op

revision = "20260318_0008"
down_revision = "20260318_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_stripe_events_valid_status", "stripe_events", type_="check")
    op.create_check_constraint(
        "ck_stripe_events_valid_status",
        "stripe_events",
        "idempotency_status IN ('processing', 'processed', 'ignored', 'error')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_stripe_events_valid_status", "stripe_events", type_="check")
    op.create_check_constraint(
        "ck_stripe_events_valid_status",
        "stripe_events",
        "idempotency_status IN ('processed', 'ignored', 'error')",
    )
