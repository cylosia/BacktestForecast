"""Add billing constraints and stripe_events.updated_at (audit items 31–33).

Note: filename predates revision renumbering; revision ID is authoritative.

Revision ID: 20260318_0013
Revises: 20260318_0012
Create Date: 2026-03-18

- Item 31: CHECK constraint for users.subscription_status
- Item 32: CHECK constraint for users.subscription_billing_interval
- Item 33: Add updated_at column to stripe_events with auto-update trigger
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0013"
down_revision = "20260318_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Item 31: subscription_status CHECK
    op.create_check_constraint(
        "ck_users_valid_subscription_status",
        "users",
        "subscription_status IS NULL OR subscription_status IN "
        "('incomplete', 'incomplete_expired', 'trialing', 'active', 'past_due', 'canceled', 'unpaid', 'paused')",
    )

    # Item 32: subscription_billing_interval CHECK
    op.create_check_constraint(
        "ck_users_valid_billing_interval",
        "users",
        "subscription_billing_interval IS NULL OR subscription_billing_interval IN ('monthly', 'yearly')",
    )

    # Item 33: Add updated_at to stripe_events
    op.add_column(
        "stripe_events",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
    )
    # Add trigger for auto-updating
    op.execute(
        """
        CREATE TRIGGER set_updated_at
        BEFORE UPDATE ON stripe_events
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS set_updated_at ON stripe_events;")
    op.drop_column("stripe_events", "updated_at")
    op.drop_constraint("ck_users_valid_billing_interval", "users", type_="check")
    op.drop_constraint("ck_users_valid_subscription_status", "users", type_="check")
