"""Add compound index on stripe_events for webhook idempotency lookups."""
from alembic import op

revision = "20260319_0032"
down_revision = "20260319_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_stripe_events_event_id_status",
        "stripe_events",
        ["stripe_event_id", "idempotency_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_stripe_events_event_id_status", table_name="stripe_events")
