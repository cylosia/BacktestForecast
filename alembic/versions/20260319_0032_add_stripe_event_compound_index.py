"""Add compound index on stripe_events for webhook idempotency lookups."""
from alembic import op
import sqlalchemy as sa

revision = "20260319_0032"
down_revision = "20260319_0031"
branch_labels = None
depends_on = None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    if not _index_exists("ix_stripe_events_event_id_status"):
        op.create_index(
            "ix_stripe_events_event_id_status",
            "stripe_events",
            ["stripe_event_id", "idempotency_status"],
        )


def downgrade() -> None:
    if _index_exists("ix_stripe_events_event_id_status"):
        op.drop_index("ix_stripe_events_event_id_status", table_name="stripe_events")
