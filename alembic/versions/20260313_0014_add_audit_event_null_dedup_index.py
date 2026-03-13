"""add partial unique index for audit_events with NULL subject_id

Revision ID: 20260313_0014
Revises: 20260313_0013
Create Date: 2026-03-13 14:30:00

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260313_0014"
down_revision = "20260313_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_audit_events_dedup_null_subject",
        "audit_events",
        ["event_type", "subject_type"],
        unique=True,
        postgresql_where=sa.text("subject_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_audit_events_dedup_null_subject", table_name="audit_events")
