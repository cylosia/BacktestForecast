"""add celery_task_id to scanner/export jobs and audit dedup constraint

Revision ID: 20260311_0009
Revises: 20260310_0008
Create Date: 2026-03-11 10:00:00

"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260311_0009"
down_revision = "20260310_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scanner_jobs",
        sa.Column("celery_task_id", sa.String(64), nullable=True),
    )
    op.create_index(
        "ix_scanner_jobs_celery_task_id",
        "scanner_jobs",
        ["celery_task_id"],
    )

    op.add_column(
        "export_jobs",
        sa.Column("celery_task_id", sa.String(64), nullable=True),
    )
    op.create_index(
        "ix_export_jobs_celery_task_id",
        "export_jobs",
        ["celery_task_id"],
    )

    op.execute(
        sa.text(
            "DELETE FROM audit_events WHERE id NOT IN ("
            "  SELECT MIN(id) FROM audit_events"
            "  GROUP BY event_type, subject_type, subject_id"
            ")"
        )
    )

    op.create_unique_constraint(
        "uq_audit_events_dedup",
        "audit_events",
        ["event_type", "subject_type", "subject_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_audit_events_dedup", "audit_events", type_="unique")
    op.drop_index("ix_export_jobs_celery_task_id", table_name="export_jobs")
    op.drop_column("export_jobs", "celery_task_id")
    op.drop_index("ix_scanner_jobs_celery_task_id", table_name="scanner_jobs")
    op.drop_column("scanner_jobs", "celery_task_id")
