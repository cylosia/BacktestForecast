"""Add indexes declared in models but missing from prior migrations.

Five indexes were defined in model __table_args__ but never created via
migration DDL:

- ix_backtest_runs_user_id   (backtest_runs.user_id)
- ix_export_jobs_user_id     (export_jobs.user_id)
- ix_scanner_jobs_user_id    (scanner_jobs.user_id)
- ix_audit_events_user_id    (audit_events.user_id)
- ix_audit_events_event_type (audit_events.event_type)

All five are single-column B-tree indexes used for foreign-key lookups
and filtered queries.  CREATE INDEX CONCURRENTLY is not used here because
Alembic runs inside a transaction by default; for zero-downtime deploys,
run each statement manually with CONCURRENTLY outside a transaction.

Revision ID: 20260314_0031
Revises: 20260314_0030
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0031"
down_revision = "20260314_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_backtest_runs_user_id", "backtest_runs", ["user_id"])
    op.create_index("ix_export_jobs_user_id", "export_jobs", ["user_id"])
    op.create_index("ix_scanner_jobs_user_id", "scanner_jobs", ["user_id"])
    op.create_index("ix_audit_events_user_id", "audit_events", ["user_id"])
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])


def downgrade() -> None:
    op.drop_index("ix_audit_events_event_type", table_name="audit_events")
    op.drop_index("ix_audit_events_user_id", table_name="audit_events")
    op.drop_index("ix_scanner_jobs_user_id", table_name="scanner_jobs")
    op.drop_index("ix_export_jobs_user_id", table_name="export_jobs")
    op.drop_index("ix_backtest_runs_user_id", table_name="backtest_runs")
