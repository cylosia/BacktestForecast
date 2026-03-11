"""add billing state, exports, and audit events

Revision ID: 20260310_0004
Revises: 20260309_0003
Create Date: 2026-03-10 10:05:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260310_0004"
down_revision = "20260309_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("stripe_customer_id", sa.String(length=64), nullable=True))
    op.add_column("users", sa.Column("stripe_subscription_id", sa.String(length=64), nullable=True))
    op.add_column("users", sa.Column("stripe_price_id", sa.String(length=64), nullable=True))
    op.add_column("users", sa.Column("subscription_status", sa.String(length=32), nullable=True))
    op.add_column("users", sa.Column("subscription_billing_interval", sa.String(length=16), nullable=True))
    op.add_column("users", sa.Column("subscription_current_period_end", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "users",
        sa.Column("cancel_at_period_end", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column("users", sa.Column("plan_updated_at", sa.DateTime(timezone=True), nullable=True))
    op.create_unique_constraint("uq_users_stripe_customer_id", "users", ["stripe_customer_id"])
    op.create_unique_constraint("uq_users_stripe_subscription_id", "users", ["stripe_subscription_id"])
    op.alter_column("users", "cancel_at_period_end", server_default=None)

    op.create_table(
        "export_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("export_format", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sha256_hex", sa.String(length=64), nullable=True),
        sa.Column("idempotency_key", sa.String(length=80), nullable=True),
        sa.Column("content_bytes", sa.LargeBinary(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["backtest_run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_export_jobs_user_idempotency_key"),
    )
    op.create_index("ix_export_jobs_user_created_at", "export_jobs", ["user_id", "created_at"], unique=False)
    op.create_index("ix_export_jobs_user_status", "export_jobs", ["user_id", "status"], unique=False)
    op.alter_column("export_jobs", "size_bytes", server_default=None)

    op.create_table(
        "audit_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("subject_type", sa.String(length=64), nullable=False),
        sa.Column("subject_id", sa.String(length=255), nullable=True),
        sa.Column("ip_hash", sa.String(length=128), nullable=True),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_events_event_type_created_at", "audit_events", ["event_type", "created_at"], unique=False)
    op.create_index("ix_audit_events_user_created_at", "audit_events", ["user_id", "created_at"], unique=False)
    op.alter_column("audit_events", "metadata_json", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_audit_events_user_created_at", table_name="audit_events")
    op.drop_index("ix_audit_events_event_type_created_at", table_name="audit_events")
    op.drop_table("audit_events")

    op.drop_index("ix_export_jobs_user_status", table_name="export_jobs")
    op.drop_index("ix_export_jobs_user_created_at", table_name="export_jobs")
    op.drop_table("export_jobs")

    op.drop_constraint("uq_users_stripe_subscription_id", "users", type_="unique")
    op.drop_constraint("uq_users_stripe_customer_id", "users", type_="unique")
    op.drop_column("users", "plan_updated_at")
    op.drop_column("users", "cancel_at_period_end")
    op.drop_column("users", "subscription_current_period_end")
    op.drop_column("users", "subscription_billing_interval")
    op.drop_column("users", "subscription_status")
    op.drop_column("users", "stripe_price_id")
    op.drop_column("users", "stripe_subscription_id")
    op.drop_column("users", "stripe_customer_id")
