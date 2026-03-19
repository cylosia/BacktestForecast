"""Add task_results table for structured task outcome tracking.

Replaces Redis-based result storage with a durable DB table that
supports querying historical task performance, SLA tracking, and
post-mortem analysis.

Revision ID: 20260319_0037
Revises: 20260319_0036
"""
from alembic import op
import sqlalchemy as sa
from backtestforecast.db.types import GUID, JSON_VARIANT, JSON_DEFAULT_EMPTY_OBJECT


revision = "20260319_0037"
down_revision = "20260319_0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_results",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("task_name", sa.String(128), nullable=False),
        sa.Column("task_id", sa.String(64), nullable=False, unique=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("correlation_id", GUID(), nullable=True),
        sa.Column("correlation_type", sa.String(64), nullable=True),
        sa.Column("duration_seconds", sa.Numeric(10, 3), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("result_summary_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("worker_hostname", sa.String(255), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_task_results_task_name_created", "task_results", ["task_name", "created_at"])
    op.create_index("ix_task_results_correlation_id", "task_results", ["correlation_id"])
    op.create_index("ix_task_results_status_created", "task_results", ["status", "created_at"])
    op.create_check_constraint(
        "ck_task_results_valid_status",
        "task_results",
        "status IN ('succeeded', 'failed', 'retried', 'timeout')",
    )


def downgrade() -> None:
    op.drop_table("task_results")
