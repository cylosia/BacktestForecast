"""Add sweep_jobs and sweep_results tables.

Revision ID: 20260317_0006
Revises: 20260317_0005
Create Date: 2026-03-17
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

import uuid
from sqlalchemy.types import CHAR, TypeDecorator
from sqlalchemy.dialects.postgresql import UUID as PG_UUID


class GUID(TypeDecorator):
    """Frozen copy — do not import from app code in migrations."""
    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value, dialect):
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))

revision = "20260317_0006"
down_revision = "20260317_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sweep_jobs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("candidate_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("evaluated_candidate_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("result_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("request_snapshot_json", sa.JSON, nullable=False),
        sa.Column("warnings_json", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("prefetch_summary_json", sa.JSON, nullable=True),
        sa.Column("engine_version", sa.String(32), nullable=False, server_default="options-multileg-v2"),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')", name="ck_sweep_jobs_valid_status"),
        sa.CheckConstraint("candidate_count >= 0", name="ck_sweep_jobs_candidate_count_nonneg"),
        sa.CheckConstraint("evaluated_candidate_count >= 0", name="ck_sweep_jobs_evaluated_count_nonneg"),
        sa.CheckConstraint("result_count >= 0", name="ck_sweep_jobs_result_count_nonneg"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_sweep_jobs_user_idempotency_key"),
    )
    op.create_index("ix_sweep_jobs_user_id", "sweep_jobs", ["user_id"])
    op.create_index("ix_sweep_jobs_user_created_at", "sweep_jobs", ["user_id", "created_at"])
    op.create_index("ix_sweep_jobs_user_status", "sweep_jobs", ["user_id", "status"])
    op.create_index("ix_sweep_jobs_celery_task_id", "sweep_jobs", ["celery_task_id"])
    op.create_index("ix_sweep_jobs_status_celery_created", "sweep_jobs", ["status", "celery_task_id", "created_at"])
    op.create_index("ix_sweep_jobs_queued", "sweep_jobs", ["created_at"], postgresql_where=sa.text("status = 'queued'"))

    op.create_table(
        "sweep_results",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("sweep_job_id", GUID(), sa.ForeignKey("sweep_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("score", sa.Numeric(18, 6), nullable=False),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("parameter_snapshot_json", sa.JSON, nullable=False),
        sa.Column("summary_json", sa.JSON, nullable=False),
        sa.Column("warnings_json", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("trades_json", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("equity_curve_json", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("sweep_job_id", "rank", name="uq_sweep_results_job_rank"),
        sa.CheckConstraint("rank >= 1", name="ck_sweep_results_rank_positive"),
    )
    op.create_index("ix_sweep_results_job_id", "sweep_results", ["sweep_job_id"])


def downgrade() -> None:
    op.drop_table("sweep_results")
    op.drop_table("sweep_jobs")
