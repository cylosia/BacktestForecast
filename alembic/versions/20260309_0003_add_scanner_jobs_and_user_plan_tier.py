"""add scanner jobs, recommendations, and user plan tier

Revision ID: 20260309_0003
Revises: 20260309_0002
Create Date: 2026-03-09 23:45:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260309_0003"
down_revision = "20260309_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("plan_tier", sa.String(length=16), nullable=False, server_default="free"),
    )
    op.alter_column("users", "plan_tier", server_default=None)

    op.create_table(
        "scanner_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=120), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("plan_tier_snapshot", sa.String(length=16), nullable=False),
        sa.Column("job_kind", sa.String(length=32), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=80), nullable=True),
        sa.Column("refresh_key", sa.String(length=120), nullable=True),
        sa.Column("refresh_daily", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("refresh_priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("candidate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("evaluated_candidate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recommendation_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("request_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "warnings_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("ranking_version", sa.String(length=32), nullable=False),
        sa.Column("engine_version", sa.String(length=32), nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["parent_job_id"], ["scanner_jobs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("refresh_key", name="uq_scanner_jobs_refresh_key"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_scanner_jobs_user_idempotency_key"),
    )
    op.create_index("ix_scanner_jobs_request_hash", "scanner_jobs", ["request_hash"], unique=False)
    op.create_index("ix_scanner_jobs_user_created_at", "scanner_jobs", ["user_id", "created_at"], unique=False)
    op.create_index("ix_scanner_jobs_user_status", "scanner_jobs", ["user_id", "status"], unique=False)

    op.create_table(
        "scanner_recommendations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scanner_job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(18, 6), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("strategy_type", sa.String(length=32), nullable=False),
        sa.Column("rule_set_name", sa.String(length=120), nullable=False),
        sa.Column("rule_set_hash", sa.String(length=64), nullable=False),
        sa.Column("request_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "warnings_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "trades_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "equity_curve_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "historical_performance_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "forecast_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "ranking_features_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["scanner_job_id"], ["scanner_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scanner_job_id", "rank", name="uq_scanner_recommendations_job_rank"),
    )
    op.create_index(
        "ix_scanner_recommendations_job_rank",
        "scanner_recommendations",
        ["scanner_job_id", "rank"],
        unique=False,
    )
    op.create_index(
        "ix_scanner_recommendations_lookup",
        "scanner_recommendations",
        ["symbol", "strategy_type", "rule_set_hash"],
        unique=False,
    )

    op.alter_column("scanner_jobs", "warnings_json", server_default=None)
    op.alter_column("scanner_recommendations", "warnings_json", server_default=None)
    op.alter_column("scanner_recommendations", "trades_json", server_default=None)
    op.alter_column("scanner_recommendations", "equity_curve_json", server_default=None)
    op.alter_column("scanner_recommendations", "historical_performance_json", server_default=None)
    op.alter_column("scanner_recommendations", "forecast_json", server_default=None)
    op.alter_column("scanner_recommendations", "ranking_features_json", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_scanner_recommendations_lookup", table_name="scanner_recommendations")
    op.drop_index("ix_scanner_recommendations_job_rank", table_name="scanner_recommendations")
    op.drop_table("scanner_recommendations")
    op.drop_index("ix_scanner_jobs_user_status", table_name="scanner_jobs")
    op.drop_index("ix_scanner_jobs_user_created_at", table_name="scanner_jobs")
    op.drop_index("ix_scanner_jobs_request_hash", table_name="scanner_jobs")
    op.drop_table("scanner_jobs")
    op.drop_column("users", "plan_tier")
