"""Baseline schema — consolidated from 36 incremental migrations.

Creates all 12 tables, indexes, CHECK constraints, partial unique indexes,
the set_updated_at() trigger function, and per-table BEFORE UPDATE triggers.

Revision ID: 20260315_0001
Revises: (root)
Create Date: 2026-03-15
"""
from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator


class GUID(TypeDecorator[uuid.UUID]):
    """Frozen copy of backtestforecast.db.types.GUID for migration stability."""

    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):  # type: ignore[override]
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):  # type: ignore[override]
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value, dialect):  # type: ignore[override]
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


JSON_VARIANT = JSON().with_variant(JSONB, "postgresql")

revision = "20260315_0001"
down_revision = None
branch_labels = None
depends_on = None

# Tables that receive the set_updated_at() BEFORE UPDATE trigger.
_TRIGGER_TABLES = [
    "users",
    "backtest_runs",
    "backtest_templates",
    "scanner_jobs",
    "scanner_recommendations",
    "export_jobs",
    "symbol_analyses",
    "nightly_pipeline_runs",
]


def upgrade() -> None:
    # ------------------------------------------------------------------ users
    op.create_table(
        "users",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("clerk_user_id", sa.String(255), nullable=False, unique=True),
        sa.Column("email", sa.String(320), nullable=True),
        sa.Column("plan_tier", sa.String(16), nullable=False, server_default="free"),
        sa.Column("stripe_customer_id", sa.String(64), nullable=True, unique=True),
        sa.Column("stripe_subscription_id", sa.String(64), nullable=True, unique=True),
        sa.Column("stripe_price_id", sa.String(64), nullable=True),
        sa.Column("subscription_status", sa.String(32), nullable=True),
        sa.Column("subscription_billing_interval", sa.String(16), nullable=True),
        sa.Column("subscription_current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_at_period_end", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("plan_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("plan_tier IN ('free', 'pro', 'premium')", name="ck_users_valid_plan_tier"),
    )

    # ----------------------------------------------------------- backtest_runs
    op.create_table(
        "backtest_runs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("date_from", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=False),
        sa.Column("target_dte", sa.Integer(), nullable=False),
        sa.Column("dte_tolerance_days", sa.Integer(), nullable=False),
        sa.Column("max_holding_days", sa.Integer(), nullable=False),
        sa.Column("account_size", sa.Numeric(18, 4), nullable=False),
        sa.Column("risk_per_trade_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("commission_per_contract", sa.Numeric(18, 4), nullable=False),
        sa.Column("input_snapshot_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("warnings_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("engine_version", sa.String(32), nullable=False, server_default="options-multileg-v2"),
        sa.Column("data_source", sa.String(32), nullable=False, server_default="massive"),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        # Result columns — server_default="0" for direct-SQL safety
        sa.Column("trade_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("win_rate", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("total_roi_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("average_win_amount", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("average_loss_amount", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("average_holding_period_days", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("average_dte_at_open", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("max_drawdown_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("total_net_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("starting_equity", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("ending_equity", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("profit_factor", sa.Numeric(10, 4), nullable=True),
        sa.Column("payoff_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("expectancy", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("sharpe_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("sortino_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("cagr_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("calmar_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("max_consecutive_wins", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_consecutive_losses", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recovery_factor", sa.Numeric(10, 4), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_backtest_runs_user_idempotency_key"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_backtest_runs_valid_run_status",
        ),
        sa.CheckConstraint("account_size > 0", name="ck_backtest_runs_account_positive"),
        sa.CheckConstraint(
            "risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100",
            name="ck_backtest_runs_risk_pct_range",
        ),
        sa.CheckConstraint("commission_per_contract >= 0", name="ck_backtest_runs_commission_nonneg"),
        sa.CheckConstraint("date_from < date_to", name="ck_backtest_runs_date_order"),
        sa.CheckConstraint("max_holding_days >= 1", name="ck_backtest_runs_holding_days_positive"),
        sa.CheckConstraint("target_dte >= 0", name="ck_backtest_runs_target_dte_nonneg"),
        sa.CheckConstraint("dte_tolerance_days >= 0", name="ck_backtest_runs_dte_tolerance_nonneg"),
    )
    op.create_index("ix_backtest_runs_user_id", "backtest_runs", ["user_id"])
    op.create_index("ix_backtest_runs_user_created_at", "backtest_runs", ["user_id", "created_at"])
    op.create_index("ix_backtest_runs_user_status", "backtest_runs", ["user_id", "status"])
    op.create_index("ix_backtest_runs_user_symbol", "backtest_runs", ["user_id", "symbol"])
    op.create_index("ix_backtest_runs_started_at", "backtest_runs", ["started_at"])
    op.create_index("ix_backtest_runs_celery_task_id", "backtest_runs", ["celery_task_id"])
    op.create_index(
        "ix_backtest_runs_status_celery_created",
        "backtest_runs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_backtest_runs_queued",
        "backtest_runs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    # --------------------------------------------------------- backtest_trades
    op.create_table(
        "backtest_trades",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("run_id", GUID(), sa.ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("option_ticker", sa.String(64), nullable=False),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("underlying_symbol", sa.String(32), nullable=False),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("exit_date", sa.Date(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("dte_at_open", sa.Integer(), nullable=False),
        sa.Column("holding_period_days", sa.Integer(), nullable=False),
        sa.Column("entry_underlying_close", sa.Numeric(18, 4), nullable=False),
        sa.Column("exit_underlying_close", sa.Numeric(18, 4), nullable=False),
        sa.Column("entry_mid", sa.Numeric(18, 4), nullable=False),
        sa.Column("exit_mid", sa.Numeric(18, 4), nullable=False),
        sa.Column("gross_pnl", sa.Numeric(18, 4), nullable=False),
        sa.Column("net_pnl", sa.Numeric(18, 4), nullable=False),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False),
        sa.Column("entry_reason", sa.String(128), nullable=False),
        sa.Column("exit_reason", sa.String(128), nullable=False),
        sa.Column("detail_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.UniqueConstraint("run_id", "entry_date", "option_ticker", name="uq_backtest_trades_dedup"),
        sa.CheckConstraint("quantity > 0", name="ck_backtest_trades_quantity_positive"),
    )
    op.create_index("ix_backtest_trades_run_entry_date", "backtest_trades", ["run_id", "entry_date"])

    # -------------------------------------------------- backtest_equity_points
    op.create_table(
        "backtest_equity_points",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("run_id", GUID(), sa.ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.Column("position_value", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.UniqueConstraint("run_id", "trade_date", name="uq_backtest_equity_points_run_date"),
    )

    # ------------------------------------------------------- backtest_templates
    op.create_table(
        "backtest_templates",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("config_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "name", name="uq_backtest_templates_user_name"),
    )
    op.create_index("ix_backtest_templates_user_created_at", "backtest_templates", ["user_id", "created_at"])
    op.create_index("ix_backtest_templates_user_strategy", "backtest_templates", ["user_id", "strategy_type"])
    op.create_index("ix_backtest_templates_user_updated_at", "backtest_templates", ["user_id", "updated_at"])

    # ------------------------------------------------------------ scanner_jobs
    op.create_table(
        "scanner_jobs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "parent_job_id", GUID(),
            sa.ForeignKey("scanner_jobs.id", ondelete="SET NULL"), nullable=True,
        ),
        sa.Column("name", sa.String(120), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("plan_tier_snapshot", sa.String(16), nullable=False),
        sa.Column("job_kind", sa.String(32), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("refresh_key", sa.String(120), nullable=True),
        sa.Column("refresh_daily", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("refresh_priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("candidate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("evaluated_candidate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recommendation_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("request_snapshot_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("warnings_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("ranking_version", sa.String(32), nullable=False, server_default="scanner-ranking-v1"),
        sa.Column("engine_version", sa.String(32), nullable=False, server_default="options-multileg-v2"),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        # Constraints
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_scanner_jobs_user_idempotency_key"),
        sa.UniqueConstraint("refresh_key", name="uq_scanner_jobs_refresh_key"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_scanner_jobs_valid_job_status",
        ),
        sa.CheckConstraint(
            "plan_tier_snapshot IN ('free', 'pro', 'premium')",
            name="ck_scanner_jobs_valid_plan_tier",
        ),
        sa.CheckConstraint("mode IN ('basic', 'advanced')", name="ck_scanner_jobs_valid_mode"),
        sa.CheckConstraint(
            "job_kind IN ('manual', 'refresh', 'nightly')",
            name="ck_scanner_jobs_valid_job_kind",
        ),
        sa.CheckConstraint("candidate_count >= 0", name="ck_scanner_jobs_candidate_count_nonneg"),
        sa.CheckConstraint("evaluated_candidate_count >= 0", name="ck_scanner_jobs_evaluated_count_nonneg"),
        sa.CheckConstraint("recommendation_count >= 0", name="ck_scanner_jobs_recommendation_count_nonneg"),
    )
    op.create_index("ix_scanner_jobs_user_id", "scanner_jobs", ["user_id"])
    op.create_index("ix_scanner_jobs_user_created_at", "scanner_jobs", ["user_id", "created_at"])
    op.create_index("ix_scanner_jobs_user_status", "scanner_jobs", ["user_id", "status"])
    op.create_index("ix_scanner_jobs_request_hash", "scanner_jobs", ["request_hash"])
    op.create_index("ix_scanner_jobs_celery_task_id", "scanner_jobs", ["celery_task_id"])
    op.create_index(
        "ix_scanner_jobs_status_celery_created",
        "scanner_jobs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_scanner_jobs_dedup_lookup",
        "scanner_jobs",
        ["user_id", "request_hash", "mode", "created_at"],
    )
    op.create_index("ix_scanner_jobs_refresh_sources", "scanner_jobs", ["refresh_daily", "status"])
    op.create_index(
        "uq_scanner_jobs_active_dedup",
        "scanner_jobs",
        ["user_id", "request_hash", "mode"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
    )
    op.create_index(
        "ix_scanner_jobs_queued",
        "scanner_jobs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    # ------------------------------------------------ scanner_recommendations
    op.create_table(
        "scanner_recommendations",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column(
            "scanner_job_id", GUID(),
            sa.ForeignKey("scanner_jobs.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(18, 6), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("rule_set_name", sa.String(120), nullable=False),
        sa.Column("rule_set_hash", sa.String(64), nullable=False),
        sa.Column("request_snapshot_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("summary_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("warnings_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("trades_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("equity_curve_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("historical_performance_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("forecast_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("ranking_features_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("scanner_job_id", "rank", name="uq_scanner_recommendations_job_rank"),
        sa.CheckConstraint("rank >= 1", name="ck_scanner_recommendations_rank_positive"),
    )
    op.create_index(
        "ix_scanner_recommendations_lookup",
        "scanner_recommendations",
        ["symbol", "strategy_type", "rule_set_hash"],
    )

    # ------------------------------------------------------------- export_jobs
    op.create_table(
        "export_jobs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "backtest_run_id", GUID(),
            sa.ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("export_format", sa.String(16), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("file_name", sa.String(255), nullable=False),
        sa.Column("mime_type", sa.String(128), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sha256_hex", sa.String(64), nullable=True),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("content_bytes", sa.LargeBinary(), nullable=True),
        sa.Column("storage_key", sa.String(512), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_export_jobs_user_idempotency_key"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="ck_export_jobs_valid_export_status",
        ),
        sa.CheckConstraint(
            "status != 'succeeded' OR content_bytes IS NOT NULL OR storage_key IS NOT NULL",
            name="ck_export_jobs_succeeded_has_storage",
        ),
    )
    op.create_index("ix_export_jobs_user_id", "export_jobs", ["user_id"])
    op.create_index("ix_export_jobs_user_created_at", "export_jobs", ["user_id", "created_at"])
    op.create_index("ix_export_jobs_user_status", "export_jobs", ["user_id", "status"])
    op.create_index("ix_export_jobs_celery_task_id", "export_jobs", ["celery_task_id"])
    op.create_index("ix_export_jobs_backtest_run_id", "export_jobs", ["backtest_run_id"])
    op.create_index(
        "ix_export_jobs_status_celery_created",
        "export_jobs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index("ix_export_jobs_status_expires_at", "export_jobs", ["status", "expires_at"])
    op.create_index(
        "ix_export_jobs_queued",
        "export_jobs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    # ----------------------------------------------------------- audit_events
    op.create_table(
        "audit_events",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("request_id", sa.String(64), nullable=True),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("subject_type", sa.String(64), nullable=False),
        sa.Column("subject_id", sa.String(255), nullable=True),
        sa.Column("ip_hash", sa.String(128), nullable=True),
        sa.Column("metadata_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("event_type", "subject_type", "subject_id", name="uq_audit_events_dedup"),
    )
    op.create_index("ix_audit_events_user_id", "audit_events", ["user_id"])
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_user_created_at", "audit_events", ["user_id", "created_at"])
    op.create_index("ix_audit_events_event_type_created_at", "audit_events", ["event_type", "created_at"])
    op.create_index(
        "uq_audit_events_dedup_null_subject",
        "audit_events",
        ["event_type", "subject_type"],
        unique=True,
        postgresql_where=sa.text("subject_id IS NULL"),
    )
    op.create_check_constraint(
        "ck_audit_events_subject_id_not_empty",
        "audit_events",
        "subject_id IS NULL OR length(subject_id) > 0",
    )

    # ------------------------------------------------- nightly_pipeline_runs
    op.create_table(
        "nightly_pipeline_runs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="running"),
        sa.Column("stage", sa.String(32), nullable=False, server_default="universe_screen"),
        sa.Column("symbols_screened", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("symbols_after_screen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pairs_generated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quick_backtests_run", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("full_backtests_run", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recommendations_produced", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_seconds", sa.Numeric(10, 2), nullable=True),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("stage_details_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_nightly_pipeline_runs_valid_pipeline_status",
        ),
    )
    op.create_index("ix_nightly_pipeline_runs_trade_date", "nightly_pipeline_runs", ["trade_date"])
    op.create_index("ix_nightly_pipeline_runs_status", "nightly_pipeline_runs", ["status"])
    op.create_index(
        "ix_nightly_pipeline_runs_date_status",
        "nightly_pipeline_runs",
        ["trade_date", "status"],
    )
    op.create_index(
        "ix_nightly_pipeline_runs_status_created",
        "nightly_pipeline_runs",
        ["status", "created_at"],
    )
    op.create_index("ix_nightly_pipeline_runs_cursor", "nightly_pipeline_runs", ["created_at", "id"])
    op.create_index(
        "uq_pipeline_runs_succeeded_trade_date",
        "nightly_pipeline_runs",
        ["trade_date"],
        unique=True,
        postgresql_where=sa.text("status = 'succeeded'"),
    )

    # ------------------------------------------------ daily_recommendations
    op.create_table(
        "daily_recommendations",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column(
            "pipeline_run_id", GUID(),
            sa.ForeignKey("nightly_pipeline_runs.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(18, 6), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("strategy_type", sa.String(48), nullable=False),
        sa.Column("regime_labels", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("close_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("target_dte", sa.Integer(), nullable=False),
        sa.Column("config_snapshot_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("summary_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("forecast_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("pipeline_run_id", "rank", name="uq_daily_recs_pipeline_rank"),
        sa.CheckConstraint("rank >= 1", name="ck_daily_recommendations_rank_positive"),
    )
    op.create_index("ix_daily_recs_trade_date", "daily_recommendations", ["trade_date"])
    op.create_index(
        "ix_daily_recs_symbol_strategy",
        "daily_recommendations",
        ["symbol", "strategy_type"],
    )

    # -------------------------------------------------------- symbol_analyses
    op.create_table(
        "symbol_analyses",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("stage", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("close_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("regime_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("landscape_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("top_results_json", JSON_VARIANT, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("forecast_json", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("strategies_tested", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("configs_tested", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("top_results_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_seconds", sa.Numeric(10, 2), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("celery_task_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_symbol_analyses_user_idempotency"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_symbol_analyses_valid_analysis_status",
        ),
        sa.CheckConstraint("strategies_tested >= 0", name="ck_symbol_analyses_strategies_tested_nonneg"),
        sa.CheckConstraint("configs_tested >= 0", name="ck_symbol_analyses_configs_tested_nonneg"),
        sa.CheckConstraint("top_results_count >= 0", name="ck_symbol_analyses_top_results_nonneg"),
        sa.CheckConstraint(
            "stage IN ('pending', 'regime', 'landscape', 'deep_dive', 'forecast')",
            name="ck_symbol_analyses_valid_stage",
        ),
    )
    op.create_index("ix_symbol_analyses_user_id", "symbol_analyses", ["user_id"])
    op.create_index("ix_symbol_analyses_user_created", "symbol_analyses", ["user_id", "created_at"])
    op.create_index("ix_symbol_analyses_symbol", "symbol_analyses", ["symbol"])
    op.create_index("ix_symbol_analyses_status_created", "symbol_analyses", ["status", "created_at"])
    op.create_index("ix_symbol_analyses_celery_task_id", "symbol_analyses", ["celery_task_id"])
    op.create_index(
        "ix_symbol_analyses_status_celery_created",
        "symbol_analyses",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_symbol_analyses_queued",
        "symbol_analyses",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    # ----------------------------------------- Trigger function + triggers
    op.execute("""
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    for table in _TRIGGER_TABLES:
        op.execute(f"""
            CREATE OR REPLACE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
        """)


def downgrade() -> None:
    for table in reversed(_TRIGGER_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at();")

    op.drop_table("daily_recommendations")
    op.drop_table("nightly_pipeline_runs")
    op.drop_table("symbol_analyses")
    op.drop_table("audit_events")
    op.drop_table("export_jobs")
    op.drop_table("scanner_recommendations")
    op.drop_table("scanner_jobs")
    op.drop_table("backtest_templates")
    op.drop_table("backtest_equity_points")
    op.drop_table("backtest_trades")
    op.drop_table("backtest_runs")
    op.drop_table("users")
