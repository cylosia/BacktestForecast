"""add multi-symbol and multi-step backtest tables

Revision ID: 20260325_0002
Revises: 20260324_0001
Create Date: 2026-03-25 00:02:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from backtestforecast.db.types import GUID, JSON_DEFAULT_EMPTY_ARRAY, JSON_DEFAULT_EMPTY_OBJECT, JSON_VARIANT

revision = "20260325_0002"
down_revision = "20260324_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = set(inspector.get_table_names())
    # The consolidated baseline revision now materializes a fixed schema
    # snapshot. On a fresh database upgraded from that baseline, these workflow tables
    # already exist and this follow-up migration should become a no-op.
    if {"multi_symbol_runs", "multi_step_runs"}.issubset(existing_tables):
        return

    op.create_table(
        "multi_symbol_runs",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("name", sa.String(length=120), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("account_size", sa.Numeric(18, 4), nullable=False),
        sa.Column("capital_allocation_mode", sa.String(length=24), nullable=False, server_default="equal_weight"),
        sa.Column("commission_per_contract", sa.Numeric(18, 4), nullable=False),
        sa.Column("slippage_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("input_snapshot_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("warnings_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_ARRAY),
        sa.Column("idempotency_key", sa.String(length=80), nullable=True),
        sa.Column("celery_task_id", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
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
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')", name="ck_multi_symbol_runs_valid_run_status"),
        sa.CheckConstraint("start_date < end_date", name="ck_multi_symbol_runs_date_order"),
        sa.CheckConstraint("account_size > 0", name="ck_multi_symbol_runs_account_positive"),
        sa.CheckConstraint("commission_per_contract >= 0", name="ck_multi_symbol_runs_commission_nonneg"),
        sa.CheckConstraint("slippage_pct >= 0 AND slippage_pct <= 5", name="ck_multi_symbol_runs_slippage_range"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_multi_symbol_runs_user_idempotency_key"),
    )
    op.create_index("ix_multi_symbol_runs_user_id", "multi_symbol_runs", ["user_id"])
    op.create_index("ix_multi_symbol_runs_user_created_at", "multi_symbol_runs", ["user_id", "created_at"])
    op.create_index("ix_multi_symbol_runs_status", "multi_symbol_runs", ["status"])

    op.create_table(
        "multi_symbol_run_symbols",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("risk_per_trade_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("max_open_positions", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("capital_allocation_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("win_rate", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("total_roi_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("max_drawdown_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("total_net_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("starting_equity", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("ending_equity", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100", name="ck_multi_symbol_run_symbols_risk_pct_range"),
        sa.CheckConstraint("max_open_positions >= 1", name="ck_multi_symbol_run_symbols_max_open_positions_positive"),
        sa.UniqueConstraint("run_id", "symbol", name="uq_multi_symbol_run_symbols_run_symbol"),
    )
    op.create_index("ix_multi_symbol_run_symbols_run_id", "multi_symbol_run_symbols", ["run_id"])

    op.create_table(
        "multi_symbol_trade_groups",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("exit_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="open"),
        sa.Column("detail_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.CheckConstraint("status IN ('open', 'closed', 'cancelled')", name="ck_multi_symbol_trade_groups_status"),
    )
    op.create_index("ix_multi_symbol_trade_groups_run_id", "multi_symbol_trade_groups", ["run_id"])

    op.create_table(
        "multi_symbol_trades",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_group_id", GUID(), sa.ForeignKey("multi_symbol_trade_groups.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("option_ticker", sa.String(length=64), nullable=False),
        sa.Column("strategy_type", sa.String(length=48), nullable=False),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("exit_date", sa.Date(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("dte_at_open", sa.Integer(), nullable=True),
        sa.Column("holding_period_days", sa.Integer(), nullable=True),
        sa.Column("entry_underlying_close", sa.Numeric(18, 4), nullable=True),
        sa.Column("exit_underlying_close", sa.Numeric(18, 4), nullable=True),
        sa.Column("entry_mid", sa.Numeric(18, 4), nullable=True),
        sa.Column("exit_mid", sa.Numeric(18, 4), nullable=True),
        sa.Column("gross_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("net_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("entry_reason", sa.String(length=128), nullable=False),
        sa.Column("exit_reason", sa.String(length=128), nullable=False),
        sa.Column("detail_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.CheckConstraint("quantity > 0", name="ck_multi_symbol_trades_quantity_positive"),
        sa.CheckConstraint("entry_date <= exit_date", name="ck_multi_symbol_trades_date_order"),
    )
    op.create_index("ix_multi_symbol_trades_run_id", "multi_symbol_trades", ["run_id"])
    op.create_index("ix_multi_symbol_trades_trade_group_id", "multi_symbol_trades", ["trade_group_id"])

    op.create_table(
        "multi_symbol_equity_points",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.Column("position_value", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.UniqueConstraint("run_id", "trade_date", name="uq_multi_symbol_equity_points_run_date"),
    )
    op.create_index("ix_multi_symbol_equity_points_run_id", "multi_symbol_equity_points", ["run_id"])

    op.create_table(
        "multi_symbol_symbol_equity_points",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_symbol_id", GUID(), sa.ForeignKey("multi_symbol_run_symbols.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.Column("position_value", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.UniqueConstraint("run_symbol_id", "trade_date", name="uq_multi_symbol_symbol_equity_points_symbol_date"),
    )
    op.create_index("ix_multi_symbol_symbol_equity_points_run_symbol_id", "multi_symbol_symbol_equity_points", ["run_symbol_id"])

    op.create_table(
        "multi_step_runs",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("name", sa.String(length=120), nullable=True),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("workflow_type", sa.String(length=80), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("account_size", sa.Numeric(18, 4), nullable=False),
        sa.Column("risk_per_trade_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("commission_per_contract", sa.Numeric(18, 4), nullable=False),
        sa.Column("slippage_pct", sa.Numeric(10, 4), nullable=False, server_default="0"),
        sa.Column("input_snapshot_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("warnings_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_ARRAY),
        sa.Column("idempotency_key", sa.String(length=80), nullable=True),
        sa.Column("celery_task_id", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
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
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')", name="ck_multi_step_runs_valid_run_status"),
        sa.CheckConstraint("start_date < end_date", name="ck_multi_step_runs_date_order"),
        sa.CheckConstraint("account_size > 0", name="ck_multi_step_runs_account_positive"),
        sa.CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100", name="ck_multi_step_runs_risk_pct_range"),
        sa.CheckConstraint("commission_per_contract >= 0", name="ck_multi_step_runs_commission_nonneg"),
        sa.CheckConstraint("slippage_pct >= 0 AND slippage_pct <= 5", name="ck_multi_step_runs_slippage_range"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_multi_step_runs_user_idempotency_key"),
    )
    op.create_index("ix_multi_step_runs_user_id", "multi_step_runs", ["user_id"])
    op.create_index("ix_multi_step_runs_user_created_at", "multi_step_runs", ["user_id", "created_at"])

    op.create_table(
        "multi_step_run_steps",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_number", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("trigger_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("contract_selection_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("failure_policy", sa.String(length=32), nullable=False, server_default="liquidate"),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.UniqueConstraint("run_id", "step_number", name="uq_multi_step_run_steps_run_step_number"),
    )
    op.create_index("ix_multi_step_run_steps_run_id", "multi_step_run_steps", ["run_id"])

    op.create_table(
        "multi_step_step_events",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_id", GUID(), sa.ForeignKey("multi_step_run_steps.id", ondelete="SET NULL"), nullable=True),
        sa.Column("step_number", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=24), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("payload_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
    )
    op.create_index("ix_multi_step_step_events_run_id", "multi_step_step_events", ["run_id"])
    op.create_index("ix_multi_step_step_events_step_number", "multi_step_step_events", ["step_number"])

    op.create_table(
        "multi_step_trades",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_number", sa.Integer(), nullable=False),
        sa.Column("option_ticker", sa.String(length=64), nullable=False),
        sa.Column("strategy_type", sa.String(length=48), nullable=False),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("exit_date", sa.Date(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("dte_at_open", sa.Integer(), nullable=True),
        sa.Column("holding_period_days", sa.Integer(), nullable=True),
        sa.Column("entry_underlying_close", sa.Numeric(18, 4), nullable=True),
        sa.Column("exit_underlying_close", sa.Numeric(18, 4), nullable=True),
        sa.Column("entry_mid", sa.Numeric(18, 4), nullable=True),
        sa.Column("exit_mid", sa.Numeric(18, 4), nullable=True),
        sa.Column("gross_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("net_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("entry_reason", sa.String(length=128), nullable=False),
        sa.Column("exit_reason", sa.String(length=128), nullable=False),
        sa.Column("detail_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.CheckConstraint("quantity > 0", name="ck_multi_step_trades_quantity_positive"),
        sa.CheckConstraint("entry_date <= exit_date", name="ck_multi_step_trades_date_order"),
    )
    op.create_index("ix_multi_step_trades_run_id", "multi_step_trades", ["run_id"])

    op.create_table(
        "multi_step_equity_points",
        sa.Column("id", GUID(), primary_key=True, nullable=False),
        sa.Column("run_id", GUID(), sa.ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.Column("position_value", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.UniqueConstraint("run_id", "trade_date", name="uq_multi_step_equity_points_run_date"),
    )
    op.create_index("ix_multi_step_equity_points_run_id", "multi_step_equity_points", ["run_id"])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    export_foreign_key_targets = {
        tuple(foreign_key.get("referred_columns") or ()): foreign_key.get("referred_table")
        for foreign_key in inspector.get_foreign_keys("export_jobs")
        if foreign_key.get("referred_table")
    }
    # If upgrade was skipped because the consolidated baseline already had this
    # schema, the export-job foreign keys may still be present with baseline /
    # metadata-derived names. In that case this migration should no-op here and
    # let the baseline downgrade drop the full schema in dependency order.
    if {"multi_symbol_runs", "multi_step_runs"} & set(export_foreign_key_targets.values()):
        return

    for table in (
        "multi_step_equity_points",
        "multi_step_trades",
        "multi_step_step_events",
        "multi_step_run_steps",
        "multi_step_runs",
        "multi_symbol_symbol_equity_points",
        "multi_symbol_equity_points",
        "multi_symbol_trades",
        "multi_symbol_trade_groups",
        "multi_symbol_run_symbols",
        "multi_symbol_runs",
    ):
        op.drop_table(table)
