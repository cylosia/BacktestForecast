"""initial backtest vertical slice

Revision ID: 20260309_0001
Revises:
Create Date: 2026-03-09 12:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260309_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("clerk_user_id", sa.String(length=255), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("clerk_user_id"),
    )

    op.create_table(
        "backtest_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("strategy_type", sa.String(length=32), nullable=False),
        sa.Column("date_from", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=False),
        sa.Column("target_dte", sa.Integer(), nullable=False),
        sa.Column("dte_tolerance_days", sa.Integer(), nullable=False),
        sa.Column("max_holding_days", sa.Integer(), nullable=False),
        sa.Column("account_size", sa.Numeric(18, 4), nullable=False),
        sa.Column("risk_per_trade_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("commission_per_contract", sa.Numeric(18, 4), nullable=False),
        sa.Column("input_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("warnings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("engine_version", sa.String(length=32), nullable=False),
        sa.Column("data_source", sa.String(length=32), nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=False),
        sa.Column("win_rate", sa.Numeric(10, 4), nullable=False),
        sa.Column("total_roi_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("average_win_amount", sa.Numeric(18, 4), nullable=False),
        sa.Column("average_loss_amount", sa.Numeric(18, 4), nullable=False),
        sa.Column("average_holding_period_days", sa.Numeric(10, 4), nullable=False),
        sa.Column("average_dte_at_open", sa.Numeric(10, 4), nullable=False),
        sa.Column("max_drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.Column("total_commissions", sa.Numeric(18, 4), nullable=False),
        sa.Column("total_net_pnl", sa.Numeric(18, 4), nullable=False),
        sa.Column("starting_equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("ending_equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_backtest_runs_user_created_at", "backtest_runs", ["user_id", "created_at"], unique=False)
    op.create_index("ix_backtest_runs_user_status", "backtest_runs", ["user_id", "status"], unique=False)

    op.create_table(
        "backtest_trades",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("option_ticker", sa.String(length=64), nullable=False),
        sa.Column("strategy_type", sa.String(length=32), nullable=False),
        sa.Column("underlying_symbol", sa.String(length=32), nullable=False),
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
        sa.Column("entry_reason", sa.String(length=128), nullable=False),
        sa.Column("exit_reason", sa.String(length=128), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_backtest_trades_run_entry_date", "backtest_trades", ["run_id", "entry_date"], unique=False)

    op.create_table(
        "backtest_equity_points",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.Column("position_value", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(10, 4), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "trade_date", name="uq_backtest_equity_points_run_date"),
    )
    op.create_index(
        "ix_backtest_equity_points_run_date", "backtest_equity_points", ["run_id", "trade_date"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_backtest_equity_points_run_date", table_name="backtest_equity_points")
    op.drop_table("backtest_equity_points")
    op.drop_index("ix_backtest_trades_run_entry_date", table_name="backtest_trades")
    op.drop_table("backtest_trades")
    op.drop_index("ix_backtest_runs_user_status", table_name="backtest_runs")
    op.drop_index("ix_backtest_runs_user_created_at", table_name="backtest_runs")
    op.drop_table("backtest_runs")
    op.drop_table("users")
