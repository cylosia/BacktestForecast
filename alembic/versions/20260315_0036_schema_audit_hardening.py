"""Schema hardening: CHECK constraints, partial indexes, and column additions.

Addresses verified findings from the migration-chain audit:
- CHECK(target_dte >= 0) and CHECK(dte_tolerance_days >= 0) on backtest_runs
- CHECK(quantity > 0) on backtest_trades
- CHECK(rank >= 1) on scanner_recommendations and daily_recommendations
- CHECK(candidate_count >= 0), CHECK(evaluated_candidate_count >= 0),
  CHECK(recommendation_count >= 0) on scanner_jobs
- CHECK(strategies_tested >= 0), CHECK(configs_tested >= 0),
  CHECK(top_results_count >= 0) on symbol_analyses
- Partial indexes WHERE status='queued' on all four job tables for worker polling
- updated_at column on scanner_recommendations
- Export content CHECK: at least one storage path when status='succeeded'
- server_default on backtest_runs result columns for direct-SQL safety

All CHECK constraints use NOT VALID + VALIDATE for zero-downtime deployment.

Revision ID: 20260315_0036
Revises: 20260314_0035
Create Date: 2026-03-15
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260315_0036"
down_revision = "20260314_0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- backtest_runs CHECK constraints ---
    op.execute(
        "ALTER TABLE backtest_runs ADD CONSTRAINT ck_backtest_runs_target_dte_nonneg "
        "CHECK (target_dte >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE backtest_runs VALIDATE CONSTRAINT ck_backtest_runs_target_dte_nonneg")

    op.execute(
        "ALTER TABLE backtest_runs ADD CONSTRAINT ck_backtest_runs_dte_tolerance_nonneg "
        "CHECK (dte_tolerance_days >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE backtest_runs VALIDATE CONSTRAINT ck_backtest_runs_dte_tolerance_nonneg")

    # --- backtest_runs server_defaults for result columns (direct-SQL safety) ---
    for col in (
        "trade_count", "win_rate", "total_roi_pct", "average_win_amount",
        "average_loss_amount", "average_holding_period_days", "average_dte_at_open",
        "max_drawdown_pct", "total_commissions", "total_net_pnl",
        "starting_equity", "ending_equity", "expectancy",
        "max_consecutive_wins", "max_consecutive_losses",
    ):
        op.alter_column("backtest_runs", col, server_default="0")
    # Leave server_defaults in place so direct SQL INSERTs work without ORM.

    # --- backtest_trades CHECK constraint ---
    op.execute(
        "ALTER TABLE backtest_trades ADD CONSTRAINT ck_backtest_trades_quantity_positive "
        "CHECK (quantity > 0) NOT VALID"
    )
    op.execute("ALTER TABLE backtest_trades VALIDATE CONSTRAINT ck_backtest_trades_quantity_positive")

    # --- scanner_recommendations CHECK constraint + updated_at ---
    op.execute(
        "ALTER TABLE scanner_recommendations ADD CONSTRAINT ck_scanner_recommendations_rank_positive "
        "CHECK (rank >= 1) NOT VALID"
    )
    op.execute("ALTER TABLE scanner_recommendations VALIDATE CONSTRAINT ck_scanner_recommendations_rank_positive")

    op.add_column(
        "scanner_recommendations",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True, server_default=sa.func.now()),
    )
    op.execute("UPDATE scanner_recommendations SET updated_at = created_at WHERE updated_at IS NULL")
    op.alter_column("scanner_recommendations", "updated_at", nullable=False)

    # --- daily_recommendations CHECK constraint ---
    op.execute(
        "ALTER TABLE daily_recommendations ADD CONSTRAINT ck_daily_recommendations_rank_positive "
        "CHECK (rank >= 1) NOT VALID"
    )
    op.execute("ALTER TABLE daily_recommendations VALIDATE CONSTRAINT ck_daily_recommendations_rank_positive")

    # --- scanner_jobs counter CHECK constraints ---
    op.execute(
        "ALTER TABLE scanner_jobs ADD CONSTRAINT ck_scanner_jobs_candidate_count_nonneg "
        "CHECK (candidate_count >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE scanner_jobs VALIDATE CONSTRAINT ck_scanner_jobs_candidate_count_nonneg")

    op.execute(
        "ALTER TABLE scanner_jobs ADD CONSTRAINT ck_scanner_jobs_evaluated_count_nonneg "
        "CHECK (evaluated_candidate_count >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE scanner_jobs VALIDATE CONSTRAINT ck_scanner_jobs_evaluated_count_nonneg")

    op.execute(
        "ALTER TABLE scanner_jobs ADD CONSTRAINT ck_scanner_jobs_recommendation_count_nonneg "
        "CHECK (recommendation_count >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE scanner_jobs VALIDATE CONSTRAINT ck_scanner_jobs_recommendation_count_nonneg")

    # --- symbol_analyses counter CHECK constraints ---
    op.execute(
        "ALTER TABLE symbol_analyses ADD CONSTRAINT ck_symbol_analyses_strategies_tested_nonneg "
        "CHECK (strategies_tested >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE symbol_analyses VALIDATE CONSTRAINT ck_symbol_analyses_strategies_tested_nonneg")

    op.execute(
        "ALTER TABLE symbol_analyses ADD CONSTRAINT ck_symbol_analyses_configs_tested_nonneg "
        "CHECK (configs_tested >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE symbol_analyses VALIDATE CONSTRAINT ck_symbol_analyses_configs_tested_nonneg")

    op.execute(
        "ALTER TABLE symbol_analyses ADD CONSTRAINT ck_symbol_analyses_top_results_nonneg "
        "CHECK (top_results_count >= 0) NOT VALID"
    )
    op.execute("ALTER TABLE symbol_analyses VALIDATE CONSTRAINT ck_symbol_analyses_top_results_nonneg")

    # --- export_jobs CHECK: succeeded exports must have storage ---
    op.execute(
        "ALTER TABLE export_jobs ADD CONSTRAINT ck_export_jobs_succeeded_has_storage "
        "CHECK (status != 'succeeded' OR content_bytes IS NOT NULL OR storage_key IS NOT NULL) NOT VALID"
    )
    op.execute("ALTER TABLE export_jobs VALIDATE CONSTRAINT ck_export_jobs_succeeded_has_storage")

    # --- Add updated_at trigger for scanner_recommendations (now has updated_at column) ---
    op.execute("""
        CREATE OR REPLACE TRIGGER trg_scanner_recommendations_updated_at
        BEFORE UPDATE ON scanner_recommendations
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
    """)

    # --- Partial indexes for worker polling (WHERE status='queued') ---
    op.create_index(
        "ix_backtest_runs_queued",
        "backtest_runs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_scanner_jobs_queued",
        "scanner_jobs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_export_jobs_queued",
        "export_jobs",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_symbol_analyses_queued",
        "symbol_analyses",
        ["created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index("ix_symbol_analyses_queued", table_name="symbol_analyses")
    op.drop_index("ix_export_jobs_queued", table_name="export_jobs")
    op.drop_index("ix_scanner_jobs_queued", table_name="scanner_jobs")
    op.drop_index("ix_backtest_runs_queued", table_name="backtest_runs")

    op.execute("DROP TRIGGER IF EXISTS trg_scanner_recommendations_updated_at ON scanner_recommendations;")

    op.execute("ALTER TABLE export_jobs DROP CONSTRAINT IF EXISTS ck_export_jobs_succeeded_has_storage")

    op.execute("ALTER TABLE symbol_analyses DROP CONSTRAINT IF EXISTS ck_symbol_analyses_top_results_nonneg")
    op.execute("ALTER TABLE symbol_analyses DROP CONSTRAINT IF EXISTS ck_symbol_analyses_configs_tested_nonneg")
    op.execute("ALTER TABLE symbol_analyses DROP CONSTRAINT IF EXISTS ck_symbol_analyses_strategies_tested_nonneg")

    op.execute("ALTER TABLE scanner_jobs DROP CONSTRAINT IF EXISTS ck_scanner_jobs_recommendation_count_nonneg")
    op.execute("ALTER TABLE scanner_jobs DROP CONSTRAINT IF EXISTS ck_scanner_jobs_evaluated_count_nonneg")
    op.execute("ALTER TABLE scanner_jobs DROP CONSTRAINT IF EXISTS ck_scanner_jobs_candidate_count_nonneg")

    op.execute("ALTER TABLE daily_recommendations DROP CONSTRAINT IF EXISTS ck_daily_recommendations_rank_positive")

    op.alter_column("scanner_recommendations", "updated_at", nullable=True)
    op.drop_column("scanner_recommendations", "updated_at")
    op.execute("ALTER TABLE scanner_recommendations DROP CONSTRAINT IF EXISTS ck_scanner_recommendations_rank_positive")

    op.execute("ALTER TABLE backtest_trades DROP CONSTRAINT IF EXISTS ck_backtest_trades_quantity_positive")

    for col in (
        "trade_count", "win_rate", "total_roi_pct", "average_win_amount",
        "average_loss_amount", "average_holding_period_days", "average_dte_at_open",
        "max_drawdown_pct", "total_commissions", "total_net_pnl",
        "starting_equity", "ending_equity", "expectancy",
        "max_consecutive_wins", "max_consecutive_losses",
    ):
        op.alter_column("backtest_runs", col, server_default=None)

    op.execute("ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS ck_backtest_runs_dte_tolerance_nonneg")
    op.execute("ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS ck_backtest_runs_target_dte_nonneg")
