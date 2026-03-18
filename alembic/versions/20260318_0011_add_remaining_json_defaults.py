"""Add server_default for remaining non-nullable JSON columns.

Ensures direct SQL inserts (outside ORM) don't violate NOT NULL constraints
on trades_json, equity_curve_json, detail_json, and related columns that
previously relied only on Python-side defaults.

Note: filename predates revision renumbering; revision ID is authoritative.

Revision ID: 20260318_0011
Revises: 20260318_0010
Create Date: 2026-03-18
"""
from __future__ import annotations

from alembic import op

revision = "20260318_0011"
down_revision = "20260318_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ScannerRecommendation JSON columns
    op.alter_column("scanner_recommendations", "trades_json", server_default="'[]'")
    op.alter_column("scanner_recommendations", "equity_curve_json", server_default="'[]'")
    op.alter_column("scanner_recommendations", "historical_performance_json", server_default="'{}'")
    op.alter_column("scanner_recommendations", "forecast_json", server_default="'{}'")
    op.alter_column("scanner_recommendations", "ranking_features_json", server_default="'{}'")

    # BacktestTrade JSON column
    op.alter_column("backtest_trades", "detail_json", server_default="'{}'")

    # Add updated_at triggers for tables that receive direct SQL UPDATEs
    # (reaper, billing cancellation) so updated_at stays accurate.
    for table in (
        "backtest_runs",
        "export_jobs",
        "scanner_jobs",
        "symbol_analyses",
    ):
        op.execute(
            f"""
            DROP TRIGGER IF EXISTS set_updated_at ON {table};
            CREATE TRIGGER set_updated_at
                BEFORE UPDATE ON {table}
                FOR EACH ROW
                EXECUTE FUNCTION set_updated_at();
            """
        )


def downgrade() -> None:
    for table in (
        "symbol_analyses",
        "scanner_jobs",
        "export_jobs",
        "backtest_runs",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS set_updated_at ON {table};")

    op.alter_column("backtest_trades", "detail_json", server_default=None)
    op.alter_column("scanner_recommendations", "ranking_features_json", server_default=None)
    op.alter_column("scanner_recommendations", "forecast_json", server_default=None)
    op.alter_column("scanner_recommendations", "historical_performance_json", server_default=None)
    op.alter_column("scanner_recommendations", "equity_curve_json", server_default=None)
    op.alter_column("scanner_recommendations", "trades_json", server_default=None)
