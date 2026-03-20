"""Add CHECK(length(symbol) > 0) to tables with symbol columns.

Prevents empty-string symbols from being persisted in backtest_runs,
scanner_recommendations, daily_recommendations, symbol_analyses, and
sweep_jobs.

Revision ID: 20260319_0043
Revises: 20260319_0042
Create Date: 2026-03-19
"""
from alembic import op

revision = "20260319_0043"
down_revision = "20260319_0042"
branch_labels = None
depends_on = None

_CONSTRAINTS = [
    ("backtest_runs", "ck_backtest_runs_symbol_not_empty", "length(symbol) > 0"),
    ("scanner_recommendations", "ck_scanner_recommendations_symbol_not_empty", "length(symbol) > 0"),
    ("daily_recommendations", "ck_daily_recommendations_symbol_not_empty", "length(symbol) > 0"),
    ("symbol_analyses", "ck_symbol_analyses_symbol_not_empty", "length(symbol) > 0"),
    ("sweep_jobs", "ck_sweep_jobs_symbol_not_empty", "length(symbol) > 0"),
]


def upgrade() -> None:
    for table, name, condition in _CONSTRAINTS:
        op.create_check_constraint(name, table, condition)


def downgrade() -> None:
    for table, name, _ in reversed(_CONSTRAINTS):
        op.drop_constraint(name, table, type_="check")
