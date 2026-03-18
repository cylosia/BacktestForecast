"""Validate NOT VALID constraints.

- Validate backtest_trades dte_at_open_nonneg, holding_period_nonneg
- Validate outbox_messages retry_count_nonneg
- Validate sweep_jobs valid_plan_tier

Revision ID: 20260318_0019
Revises: 20260318_0018
Create Date: 2026-03-18
"""
from __future__ import annotations

from alembic import op

revision = "20260318_0019"
down_revision = "20260318_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table, name in [
        ("backtest_trades", "ck_backtest_trades_dte_at_open_nonneg"),
        ("backtest_trades", "ck_backtest_trades_holding_period_nonneg"),
        ("outbox_messages", "ck_outbox_messages_retry_count_nonneg"),
        ("sweep_jobs", "ck_sweep_jobs_valid_plan_tier"),
    ]:
        op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}")


def downgrade() -> None:
    # Validating constraints is not reversible; no-op.
    pass
