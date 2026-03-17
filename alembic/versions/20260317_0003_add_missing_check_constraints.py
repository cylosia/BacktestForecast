"""Add missing CHECK constraints for data integrity.

Revision ID: 20260317_0003
Revises: 20260317_0002
Create Date: 2026-03-17
"""
from __future__ import annotations

import uuid

from alembic import op
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


revision = "20260317_0003"
down_revision = "20260317_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_backtest_runs_valid_engine_version",
        "backtest_runs",
        "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
    )
    op.create_check_constraint(
        "ck_backtest_runs_valid_data_source",
        "backtest_runs",
        "data_source IN ('massive', 'manual')",
    )
    op.create_check_constraint(
        "ck_backtest_trades_date_order",
        "backtest_trades",
        "entry_date <= exit_date",
    )
    op.create_check_constraint(
        "ck_export_jobs_size_bytes_nonneg",
        "export_jobs",
        "size_bytes >= 0",
    )
    op.create_check_constraint(
        "ck_nightly_pipeline_runs_valid_stage",
        "nightly_pipeline_runs",
        "stage IN ('universe_screen', 'strategy_match', 'quick_backtest', 'full_backtest', 'forecast_rank')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_nightly_pipeline_runs_valid_stage", "nightly_pipeline_runs", type_="check")
    op.drop_constraint("ck_export_jobs_size_bytes_nonneg", "export_jobs", type_="check")
    op.drop_constraint("ck_backtest_trades_date_order", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_runs_valid_data_source", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_valid_engine_version", "backtest_runs", type_="check")
