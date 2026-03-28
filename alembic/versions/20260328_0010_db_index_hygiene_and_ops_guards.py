"""Tighten index hygiene for cleanup, scanner refresh, and historical imports.

Revision ID: 20260328_0010
Revises: 20260328_0009
Create Date: 2026-03-28 16:05:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError

from alembic import op

revision = "20260328_0010"
down_revision = "20260328_0009"
branch_labels = None
depends_on = None


_DROP_INDEXES: dict[str, tuple[str, ...]] = {
    "backtest_trades": (
        "ix_backtest_trades_run_id",
    ),
    "backtest_equity_points": (
        "ix_backtest_equity_points_run_id",
    ),
    "multi_symbol_run_symbols": (
        "ix_multi_symbol_run_symbols_run_id",
    ),
    "multi_symbol_trade_groups": (
        "ix_multi_symbol_trade_groups_run_id",
    ),
    "multi_symbol_trades": (
        "ix_multi_symbol_trades_run_id",
    ),
    "multi_symbol_equity_points": (
        "ix_multi_symbol_equity_points_run_id",
    ),
    "multi_step_run_steps": (
        "ix_multi_step_run_steps_run_id",
    ),
    "multi_step_step_events": (
        "ix_multi_step_step_events_run_id",
    ),
    "multi_step_trades": (
        "ix_multi_step_trades_run_id",
    ),
    "multi_step_equity_points": (
        "ix_multi_step_equity_points_run_id",
    ),
    "sweep_results": (
        "ix_sweep_results_job_id",
    ),
    "historical_underlying_day_bars": (
        "ix_historical_underlying_day_bars_symbol_date",
    ),
    "historical_ex_dividend_dates": (
        "ix_historical_ex_dividend_dates_symbol_date",
    ),
    "historical_treasury_yields": (
        "ix_historical_treasury_yields_trade_date",
    ),
}

_CREATE_INDEXES: dict[str, tuple[tuple[str, list[object], dict[str, object]], ...]] = {
    "task_results": (
        ("ix_task_results_created_at", ["created_at"], {}),
    ),
    "daily_recommendations": (
        ("ix_daily_recs_created_at", ["created_at"], {}),
    ),
    "scanner_jobs": (
        (
            "ix_scanner_jobs_refresh_sources_lookup",
            ["user_id", "request_hash", "mode", sa.text("completed_at DESC")],
            {
                "postgresql_where": sa.text(
                    "refresh_daily = true AND status = 'succeeded' AND completed_at IS NOT NULL"
                ),
            },
        ),
    ),
}


def _get_existing_indexes(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        return {index["name"] for index in inspector.get_indexes(table_name)}
    except NoSuchTableError:
        return set()


def upgrade() -> None:
    for table_name, indexes in _DROP_INDEXES.items():
        existing = _get_existing_indexes(table_name)
        for index_name in indexes:
            if index_name in existing:
                op.drop_index(index_name, table_name=table_name)

    for table_name, specs in _CREATE_INDEXES.items():
        existing = _get_existing_indexes(table_name)
        for index_name, columns, kwargs in specs:
            if index_name not in existing:
                op.create_index(index_name, table_name, columns, unique=False, **kwargs)


def downgrade() -> None:
    for table_name, specs in _CREATE_INDEXES.items():
        existing = _get_existing_indexes(table_name)
        for index_name, _columns, _kwargs in reversed(specs):
            if index_name in existing:
                op.drop_index(index_name, table_name=table_name)

    recreate_specs: dict[str, tuple[tuple[str, list[str], dict[str, object]], ...]] = {
        "backtest_trades": (
            ("ix_backtest_trades_run_id", ["run_id"], {}),
        ),
        "backtest_equity_points": (
            ("ix_backtest_equity_points_run_id", ["run_id"], {}),
        ),
        "multi_symbol_run_symbols": (
            ("ix_multi_symbol_run_symbols_run_id", ["run_id"], {}),
        ),
        "multi_symbol_trade_groups": (
            ("ix_multi_symbol_trade_groups_run_id", ["run_id"], {}),
        ),
        "multi_symbol_trades": (
            ("ix_multi_symbol_trades_run_id", ["run_id"], {}),
        ),
        "multi_symbol_equity_points": (
            ("ix_multi_symbol_equity_points_run_id", ["run_id"], {}),
        ),
        "multi_step_run_steps": (
            ("ix_multi_step_run_steps_run_id", ["run_id"], {}),
        ),
        "multi_step_step_events": (
            ("ix_multi_step_step_events_run_id", ["run_id"], {}),
        ),
        "multi_step_trades": (
            ("ix_multi_step_trades_run_id", ["run_id"], {}),
        ),
        "multi_step_equity_points": (
            ("ix_multi_step_equity_points_run_id", ["run_id"], {}),
        ),
        "sweep_results": (
            ("ix_sweep_results_job_id", ["sweep_job_id"], {}),
        ),
        "historical_underlying_day_bars": (
            ("ix_historical_underlying_day_bars_symbol_date", ["symbol", "trade_date"], {}),
        ),
        "historical_ex_dividend_dates": (
            ("ix_historical_ex_dividend_dates_symbol_date", ["symbol", "ex_dividend_date"], {}),
        ),
        "historical_treasury_yields": (
            ("ix_historical_treasury_yields_trade_date", ["trade_date"], {}),
        ),
    }

    for table_name, specs in recreate_specs.items():
        existing = _get_existing_indexes(table_name)
        for index_name, columns, kwargs in specs:
            if index_name not in existing:
                op.create_index(index_name, table_name, columns, unique=False, **kwargs)
