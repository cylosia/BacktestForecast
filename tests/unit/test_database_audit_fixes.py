from __future__ import annotations

import inspect


def test_expected_schema_tables_include_current_workflow_and_market_data_tables() -> None:
    from backtestforecast.db.session import expected_schema_tables

    tables = set(expected_schema_tables())

    assert "users" in tables
    assert "multi_symbol_runs" in tables
    assert "multi_step_runs" in tables
    assert "historical_underlying_day_bars" in tables
    assert len(tables) >= 30


def test_health_ready_checks_for_missing_tables() -> None:
    from apps.api.app.routers import health

    source = inspect.getsource(health.ready)
    assert "get_missing_schema_tables" in source
    assert "missing_tables" in source
    assert "schema_incomplete" in source


def test_startup_lifespan_blocks_production_when_schema_is_incomplete() -> None:
    from apps.api.app.main import _lifespan

    source = inspect.getsource(_lifespan)
    assert "get_missing_schema_tables" in source
    assert "get_migration_status" in source
    assert "startup.schema_incomplete" in source
    assert "startup.migration_drift" in source
    assert "DATABASE_URL points to an incomplete schema" in source
    assert "DATABASE_URL points to a schema revision that does not match Alembic head" in source


def test_consolidated_baseline_is_static_snapshot_not_runtime_create_all() -> None:
    from pathlib import Path

    source = Path("alembic/versions/20260324_0001_consolidated_baseline.py").read_text(encoding="utf-8")

    assert "POSTGRESQL_DDL_STATEMENTS" in source
    assert "SQLITE_DDL_STATEMENTS" in source
    assert "Base.metadata.create_all" not in source


def test_historical_coverage_uses_date_only_lookup() -> None:
    from backtestforecast.market_data.historical_store import HistoricalMarketDataStore

    source = inspect.getsource(HistoricalMarketDataStore.has_underlying_coverage)
    assert "_get_underlying_trade_dates" in source
    assert "get_underlying_day_bars" not in source


def test_repair_database_schema_script_requires_explicit_confirmation_for_rebuild() -> None:
    from pathlib import Path

    source = Path("scripts/repair_database_schema.py").read_text(encoding="utf-8")

    assert "--rebuild-public-schema" in source
    assert "--confirm-database" in source
    assert "Refusing to rebuild schema" in source


def test_check_migration_drift_script_bootstraps_repo_and_understands_trigger_helpers() -> None:
    from pathlib import Path

    source = Path("scripts/check_migration_drift.py").read_text(encoding="utf-8")

    assert "bootstrap_repo(load_api_env=True)" in source
    assert "_create_updated_at_trigger" in source
    assert 'constant_bindings["_TABLE_NAME"]' in source


def test_metadata_reflects_db_audit_index_hygiene_changes() -> None:
    from backtestforecast.models import (
        BacktestEquityPoint,
        BacktestTrade,
        DailyRecommendation,
        HistoricalExDividendDate,
        HistoricalTreasuryYield,
        HistoricalUnderlyingDayBar,
        MultiStepEquityPoint,
        MultiStepRunStep,
        MultiStepStepEvent,
        MultiStepTrade,
        MultiSymbolEquityPoint,
        MultiSymbolRunSymbol,
        MultiSymbolTrade,
        MultiSymbolTradeGroup,
        ScannerJob,
        SweepResult,
        TaskResult,
    )

    assert "ix_task_results_created_at" in {idx.name for idx in TaskResult.__table__.indexes}
    assert "ix_daily_recs_created_at" in {idx.name for idx in DailyRecommendation.__table__.indexes}
    assert "ix_scanner_jobs_refresh_sources_lookup" in {idx.name for idx in ScannerJob.__table__.indexes}

    assert "ix_historical_underlying_day_bars_symbol_date" not in {
        idx.name for idx in HistoricalUnderlyingDayBar.__table__.indexes
    }
    assert "ix_historical_ex_dividend_dates_symbol_date" not in {
        idx.name for idx in HistoricalExDividendDate.__table__.indexes
    }
    assert "ix_historical_treasury_yields_trade_date" not in {
        idx.name for idx in HistoricalTreasuryYield.__table__.indexes
    }

    assert "ix_backtest_trades_run_id" not in {idx.name for idx in BacktestTrade.__table__.indexes}
    assert "ix_backtest_equity_points_run_id" not in {idx.name for idx in BacktestEquityPoint.__table__.indexes}
    assert "ix_multi_symbol_run_symbols_run_id" not in {idx.name for idx in MultiSymbolRunSymbol.__table__.indexes}
    assert "ix_multi_symbol_trade_groups_run_id" not in {idx.name for idx in MultiSymbolTradeGroup.__table__.indexes}
    assert "ix_multi_symbol_trades_run_id" not in {idx.name for idx in MultiSymbolTrade.__table__.indexes}
    assert "ix_multi_symbol_equity_points_run_id" not in {idx.name for idx in MultiSymbolEquityPoint.__table__.indexes}
    assert "ix_multi_step_run_steps_run_id" not in {idx.name for idx in MultiStepRunStep.__table__.indexes}
    assert "ix_multi_step_step_events_run_id" not in {idx.name for idx in MultiStepStepEvent.__table__.indexes}
    assert "ix_multi_step_trades_run_id" not in {idx.name for idx in MultiStepTrade.__table__.indexes}
    assert "ix_multi_step_equity_points_run_id" not in {idx.name for idx in MultiStepEquityPoint.__table__.indexes}
    assert "ix_sweep_results_job_id" not in {idx.name for idx in SweepResult.__table__.indexes}


def test_startup_includes_timezone_and_export_storage_operational_warnings() -> None:
    from apps.api.app.main import _lifespan

    source = inspect.getsource(_lifespan)

    assert "get_database_timezones" in source
    assert "startup.database_server_timezone_not_utc" in source
    assert "startup.export_storage_using_database" in source
