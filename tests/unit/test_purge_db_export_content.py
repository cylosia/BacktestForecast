"""Tests for purge_db_export_content management command."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.management.purge_db_export_content import purge_db_export_content
from backtestforecast.models import BacktestRun, ExportJob, User


def _strip_partial_indexes_for_sqlite(engine) -> None:
    """Remove PostgreSQL-specific partial indexes so SQLite create_all succeeds."""
    if engine.dialect.name != "sqlite":
        return
    for table in Base.metadata.tables.values():
        indexes_to_remove = [
            idx for idx in table.indexes
            if idx.dialect_options.get("postgresql", {}).get("where") is not None
        ]
        for idx in indexes_to_remove:
            table.indexes.discard(idx)


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:")
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()


def _make_user(session: Session) -> User:
    user = User(clerk_user_id=f"clerk_{uuid.uuid4().hex[:8]}", email="test@example.com")
    session.add(user)
    session.flush()
    return user


def _make_run(session: Session, user: User) -> BacktestRun:
    from datetime import date
    from decimal import Decimal

    run = BacktestRun(
        user_id=user.id,
        symbol="SPY",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
        engine_version="options-multileg-v2",
        data_source="massive",
    )
    session.add(run)
    session.flush()
    return run


def test_purge_clears_content_bytes_when_storage_exists(db_session: Session):
    user = _make_user(db_session)
    run = _make_run(db_session, user)

    export = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        content_bytes=b"data",
        storage_key="exports/test.csv",
    )
    db_session.add(export)
    db_session.commit()

    storage = MagicMock()
    storage.exists.return_value = True

    count = purge_db_export_content(db_session, storage, dry_run=False)
    assert count == 1

    db_session.refresh(export)
    assert export.content_bytes is None


def test_purge_skips_when_storage_missing(db_session: Session):
    user = _make_user(db_session)
    run = _make_run(db_session, user)

    export = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        content_bytes=b"data",
        storage_key="exports/test.csv",
    )
    db_session.add(export)
    db_session.commit()

    storage = MagicMock()
    storage.exists.return_value = False

    count = purge_db_export_content(db_session, storage, dry_run=False)
    assert count == 0

    db_session.refresh(export)
    assert export.content_bytes is not None


def test_purge_dry_run_does_not_modify(db_session: Session):
    user = _make_user(db_session)
    run = _make_run(db_session, user)

    export = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        content_bytes=b"data",
        storage_key="exports/test.csv",
    )
    db_session.add(export)
    db_session.commit()

    storage = MagicMock()
    storage.exists.return_value = True

    count = purge_db_export_content(db_session, storage, dry_run=True)
    assert count == 1

    db_session.refresh(export)
    assert export.content_bytes is not None
