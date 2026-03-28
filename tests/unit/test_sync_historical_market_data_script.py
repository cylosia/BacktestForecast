from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore


def _load_sync_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "sync_historical_market_data.py"
    sys.path.insert(0, str(script_path.parent))
    try:
        spec = importlib.util.spec_from_file_location("sync_historical_market_data", script_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def _store() -> HistoricalMarketDataStore:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    return HistoricalMarketDataStore(factory, factory)


class _FakeFlatFilesClient:
    def __init__(self) -> None:
        self._rows = {
            ("us_stocks_sip/day_aggs_v1", date(2025, 4, 1)): [
                {"ticker": "AAPL", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
                {"ticker": "MSFT", "open": "200", "high": "201", "low": "199", "close": "200", "volume": "2000"},
            ],
            ("us_options_opra/day_aggs_v1", date(2025, 4, 1)): [
                {
                    "ticker": "O:AAPL250418C00190000",
                    "open": "5.10",
                    "high": "5.40",
                    "low": "4.80",
                    "close": "5.25",
                    "volume": "10",
                }
            ],
        }

    def iter_csv_rows(self, dataset: str, trade_date: date):
        yield from self._rows[(dataset, trade_date)]


def test_sync_stock_day_streams_and_upserts_payload_batches() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_stock_day(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=1,
    )

    assert inserted == 2
    aapl = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    msft = store.get_underlying_day_bars("MSFT", date(2025, 4, 1), date(2025, 4, 1))
    assert len(aapl) == 1
    assert len(msft) == 1
    assert aapl[0].close_price == 100.0
    assert msft[0].close_price == 200.0


def test_sync_option_day_streams_and_upserts_payload_batches() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_option_day(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL"},
        batch_size=2,
    )

    assert inserted == 1
    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None
    assert quote.mid_price == 5.25


def test_sync_stock_day_dry_run_counts_rows_without_store() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_stock_day(
        None,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=1,
        dry_run=True,
    )

    assert inserted == 2


def test_sync_option_day_dry_run_counts_rows_without_store() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_option_day(
        None,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL"},
        batch_size=2,
        dry_run=True,
    )

    assert inserted == 1
