from __future__ import annotations

import importlib.util
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.errors import ExternalServiceError
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
        sys.modules[spec.name] = module
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
            ("us_stocks_sip/day_aggs_v1", date(2025, 4, 2)): [
                {"ticker": "AAPL", "open": "101", "high": "102", "low": "100", "close": "101", "volume": "1100"},
                {"ticker": "MSFT", "open": "201", "high": "202", "low": "200", "close": "201", "volume": "2100"},
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
            ("us_options_opra/day_aggs_v1", date(2025, 4, 2)): [
                {
                    "ticker": "O:AAPL250418C00195000",
                    "open": "4.10",
                    "high": "4.40",
                    "low": "3.80",
                    "close": "4.25",
                    "volume": "12",
                }
            ],
        }

    def iter_csv_rows(self, dataset: str, trade_date: date):
        yield from self._rows[(dataset, trade_date)]


class _MissingStockFlatFilesClient(_FakeFlatFilesClient):
    def iter_csv_rows(self, dataset: str, trade_date: date):
        if dataset == "us_stocks_sip/day_aggs_v1":
            raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}")
        yield from super().iter_csv_rows(dataset, trade_date)


class _FakeRestClient:
    def __init__(self) -> None:
        self.treasury_calls: list[date] = []
        self.dividend_calls: list[tuple[str, date]] = []

    def get_average_treasury_yield(self, start_date: date, end_date: date):
        assert start_date == end_date
        self.treasury_calls.append(start_date)
        return Decimal("0.041")

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date):
        assert start_date == end_date
        self.dividend_calls.append((symbol, start_date))
        return {start_date} if symbol == "AAPL" else set()

    def close(self) -> None:
        return None


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


def test_sync_trade_date_skips_missing_stock_file_and_keeps_option_rows() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _MissingStockFlatFilesClient()

    result = module._sync_trade_date(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=2,
    )

    assert result.stock_count == 0
    assert result.option_count == 1
    assert result.stock_error is not None
    assert result.option_error is None

    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None


def test_sync_trade_dates_supports_parallel_workers_and_keeps_date_order() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    results = module._sync_trade_dates(
        None,
        flatfiles,
        [date(2025, 4, 1), date(2025, 4, 2)],
        symbols={"AAPL", "MSFT"},
        batch_size=2,
        dry_run=True,
        workers=2,
    )

    assert [item.trade_date for item in results] == [date(2025, 4, 1), date(2025, 4, 2)]
    assert [item.stock_count for item in results] == [2, 2]
    assert [item.option_count for item in results] == [1, 1]


def test_maybe_enrich_trade_date_can_be_skipped() -> None:
    module = _load_sync_script_module()
    store = _store()
    rest_client = _FakeRestClient()
    result = module.TradeDateSyncResult(trade_date=date(2025, 4, 1), stock_count=2, option_count=1)

    module._maybe_enrich_trade_date(
        store,
        rest_client,
        result,
        symbols={"AAPL"},
        skip_rest_enrichment=True,
    )

    assert rest_client.treasury_calls == []
    assert rest_client.dividend_calls == []
    assert store.get_average_treasury_yield(date(2025, 4, 1), date(2025, 4, 1)) is None


def test_maybe_enrich_trade_date_writes_treasury_and_dividends_when_enabled() -> None:
    module = _load_sync_script_module()
    store = _store()
    rest_client = _FakeRestClient()
    result = module.TradeDateSyncResult(trade_date=date(2025, 4, 1), stock_count=2, option_count=1)

    module._maybe_enrich_trade_date(
        store,
        rest_client,
        result,
        symbols={"AAPL", "MSFT"},
        skip_rest_enrichment=False,
    )

    assert rest_client.treasury_calls == [date(2025, 4, 1)]
    assert rest_client.dividend_calls == [("AAPL", date(2025, 4, 1)), ("MSFT", date(2025, 4, 1))]
    assert store.get_average_treasury_yield(date(2025, 4, 1), date(2025, 4, 1)) == 0.041
    assert store.list_ex_dividend_dates("AAPL", date(2025, 4, 1), date(2025, 4, 1)) == {date(2025, 4, 1)}


def test_print_trade_date_result_includes_progress_prefix_and_flushes() -> None:
    module = _load_sync_script_module()
    result = module.TradeDateSyncResult(
        trade_date=date(2025, 4, 1),
        stock_count=2,
        option_count=1,
    )

    with patch("builtins.print") as mock_print:
        module._print_trade_date_result(
            result,
            dry_run=False,
            completed=3,
            total=10,
        )

    mock_print.assert_called_once()
    args, kwargs = mock_print.call_args
    assert args[0].startswith("[3/10] 2025-04-01: synced stock_rows=2 option_rows=1")
    assert kwargs["flush"] is True
