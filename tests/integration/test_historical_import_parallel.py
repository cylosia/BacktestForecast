from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from tests.postgres_support import reset_database


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


def test_sync_trade_dates_persists_rows_with_parallel_workers(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    reset_database(postgres_session_factory)
    module = _load_sync_script_module()
    store = HistoricalMarketDataStore(postgres_session_factory, postgres_session_factory)
    flatfiles = _FakeFlatFilesClient()

    results = module._sync_trade_dates(
        store,
        flatfiles,
        [date(2025, 4, 1), date(2025, 4, 2)],
        symbols={"AAPL", "MSFT"},
        batch_size=1,
        dry_run=False,
        workers=2,
    )

    assert [item.trade_date for item in results] == [date(2025, 4, 1), date(2025, 4, 2)]
    assert [item.stock_count for item in results] == [2, 2]
    assert [item.option_count for item in results] == [1, 1]

    aapl_bars = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 2))
    msft_bars = store.get_underlying_day_bars("MSFT", date(2025, 4, 1), date(2025, 4, 2))

    assert [bar.trade_date for bar in aapl_bars] == [date(2025, 4, 1), date(2025, 4, 2)]
    assert [bar.close_price for bar in aapl_bars] == [100.0, 101.0]
    assert [bar.trade_date for bar in msft_bars] == [date(2025, 4, 1), date(2025, 4, 2)]
    assert [bar.close_price for bar in msft_bars] == [200.0, 201.0]

    gateway = HistoricalOptionGateway(store, "AAPL")
    first_quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    second_quote = gateway.get_quote("O:AAPL250418C00195000", date(2025, 4, 2))

    assert first_quote is not None
    assert second_quote is not None
    assert first_quote.mid_price == 5.25
    assert second_quote.mid_price == 4.25
