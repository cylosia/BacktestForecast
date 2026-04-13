from __future__ import annotations

import gzip
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.integrations.massive_flatfiles import (
    chunked,
    iter_option_day_bar_records,
    iter_option_day_bar_payloads,
    iter_stock_day_bar_payloads,
    option_day_dataset,
    stock_day_dataset,
    MassiveFlatFilesClient,
)
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore, parse_option_ticker_metadata
from backtestforecast.market_data.service import MarketDataService
from backtestforecast.models import (
    HistoricalEarningsEvent,
    HistoricalExDividendDate,
    HistoricalOptionDayBar,
    HistoricalTreasuryYield,
    HistoricalUnderlyingDayBar,
)
from backtestforecast.services.risk_free_rate import build_backtest_risk_free_rate_curve
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType


class _UnusedClient(MassiveClient):
    def __init__(self) -> None:  # pragma: no cover - constructor intentionally bypasses parent
        pass


def _store() -> HistoricalMarketDataStore:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    return HistoricalMarketDataStore(factory, factory)


def test_parse_option_ticker_metadata() -> None:
    parsed = parse_option_ticker_metadata("O:AAPL250418C00190000")
    assert parsed == ("AAPL", date(2025, 4, 18), "call", 190.0)


def test_flatfile_day_key_shape() -> None:
    from backtestforecast.integrations.massive_flatfiles import _day_key

    assert _day_key(option_day_dataset(), date(2025, 4, 1)) == "us_options_opra/day_aggs_v1/2025/04/2025-04-01.csv.gz"
    assert _day_key(stock_day_dataset(), date(2025, 4, 1)) == "us_stocks_sip/day_aggs_v1/2025/04/2025-04-01.csv.gz"


def test_http_flatfile_streaming_iterates_gzip_csv_rows() -> None:
    compressed = gzip.compress(
        b"ticker,open,high,low,close,volume\nAAPL,100,101,99,100,1000\nMSFT,200,201,199,200,2000\n"
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path.endswith("/us_stocks_sip/day_aggs_v1/2025/04/2025-04-01.csv.gz")
        return httpx.Response(200, content=compressed)

    client = MassiveFlatFilesClient(base_url="https://files.massive.com", api_key="test-key", use_s3=False)
    http_client = httpx.Client(transport=httpx.MockTransport(_handler), trust_env=False)
    client._http_client = http_client

    try:
        rows = list(client.iter_csv_rows(stock_day_dataset(), date(2025, 4, 1)))
    finally:
        http_client.close()

    assert rows == [
        {"ticker": "AAPL", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
        {"ticker": "MSFT", "open": "200", "high": "201", "low": "199", "close": "200", "volume": "2000"},
    ]


def test_historical_option_gateway_uses_close_as_mid() -> None:
    store = _store()
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418C00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            )
        ]
    )
    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None
    assert quote.bid_price == 5.25
    assert quote.ask_price == 5.25
    assert quote.mid_price == 5.25


def test_risk_free_rate_curve_prefers_local_store(monkeypatch) -> None:
    store = _store()
    store.upsert_treasury_yields(
        [
            HistoricalTreasuryYield(
                trade_date=date(2025, 4, 1),
                yield_3_month=Decimal("0.041"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalTreasuryYield(
                trade_date=date(2025, 4, 2),
                yield_3_month=Decimal("0.042"),
                source_file_date=date(2025, 4, 2),
            ),
        ]
    )
    monkeypatch.setattr("backtestforecast.services.risk_free_rate._historical_store", lambda: store)
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type=StrategyType.LONG_CALL,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 2),
        target_dte=7,
        dte_tolerance_days=3,
        max_holding_days=5,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
    )
    curve = build_backtest_risk_free_rate_curve(request, default_rate=0.05, client=None)
    assert curve.rate_for(date(2025, 4, 1)) == 0.041
    assert curve.rate_for(date(2025, 4, 2)) == 0.042


def test_market_data_service_fetches_local_bars_first() -> None:
    store = _store()
    start = date(2025, 1, 2)
    end = date(2025, 1, 6)
    current = start
    while current <= end:
        if current.weekday() < 5:
            store.upsert_underlying_day_bars(
                [
                    HistoricalUnderlyingDayBar(
                        symbol="AAPL",
                        trade_date=current,
                        open_price=Decimal("100"),
                        high_price=Decimal("101"),
                        low_price=Decimal("99"),
                        close_price=Decimal("100"),
                        volume=Decimal("1000"),
                        source_file_date=current,
                    )
                ]
            )
        current += timedelta(days=1)

    service = MarketDataService(_UnusedClient())
    service._historical_store = store
    bars = service._fetch_bars_coalesced("AAPL", start, end)
    assert len(bars) == 3
    assert [bar.trade_date for bar in bars] == [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)]


def test_historical_store_batches_contracts_for_multiple_expirations() -> None:
    store = _store()
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418C00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250425C00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 25),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.50"),
                high_price=Decimal("5.90"),
                low_price=Decimal("5.10"),
                close_price=Decimal("5.70"),
                volume=Decimal("12"),
                source_file_date=date(2025, 4, 1),
            ),
        ]
    )

    contracts_by_expiration = store.list_option_contracts_for_expirations(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_dates=[date(2025, 4, 18), date(2025, 4, 25)],
    )

    assert [contract.ticker for contract in contracts_by_expiration[date(2025, 4, 18)]] == [
        "O:AAPL250418C00190000"
    ]
    assert [contract.ticker for contract in contracts_by_expiration[date(2025, 4, 25)]] == [
        "O:AAPL250425C00190000"
    ]
    assert contracts_by_expiration[date(2025, 4, 18)][0].as_of_mid_price == 5.25
    assert contracts_by_expiration[date(2025, 4, 25)][0].as_of_mid_price == 5.7


def test_historical_store_batches_contracts_for_multiple_expirations_by_type() -> None:
    store = _store()
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418C00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418P00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="put",
                strike_price=Decimal("190"),
                open_price=Decimal("4.10"),
                high_price=Decimal("4.40"),
                low_price=Decimal("3.80"),
                close_price=Decimal("4.25"),
                volume=Decimal("11"),
                source_file_date=date(2025, 4, 1),
            ),
        ]
    )

    contracts_by_type = store.list_option_contracts_for_expirations_by_type(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_types=["call", "put"],
        expiration_dates=[date(2025, 4, 18), date(2025, 4, 25)],
    )

    assert [contract.ticker for contract in contracts_by_type["call"][date(2025, 4, 18)]] == [
        "O:AAPL250418C00190000"
    ]
    assert [contract.ticker for contract in contracts_by_type["put"][date(2025, 4, 18)]] == [
        "O:AAPL250418P00190000"
    ]
    assert contracts_by_type["call"][date(2025, 4, 25)] == []
    assert contracts_by_type["put"][date(2025, 4, 25)] == []


def test_historical_store_batches_quotes_for_same_trade_date() -> None:
    store = _store()
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418C00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418P00190000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="put",
                strike_price=Decimal("190"),
                open_price=Decimal("4.10"),
                high_price=Decimal("4.40"),
                low_price=Decimal("3.80"),
                close_price=Decimal("4.25"),
                volume=Decimal("11"),
                source_file_date=date(2025, 4, 1),
            ),
        ]
    )

    quotes = store.get_option_quotes_for_date(
        ["O:AAPL250418C00190000", "O:AAPL250418P00190000", "O:MISSING"],
        date(2025, 4, 1),
    )

    assert quotes["O:AAPL250418C00190000"] is not None
    assert quotes["O:AAPL250418C00190000"].mid_price == 5.25
    assert quotes["O:AAPL250418P00190000"] is not None
    assert quotes["O:AAPL250418P00190000"].mid_price == 4.25
    assert quotes["O:MISSING"] is None


def test_related_root_symbol_lookup_only_returns_numeric_successors() -> None:
    store = _store()
    trade_date = date(2025, 4, 1)
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:SPY250418C00500000",
                underlying_symbol="SPY",
                trade_date=trade_date,
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("500"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=trade_date,
            ),
            HistoricalOptionDayBar(
                option_ticker="O:SPY1250418C00500000",
                underlying_symbol="SPY1",
                trade_date=trade_date,
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("500"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=trade_date,
            ),
            HistoricalOptionDayBar(
                option_ticker="O:SPYV250418C00500000",
                underlying_symbol="SPYV",
                trade_date=trade_date,
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("500"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=trade_date,
            ),
        ]
    )

    with store._session(readonly=True) as session:
        related = store._list_related_root_symbols(session, "SPY", on_date=trade_date)

    assert related == ["SPY", "SPY1"]


def test_related_root_symbol_cache_invalidates_after_option_upsert() -> None:
    store = _store()
    trade_date = date(2025, 4, 1)
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:SPY250418C00500000",
                underlying_symbol="SPY",
                trade_date=trade_date,
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("500"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.40"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=trade_date,
            ),
        ]
    )

    with store._session(readonly=True) as session:
        related = store._get_related_root_symbols(session, "SPY", on_date=trade_date)

    assert related == ["SPY"]

    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:SPY1250418C00505000",
                underlying_symbol="SPY1",
                trade_date=trade_date,
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("505"),
                open_price=Decimal("4.80"),
                high_price=Decimal("5.10"),
                low_price=Decimal("4.40"),
                close_price=Decimal("4.95"),
                volume=Decimal("12"),
                source_file_date=trade_date,
            ),
        ]
    )

    with store._session(readonly=True) as session:
        refreshed = store._get_related_root_symbols(session, "SPY", on_date=trade_date)

    assert refreshed == ["SPY", "SPY1"]


def test_deliverable_cache_invalidates_after_underlying_upsert() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 19),
                open_price=Decimal("7.80"),
                high_price=Decimal("8.20"),
                low_price=Decimal("7.60"),
                close_price=Decimal("8.00"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 19),
            ),
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 20),
                open_price=Decimal("31.50"),
                high_price=Decimal("32.50"),
                low_price=Decimal("31.00"),
                close_price=Decimal("32.00"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:UVXY1150619C00007000",
                underlying_symbol="UVXY1",
                trade_date=date(2015, 5, 20),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("7"),
                open_price=Decimal("1.10"),
                high_price=Decimal("1.40"),
                low_price=Decimal("0.90"),
                close_price=Decimal("1.20"),
                volume=Decimal("15"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )

    first_contracts = store.list_option_contracts_for_expiration(
        symbol="UVXY",
        as_of_date=date(2015, 5, 20),
        contract_type="call",
        expiration_date=date(2015, 6, 19),
    )

    assert first_contracts[0].shares_per_contract == 25.0

    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 20),
                open_price=Decimal("15.50"),
                high_price=Decimal("16.50"),
                low_price=Decimal("15.10"),
                close_price=Decimal("16.00"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )

    refreshed_contracts = store.list_option_contracts_for_expiration(
        symbol="UVXY",
        as_of_date=date(2015, 5, 20),
        contract_type="call",
        expiration_date=date(2015, 6, 19),
    )

    assert refreshed_contracts[0].shares_per_contract == 50.0


def test_historical_store_falls_back_to_adjusted_root_when_standard_contracts_are_missing() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 19),
                open_price=Decimal("8.10"),
                high_price=Decimal("8.40"),
                low_price=Decimal("7.90"),
                close_price=Decimal("8.23"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 19),
            ),
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 20),
                open_price=Decimal("40.50"),
                high_price=Decimal("41.80"),
                low_price=Decimal("39.90"),
                close_price=Decimal("41.29"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:UVXY1150619C00007000",
                underlying_symbol="UVXY1",
                trade_date=date(2015, 5, 20),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("7"),
                open_price=Decimal("1.10"),
                high_price=Decimal("1.40"),
                low_price=Decimal("0.90"),
                close_price=Decimal("1.20"),
                volume=Decimal("15"),
                source_file_date=date(2015, 5, 20),
            )
        ]
    )

    contracts = store.list_option_contracts_for_expiration(
        symbol="UVXY",
        as_of_date=date(2015, 5, 20),
        contract_type="call",
        expiration_date=date(2015, 6, 19),
    )

    assert [contract.ticker for contract in contracts] == ["O:UVXY1150619C00007000"]
    assert contracts[0].underlying_symbol == "UVXY1"
    assert contracts[0].shares_per_contract == 20.0


def test_historical_store_prefers_standard_root_for_new_entry_contracts() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 21),
                open_price=Decimal("39.10"),
                high_price=Decimal("40.20"),
                low_price=Decimal("38.70"),
                close_price=Decimal("38.99"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 21),
            )
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:UVXY150619C00040000",
                underlying_symbol="UVXY",
                trade_date=date(2015, 5, 21),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("40"),
                open_price=Decimal("4.80"),
                high_price=Decimal("5.10"),
                low_price=Decimal("4.50"),
                close_price=Decimal("4.90"),
                volume=Decimal("20"),
                source_file_date=date(2015, 5, 21),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:UVXY1150619C00010000",
                underlying_symbol="UVXY1",
                trade_date=date(2015, 5, 21),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("10"),
                open_price=Decimal("0.90"),
                high_price=Decimal("1.10"),
                low_price=Decimal("0.70"),
                close_price=Decimal("0.95"),
                volume=Decimal("20"),
                source_file_date=date(2015, 5, 21),
            ),
        ]
    )

    contracts = store.list_option_contracts_for_expiration(
        symbol="UVXY",
        as_of_date=date(2015, 5, 21),
        contract_type="call",
        expiration_date=date(2015, 6, 19),
    )

    assert [contract.underlying_symbol for contract in contracts] == ["UVXY"]
    assert [contract.ticker for contract in contracts] == ["O:UVXY150619C00040000"]


def test_historical_store_quote_series_switches_to_successor_root_during_split() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 19),
                open_price=Decimal("8.10"),
                high_price=Decimal("8.40"),
                low_price=Decimal("7.90"),
                close_price=Decimal("8.23"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 19),
            ),
            HistoricalUnderlyingDayBar(
                symbol="UVXY",
                trade_date=date(2015, 5, 20),
                open_price=Decimal("40.50"),
                high_price=Decimal("41.80"),
                low_price=Decimal("39.90"),
                close_price=Decimal("41.29"),
                volume=Decimal("1000"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:UVXY150619C00010000",
                underlying_symbol="UVXY",
                trade_date=date(2015, 5, 19),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("10"),
                open_price=Decimal("1.20"),
                high_price=Decimal("1.40"),
                low_price=Decimal("1.10"),
                close_price=Decimal("1.30"),
                volume=Decimal("10"),
                source_file_date=date(2015, 5, 19),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:UVXY150619C00010000",
                underlying_symbol="UVXY",
                trade_date=date(2015, 5, 20),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("10"),
                open_price=Decimal("4.90"),
                high_price=Decimal("5.30"),
                low_price=Decimal("4.60"),
                close_price=Decimal("5.10"),
                volume=Decimal("10"),
                source_file_date=date(2015, 5, 20),
            ),
            HistoricalOptionDayBar(
                option_ticker="O:UVXY1150619C00010000",
                underlying_symbol="UVXY1",
                trade_date=date(2015, 5, 20),
                expiration_date=date(2015, 6, 19),
                contract_type="call",
                strike_price=Decimal("10"),
                open_price=Decimal("0.80"),
                high_price=Decimal("1.00"),
                low_price=Decimal("0.70"),
                close_price=Decimal("0.90"),
                volume=Decimal("10"),
                source_file_date=date(2015, 5, 20),
            ),
        ]
    )

    series = store.get_option_quote_series(
        ["O:UVXY150619C00010000"],
        start_date=date(2015, 5, 19),
        end_date=date(2015, 5, 20),
    )

    assert series["O:UVXY150619C00010000"][date(2015, 5, 19)].source_option_ticker == "O:UVXY150619C00010000"
    assert series["O:UVXY150619C00010000"][date(2015, 5, 19)].deliverable_shares_per_contract == 100.0
    assert series["O:UVXY150619C00010000"][date(2015, 5, 20)].source_option_ticker == "O:UVXY1150619C00010000"
    assert series["O:UVXY150619C00010000"][date(2015, 5, 20)].deliverable_shares_per_contract == 20.0
    assert series["O:UVXY150619C00010000"][date(2015, 5, 20)].mid_price == 0.9


def test_historical_store_quote_series_uses_exact_ticker_fast_path(
    monkeypatch,
) -> None:
    store = _store()
    option_ticker = "O:AAPL250418C00190000"
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker=option_ticker,
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.10"),
                high_price=Decimal("5.50"),
                low_price=Decimal("4.80"),
                close_price=Decimal("5.25"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalOptionDayBar(
                option_ticker=option_ticker,
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 2),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.30"),
                high_price=Decimal("5.70"),
                low_price=Decimal("5.00"),
                close_price=Decimal("5.45"),
                volume=Decimal("11"),
                source_file_date=date(2025, 4, 2),
            ),
            HistoricalOptionDayBar(
                option_ticker=option_ticker,
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 3),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("190"),
                open_price=Decimal("5.40"),
                high_price=Decimal("5.80"),
                low_price=Decimal("5.10"),
                close_price=Decimal("5.60"),
                volume=Decimal("12"),
                source_file_date=date(2025, 4, 3),
            ),
        ]
    )

    def _unexpected_related_root_lookup(*args, **kwargs):
        raise AssertionError("exact quote-series lookup should not need related-root discovery")

    monkeypatch.setattr(
        HistoricalMarketDataStore,
        "_get_related_root_symbols",
        _unexpected_related_root_lookup,
    )

    series = store.get_option_quote_series(
        [option_ticker],
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
    )

    assert series[option_ticker][date(2025, 4, 1)].mid_price == 5.25
    assert series[option_ticker][date(2025, 4, 3)].mid_price == 5.6


def test_historical_store_pinned_readonly_session_reuses_single_session() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    base_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    readonly_session_creations = 0

    def counting_factory() -> Session:
        nonlocal readonly_session_creations
        readonly_session_creations += 1
        return base_factory()

    store = HistoricalMarketDataStore(counting_factory, counting_factory)
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="AAPL",
                trade_date=date(2025, 4, 1),
                open_price=Decimal("100"),
                high_price=Decimal("101"),
                low_price=Decimal("99"),
                close_price=Decimal("100"),
                volume=Decimal("1000"),
                source_file_date=date(2025, 4, 1),
            )
        ]
    )

    readonly_session_creations = 0
    store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    assert readonly_session_creations == 2

    readonly_session_creations = 0
    with store.pinned_readonly_session():
        with store.pinned_readonly_session():
            bars = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
        repeated_bars = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    assert readonly_session_creations == 1
    assert len(bars) == 1
    assert len(repeated_bars) == 1


def test_ex_dividend_upsert_preserves_multiple_provider_records_for_same_day() -> None:
    store = _store()
    assert store.upsert_ex_dividend_dates(
        [
            HistoricalExDividendDate(
                symbol="F",
                ex_dividend_date=date(2016, 1, 27),
                provider_dividend_id="div-recurring",
                cash_amount=Decimal("0.15"),
                distribution_type="recurring",
                source_file_date=date(2016, 1, 27),
            ),
            HistoricalExDividendDate(
                symbol="F",
                ex_dividend_date=date(2016, 1, 27),
                provider_dividend_id="div-supplemental",
                cash_amount=Decimal("0.25"),
                distribution_type="supplemental",
                source_file_date=date(2016, 1, 27),
            ),
        ]
    ) == 2

    assert store.list_ex_dividend_dates("F", date(2016, 1, 1), date(2016, 12, 31)) == {date(2016, 1, 27)}


def test_list_imported_symbols_for_window_unifies_underlying_and_option_sources() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="AAPL",
                trade_date=date(2025, 4, 1),
                open_price=Decimal("100"),
                high_price=Decimal("101"),
                low_price=Decimal("99"),
                close_price=Decimal("100"),
                volume=Decimal("1000"),
                source_file_date=date(2025, 4, 1),
            )
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:MSFT250418C00200000",
                underlying_symbol="MSFT",
                trade_date=date(2025, 4, 1),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("200"),
                open_price=Decimal("5"),
                high_price=Decimal("6"),
                low_price=Decimal("4"),
                close_price=Decimal("5.5"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 1),
            )
        ]
    )

    assert store.list_imported_symbols_for_window(date(2025, 4, 1), date(2025, 4, 30)) == {"AAPL", "MSFT"}


def test_earnings_event_upsert_and_lookup_preserves_multiple_types_for_same_day() -> None:
    store = _store()
    assert store.upsert_earnings_events(
        [
            HistoricalEarningsEvent(
                symbol="F",
                event_date=date(2016, 1, 27),
                event_type="earnings_announcement_date",
                provider_event_id="earn-announcement",
                source_file_date=date(2016, 1, 27),
            ),
            HistoricalEarningsEvent(
                symbol="F",
                event_date=date(2016, 1, 27),
                event_type="earnings_conference_call",
                provider_event_id="earn-call",
                source_file_date=date(2016, 1, 27),
            ),
        ]
    ) == 2

    assert store.list_earnings_event_dates("F", date(2016, 1, 1), date(2016, 12, 31)) == {date(2016, 1, 27)}


def test_freshness_summary_exposes_row_estimate_field() -> None:
    store = _store()
    summary = store.get_freshness_summary()
    assert "row_estimate" in summary["underlying_day_bars"]
    assert summary["underlying_day_bars"]["row_estimate"] is None


def test_get_freshness_summary_reports_latest_dates_without_table_scan_helpers() -> None:
    store = _store()
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="AAPL",
                trade_date=date(2025, 4, 2),
                open_price=Decimal("100"),
                high_price=Decimal("101"),
                low_price=Decimal("99"),
                close_price=Decimal("100"),
                volume=Decimal("1000"),
                source_file_date=date(2025, 4, 2),
            )
        ]
    )
    store.upsert_option_day_bars(
        [
            HistoricalOptionDayBar(
                option_ticker="O:AAPL250418C00100000",
                underlying_symbol="AAPL",
                trade_date=date(2025, 4, 3),
                expiration_date=date(2025, 4, 18),
                contract_type="call",
                strike_price=Decimal("100"),
                open_price=Decimal("4"),
                high_price=Decimal("5"),
                low_price=Decimal("3"),
                close_price=Decimal("4.5"),
                volume=Decimal("10"),
                source_file_date=date(2025, 4, 3),
            )
        ]
    )
    store.upsert_ex_dividend_dates(
        [
            HistoricalExDividendDate(
                symbol="AAPL",
                ex_dividend_date=date(2025, 4, 4),
                provider_dividend_id="div-1",
                source_file_date=date(2025, 4, 4),
            )
        ]
    )
    store.upsert_earnings_events(
        [
            HistoricalEarningsEvent(
                symbol="AAPL",
                event_date=date(2025, 4, 4),
                event_type="earnings_announcement_date",
                provider_event_id="earn-1",
                source_file_date=date(2025, 4, 4),
            )
        ]
    )
    store.upsert_treasury_yields(
        [
            HistoricalTreasuryYield(
                trade_date=date(2025, 4, 5),
                yield_3_month=Decimal("0.041"),
                source_file_date=date(2025, 4, 5),
            )
        ]
    )

    summary = store.get_freshness_summary()

    assert summary["underlying_day_bars"]["latest_date"] == "2025-04-02"
    assert summary["option_day_bars"]["latest_date"] == "2025-04-03"
    assert summary["ex_dividend_dates"]["latest_date"] == "2025-04-04"
    assert summary["earnings_events"]["latest_date"] == "2025-04-04"
    assert summary["treasury_yields"]["latest_date"] == "2025-04-05"


def test_streaming_stock_payloads_can_be_chunked_and_upserted() -> None:
    store = _store()
    rows = [
        {"ticker": "AAPL", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
        {"ticker": "MSFT", "open": "200", "high": "201", "low": "199", "close": "200", "volume": "2000"},
    ]

    payload_iter = iter_stock_day_bar_payloads(rows, date(2025, 4, 1))
    batches = list(chunked(payload_iter, 1))

    assert len(batches) == 2
    assert store.upsert_underlying_day_bar_payloads(batches[0]) == 1
    assert store.upsert_underlying_day_bar_payloads(batches[1]) == 1

    aapl = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    msft = store.get_underlying_day_bars("MSFT", date(2025, 4, 1), date(2025, 4, 1))
    assert len(aapl) == 1
    assert len(msft) == 1
    assert aapl[0].close_price == 100.0
    assert msft[0].close_price == 200.0


def test_streaming_stock_payloads_dedupe_duplicate_symbol_trade_date_rows() -> None:
    store = _store()
    rows = [
        {"ticker": "AAPL", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
        {"ticker": "AAPL", "open": "110", "high": "111", "low": "109", "close": "110", "volume": "2000"},
    ]

    payloads = list(iter_stock_day_bar_payloads(rows, date(2025, 4, 1)))
    assert store.upsert_underlying_day_bar_payloads(payloads) == 1

    aapl = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    assert len(aapl) == 1
    assert aapl[0].open_price == 110.0
    assert aapl[0].close_price == 110.0
    assert aapl[0].volume == 2000.0


def test_streaming_option_payloads_can_be_upserted() -> None:
    store = _store()
    rows = [
        {
            "ticker": "O:AAPL250418C00190000",
            "open": "5.10",
            "high": "5.40",
            "low": "4.80",
            "close": "5.25",
            "volume": "10",
        }
    ]

    payloads = list(iter_option_day_bar_payloads(rows, date(2025, 4, 1)))
    assert store.upsert_option_day_bar_payloads(payloads) == 1

    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None
    assert quote.mid_price == 5.25


def test_streaming_option_records_can_be_upserted() -> None:
    store = _store()
    rows = [
        {
            "ticker": "O:AAPL250418C00190000",
            "open": "5.10",
            "high": "5.40",
            "low": "4.80",
            "close": "5.25",
            "volume": "10",
        }
    ]

    records = list(iter_option_day_bar_records(rows, date(2025, 4, 1)))
    assert store.upsert_option_day_bar_records(records) == 1

    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None
    assert quote.mid_price == 5.25


def test_postgres_bulk_batch_size_stays_under_bind_limit_for_option_payloads() -> None:
    rows = [
        {
            "id": "ignored",
            "option_ticker": "O:AAPL250418C00190000",
            "underlying_symbol": "AAPL",
            "trade_date": date(2025, 4, 1),
            "expiration_date": date(2025, 4, 18),
            "contract_type": "call",
            "strike_price": Decimal("190"),
            "open_price": Decimal("5.10"),
            "high_price": Decimal("5.40"),
            "low_price": Decimal("4.80"),
            "close_price": Decimal("5.25"),
            "volume": Decimal("10"),
            "source_dataset": "massive",
            "source_file_date": date(2025, 4, 1),
        }
    ]

    batch_size = HistoricalMarketDataStore._postgres_bulk_batch_size(HistoricalOptionDayBar.__table__, rows)
    assert batch_size < 5000
    assert batch_size * len(rows[0]) <= 65000


def test_option_payloads_use_postgres_copy_fast_path_when_available(monkeypatch) -> None:
    fake_session = MagicMock()
    fake_session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    store = HistoricalMarketDataStore(lambda: fake_session, lambda: fake_session)

    rows = [
        {
            "id": "ignored",
            "option_ticker": "O:AAPL250418C00190000",
            "underlying_symbol": "AAPL",
            "trade_date": date(2025, 4, 1),
            "expiration_date": date(2025, 4, 18),
            "contract_type": "call",
            "strike_price": Decimal("190"),
            "open_price": Decimal("5.10"),
            "high_price": Decimal("5.40"),
            "low_price": Decimal("4.80"),
            "close_price": Decimal("5.25"),
            "volume": Decimal("10"),
            "source_dataset": "flatfile_day_aggs",
            "source_file_date": date(2025, 4, 1),
        }
    ]

    calls = {"copy": 0, "insert": 0}

    monkeypatch.setattr(
        HistoricalMarketDataStore,
        "_can_use_postgres_copy_fast_path",
        classmethod(lambda cls, session, model: model is HistoricalOptionDayBar),
    )

    def _fake_copy(self, session, payloads, model, key_fields):
        calls["copy"] += 1
        assert session is fake_session
        assert payloads == rows
        assert model is HistoricalOptionDayBar
        assert key_fields == ("option_ticker", "trade_date")

    def _fake_insert(self, session, payloads, model, key_fields):
        calls["insert"] += 1

    monkeypatch.setattr(HistoricalMarketDataStore, "_bulk_upsert_postgres_copy", _fake_copy)
    monkeypatch.setattr(HistoricalMarketDataStore, "_bulk_upsert_postgres_insert", _fake_insert)

    assert store.upsert_option_day_bar_payloads(rows) == 1
    assert calls == {"copy": 1, "insert": 0}
    fake_session.commit.assert_called_once()
