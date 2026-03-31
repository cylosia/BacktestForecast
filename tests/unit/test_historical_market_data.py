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
