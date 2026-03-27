from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.integrations.massive_flatfiles import option_day_dataset, stock_day_dataset
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore, parse_option_ticker_metadata
from backtestforecast.market_data.service import MarketDataService
from backtestforecast.models import (
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
