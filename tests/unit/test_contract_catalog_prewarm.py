from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backtestforecast.market_data.prewarm import (
    collect_trade_dates,
    prewarm_long_option_bundle,
    resolve_long_option_contract_type,
)
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType


class _Gateway:
    def __init__(self) -> None:
        self.exact_calls: list[tuple] = []
        self.quote_calls: list[tuple[str, date]] = []

    def list_contracts_for_preferred_expiration(self, **kwargs):
        self.exact_calls.append(
            (
                kwargs["entry_date"],
                kwargs["contract_type"],
                kwargs["target_dte"],
                kwargs["dte_tolerance_days"],
                kwargs.get("strike_price_gte"),
                kwargs.get("strike_price_lte"),
            )
        )
        return [
            OptionContractRecord(
                ticker="O:AAPL250404C00200000",
                contract_type="call",
                expiration_date=date(2025, 4, 4),
                strike_price=200.0,
                shares_per_contract=100.0,
            )
        ]

    def get_quote(self, option_ticker: str, trade_date: date):
        self.quote_calls.append((option_ticker, trade_date))
        return OptionQuoteRecord(trade_date=trade_date, bid_price=1.0, ask_price=1.2, participant_timestamp=None)


def _request(strategy_type: StrategyType = StrategyType.LONG_CALL) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type=strategy_type,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
        target_dte=7,
        dte_tolerance_days=2,
        max_holding_days=7,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
        strategy_overrides={"long_call_strike": {"mode": "atm_offset_steps", "value": 0}},
    )


def test_collect_trade_dates_respects_max_dates():
    bars = [
        DailyBar(date(2025, 4, 1), 100, 101, 99, 100, 1_000),
        DailyBar(date(2025, 4, 2), 101, 102, 100, 101, 1_000),
        DailyBar(date(2025, 4, 3), 102, 103, 101, 102, 1_000),
    ]

    result = collect_trade_dates(bars, start_date=date(2025, 4, 1), end_date=date(2025, 4, 3), max_dates=2)

    assert [bar.trade_date for bar in result] == [date(2025, 4, 1), date(2025, 4, 2)]


def test_prewarm_long_option_bundle_uses_exact_lookup_and_quotes():
    gateway = _Gateway()
    request = _request()
    bars = [
        DailyBar(date(2025, 4, 1), 200, 201, 199, 200, 1_000_000),
        DailyBar(date(2025, 4, 2), 202, 203, 201, 202, 1_000_000),
    ]
    bundle = HistoricalDataBundle(bars=bars, earnings_dates=set(), ex_dividend_dates=set(), option_gateway=gateway)

    summary = prewarm_long_option_bundle(request, bundle=bundle, include_quotes=True)

    assert summary.dates_processed == 2
    assert summary.contracts_fetched == 2
    assert summary.quotes_fetched == 2
    assert gateway.exact_calls == [
        (date(2025, 4, 1), "call", 7, 2, 170.0, 230.0),
        (date(2025, 4, 2), "call", 7, 2, 171.7, 232.3),
    ]


def test_resolve_long_option_contract_type_rejects_other_strategies():
    with pytest.raises(ValueError, match="supports only long_call/long_put"):
        resolve_long_option_contract_type(StrategyType.COVERED_CALL)
