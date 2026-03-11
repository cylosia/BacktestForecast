from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord


@dataclass
class FakeGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        return self.contracts.get((entry_date, contract_type), [])

    def select_contract(
        self,
        entry_date: date,
        strategy_type: str,
        underlying_close: float,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> OptionContractRecord:
        contract_type = "call" if strategy_type in {"long_call", "covered_call"} else "put"
        contracts = self.list_contracts(entry_date, contract_type, target_dte, dte_tolerance_days)
        if not contracts:
            raise DataUnavailableError("No contracts available in FakeGateway")
        return contracts[0]

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return self.quotes.get((option_ticker, trade_date))


def make_bar(trade_date: date, close_price: float, volume: float = 1_000_000) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        volume=volume,
    )


def make_quote(trade_date: date, mid: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None)


def test_bull_call_debit_spread_realizes_expected_profit() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 99),
        make_bar(date(2025, 1, 2), 100),
        make_bar(date(2025, 1, 3), 102),
        make_bar(date(2025, 1, 4), 104),
        make_bar(date(2025, 1, 5), 108),
    ]
    contracts = {
        (date(2025, 1, 2), "call"): [
            OptionContractRecord("C100", "call", date(2025, 1, 5), 100, 100),
            OptionContractRecord("C105", "call", date(2025, 1, 5), 105, 100),
        ]
    }
    quotes = {
        ("C100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 3.0),
        ("C105", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 1.0),
    }
    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="bull_call_debit_spread",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 3),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=10,
            account_size=10_000,
            risk_per_trade_pct=2,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert round(trade.gross_pnl, 2) == 300.0
    assert round(trade.net_pnl, 2) == 300.0
    assert trade.detail_json["max_profit_per_unit"] == 300.0
    assert trade.detail_json["actual_units"] == 1


def test_calendar_spread_exits_on_near_leg_expiration() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(date(2025, 2, 2), 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(date(2025, 2, 4), 100),
        make_bar(date(2025, 2, 5), 100),
    ]
    contracts = {
        (date(2025, 2, 2), "call"): [
            OptionContractRecord("NEAR100", "call", date(2025, 2, 4), 100, 100),
            OptionContractRecord("FAR100", "call", date(2025, 2, 18), 100, 100),
        ]
    }
    quotes = {
        ("NEAR100", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 1.0),
        ("FAR100", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 4.0),
        ("FAR100", date(2025, 2, 4)): make_quote(date(2025, 2, 4), 3.5),
    }
    result = engine.run(
        BacktestConfig(
            symbol="SPY",
            strategy_type="calendar_spread",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 3),
            target_dte=2,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=3,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.exit_date == date(2025, 2, 4)
    assert round(trade.net_pnl, 2) == 50.0
    assert trade.detail_json["legs"][0]["ticker"] == "FAR100"


def test_wheel_records_assignment_callaway_and_stock_exit() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 3, 1), 101),
        make_bar(date(2025, 3, 2), 100),
        make_bar(date(2025, 3, 3), 95),
        make_bar(date(2025, 3, 4), 105),
    ]
    contracts = {
        (date(2025, 3, 2), "put"): [OptionContractRecord("P100", "put", date(2025, 3, 3), 100, 100)],
        (date(2025, 3, 3), "call"): [OptionContractRecord("C100", "call", date(2025, 3, 4), 100, 100)],
    }
    quotes = {
        ("P100", date(2025, 3, 2)): make_quote(date(2025, 3, 2), 2.0),
        ("C100", date(2025, 3, 3)): make_quote(date(2025, 3, 3), 1.0),
    }
    result = engine.run(
        BacktestConfig(
            symbol="AMD",
            strategy_type="wheel_strategy",
            start_date=date(2025, 3, 1),
            end_date=date(2025, 3, 4),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=20_000,
            risk_per_trade_pct=100,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 3
    phases = [trade.detail_json.get("phase") for trade in result.trades]
    assert phases == ["cash_secured_put", "covered_call", "stock_inventory"]
    assert result.trades[0].exit_reason == "assignment"
    assert result.trades[1].exit_reason == "call_assignment"
    assert result.trades[2].exit_reason == "called_away"
    assert round(result.summary.total_net_pnl, 2) == -400.0
