from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import pytest

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord


@dataclass
class AssignmentGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]
    ex_dividend_dates: set[date] = field(default_factory=set)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        return self.contracts.get((entry_date, contract_type), [])

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return self.quotes.get((option_ticker, trade_date))

    def get_chain_delta_lookup(self, contracts):
        return {}

    def get_ex_dividend_dates(self, start_date: date, end_date: date) -> set[date]:
        return {d for d in self.ex_dividend_dates if start_date <= d <= end_date}


def make_bar(trade_date: date, close_price: float) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        volume=1_000_000,
    )


def make_quote(trade_date: date, mid: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None)


def make_config(
    strategy_type: str,
    *,
    account_size: str = "10000",
    risk_per_trade_pct: str = "5",
) -> BacktestConfig:
    return BacktestConfig(
        symbol="AAPL",
        strategy_type=strategy_type,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 10),
        target_dte=7,
        dte_tolerance_days=7,
        max_holding_days=30,
        account_size=Decimal(account_size),
        risk_per_trade_pct=Decimal(risk_per_trade_pct),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
    )


def test_short_call_is_assigned_before_ex_dividend_date() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 112.0),
        make_bar(date(2025, 1, 3), 112.0),
    ]
    contracts = {
        (date(2025, 1, 1), "call"): [
            OptionContractRecord("NEAR100", "call", date(2025, 1, 10), 100.0, 100),
            OptionContractRecord("FAR100", "call", date(2025, 2, 7), 100.0, 100),
        ],
    }
    quotes = {
        ("NEAR100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 2.4),
        ("FAR100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 4.0),
        ("NEAR100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 12.05),
        ("FAR100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 12.6),
    }

    result = engine.run(
        config=make_config("calendar_spread", account_size="200", risk_per_trade_pct="100"),
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates={date(2025, 1, 3)},
        option_gateway=AssignmentGateway(contracts=contracts, quotes=quotes),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.quantity == 1
    assert trade.exit_date == date(2025, 1, 2)
    assert trade.exit_reason == "early_assignment_call_ex_div"
    assert trade.gross_pnl == Decimal("-100.0")
    assert trade.detail_json["assignment"] is True
    assert trade.detail_json["assignment_detail"]["assignment_trigger"] == "ex_dividend"
    assert trade.detail_json["assignment_detail"]["settlement_price"] == 12.0
    assert trade.detail_json["legs"][1]["exit_mid"] == 12.0
    assert any(w["code"] == "ex_dividend" for w in result.warnings)
    assert trade.warnings


def test_naked_call_is_assigned_before_ex_dividend_date() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 112.0),
        make_bar(date(2025, 1, 3), 112.0),
    ]
    contracts = {
        (date(2025, 1, 1), "call"): [
            OptionContractRecord("CALL100", "call", date(2025, 1, 10), 100.0, 100),
        ],
    }
    quotes = {
        ("CALL100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 2.4),
        ("CALL100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 12.05),
    }

    result = engine.run(
        config=make_config("naked_call", account_size="3000", risk_per_trade_pct="100"),
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates={date(2025, 1, 3)},
        option_gateway=AssignmentGateway(contracts=contracts, quotes=quotes),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.quantity == 1
    assert trade.exit_reason == "early_assignment_call_ex_div"
    assert trade.exit_date == date(2025, 1, 2)
    assert trade.detail_json["assignment_detail"]["assignment_trigger"] == "ex_dividend"


def test_deep_itm_short_put_is_assigned_near_expiry() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 90.0),
        make_bar(date(2025, 1, 3), 90.0),
    ]
    contracts = {
        (date(2025, 1, 1), "put"): [
            OptionContractRecord("P100", "put", date(2025, 1, 4), 100.0, 100),
        ],
    }
    quotes = {
        ("P100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 1.2),
        ("P100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 10.05),
    }

    result = engine.run(
        config=make_config("naked_put", account_size="10000", risk_per_trade_pct="100"),
        bars=bars,
        earnings_dates=set(),
        option_gateway=AssignmentGateway(contracts=contracts, quotes=quotes),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.quantity == 1
    assert trade.exit_date == date(2025, 1, 2)
    assert trade.exit_reason == "early_assignment_put_deep_itm"
    assert trade.gross_pnl == Decimal("-880")
    assert trade.detail_json["assignment_detail"]["days_to_expiration"] == 2
    assert trade.detail_json["assignment_detail"]["time_value"] == pytest.approx(0.05)
    assert any(w["code"] == "deep_itm_put" for w in result.warnings)


def test_assignment_only_marks_the_short_leg_in_mixed_position() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 112.0),
        make_bar(date(2025, 1, 3), 112.0),
    ]
    contracts = {
        (date(2025, 1, 1), "call"): [
            OptionContractRecord("NEAR100", "call", date(2025, 1, 10), 100.0, 100),
            OptionContractRecord("FAR100", "call", date(2025, 2, 7), 100.0, 100),
        ],
    }
    quotes = {
        ("NEAR100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 2.4),
        ("FAR100", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 4.0),
        ("NEAR100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 12.05),
        ("FAR100", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 13.0),
    }

    result = engine.run(
        config=make_config("calendar_spread", account_size="200", risk_per_trade_pct="100"),
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates={date(2025, 1, 3)},
        option_gateway=AssignmentGateway(contracts=contracts, quotes=quotes),
    )

    trade = result.trades[0]
    assert trade.detail_json["legs"][0]["ticker"] == "FAR100"
    assert trade.detail_json["legs"][0]["exit_mid"] == 13.0
    assert trade.detail_json["legs"][1]["ticker"] == "NEAR100"
    assert trade.detail_json["legs"][1]["exit_mid"] == 12.0
    assert trade.detail_json["exit_package_market_value"] == 100.0


def test_intrinsic_value_uses_deliverable_shares_for_nonstandard_contracts() -> None:
    engine = OptionsBacktestEngine()

    intrinsic = engine._intrinsic_value(
        "call",
        7.0,
        40.0,
        deliverable_shares_per_contract=20.0,
    )

    assert intrinsic == Decimal("1")


def test_nonstandard_short_call_skips_early_assignment_heuristic() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 112.0),
        make_bar(date(2025, 1, 3), 112.0),
    ]
    contracts = {
        (date(2025, 1, 1), "call"): [
            OptionContractRecord("CALL100", "call", date(2025, 1, 10), 100.0, 100),
        ],
    }
    quotes = {
        (
            "CALL100",
            date(2025, 1, 1),
        ): OptionQuoteRecord(
            trade_date=date(2025, 1, 1),
            bid_price=2.4,
            ask_price=2.4,
            participant_timestamp=None,
            source_option_ticker="O:UVXY1250110C00100000",
            deliverable_shares_per_contract=20.0,
        ),
        (
            "CALL100",
            date(2025, 1, 2),
        ): OptionQuoteRecord(
            trade_date=date(2025, 1, 2),
            bid_price=12.05,
            ask_price=12.05,
            participant_timestamp=None,
            source_option_ticker="O:UVXY1250110C00100000",
            deliverable_shares_per_contract=20.0,
        ),
    }

    config = BacktestConfig(
        symbol="UVXY",
        strategy_type="naked_call",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        target_dte=7,
        dte_tolerance_days=7,
        max_holding_days=30,
        account_size=Decimal("3000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
    )

    result = engine.run(
        config=config,
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates={date(2025, 1, 3)},
        option_gateway=AssignmentGateway(contracts=contracts, quotes=quotes),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "backtest_end"
    assert trade.detail_json["legs"][0]["is_nonstandard"] is True
    assert trade.detail_json["legs"][0]["deliverable_shares_per_contract"] == 20.0
    assert not any(w["code"] == "ex_dividend" for w in result.warnings)
