from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import (
    AvoidEarningsRule,
    CreateBacktestRunRequest,
    CustomLegDefinition,
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
)


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

    def get_chain_delta_lookup(self, contracts):
        return {}


@dataclass
class FilteringGateway(FakeGateway):
    list_calls: list[tuple[date, str, int, int]] = field(default_factory=list)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        self.list_calls.append((entry_date, contract_type, target_dte, dte_tolerance_days))
        contracts = super().list_contracts(entry_date, contract_type, target_dte, dte_tolerance_days)
        return [
            contract for contract in contracts
            if abs((contract.expiration_date - entry_date).days - target_dte) <= dte_tolerance_days
        ]


@dataclass
class ExactCalendarGateway(FakeGateway):
    exact_calls: list[tuple[date, str, date]] = field(default_factory=list)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        raise AssertionError("calendar strategy should use exact-expiration lookups when available")

    def list_contracts_for_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        self.exact_calls.append((entry_date, contract_type, expiration_date))
        contracts = super().list_contracts(entry_date, contract_type, 0, 0)
        return [
            contract
            for contract in contracts
            if contract.expiration_date == expiration_date
            and (strike_price_gte is None or contract.strike_price >= strike_price_gte)
            and (strike_price_lte is None or contract.strike_price <= strike_price_lte)
        ]


@dataclass
class ExactCustomGateway(FakeGateway):
    exact_calls: list[tuple[date, str, date]] = field(default_factory=list)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        raise AssertionError("custom explicit expirations should use exact-expiration lookups when available")

    def list_contracts_for_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        self.exact_calls.append((entry_date, contract_type, expiration_date))
        contracts = super().list_contracts(entry_date, contract_type, 0, 0)
        return [
            contract
            for contract in contracts
            if contract.expiration_date == expiration_date
            and (strike_price_gte is None or contract.strike_price >= strike_price_gte)
            and (strike_price_lte is None or contract.strike_price <= strike_price_lte)
        ]

    def get_chain_delta_lookup(self, contracts):
        lookup = {}
        for contract in contracts:
            if contract.contract_type != "put":
                continue
            if contract.strike_price == 14:
                lookup[(contract.strike_price, contract.expiration_date)] = -0.20
            elif contract.strike_price == 15:
                lookup[(contract.strike_price, contract.expiration_date)] = -0.50
            elif contract.strike_price == 16:
                lookup[(contract.strike_price, contract.expiration_date)] = -0.80
        return lookup


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


def make_spread_quote(trade_date: date, bid: float, ask: float) -> OptionQuoteRecord:
    """Quote with explicit bid/ask spread - mid_price = (bid + ask) / 2."""
    return OptionQuoteRecord(trade_date=trade_date, bid_price=bid, ask_price=ask, participant_timestamp=None)


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
    assert round(float(trade.gross_pnl), 2) == 300.0
    assert round(float(trade.net_pnl), 2) == 300.0
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
    assert round(float(trade.net_pnl), 2) == 50.0
    assert trade.detail_json["legs"][0]["ticker"] == "FAR100"


def test_put_calendar_spread_uses_put_contracts_without_override() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(date(2025, 2, 2), 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(date(2025, 2, 4), 100),
        make_bar(date(2025, 2, 5), 100),
    ]
    contracts = {
        (date(2025, 2, 2), "put"): [
            OptionContractRecord("NEARP100", "put", date(2025, 2, 4), 100, 100),
            OptionContractRecord("FARP100", "put", date(2025, 2, 18), 100, 100),
        ]
    }
    quotes = {
        ("NEARP100", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 1.0),
        ("FARP100", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 4.0),
        ("FARP100", date(2025, 2, 4)): make_quote(date(2025, 2, 4), 3.5),
    }
    result = engine.run(
        BacktestConfig(
            symbol="SPY",
            strategy_type="put_calendar_spread",
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
    assert round(float(trade.net_pnl), 2) == 50.0
    assert trade.detail_json["legs"][0]["contract_type"] == "put"
    assert trade.detail_json["legs"][1]["contract_type"] == "put"


def test_put_calendar_spread_uses_shared_put_strike_override() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(date(2025, 2, 2), 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(date(2025, 2, 4), 100),
        make_bar(date(2025, 2, 5), 100),
    ]
    contracts = {
        (date(2025, 2, 2), "put"): [
            OptionContractRecord("NEARP100", "put", date(2025, 2, 4), 100, 100),
            OptionContractRecord("NEARP95", "put", date(2025, 2, 4), 95, 100),
            OptionContractRecord("NEARP90", "put", date(2025, 2, 4), 90, 100),
            OptionContractRecord("FARP100", "put", date(2025, 2, 18), 100, 100),
            OptionContractRecord("FARP95", "put", date(2025, 2, 18), 95, 100),
            OptionContractRecord("FARP90", "put", date(2025, 2, 18), 90, 100),
        ]
    }
    quotes = {
        ("NEARP90", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 1.0),
        ("FARP90", date(2025, 2, 2)): make_quote(date(2025, 2, 2), 4.0),
        ("FARP90", date(2025, 2, 4)): make_quote(date(2025, 2, 4), 3.5),
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
            strategy_overrides=StrategyOverrides(
                calendar_contract_type="put",
                short_put_strike=StrikeSelection(
                    mode=StrikeSelectionMode.ATM_OFFSET_STEPS,
                    value=2,
                ),
            ),
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.detail_json["legs"][0]["strike_price"] == 90
    assert trade.detail_json["legs"][1]["strike_price"] == 90
    assert trade.detail_json["legs"][0]["contract_type"] == "put"
    assert trade.detail_json["legs"][1]["contract_type"] == "put"


def test_calendar_spread_uses_one_day_far_leg_and_minimum_eight_day_tolerance() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 2, 2)
    near_expiration = date(2025, 2, 9)
    far_expiration = date(2025, 2, 10)
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(near_expiration, 100),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("NEAR100", "call", near_expiration, 100, 100),
            OptionContractRecord("FAR100", "call", far_expiration, 100, 100),
        ]
    }
    quotes = {
        ("NEAR100", entry_date): make_quote(entry_date, 1.0),
        ("FAR100", entry_date): make_quote(entry_date, 1.5),
        ("FAR100", near_expiration): make_quote(near_expiration, 1.2),
    }
    gateway = FilteringGateway(contracts=contracts, quotes=quotes)

    result = engine.run(
        BacktestConfig(
            symbol="F",
            strategy_type="calendar_spread",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 3),
            target_dte=7,
            dte_tolerance_days=0,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=3,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        gateway,
    )

    assert gateway.list_calls
    assert all(call[3] == 8 for call in gateway.list_calls)
    assert (entry_date, "call", 7, 8) in gateway.list_calls
    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.detail_json["legs"][0]["ticker"] == "FAR100"
    assert trade.detail_json["legs"][1]["ticker"] == "NEAR100"
    assert "at least 1 day farther out" in trade.detail_json["assumptions"][1]
    assert "8 DTE tolerance days" in trade.detail_json["assumptions"][1]


def test_calendar_spread_prefers_exact_expiration_fetch_when_gateway_supports_it() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 2, 2)
    near_expiration = date(2025, 2, 9)
    far_expiration = date(2025, 2, 10)
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(near_expiration, 100),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("NEAR100", "call", near_expiration, 100, 100),
            OptionContractRecord("FAR100", "call", far_expiration, 100, 100),
        ]
    }
    quotes = {
        ("NEAR100", entry_date): make_quote(entry_date, 1.0),
        ("FAR100", entry_date): make_quote(entry_date, 1.5),
        ("FAR100", near_expiration): make_quote(near_expiration, 1.2),
    }
    gateway = ExactCalendarGateway(contracts=contracts, quotes=quotes)

    result = engine.run(
        BacktestConfig(
            symbol="F",
            strategy_type="calendar_spread",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 3),
            target_dte=7,
            dte_tolerance_days=0,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=3,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        gateway,
    )

    assert result.summary.trade_count == 1
    assert (entry_date, "call", near_expiration) in gateway.exact_calls
    assert (entry_date, "call", far_expiration) in gateway.exact_calls


def test_calendar_spread_can_target_explicit_far_leg_dte() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 2, 2)
    near_expiration = date(2025, 2, 4)
    intermediate_expiration = date(2025, 2, 7)
    far_expiration = date(2025, 2, 12)
    bars = [
        make_bar(date(2025, 2, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 2, 3), 100),
        make_bar(near_expiration, 100),
    ]
    contracts = {
        (entry_date, "put"): [
            OptionContractRecord("NEARP100", "put", near_expiration, 100, 100),
            OptionContractRecord("MIDP100", "put", intermediate_expiration, 100, 100),
            OptionContractRecord("FARP100", "put", far_expiration, 100, 100),
        ]
    }
    quotes = {
        ("NEARP100", entry_date): make_quote(entry_date, 1.0),
        ("FARP100", entry_date): make_quote(entry_date, 2.4),
        ("FARP100", near_expiration): make_quote(near_expiration, 2.0),
    }
    gateway = ExactCalendarGateway(contracts=contracts, quotes=quotes)

    result = engine.run(
        BacktestConfig(
            symbol="F",
            strategy_type="calendar_spread",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 3),
            target_dte=2,
            dte_tolerance_days=0,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=3,
            commission_per_contract=0,
            entry_rules=[],
            strategy_overrides=StrategyOverrides(
                calendar_contract_type="put",
                calendar_far_leg_target_dte=10,
            ),
        ),
        bars,
        set(),
        gateway,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.detail_json["legs"][0]["ticker"] == "FARP100"
    assert trade.detail_json["legs"][1]["ticker"] == "NEARP100"
    assert (entry_date, "put", near_expiration) in gateway.exact_calls
    assert (entry_date, "put", far_expiration) in gateway.exact_calls
    assert (entry_date, "put", intermediate_expiration) not in gateway.exact_calls
    assert "calendar_far_leg_target_dte=10" in trade.detail_json["assumptions"][1]


def test_custom_2_leg_stock_only_uses_end_date() -> None:
    """Stock-only custom legs use config.end_date as scheduled_exit_date."""
    engine = OptionsBacktestEngine()
    bars = [make_bar(date(2025, 1, d), 100 + d, 1_000_000) for d in range(1, 10)]
    config = BacktestConfig(
        symbol="TSLA",
        strategy_type="custom_2_leg",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 9),
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=30,
        account_size=100_000,
        risk_per_trade_pct=5,
        commission_per_contract=0,
        entry_rules=[],
        custom_legs=[
            CustomLegDefinition(asset_type="stock", side="long", quantity_ratio=1),
            CustomLegDefinition(asset_type="stock", side="short", quantity_ratio=1),
        ],
    )
    result = engine.run(config, bars, set(), FakeGateway(contracts={}, quotes={}))
    assert result.summary is not None
    for trade in result.trades:
        assert trade.exit_date <= date(2025, 1, 9)


def test_custom_strategy_supports_three_explicit_expirations_and_furthest_exit() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 5, 1)
    exp_1 = date(2025, 6, 5)
    exp_2 = date(2025, 6, 12)
    exp_3 = date(2025, 10, 2)
    bars = [
        make_bar(date(2025, 4, 30), 15.0),
        make_bar(entry_date, 15.0),
        make_bar(exp_3, 15.0),
    ]
    contracts = {
        (entry_date, "put"): [
            OptionContractRecord("P15_0605", "put", exp_1, 15.0, 100),
            OptionContractRecord("P14_0612", "put", exp_2, 14.0, 100),
            OptionContractRecord("P15_0612", "put", exp_2, 15.0, 100),
            OptionContractRecord("P16_0612", "put", exp_2, 16.0, 100),
            OptionContractRecord("P15_1002", "put", exp_3, 15.0, 100),
        ],
        (entry_date, "call"): [
            OptionContractRecord("C15_1002", "call", exp_3, 15.0, 100),
        ],
    }
    quotes = {
        ("P15_0605", entry_date): make_quote(entry_date, 1.2),
        ("P15_0612", entry_date): make_quote(entry_date, 1.4),
        ("P14_0612", entry_date): make_quote(entry_date, 0.8),
        ("P15_1002", entry_date): make_quote(entry_date, 2.1),
        ("C15_1002", entry_date): make_quote(entry_date, 2.3),
    }
    gateway = ExactCustomGateway(contracts=contracts, quotes=quotes)

    result = engine.run(
        BacktestConfig(
            symbol="F",
            strategy_type="custom_5_leg",
            start_date=date(2025, 4, 30),
            end_date=entry_date,
            target_dte=21,
            dte_tolerance_days=5,
            max_holding_days=120,
            account_size=100_000,
            risk_per_trade_pct=10,
            commission_per_contract=0,
            entry_rules=[],
            custom_legs=[
                CustomLegDefinition(
                    asset_type="option",
                    contract_type="put",
                    side="short",
                    expiration_date=exp_1,
                    strike_selection={"mode": "atm_offset_steps", "value": 0},
                    quantity_ratio=1,
                ),
                CustomLegDefinition(
                    asset_type="option",
                    contract_type="put",
                    side="long",
                    expiration_date=exp_2,
                    strike_selection={"mode": "atm_offset_steps", "value": 0},
                    quantity_ratio=1,
                ),
                CustomLegDefinition(
                    asset_type="option",
                    contract_type="put",
                    side="long",
                    expiration_date=exp_2,
                    strike_selection={"mode": "delta_target", "value": 20},
                    quantity_ratio=1,
                ),
                CustomLegDefinition(
                    asset_type="option",
                    contract_type="call",
                    side="short",
                    expiration_date=exp_3,
                    strike_selection={"mode": "atm_offset_steps", "value": 0},
                    quantity_ratio=1,
                ),
                CustomLegDefinition(
                    asset_type="option",
                    contract_type="put",
                    side="short",
                    expiration_date=exp_3,
                    strike_selection={"mode": "atm_offset_steps", "value": 0},
                    quantity_ratio=1,
                ),
            ],
        ),
        bars,
        set(),
        gateway,
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    assert trade.exit_date == exp_3
    assert trade.expiration_date == exp_3
    assert (entry_date, "put", exp_1) in gateway.exact_calls
    assert (entry_date, "put", exp_2) in gateway.exact_calls
    assert (entry_date, "call", exp_3) in gateway.exact_calls
    legs_by_ticker = {leg["ticker"]: leg for leg in trade.detail_json["legs"]}
    assert "P14_0612" in legs_by_ticker
    assert trade.detail_json["custom_legs"][2]["strike_selection"]["mode"] == "delta_target"
    assert trade.detail_json["resolved_option_expirations"] == [
        exp_1.isoformat(),
        exp_2.isoformat(),
        exp_3.isoformat(),
    ]


def test_custom_request_rejects_more_than_three_explicit_expirations() -> None:
    with pytest.raises(PydanticValidationError, match="at most 3 unique option expiration_date"):
        CreateBacktestRunRequest(
            symbol="F",
            strategy_type="custom_5_leg",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 2, 1),
            target_dte=21,
            dte_tolerance_days=5,
            max_holding_days=30,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("10"),
            commission_per_contract=Decimal("1"),
            entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            custom_legs=[
                CustomLegDefinition(asset_type="option", contract_type="put", side="short", expiration_date=date(2025, 2, 7), quantity_ratio=1),
                CustomLegDefinition(asset_type="option", contract_type="put", side="long", expiration_date=date(2025, 2, 14), quantity_ratio=1),
                CustomLegDefinition(asset_type="option", contract_type="call", side="short", expiration_date=date(2025, 2, 21), quantity_ratio=1),
                CustomLegDefinition(asset_type="option", contract_type="call", side="long", expiration_date=date(2025, 2, 28), quantity_ratio=1),
                CustomLegDefinition(asset_type="stock", side="long", quantity_ratio=1),
            ],
        )


def test_zero_trade_run_has_empty_stats() -> None:
    engine = OptionsBacktestEngine()
    bars = [
        make_bar(date(2025, 1, 1), 100),
        make_bar(date(2025, 1, 2), 101),
        make_bar(date(2025, 1, 3), 102),
    ]
    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 3),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts={}, quotes={}),
    )
    assert result.summary.trade_count == 0
    assert result.summary.total_net_pnl == 0.0
    assert result.summary.total_roi_pct == 0.0
    assert result.summary.win_rate == 0.0
    assert result.summary.max_drawdown_pct == 0.0
    assert len(result.equity_curve) >= 1
    assert result.equity_curve[0].equity == pytest.approx(10_000)


# ---------------------------------------------------------------------------
# Parametrized strategy smoke tests
# ---------------------------------------------------------------------------


class SyntheticGateway:
    """Auto-generates option chains to support all strategy types."""

    def __init__(self, underlying_close: float = 100.0) -> None:
        self.underlying_close = underlying_close
        self._near_exp = date(2025, 2, 1)
        self._far_exp = date(2025, 3, 1)
        self._strikes = [80, 85, 90, 95, 97, 100, 103, 105, 110, 115, 120]

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        contracts = []
        for exp in [self._near_exp, self._far_exp]:
            for strike in self._strikes:
                ticker = f"{contract_type[0].upper()}{strike}_{exp.isoformat()}"
                contracts.append(
                    OptionContractRecord(ticker, contract_type, exp, float(strike), 100)
                )
        return contracts

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
            raise DataUnavailableError("No contracts")
        return contracts[0]

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        parts = option_ticker.split("_")[0]
        try:
            strike = float("".join(c for c in parts if c.isdigit()))
        except ValueError:
            strike = 100.0
        distance = abs(strike - self.underlying_close)
        mid = max(0.50, 5.0 - distance * 0.1)
        return make_quote(trade_date, mid)

    def get_chain_delta_lookup(self, contracts):
        return {}


ALL_NON_WHEEL_STRATEGIES = [
    "long_call",
    "long_put",
    "covered_call",
    "cash_secured_put",
    "bull_call_debit_spread",
    "bear_put_debit_spread",
    "bull_put_credit_spread",
    "bear_call_credit_spread",
    "iron_condor",
    "long_straddle",
    "long_strangle",
    "calendar_spread",
    "butterfly",
    "short_straddle",
    "short_strangle",
    "collar",
    "covered_strangle",
    "poor_mans_covered_call",
    "diagonal_spread",
    "double_diagonal",
    "ratio_call_backspread",
    "ratio_put_backspread",
    "synthetic_put",
    "reverse_conversion",
    "jade_lizard",
    "iron_butterfly",
    "naked_call",
    "naked_put",
]


@pytest.mark.parametrize("strategy_type", ALL_NON_WHEEL_STRATEGIES)
def test_strategy_runs_without_error(strategy_type: str) -> None:
    """Smoke test: every strategy can run end-to-end without raising."""
    engine = OptionsBacktestEngine()
    gateway = SyntheticGateway(underlying_close=100.0)
    start = date(2025, 1, 1)
    bars = [make_bar(start + timedelta(days=d), 99 + d * 0.3, 1_000_000) for d in range(40)]
    config = BacktestConfig(
        symbol="SMKE",
        strategy_type=strategy_type,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 20),
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=15,
        account_size=100_000,
        risk_per_trade_pct=5,
        commission_per_contract=0,
        entry_rules=[],
    )
    result = engine.run(config, bars, set(), gateway)
    assert result.summary is not None
    assert result.summary.starting_equity == 100_000
    if result.trades:
        trade = result.trades[0]
        assert trade.strategy_type == strategy_type
        assert trade.entry_date is not None
        assert trade.exit_date is not None
        assert trade.exit_date >= trade.entry_date


def test_wheel_records_assignment_and_stock_exit() -> None:
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

    assert result.summary.trade_count == 2
    phases = [trade.detail_json.get("phase") for trade in result.trades]
    assert phases == ["cash_secured_put", "stock_inventory"]
    assert result.trades[0].exit_reason == "assignment"
    assert result.trades[1].exit_reason == "backtest_end_share_liquidation"
    assert result.trades[1].exit_mid == 105.0
    # CSP premium: 2x100x2=$400, then assigned shares liquidated at +$5/share for 200 shares = $1000
    assert round(result.summary.total_net_pnl, 2) == 1400.0


# ---------------------------------------------------------------------------
# Financial correctness tests - verifiable P&L against hand-calculated values
# ---------------------------------------------------------------------------


class TestWheelAssignmentCommission:
    """Item 87: In a put-assignment scenario the exit is assignment (shares
    acquired), so no exit commission is charged. total_commissions must equal
    only the entry commission."""

    COMMISSION = 0.65

    def test_put_assignment_commission_is_entry_only(self) -> None:
        engine = OptionsBacktestEngine()
        bars = [
            make_bar(date(2025, 3, 1), 101),
            make_bar(date(2025, 3, 2), 100),
            make_bar(date(2025, 3, 3), 95),
        ]
        contracts = {
            (date(2025, 3, 2), "put"): [
                OptionContractRecord("P100", "put", date(2025, 3, 3), 100, 100),
            ],
        }
        quotes = {
            ("P100", date(2025, 3, 2)): make_quote(date(2025, 3, 2), 2.0),
        }
        result = engine.run(
            BacktestConfig(
                symbol="AMD",
                strategy_type="wheel_strategy",
                start_date=date(2025, 3, 1),
                end_date=date(2025, 3, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=20_000,
                risk_per_trade_pct=100,
                commission_per_contract=self.COMMISSION,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        put_trades = [t for t in result.trades if t.detail_json.get("phase") == "cash_secured_put"]
        assert len(put_trades) >= 1
        trade = put_trades[0]
        assert trade.exit_reason == "assignment"

        expected_entry_commission = self.COMMISSION * trade.quantity
        assert round(float(trade.total_commissions), 2) == round(expected_entry_commission, 2), (
            f"Assignment exit should have zero exit commission; "
            f"total_commissions ({trade.total_commissions}) should equal "
            f"entry-only commission ({expected_entry_commission})"
        )


class TestBullCallSpreadCorrectness:
    """Verify exact P&L, commissions, and position sizing for a bull call debit spread.

    Setup:
        Long C100 (mid 4.00) / Short C105 (mid 1.50)
        Debit per unit = 2.50 x 100 = $250
        Width = $500, max_profit = $250, max_loss = $250
        Account $10,000 @ 5% risk -> 2 units
        Commission $0.65/contract
    """

    def test_profit_scenario_with_commissions(self) -> None:
        engine = OptionsBacktestEngine()
        expiration = date(2025, 4, 5)
        entry_date = date(2025, 4, 2)
        bars = [
            make_bar(date(2025, 4, 1), 99),
            make_bar(entry_date, 100),
            make_bar(date(2025, 4, 3), 102),
            make_bar(date(2025, 4, 4), 106),
            make_bar(expiration, 108),
        ]
        contracts = {
            (entry_date, "call"): [
                OptionContractRecord("C95", "call", expiration, 95, 100),
                OptionContractRecord("C100", "call", expiration, 100, 100),
                OptionContractRecord("C105", "call", expiration, 105, 100),
            ]
        }
        quotes = {
            ("C100", entry_date): make_spread_quote(entry_date, bid=3.80, ask=4.20),
            ("C105", entry_date): make_spread_quote(entry_date, bid=1.30, ask=1.70),
        }
        commission = 0.65
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="bull_call_debit_spread",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=10_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count == 1
        trade = result.trades[0]

        # risk_budget = $500, max_loss_per_unit = $250 -> by_risk = 2
        assert trade.quantity == 2

        # At expiration: C100 intrinsic = 8.00, C105 intrinsic = 3.00
        # exit_value_per_unit = (8 - 3) x 100 = 500
        # gross = (500 - 250) x 2 = 500
        assert round(float(trade.gross_pnl), 2) == 500.0

        # 2 legs x 2 units x $0.65, charged at entry only because both legs settle at expiration
        expected_comm = commission * 2 * 2  # 2.60
        assert round(float(trade.total_commissions), 2) == round(expected_comm, 2)

        assert round(float(trade.net_pnl), 2) == round(500.0 - expected_comm, 2)
        assert trade.detail_json["max_profit_per_unit"] == 250.0
        assert trade.detail_json["actual_units"] == 2


class TestIronCondorCorrectness:
    """Verify iron condor P&L for range-bound and breakout scenarios.

    Setup (shared):
        Short C100 (3.00) / Long C105 (1.00) / Short P100 (3.00) / Long P95 (1.00)
        Net credit per unit = (3+3-1-1) x 100 = $400
        Wing width = $500, max_loss_per_unit = $100
        Account $10,000 @ 2% risk -> 2 units
        Commission $0.65/contract
    """

    @staticmethod
    def _build_fixtures(
        exit_underlying: float,
    ) -> tuple[list, dict, dict]:
        entry_date = date(2025, 5, 2)
        expiration = date(2025, 5, 5)
        bars = [
            make_bar(date(2025, 5, 1), 100),
            make_bar(entry_date, 100),
            make_bar(date(2025, 5, 3), 101),
            make_bar(date(2025, 5, 4), 102),
            make_bar(expiration, exit_underlying),
        ]
        contracts = {
            (entry_date, "call"): [
                OptionContractRecord("C95", "call", expiration, 95, 100),
                OptionContractRecord("C100", "call", expiration, 100, 100),
                OptionContractRecord("C105", "call", expiration, 105, 100),
            ],
            (entry_date, "put"): [
                OptionContractRecord("P95", "put", expiration, 95, 100),
                OptionContractRecord("P100", "put", expiration, 100, 100),
                OptionContractRecord("P105", "put", expiration, 105, 100),
            ],
        }
        quotes = {
            ("C100", entry_date): make_quote(entry_date, 3.0),
            ("C105", entry_date): make_quote(entry_date, 1.0),
            ("P100", entry_date): make_quote(entry_date, 3.0),
            ("P95", entry_date): make_quote(entry_date, 1.0),
        }
        return bars, contracts, quotes

    def _run(self, exit_underlying: float, commission: float = 0.65):
        bars, contracts, quotes = self._build_fixtures(exit_underlying)
        engine = OptionsBacktestEngine()
        return engine.run(
            BacktestConfig(
                symbol="SPY",
                strategy_type="iron_condor",
                start_date=date(2025, 5, 1),
                end_date=date(2025, 5, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=10_000,
                risk_per_trade_pct=2,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

    def test_profit_when_range_bound(self) -> None:
        """Underlying stays at 100 - all legs expire worthless, full credit kept."""
        commission = 0.65
        result = self._run(exit_underlying=100, commission=commission)

        assert result.summary.trade_count == 1
        trade = result.trades[0]

        # risk_budget = $200, max_loss_per_unit = $100 -> 2 units
        assert trade.quantity == 2

        # All intrinsics = 0 at expiration with underlying = 100
        # gross = (0 - (-400)) x 2 = 800
        assert round(float(trade.gross_pnl), 2) == 800.0

        # 4 legs x 2 units x $0.65, charged at entry only because all legs settle at expiration
        expected_comm = commission * 4 * 2
        assert round(float(trade.total_commissions), 2) == round(expected_comm, 2)

        assert round(float(trade.net_pnl), 2) == round(800.0 - expected_comm, 2)
        assert trade.net_pnl > 0

        # Gross profit does not exceed wing_width x quantity
        wing_width = 500
        assert abs(trade.gross_pnl) <= wing_width * trade.quantity

    def test_max_loss_when_market_breaks_out(self) -> None:
        """Underlying surges to 110 - call side fully breached, max loss realised."""
        commission = 0.65
        result = self._run(exit_underlying=110, commission=commission)

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        assert trade.quantity == 2

        # At 110: C100 intrinsic = 10, C105 = 5, puts = 0
        # exit_value_per_unit = (-10 + 5) x 100 = -500
        # gross = (-500 - (-400)) x 2 = -200
        assert round(float(trade.gross_pnl), 2) == -200.0
        assert trade.net_pnl < 0

        # Max loss per unit ($100) x 2 units - gross loss exactly equals maximum
        assert abs(trade.gross_pnl) == trade.detail_json["max_loss_total"]

        # Gross loss bounded by wing width x quantity
        wing_width_per_unit = 500
        assert abs(trade.gross_pnl) <= wing_width_per_unit * trade.quantity

        expected_comm = commission * 4 * 2
        assert round(float(trade.total_commissions), 2) == round(expected_comm, 2)
        assert round(float(trade.net_pnl), 2) == round(-200.0 - expected_comm, 2)


class TestCashSecuredPutCorrectness:
    """Verify cash-secured put P&L for OTM expiration and ITM assignment loss.

    Setup:
        Short P100 (mid 2.00), underlying at 105 on entry
        Credit = $200, cash required = strike x 100 = $10,000
        Account $100,000 @ 10% risk -> 1 unit
        Commission $0.65/contract
    """

    COMMISSION = 0.65
    STRIKE = 100.0
    PREMIUM = 2.0
    ENTRY_DATE = date(2025, 6, 2)
    EXPIRATION = date(2025, 6, 5)

    def _config(self) -> BacktestConfig:
        return BacktestConfig(
            symbol="TSLA",
            strategy_type="cash_secured_put",
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 3),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=100_000,
            risk_per_trade_pct=10,
            commission_per_contract=self.COMMISSION,
            entry_rules=[],
        )

    def _gateway(self) -> FakeGateway:
        return FakeGateway(
            contracts={
                (self.ENTRY_DATE, "put"): [
                    OptionContractRecord("P95", "put", self.EXPIRATION, 95, 100),
                    OptionContractRecord("P100", "put", self.EXPIRATION, 100, 100),
                ]
            },
            quotes={
                ("P100", self.ENTRY_DATE): make_quote(self.ENTRY_DATE, self.PREMIUM),
            },
        )

    def test_otm_collects_full_premium(self) -> None:
        """Underlying stays at 105 - put expires worthless, full premium is profit."""
        bars = [
            make_bar(date(2025, 6, 1), 105),
            make_bar(self.ENTRY_DATE, 105),
            make_bar(date(2025, 6, 3), 106),
            make_bar(date(2025, 6, 4), 107),
            make_bar(self.EXPIRATION, 105),
        ]
        engine = OptionsBacktestEngine()
        result = engine.run(self._config(), bars, set(), self._gateway())

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        assert trade.quantity == 1

        # cash_required = strike x 100 x quantity
        assert trade.detail_json["capital_required_total"] == self.STRIKE * 100 * trade.quantity

        # Full premium: gross = 2.00 x 100 = $200
        premium_collected = self.PREMIUM * 100
        assert round(float(trade.gross_pnl), 2) == premium_collected

        # 1 leg x 1 unit x $0.65, charged at entry only because the option expires
        expected_comm = self.COMMISSION * 1 * 1
        assert round(float(trade.total_commissions), 2) == round(expected_comm, 2)
        assert round(float(trade.net_pnl), 2) == round(premium_collected - expected_comm, 2)

    def test_itm_realizes_assignment_loss(self) -> None:
        """Deep ITM short put is assigned before expiry under the engine's early-assignment model."""
        bars = [
            make_bar(date(2025, 6, 1), 105),
            make_bar(self.ENTRY_DATE, 105),
            make_bar(date(2025, 6, 3), 100),
            make_bar(date(2025, 6, 4), 95),
            make_bar(self.EXPIRATION, 90),
        ]
        engine = OptionsBacktestEngine()
        result = engine.run(self._config(), bars, set(), self._gateway())

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        assert trade.quantity == 1
        assert trade.exit_reason == "early_assignment_put_deep_itm"

        # The engine assigns the short put on 2025-06-04 when it is deep ITM near expiry.
        # intrinsic = (100 - 95) x 100 = $500 loss on short put
        # offset by premium = $200 -> gross = -$300
        intrinsic_loss = (self.STRIKE - 95) * 100
        expected_gross = -(intrinsic_loss - self.PREMIUM * 100)  # -300
        assert round(float(trade.gross_pnl), 2) == expected_gross

        expected_comm = self.COMMISSION * 1 * 1
        assert round(float(trade.total_commissions), 2) == round(expected_comm, 2)
        assert round(float(trade.net_pnl), 2) == round(expected_gross - expected_comm, 2)
        assert trade.net_pnl < 0


def test_naked_put_waives_buy_to_close_fee_at_or_below_five_cents() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 7, 2)
    expiration = date(2025, 7, 10)
    bars = [
        make_bar(date(2025, 7, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 7, 3), 101),
    ]
    contracts = {
        (entry_date, "put"): [
            OptionContractRecord("P95", "put", expiration, 95, 100),
        ],
    }
    quotes = {
        ("P95", entry_date): make_quote(entry_date, 1.0),
        ("P95", date(2025, 7, 3)): make_quote(date(2025, 7, 3), 0.03),
    }
    commission = 0.65
    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="naked_put",
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 10),
            target_dte=8,
            dte_tolerance_days=2,
            max_holding_days=1,
            account_size=10_000,
            risk_per_trade_pct=100,
            commission_per_contract=commission,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 1
    trade = result.trades[0]
    expected_entry_only = commission * trade.quantity
    assert round(float(trade.total_commissions), 2) == round(expected_entry_only, 2)
    assert round(float(trade.detail_json["entry_commissions"]), 2) == round(expected_entry_only, 2)
    assert round(float(trade.detail_json["exit_commissions"]), 2) == 0.0
    assert trade.detail_json["commission_waivers"] == [
        {
            "ticker": "P95",
            "reason": "buy_to_close_0.05_or_less",
            "contracts": trade.quantity,
            "exit_mid": 0.03,
        }
    ]


def test_run_exit_policy_variants_matches_individual_runs() -> None:
    entry_date = date(2025, 9, 2)
    expiration = date(2025, 10, 17)
    bars = [
        make_bar(date(2025, 9, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 9, 3), 100),
        make_bar(date(2025, 9, 4), 100),
        make_bar(date(2025, 9, 5), 100),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("C100", "call", expiration, 100, 100),
        ],
    }
    quotes = {
        ("C100", entry_date): make_quote(entry_date, 2.0),
        ("C100", date(2025, 9, 3)): make_quote(date(2025, 9, 3), 3.2),
        ("C100", date(2025, 9, 4)): make_quote(date(2025, 9, 4), 3.7),
        ("C100", date(2025, 9, 5)): make_quote(date(2025, 9, 5), 3.5),
    }
    gateway = FakeGateway(contracts=contracts, quotes=quotes)

    base_kwargs = dict(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=entry_date,
        end_date=entry_date,
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=10,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=0.65,
        entry_rules=[],
    )
    config_pt50 = BacktestConfig(**base_kwargs, profit_target_pct=50)
    config_pt75 = BacktestConfig(**base_kwargs, profit_target_pct=75)

    expected_engine = OptionsBacktestEngine()
    expected = [
        expected_engine.run(config_pt50, bars, set(), gateway),
        expected_engine.run(config_pt75, bars, set(), gateway),
    ]

    actual_engine = OptionsBacktestEngine()
    actual = actual_engine.run_exit_policy_variants(
        configs=[config_pt50, config_pt75],
        bars=bars,
        earnings_dates=set(),
        option_gateway=gateway,
    )

    assert len(actual) == 2
    for actual_result, expected_result in zip(actual, expected, strict=True):
        assert actual_result.summary.trade_count == expected_result.summary.trade_count
        assert actual_result.summary.ending_equity == expected_result.summary.ending_equity
        assert len(actual_result.trades) == len(expected_result.trades) == 1
        assert actual_result.trades[0].exit_date == expected_result.trades[0].exit_date
        assert actual_result.trades[0].exit_reason == expected_result.trades[0].exit_reason
        assert actual_result.trades[0].net_pnl == expected_result.trades[0].net_pnl


def test_run_exit_policy_variants_matches_individual_runs_with_different_entry_rules() -> None:
    entry_date = date(2025, 9, 2)
    expiration = date(2025, 10, 17)
    bars = [
        make_bar(date(2025, 9, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 9, 3), 100),
        make_bar(date(2025, 9, 4), 100),
        make_bar(date(2025, 9, 5), 100),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("C100", "call", expiration, 100, 100),
        ],
    }
    quotes = {
        ("C100", entry_date): make_quote(entry_date, 2.0),
        ("C100", date(2025, 9, 3)): make_quote(date(2025, 9, 3), 3.2),
        ("C100", date(2025, 9, 4)): make_quote(date(2025, 9, 4), 3.7),
        ("C100", date(2025, 9, 5)): make_quote(date(2025, 9, 5), 3.5),
    }
    gateway = FakeGateway(contracts=contracts, quotes=quotes)
    earnings_dates = {entry_date}

    base_kwargs = dict(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=entry_date,
        end_date=entry_date,
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=10,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=0.65,
    )
    config_no_filter = BacktestConfig(**base_kwargs, entry_rules=[], profit_target_pct=50)
    config_avoid_earnings = BacktestConfig(
        **base_kwargs,
        entry_rules=[AvoidEarningsRule(type="avoid_earnings", days_before=1, days_after=0)],
        profit_target_pct=50,
    )

    expected_engine = OptionsBacktestEngine()
    expected = [
        expected_engine.run(config_no_filter, bars, earnings_dates, gateway),
        expected_engine.run(config_avoid_earnings, bars, earnings_dates, gateway),
    ]

    actual_engine = OptionsBacktestEngine()
    actual = actual_engine.run_exit_policy_variants(
        configs=[config_no_filter, config_avoid_earnings],
        bars=bars,
        earnings_dates=earnings_dates,
        option_gateway=gateway,
    )

    assert len(actual) == 2
    for actual_result, expected_result in zip(actual, expected, strict=True):
        assert actual_result.summary.trade_count == expected_result.summary.trade_count
        assert actual_result.summary.ending_equity == expected_result.summary.ending_equity
        assert actual_result.summary.total_net_pnl == expected_result.summary.total_net_pnl
    assert len(actual_result.trades) == len(expected_result.trades)
    assert actual[0].summary.trade_count == 1
    assert actual[1].summary.trade_count == 0


def test_engine_skips_entry_when_option_strike_scale_mismatches_underlying() -> None:
    entry_date = date(2025, 9, 2)
    expiration = date(2025, 10, 17)
    bars = [
        make_bar(date(2025, 9, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 9, 3), 101),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("C2500", "call", expiration, 2500, 100),
        ],
    }
    quotes = {
        ("C2500", entry_date): make_quote(entry_date, 1.5),
        ("C2500", date(2025, 9, 3)): make_quote(date(2025, 9, 3), 1.8),
    }

    result = OptionsBacktestEngine().run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=entry_date,
            end_date=entry_date,
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=10,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0.65,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    assert result.summary.trade_count == 0
    assert result.trades == []
    assert any(w["code"] == "option_underlying_scale_mismatch" for w in result.warnings)


def test_run_exit_policy_variants_skips_mismatched_option_scales_for_all_lanes() -> None:
    entry_date = date(2025, 9, 2)
    expiration = date(2025, 10, 17)
    bars = [
        make_bar(date(2025, 9, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 9, 3), 101),
    ]
    contracts = {
        (entry_date, "call"): [
            OptionContractRecord("C2500", "call", expiration, 2500, 100),
        ],
    }
    quotes = {
        ("C2500", entry_date): make_quote(entry_date, 1.5),
        ("C2500", date(2025, 9, 3)): make_quote(date(2025, 9, 3), 1.8),
    }
    gateway = FakeGateway(contracts=contracts, quotes=quotes)
    base_kwargs = dict(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=entry_date,
        end_date=entry_date,
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=10,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=0.65,
        entry_rules=[],
    )

    results = OptionsBacktestEngine().run_exit_policy_variants(
        configs=[
            BacktestConfig(**base_kwargs, profit_target_pct=50),
            BacktestConfig(**base_kwargs, profit_target_pct=75),
        ],
        bars=bars,
        earnings_dates=set(),
        option_gateway=gateway,
    )

    assert len(results) == 2
    for result in results:
        assert result.summary.trade_count == 0
        assert result.trades == []
        assert any(w["code"] == "option_underlying_scale_mismatch" for w in result.warnings)


def test_engine_run_prefers_quote_series_for_open_position_marks() -> None:
    entry_date = date(2025, 9, 2)
    expiration = date(2025, 10, 17)
    bars = [
        make_bar(date(2025, 9, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 9, 3), 101),
        make_bar(date(2025, 9, 4), 102),
        make_bar(date(2025, 9, 5), 103),
    ]
    contract = OptionContractRecord("C100", "call", expiration, 100, 100)

    class _SeriesGateway:
        def __init__(self) -> None:
            self.quote_calls: list[date] = []
            self.series_calls = 0

        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            return [contract]

        def get_quote(self, option_ticker, trade_date):
            self.quote_calls.append(trade_date)
            if trade_date != entry_date:
                raise AssertionError("later mark dates should use quote series, not get_quote")
            return make_quote(trade_date, 2.0)

        def get_quote_series(self, option_tickers, start_date, end_date):
            self.series_calls += 1
            assert option_tickers == ["C100"]
            assert start_date == entry_date
            assert end_date == date(2025, 9, 5)
            return {
                "C100": {
                    date(2025, 9, 3): make_quote(date(2025, 9, 3), 2.8),
                    date(2025, 9, 4): make_quote(date(2025, 9, 4), 3.1),
                    date(2025, 9, 5): make_quote(date(2025, 9, 5), 3.4),
                }
            }

        def get_chain_delta_lookup(self, contracts):
            return {}

    gateway = _SeriesGateway()
    engine = OptionsBacktestEngine()
    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=entry_date,
            end_date=entry_date,
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=10,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0.65,
            entry_rules=[],
        ),
        bars,
        set(),
        gateway,
    )

    assert result.summary.trade_count == 1
    assert gateway.series_calls == 1
    assert gateway.quote_calls == [entry_date]


def test_wheel_waives_buy_to_close_fee_at_or_below_five_cents() -> None:
    engine = OptionsBacktestEngine()
    entry_date = date(2025, 8, 2)
    expiration = date(2025, 8, 15)
    bars = [
        make_bar(date(2025, 8, 1), 100),
        make_bar(entry_date, 100),
        make_bar(date(2025, 8, 3), 101),
    ]
    contracts = {
        (entry_date, "put"): [
            OptionContractRecord("P95", "put", expiration, 95, 100),
        ],
    }
    quotes = {
        ("P95", entry_date): make_quote(entry_date, 1.0),
        ("P95", date(2025, 8, 3)): make_quote(date(2025, 8, 3), 0.03),
    }
    commission = 0.65
    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="wheel_strategy",
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 15),
            target_dte=14,
            dte_tolerance_days=5,
            max_holding_days=1,
            account_size=20_000,
            risk_per_trade_pct=100,
            commission_per_contract=commission,
            entry_rules=[],
        ),
        bars,
        set(),
        FakeGateway(contracts=contracts, quotes=quotes),
    )

    option_trade = next(trade for trade in result.trades if trade.detail_json.get("phase") == "cash_secured_put")
    expected_entry_only = commission * option_trade.quantity
    assert round(float(option_trade.total_commissions), 2) == round(expected_entry_only, 2)
    assert round(float(option_trade.detail_json["entry_commissions"]), 2) == round(expected_entry_only, 2)
    assert round(float(option_trade.detail_json["exit_commissions"]), 2) == 0.0
    assert option_trade.detail_json["commission_waivers"] == [
        {
            "ticker": "P95",
            "reason": "buy_to_close_0.05_or_less",
            "contracts": option_trade.quantity,
            "exit_mid": 0.03,
        }
    ]


# ---------------------------------------------------------------------------
# Item 61: Wheel exit slippage reflected in cash delta
# ---------------------------------------------------------------------------


class TestWheelExitSlippageInCash:
    """Verify that after a normal (non-assignment) wheel exit, cumulative cash
    change equals sum of net P&L (which includes slippage). This confirms
    exit_slippage is deducted from the cash delta on the else-branch."""

    SLIPPAGE_PCT = 1.0
    COMMISSION = 0.0

    def test_cash_delta_equals_net_pnl_sum(self) -> None:
        engine = OptionsBacktestEngine()
        entry = date(2025, 7, 2)
        expiration = date(2025, 7, 10)
        bars = [
            make_bar(date(2025, 7, 1), 100),
            make_bar(entry, 100),
            make_bar(date(2025, 7, 3), 100),
            make_bar(date(2025, 7, 4), 102),
        ]
        contracts = {
            (entry, "put"): [
                OptionContractRecord("P100", "put", expiration, 100, 100),
            ]
        }
        quotes = {
            ("P100", entry): make_quote(entry, 3.0),
            ("P100", date(2025, 7, 3)): make_quote(date(2025, 7, 3), 2.5),
            ("P100", date(2025, 7, 4)): make_quote(date(2025, 7, 4), 2.0),
        }
        account_size = 50_000.0
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="wheel_strategy",
                start_date=date(2025, 7, 1),
                end_date=date(2025, 7, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=2,
                account_size=account_size,
                risk_per_trade_pct=100,
                commission_per_contract=self.COMMISSION,
                slippage_pct=self.SLIPPAGE_PCT,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count >= 1
        total_net_pnl = sum(t.net_pnl for t in result.trades)
        ending_cash = result.equity_curve[-1].cash
        cash_delta = float(ending_cash) - account_size
        assert round(float(cash_delta), 2) == round(total_net_pnl, 2), (
            f"Cash delta ({cash_delta:.2f}) should equal net P&L sum ({total_net_pnl:.2f}) "
            "when slippage is correctly included in exit cash flow"
        )


# ---------------------------------------------------------------------------
# Item 62: Calendar net-credit position sizing (max_loss != 0)
# ---------------------------------------------------------------------------


class TestCalendarNetCreditPositionSizing:
    """When a calendar spread entry results in a net credit (entry_value_per_unit < 0),
    max_loss must be set to the margin requirement, not zero. This prevents the
    position sizer from allocating unlimited contracts."""

    def test_net_credit_max_loss_equals_margin(self) -> None:
        from backtestforecast.backtests.margin import naked_call_margin
        from backtestforecast.backtests.strategies.calendar import CalendarSpreadStrategy

        underlying_close = 100.0
        strike = 100.0
        short_mid = 5.0
        long_mid = 3.0

        bars_for_test = [make_bar(date(2025, 8, 2), underlying_close)]
        near_exp = date(2025, 8, 15)
        far_exp = date(2025, 9, 1)

        contracts = {
            (date(2025, 8, 2), "call"): [
                OptionContractRecord("NEAR100", "call", near_exp, strike, 100),
                OptionContractRecord("FAR100", "call", far_exp, strike, 100),
            ]
        }
        quotes = {
            ("NEAR100", date(2025, 8, 2)): make_quote(date(2025, 8, 2), short_mid),
            ("FAR100", date(2025, 8, 2)): make_quote(date(2025, 8, 2), long_mid),
        }

        strategy = CalendarSpreadStrategy()
        config = BacktestConfig(
            symbol="SPY",
            strategy_type="calendar_spread",
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 10),
            target_dte=13,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=100_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )
        gateway = FakeGateway(contracts=contracts, quotes=quotes)

        entry_value_per_unit = (long_mid - short_mid) * 100.0
        assert entry_value_per_unit < 0, "Test setup: should be a net credit"

        position = strategy.build_position(config, bars_for_test[0], 0, gateway)
        assert position is not None

        full_margin = naked_call_margin(underlying_close, strike, short_mid)
        long_leg_value = long_mid * 100.0
        net_debit = max(entry_value_per_unit, 0.0)
        expected_margin = max(full_margin - long_leg_value, net_debit)
        assert position.max_loss_per_unit == expected_margin
        assert position.max_loss_per_unit > 0, "max_loss must not be zero for net-credit calendar"
        assert position.capital_required_per_unit == expected_margin


# ---------------------------------------------------------------------------
# Item 64: entry_mid unit_convention is set in detail_json
# ---------------------------------------------------------------------------


class TestUnitConventionPresent:
    """Verify that both wheel and generic (covered_call) backtests set
    'unit_convention' in trade detail_json."""

    def test_wheel_has_unit_convention(self) -> None:
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
        assert result.summary.trade_count >= 1
        for trade in result.trades:
            assert "unit_convention" in trade.detail_json, (
                f"Wheel trade missing unit_convention: {trade.detail_json.keys()}"
            )

    def test_covered_call_has_unit_convention(self) -> None:
        engine = OptionsBacktestEngine()
        gateway = SyntheticGateway(underlying_close=100.0)
        start = date(2025, 1, 1)
        bars = [make_bar(start + timedelta(days=d), 99 + d * 0.3, 1_000_000) for d in range(40)]
        config = BacktestConfig(
            symbol="TEST",
            strategy_type="covered_call",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 20),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=15,
            account_size=100_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )
        result = engine.run(config, bars, set(), gateway)
        assert result.summary is not None
        for trade in result.trades:
            assert "unit_convention" in trade.detail_json, (
                f"Covered call trade missing unit_convention: {trade.detail_json.keys()}"
            )


# ---------------------------------------------------------------------------
# Item 65: _estimate_iv_for_strike handles strike offset correctly
# ---------------------------------------------------------------------------


class TestEstimateIvForStrikeOffset:
    """Verify that _estimate_iv_for_strike with a strike that's 0.003 away
    from the contract strike still finds the contract (threshold is 0.005)."""

    def test_strike_within_threshold_is_found(self) -> None:
        from backtestforecast.backtests.strategies.common import _estimate_iv_for_strike
        from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord

        trade_date = date(2025, 5, 1)
        exact_strike = 100.0
        offset_strike = exact_strike + 0.003

        contract = OptionContractRecord("C100", "call", date(2025, 6, 1), exact_strike, 100)

        class FakeGW:
            def get_quote(self, ticker, dt):
                return OptionQuoteRecord(
                    trade_date=dt, bid_price=4.0, ask_price=4.0, participant_timestamp=None,
                )

        iv = _estimate_iv_for_strike(
            strike=offset_strike,
            contract_type="call",
            underlying_close=100.0,
            dte_days=31,
            contracts=[contract],
            option_gateway=FakeGW(),
            trade_date=trade_date,
        )
        assert iv is not None, (
            "Strike 0.003 away should still match the contract (threshold 0.005)"
        )
        assert iv > 0

    def test_strike_beyond_threshold_returns_none(self) -> None:
        from backtestforecast.backtests.strategies.common import _estimate_iv_for_strike
        from backtestforecast.market_data.types import OptionContractRecord

        trade_date = date(2025, 5, 1)
        contract = OptionContractRecord("C100", "call", date(2025, 6, 1), 100.0, 100)

        class FakeGW:
            def get_quote(self, ticker, dt):
                return None

        iv = _estimate_iv_for_strike(
            strike=100.01,
            contract_type="call",
            underlying_close=100.0,
            dte_days=31,
            contracts=[contract],
            option_gateway=FakeGW(),
            trade_date=trade_date,
        )
        assert iv is None, "Strike 0.01 away exceeds 0.005 threshold"


# ---------------------------------------------------------------------------
# Item 73: Iron condor allows zero-credit entries
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Item 99: offset_strike doesn't return phantom strikes
# ---------------------------------------------------------------------------


class TestOffsetStrikeNoPhantom:
    """Verify that offset_strike with a non-listed base strike returns a real
    listed strike, not the phantom base_strike that was temporarily inserted
    for position calculation."""

    def test_non_listed_base_returns_real_strike(self) -> None:
        from backtestforecast.backtests.strategies.common import offset_strike

        strikes = [95.0, 100.0, 105.0, 110.0]
        base_strike = 102.5  # Not in the listed strikes

        result = offset_strike(strikes, base_strike, 1)
        assert result is not None
        assert result in strikes, (
            f"offset_strike returned {result} which is not a listed strike. "
            f"It must return a real listed strike, not the phantom {base_strike}."
        )
        assert result == 105.0

    def test_non_listed_base_step_minus_one(self) -> None:
        from backtestforecast.backtests.strategies.common import offset_strike

        strikes = [95.0, 100.0, 105.0, 110.0]
        base_strike = 102.5

        result = offset_strike(strikes, base_strike, -1)
        assert result is not None
        assert result in strikes
        assert result == 100.0

    def test_phantom_at_exact_offset_returns_none(self) -> None:
        """If stepping exactly lands on the phantom base_strike, return None."""
        from backtestforecast.backtests.strategies.common import offset_strike

        strikes = [95.0, 105.0]
        base_strike = 100.0  # Not listed, sits between 95 and 105

        result_up = offset_strike(strikes, base_strike, 0)
        assert result_up is None, (
            "Stepping 0 from a non-listed base should return None (phantom)"
        )

    def test_listed_base_works_normally(self) -> None:
        from backtestforecast.backtests.strategies.common import offset_strike

        strikes = [95.0, 100.0, 105.0, 110.0]
        result = offset_strike(strikes, 100.0, 1)
        assert result == 105.0

        result_neg = offset_strike(strikes, 100.0, -1)
        assert result_neg == 95.0


# ---------------------------------------------------------------------------
# Item 59: Slippage per-leg for iron condor (4 legs)
# ---------------------------------------------------------------------------


class TestIronCondorSlippageGrossNotional:
    """Verify slippage is computed on gross notional (sum of abs leg values),
    not on net premium."""

    def test_slippage_on_gross_not_net(self) -> None:
        engine = OptionsBacktestEngine()
        entry_date = date(2025, 5, 2)
        expiration = date(2025, 5, 5)
        bars = [
            make_bar(date(2025, 5, 1), 100),
            make_bar(entry_date, 100),
            make_bar(date(2025, 5, 3), 100),
            make_bar(date(2025, 5, 4), 100),
            make_bar(expiration, 100),
        ]
        contracts = {
            (entry_date, "call"): [
                OptionContractRecord("C100", "call", expiration, 100, 100),
                OptionContractRecord("C105", "call", expiration, 105, 100),
            ],
            (entry_date, "put"): [
                OptionContractRecord("P95", "put", expiration, 95, 100),
                OptionContractRecord("P100", "put", expiration, 100, 100),
            ],
        }
        quotes = {
            ("C100", entry_date): make_quote(entry_date, 3.0),
            ("C105", entry_date): make_quote(entry_date, 1.0),
            ("P100", entry_date): make_quote(entry_date, 3.0),
            ("P95", entry_date): make_quote(entry_date, 1.0),
        }
        slippage_pct = 1.0
        result_with_slippage = engine.run(
            BacktestConfig(
                symbol="SPY",
                strategy_type="iron_condor",
                start_date=date(2025, 5, 1),
                end_date=date(2025, 5, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=0,
                slippage_pct=slippage_pct,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )
        result_no_slippage = engine.run(
            BacktestConfig(
                symbol="SPY",
                strategy_type="iron_condor",
                start_date=date(2025, 5, 1),
                end_date=date(2025, 5, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=0,
                slippage_pct=0.0,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        if result_with_slippage.trades and result_no_slippage.trades:
            trade_slip = result_with_slippage.trades[0]
            trade_no = result_no_slippage.trades[0]
            assert trade_slip.net_pnl <= trade_no.net_pnl, (
                "With slippage, net PnL should be less than or equal to without slippage"
            )


class TestIronCondorZeroCreditEntry:
    """Iron condor with max_profit_per_unit == 0 should not be rejected (not None)."""

    def test_zero_credit_allowed(self) -> None:
        engine = OptionsBacktestEngine()
        entry_date = date(2025, 5, 2)
        expiration = date(2025, 5, 5)
        bars = [
            make_bar(date(2025, 5, 1), 100),
            make_bar(entry_date, 100),
            make_bar(date(2025, 5, 3), 100),
            make_bar(date(2025, 5, 4), 100),
            make_bar(expiration, 100),
        ]
        contracts = {
            (entry_date, "call"): [
                OptionContractRecord("C100", "call", expiration, 100, 100),
                OptionContractRecord("C105", "call", expiration, 105, 100),
            ],
            (entry_date, "put"): [
                OptionContractRecord("P95", "put", expiration, 95, 100),
                OptionContractRecord("P100", "put", expiration, 100, 100),
            ],
        }
        quotes = {
            ("C100", entry_date): make_quote(entry_date, 2.5),
            ("C105", entry_date): make_quote(entry_date, 2.5),
            ("P100", entry_date): make_quote(entry_date, 2.5),
            ("P95", entry_date): make_quote(entry_date, 2.5),
        }
        result = engine.run(
            BacktestConfig(
                symbol="SPY",
                strategy_type="iron_condor",
                start_date=date(2025, 5, 1),
                end_date=date(2025, 5, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=0,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary is not None

