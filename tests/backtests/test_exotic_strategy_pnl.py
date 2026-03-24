"""Exotic strategy tests beyond the parametrized smoke test.

Covers interface conformance, losing-trade PnL correctness for jade_lizard
and iron_butterfly, and edge cases not in test_all_strategy_pnl.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.strategies.exotic import (
    IRON_BUTTERFLY_STRATEGY,
    JADE_LIZARD_STRATEGY,
    IronButterflyStrategy,
    JadeLizardStrategy,
)
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord


def _bar(trade_date: date, close: float) -> DailyBar:
    return DailyBar(
        trade_date=trade_date, open_price=close, high_price=close,
        low_price=close, close_price=close, volume=1_000_000,
    )


def _quote(trade_date: date, mid: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None)


@dataclass
class SimpleGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]

    def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
        return self.contracts.get((entry_date, contract_type), [])

    def get_quote(self, option_ticker, trade_date):
        return self.quotes.get((option_ticker, trade_date))

    def get_chain_delta_lookup(self, contracts):
        return {}


_ENTRY = date(2025, 4, 2)
_MID = date(2025, 4, 3)
_EXP = date(2025, 4, 5)
_COMM = Decimal("0.65")
_COMM_F = float(_COMM)


def _cfg(strategy_type: str) -> BacktestConfig:
    return BacktestConfig(
        symbol="AAPL",
        strategy_type=strategy_type,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=30,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("50"),
        commission_per_contract=_COMM,
        entry_rules=[],
    )


# =====================================================================
# Interface conformance
# =====================================================================


class TestJadeLizardInterface:
    def test_strategy_type_attribute(self):
        assert JADE_LIZARD_STRATEGY.strategy_type == "jade_lizard"

    def test_has_build_position_method(self):
        assert hasattr(JadeLizardStrategy, "build_position")
        assert callable(JadeLizardStrategy.build_position)

    def test_has_margin_warning(self):
        assert JADE_LIZARD_STRATEGY.margin_warning_message is not None

    def test_has_all_protocol_attributes(self):
        assert hasattr(JADE_LIZARD_STRATEGY, "strategy_type")
        assert hasattr(JADE_LIZARD_STRATEGY, "margin_warning_message")
        assert hasattr(JADE_LIZARD_STRATEGY, "build_position")


class TestIronButterflyInterface:
    def test_strategy_type_attribute(self):
        assert IRON_BUTTERFLY_STRATEGY.strategy_type == "iron_butterfly"

    def test_has_build_position_method(self):
        assert hasattr(IronButterflyStrategy, "build_position")
        assert callable(IronButterflyStrategy.build_position)

    def test_has_margin_warning(self):
        assert IRON_BUTTERFLY_STRATEGY.margin_warning_message is not None

    def test_has_all_protocol_attributes(self):
        assert hasattr(IRON_BUTTERFLY_STRATEGY, "strategy_type")
        assert hasattr(IRON_BUTTERFLY_STRATEGY, "margin_warning_message")
        assert hasattr(IRON_BUTTERFLY_STRATEGY, "build_position")


# =====================================================================
# Jade Lizard losing trade (put side)
# =====================================================================


class TestJadeLizardLosingPnl:
    """Underlying drops to 80 -- short put goes deep ITM.

    Short P95 at 2.00, Short C105 at 2.50, Long C110 at 1.00.
    Exit at expiration with underlying at 80.
    """

    def test_losing_trade(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 90), _bar(_EXP, 80)]
        contracts = {
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
            (_ENTRY, "call"): [
                OptionContractRecord("C105", "call", _EXP, 105, 100),
                OptionContractRecord("C110", "call", _EXP, 110, 100),
            ],
        }
        quotes = {
            ("P95", _ENTRY): _quote(_ENTRY, 2.00),
            ("C105", _ENTRY): _quote(_ENTRY, 2.50), ("C110", _ENTRY): _quote(_ENTRY, 1.00),
            ("P95", _MID): _quote(_MID, 7.00),
            ("C105", _MID): _quote(_MID, 0.10), ("C110", _MID): _quote(_MID, 0.05),
            ("P95", _EXP): _quote(_EXP, 15.00),
            ("C105", _EXP): _quote(_EXP, 0.01), ("C110", _EXP): _quote(_EXP, 0.01),
        }
        result = OptionsBacktestEngine().run(
            _cfg("jade_lizard"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert float(t.net_pnl) < 0, f"Expected losing trade, got net_pnl={t.net_pnl}"
        assert t.quantity >= 1, "Must have at least 1 unit"

        detail = t.detail_json
        entry_credit = detail["total_credit"]
        assert entry_credit > 0, "Jade lizard should open with a net credit"

        assert float(t.gross_pnl) < -entry_credit * t.quantity, (
            "Gross loss must exceed the initial credit received"
        )


# =====================================================================
# Iron Butterfly losing trade
# =====================================================================


class TestIronButterflyLosingPnl:
    """Underlying breaks through put wing to 88.

    Buy P95 at 0.50, Sell P100 at 2.00, Sell C100 at 2.00, Buy C105 at 0.50.
    Credit = (2+2-0.5-0.5)*100 = 300 per unit.
    Max loss = wing_width(500) - credit(300) = 200 per unit + commissions.
    """

    def test_losing_trade(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 92), _bar(_EXP, 88)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C100", "call", _EXP, 100, 100),
                OptionContractRecord("C105", "call", _EXP, 105, 100),
            ],
            (_ENTRY, "put"): [
                OptionContractRecord("P95", "put", _EXP, 95, 100),
                OptionContractRecord("P100", "put", _EXP, 100, 100),
            ],
        }
        quotes = {
            ("P95", _ENTRY): _quote(_ENTRY, 0.50), ("P100", _ENTRY): _quote(_ENTRY, 2.00),
            ("C100", _ENTRY): _quote(_ENTRY, 2.00), ("C105", _ENTRY): _quote(_ENTRY, 0.50),
            ("P95", _MID): _quote(_MID, 4.00), ("P100", _MID): _quote(_MID, 8.50),
            ("C100", _MID): _quote(_MID, 0.10), ("C105", _MID): _quote(_MID, 0.01),
            ("P95", _EXP): _quote(_EXP, 7.0), ("P100", _EXP): _quote(_EXP, 12.0),
            ("C100", _EXP): _quote(_EXP, 0.01), ("C105", _EXP): _quote(_EXP, 0.01),
        }
        result = OptionsBacktestEngine().run(
            _cfg("iron_butterfly"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]

        assert float(t.net_pnl) < 0, f"Expected losing trade, got net_pnl={t.net_pnl}"
        assert t.quantity >= 1

        detail = t.detail_json
        credit = detail["credit"]
        assert credit > 0, "Iron butterfly should open with a net credit"

        wing_width = 5
        max_loss_per_unit = (wing_width * 100 - credit) + _COMM_F * 4 * 2
        assert float(t.net_pnl) >= -(max_loss_per_unit * t.quantity) - 1, (
            "Loss exceeds theoretical max"
        )

        assert float(t.gross_pnl) < 0, "Gross PnL should be negative on a losing trade"


class TestJadeLizardCallSideLoss:
    """Underlying rises through the call spread -- call wing risk materialises.

    Short P95 at 2.00, Short C105 at 2.50, Long C110 at 1.00.
    Underlying at 115: calls deep ITM, put expires near worthless.
    """

    def test_call_side_loss(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 110), _bar(_EXP, 115)]
        contracts = {
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
            (_ENTRY, "call"): [
                OptionContractRecord("C105", "call", _EXP, 105, 100),
                OptionContractRecord("C110", "call", _EXP, 110, 100),
            ],
        }
        quotes = {
            ("P95", _ENTRY): _quote(_ENTRY, 2.00),
            ("C105", _ENTRY): _quote(_ENTRY, 2.50), ("C110", _ENTRY): _quote(_ENTRY, 1.00),
            ("P95", _MID): _quote(_MID, 0.10),
            ("C105", _MID): _quote(_MID, 6.00), ("C110", _MID): _quote(_MID, 3.00),
            ("P95", _EXP): _quote(_EXP, 0.01),
            ("C105", _EXP): _quote(_EXP, 10.0), ("C110", _EXP): _quote(_EXP, 5.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("jade_lizard"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert float(t.net_pnl) < 0, f"Expected losing trade, got net_pnl={t.net_pnl}"

        detail = t.detail_json
        detail["total_credit"]
        call_spread_width = (detail["call_long_strike"] - detail["call_short_strike"]) * 100
        upside_risk = detail["upside_risk"]

        assert upside_risk > 0, "Jade lizard should have upside risk through the call spread"
        assert float(t.gross_pnl) < 0, "Gross should be negative when call wing breaches"

        max_call_loss = call_spread_width + _COMM_F * 3 * 2
        assert abs(float(t.net_pnl)) <= (max_call_loss) * t.quantity, (
            "Loss on call side should not exceed spread width + commissions"
        )
