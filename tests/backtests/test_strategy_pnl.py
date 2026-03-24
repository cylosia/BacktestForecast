"""FIX 79: Hand-verified P&L correctness tests for long_call and long_put strategies."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord


def _bar(trade_date: date, close: float) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        volume=1_000_000,
    )


def _quote(trade_date: date, mid: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None)


@dataclass
class SimpleGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]

    def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
        return self.contracts.get((entry_date, contract_type), [])

    def select_contract(self, entry_date, strategy_type, underlying_close, target_dte, dte_tolerance_days):
        ct = "call" if strategy_type in {"long_call", "covered_call"} else "put"
        contracts = self.list_contracts(entry_date, ct, target_dte, dte_tolerance_days)
        if not contracts:
            from backtestforecast.errors import DataUnavailableError
            raise DataUnavailableError("No contracts")
        return contracts[0]

    def get_quote(self, option_ticker, trade_date):
        return self.quotes.get((option_ticker, trade_date))

    def get_chain_delta_lookup(self, contracts):
        return {}


class TestLongCallPnl:
    """Hand-verified P&L for a long call.

    Setup:
        Buy C100 at mid 3.00 on 2025-04-02
        Underlying rises to 110 at expiration 2025-04-05
        Intrinsic at expiration = max(110 - 100, 0) = 10.00
        Gross PnL per contract = (10.00 - 3.00) * 100 = $700
        Commission = 0.65 * 1 * 2 (entry + exit) = $1.30
        Net PnL = $700 - $1.30 = $698.70
    """

    def test_long_call_profitable_at_expiration(self):
        entry = date(2025, 4, 2)
        expiration = date(2025, 4, 5)
        bars = [
            _bar(date(2025, 4, 1), 99),
            _bar(entry, 100),
            _bar(date(2025, 4, 3), 105),
            _bar(date(2025, 4, 4), 108),
            _bar(expiration, 110),
        ]
        contracts = {
            (entry, "call"): [
                OptionContractRecord("C100", "call", expiration, 100, 100),
            ]
        }
        quotes = {
            ("C100", entry): _quote(entry, 3.0),
            ("C100", date(2025, 4, 3)): _quote(date(2025, 4, 3), 5.0),
            ("C100", expiration): _quote(expiration, 10.0),
        }
        commission = 0.65
        engine = OptionsBacktestEngine()
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_call",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        assert trade.quantity >= 1

        entry_cost = 3.0 * 100 * trade.quantity
        exit_value = 10.0 * 100 * trade.quantity
        expected_gross = exit_value - entry_cost
        expected_comm = commission * trade.quantity * 2
        expected_net = expected_gross - expected_comm

        assert abs(trade.gross_pnl - expected_gross) < 0.01
        assert abs(trade.total_commissions - expected_comm) < 0.01
        assert abs(trade.net_pnl - expected_net) < 0.01

    def test_long_call_expires_worthless(self):
        """Underlying stays below strike - call expires worthless, full premium lost."""
        entry = date(2025, 4, 2)
        expiration = date(2025, 4, 5)
        bars = [
            _bar(date(2025, 4, 1), 99),
            _bar(entry, 100),
            _bar(date(2025, 4, 3), 98),
            _bar(date(2025, 4, 4), 97),
            _bar(expiration, 95),
        ]
        contracts = {
            (entry, "call"): [
                OptionContractRecord("C100", "call", expiration, 100, 100),
            ]
        }
        quotes = {
            ("C100", entry): _quote(entry, 3.0),
            ("C100", date(2025, 4, 3)): _quote(date(2025, 4, 3), 1.0),
            ("C100", expiration): _quote(expiration, 0.0),
        }
        commission = 0.65
        engine = OptionsBacktestEngine()
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_call",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        expected_gross = -3.0 * 100 * trade.quantity
        assert abs(trade.gross_pnl - expected_gross) < 0.01
        assert trade.net_pnl < 0


class TestLongPutPnl:
    """Hand-verified P&L for a long put.

    Setup:
        Buy P100 at mid 2.50 on 2025-04-02
        Underlying drops to 90 at expiration 2025-04-05
        Intrinsic at expiration = max(100 - 90, 0) = 10.00
        Gross PnL per contract = (10.00 - 2.50) * 100 = $750
        Commission = 0.65 * 1 * 2 = $1.30
        Net PnL = $750 - $1.30 = $748.70
    """

    def test_long_put_profitable_at_expiration(self):
        entry = date(2025, 4, 2)
        expiration = date(2025, 4, 5)
        bars = [
            _bar(date(2025, 4, 1), 101),
            _bar(entry, 100),
            _bar(date(2025, 4, 3), 95),
            _bar(date(2025, 4, 4), 92),
            _bar(expiration, 90),
        ]
        contracts = {
            (entry, "put"): [
                OptionContractRecord("P100", "put", expiration, 100, 100),
            ]
        }
        quotes = {
            ("P100", entry): _quote(entry, 2.50),
            ("P100", date(2025, 4, 3)): _quote(date(2025, 4, 3), 6.0),
            ("P100", expiration): _quote(expiration, 10.0),
        }
        commission = 0.65
        engine = OptionsBacktestEngine()
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_put",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count == 1
        trade = result.trades[0]

        entry_cost = 2.50 * 100 * trade.quantity
        exit_value = 10.0 * 100 * trade.quantity
        expected_gross = exit_value - entry_cost
        expected_comm = commission * trade.quantity * 2
        expected_net = expected_gross - expected_comm

        assert abs(trade.gross_pnl - expected_gross) < 0.01
        assert abs(trade.total_commissions - expected_comm) < 0.01
        assert abs(trade.net_pnl - expected_net) < 0.01

    def test_long_put_expires_worthless(self):
        """Underlying rises above strike - put expires worthless, full premium lost."""
        entry = date(2025, 4, 2)
        expiration = date(2025, 4, 5)
        bars = [
            _bar(date(2025, 4, 1), 99),
            _bar(entry, 100),
            _bar(date(2025, 4, 3), 102),
            _bar(date(2025, 4, 4), 105),
            _bar(expiration, 110),
        ]
        contracts = {
            (entry, "put"): [
                OptionContractRecord("P100", "put", expiration, 100, 100),
            ]
        }
        quotes = {
            ("P100", entry): _quote(entry, 2.50),
            ("P100", date(2025, 4, 3)): _quote(date(2025, 4, 3), 1.0),
            ("P100", expiration): _quote(expiration, 0.0),
        }
        commission = 0.65
        engine = OptionsBacktestEngine()
        result = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_put",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=50_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                entry_rules=[],
            ),
            bars,
            set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        expected_gross = -2.50 * 100 * trade.quantity
        assert abs(trade.gross_pnl - expected_gross) < 0.01
        assert trade.net_pnl < 0
