"""Hand-verified P&L correctness tests for all strategy types NOT covered by test_strategy_pnl.py.

test_strategy_pnl.py covers: long_call, long_put.
This file covers every remaining registered strategy with at least one profitable-trade test each.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from backtestforecast.backtests.engine import OptionsBacktestEngine
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
_FAR_EXP = date(2025, 4, 19)
_COMM = 0.65


def _cfg(strategy_type: str, *, target_dte: int = 30) -> BacktestConfig:
    return BacktestConfig(
        symbol="AAPL",
        strategy_type=strategy_type,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
        target_dte=target_dte,
        dte_tolerance_days=30,
        max_holding_days=30,
        account_size=100_000,
        risk_per_trade_pct=50,
        commission_per_contract=_COMM,
        entry_rules=[],
    )


# =====================================================================
# 2-leg credit spreads
# =====================================================================


class TestBullPutCreditSpreadPnl:
    """Hand-verified P&L for bull_put_credit_spread.

    Sell P100 at 2.50, Buy P95 at 1.00.  Underlying stays above 100.
    entry_vpu = (1*1.00 − 1*2.50)*100 = −150  (net credit $150)
    exit_vpu  = 0  (both expire worthless)
    Gross PnL/unit = 0 − (−150) = +150
    Commission/unit = 0.65 × 2 legs × 2 (entry+exit) = 2.60
    Net PnL/unit = 147.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 101), _bar(_EXP, 102)]
        contracts = {
            (_ENTRY, "put"): [
                OptionContractRecord("P95", "put", _EXP, 95, 100),
                OptionContractRecord("P100", "put", _EXP, 100, 100),
            ],
        }
        quotes = {
            ("P95", _ENTRY): _quote(_ENTRY, 1.00), ("P100", _ENTRY): _quote(_ENTRY, 2.50),
            ("P95", _MID): _quote(_MID, 0.50), ("P100", _MID): _quote(_MID, 1.25),
            ("P95", _EXP): _quote(_EXP, 0.0), ("P100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("bull_put_credit_spread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1.00 - 2.50) * 100
        exit_vpu = 0.0
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        expected_net = expected_gross - expected_comm
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - expected_net) < 0.01


class TestBearCallCreditSpreadPnl:
    """Hand-verified P&L for bear_call_credit_spread.

    Sell C100 at 3.00, Buy C105 at 1.00.  Underlying stays below 100.
    entry_vpu = (1*1.00 − 1*3.00)*100 = −200  (net credit $200)
    exit_vpu  = 0
    Gross PnL/unit = +200
    Commission/unit = 2.60
    Net PnL/unit = 197.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 99), _bar(_EXP, 98)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C100", "call", _EXP, 100, 100),
                OptionContractRecord("C105", "call", _EXP, 105, 100),
            ],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.00), ("C105", _ENTRY): _quote(_ENTRY, 1.00),
            ("C100", _MID): _quote(_MID, 1.50), ("C105", _MID): _quote(_MID, 0.50),
            ("C100", _EXP): _quote(_EXP, 0.0), ("C105", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("bear_call_credit_spread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1.00 - 3.00) * 100
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# 2-leg debit spreads
# =====================================================================


class TestBullCallDebitSpreadPnl:
    """Hand-verified P&L for bull_call_debit_spread.

    Buy C100 at 3.00, Sell C105 at 1.00.  Underlying rises to 110.
    entry_vpu = (1*3.00 − 1*1.00)*100 = 200  (debit)
    exit_vpu  = (10 − 5)*100 = 500  (both ITM at expiration)
    Gross PnL/unit = 500 − 200 = 300
    Commission/unit = 2.60
    Net PnL/unit = 297.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 105), _bar(_EXP, 110)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C100", "call", _EXP, 100, 100),
                OptionContractRecord("C105", "call", _EXP, 105, 100),
            ],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.00), ("C105", _ENTRY): _quote(_ENTRY, 1.00),
            ("C100", _MID): _quote(_MID, 6.50), ("C105", _MID): _quote(_MID, 4.00),
            ("C100", _EXP): _quote(_EXP, 10.0), ("C105", _EXP): _quote(_EXP, 5.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("bull_call_debit_spread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (3.00 - 1.00) * 100
        exit_vpu = (10.0 - 5.0) * 100
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestBearPutDebitSpreadPnl:
    """Hand-verified P&L for bear_put_debit_spread.

    Buy P105 at 4.00, Sell P100 at 1.50.  Underlying drops from 105 to 90.
    entry_vpu = (1*4.00 − 1*1.50)*100 = 250  (debit)
    exit_vpu  = (15 − 10)*100 = 500  (P105 intrinsic=15, P100 intrinsic=10)
    Gross PnL/unit = 500 − 250 = 250  (= width $500 − debit $250)
    Commission/unit = 2.60
    Net PnL/unit = 247.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 106), _bar(_ENTRY, 105), _bar(_MID, 97), _bar(_EXP, 90)]
        contracts = {
            (_ENTRY, "put"): [
                OptionContractRecord("P100", "put", _EXP, 100, 100),
                OptionContractRecord("P105", "put", _EXP, 105, 100),
            ],
        }
        quotes = {
            ("P105", _ENTRY): _quote(_ENTRY, 4.00), ("P100", _ENTRY): _quote(_ENTRY, 1.50),
            ("P105", _MID): _quote(_MID, 9.50), ("P100", _MID): _quote(_MID, 5.50),
            ("P105", _EXP): _quote(_EXP, 15.0), ("P100", _EXP): _quote(_EXP, 10.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("bear_put_debit_spread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (4.00 - 1.50) * 100
        exit_vpu = (15.0 - 10.0) * 100
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Multi-leg
# =====================================================================


class TestIronCondorPnl:
    """Hand-verified P&L for iron_condor (4 legs).

    Long P90 at 0.30, Short P95 at 1.00, Short C105 at 1.00, Long C110 at 0.30.
    Underlying stays at 100 — all expire worthless.
    entry_vpu = (0.30 + 0.30 − 1.00 − 1.00)*100 = −140  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +140
    Commission/unit = 0.65 × 4 × 2 = 5.20
    Net PnL/unit = 134.80
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "put"): [
                OptionContractRecord("P90", "put", _EXP, 90, 100),
                OptionContractRecord("P95", "put", _EXP, 95, 100),
            ],
            (_ENTRY, "call"): [
                OptionContractRecord("C105", "call", _EXP, 105, 100),
                OptionContractRecord("C110", "call", _EXP, 110, 100),
            ],
        }
        quotes = {
            ("P90", _ENTRY): _quote(_ENTRY, 0.30), ("P95", _ENTRY): _quote(_ENTRY, 1.00),
            ("C105", _ENTRY): _quote(_ENTRY, 1.00), ("C110", _ENTRY): _quote(_ENTRY, 0.30),
            ("P90", _MID): _quote(_MID, 0.15), ("P95", _MID): _quote(_MID, 0.50),
            ("C105", _MID): _quote(_MID, 0.50), ("C110", _MID): _quote(_MID, 0.15),
            ("P90", _EXP): _quote(_EXP, 0.0), ("P95", _EXP): _quote(_EXP, 0.0),
            ("C105", _EXP): _quote(_EXP, 0.0), ("C110", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("iron_condor"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (0.30 + 0.30 - 1.00 - 1.00) * 100  # long_put + long_call - short_put - short_call
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 4 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestButterflyPnl:
    """Hand-verified P&L for butterfly (3 contracts, 4 option-units).

    Buy C95 at 6.00, Sell 2×C100 at 3.50, Buy C105 at 1.50.
    Underlying pins at 100 — max profit scenario.
    entry_vpu = (1*6.00 + 1*1.50 − 2*3.50)*100 = 50  (debit)
    exit_vpu  = (1*5 + 1*0 − 2*0)*100 = 500  (C95 intrinsic=5, rest=0)
    Gross PnL/unit = 500 − 50 = 450
    Commission/unit = 0.65 × 4 (1+2+1 contracts) × 2 = 5.20
    Net PnL/unit = 444.80
    """

    def test_max_profit_at_center(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C95", "call", _EXP, 95, 100),
                OptionContractRecord("C100", "call", _EXP, 100, 100),
                OptionContractRecord("C105", "call", _EXP, 105, 100),
            ],
        }
        quotes = {
            ("C95", _ENTRY): _quote(_ENTRY, 6.00),
            ("C100", _ENTRY): _quote(_ENTRY, 3.50),
            ("C105", _ENTRY): _quote(_ENTRY, 1.50),
            ("C95", _MID): _quote(_MID, 5.50),
            ("C100", _MID): _quote(_MID, 2.50),
            ("C105", _MID): _quote(_MID, 0.50),
            ("C95", _EXP): _quote(_EXP, 5.0),
            ("C100", _EXP): _quote(_EXP, 0.0),
            ("C105", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("butterfly"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1 * 6.00 + 1 * 1.50 - 2 * 3.50) * 100  # 50
        exit_vpu = (1 * 5.0 + 1 * 0.0 - 2 * 0.0) * 100  # 500
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 4 * t.quantity * 2  # 1+2+1 = 4 contracts
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestIronButterflyPnl:
    """Hand-verified P&L for iron_butterfly (4 legs).

    Buy P95 at 0.50, Sell P100 at 3.00, Sell C100 at 3.00, Buy C105 at 0.50.
    Underlying stays at 100 — max profit (all ATM options expire at 0).
    entry_vpu = (0.50 + 0.50 − 3.00 − 3.00)*100 = −500  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +500
    Commission/unit = 0.65 × 4 × 2 = 5.20
    Net PnL/unit = 494.80
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
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
            ("P95", _ENTRY): _quote(_ENTRY, 0.50), ("P100", _ENTRY): _quote(_ENTRY, 3.00),
            ("C100", _ENTRY): _quote(_ENTRY, 3.00), ("C105", _ENTRY): _quote(_ENTRY, 0.50),
            ("P95", _MID): _quote(_MID, 0.25), ("P100", _MID): _quote(_MID, 1.50),
            ("C100", _MID): _quote(_MID, 1.50), ("C105", _MID): _quote(_MID, 0.25),
            ("P95", _EXP): _quote(_EXP, 0.0), ("P100", _EXP): _quote(_EXP, 0.0),
            ("C100", _EXP): _quote(_EXP, 0.0), ("C105", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("iron_butterfly"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (0.50 + 0.50 - 3.00 - 3.00) * 100  # -500
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 4 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Stock + option strategies
# =====================================================================


class TestCoveredCallPnl:
    """Hand-verified P&L for covered_call.

    Long 100 shares at 100, Short C105 at 2.00.  Underlying rises to 105.
    entry_vpu = (−1*2.00*100) + (1*100*100) = 9800
    exit_vpu  = (−1*0*100) + (1*100*105) = 10500  (call expires worthless)
    Gross PnL/unit = 10500 − 9800 = 700
    Commission/unit = 0.65 × 1 × 2 = 1.30  (only option legs)
    Net PnL/unit = 698.70
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 102), _bar(_EXP, 105)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 2.00),
            ("C105", _MID): _quote(_MID, 1.00),
            ("C105", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("covered_call"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 2.00 * 100) + (1 * 100 * 100)  # 9800
        exit_vpu = (-1 * 0.0 * 100) + (1 * 100 * 105)  # 10500
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 1 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestCashSecuredPutPnl:
    """Hand-verified P&L for cash_secured_put.

    Short P100 at 3.00.  Underlying stays above 100, put expires worthless.
    entry_vpu = −1*3.00*100 = −300  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +300
    Commission/unit = 0.65 × 1 × 2 = 1.30
    Net PnL/unit = 298.70
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 101), _bar(_EXP, 102)]
        contracts = {
            (_ENTRY, "put"): [OptionContractRecord("P100", "put", _EXP, 100, 100)],
        }
        quotes = {
            ("P100", _ENTRY): _quote(_ENTRY, 3.00),
            ("P100", _MID): _quote(_MID, 1.50),
            ("P100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("cash_secured_put"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = -1 * 3.00 * 100  # -300
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 1 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestCollarPnl:
    """Hand-verified P&L for collar.

    Long 100 shares at 100, Short C105 at 2.00, Long P95 at 1.50.
    Underlying stays at 100 — both options expire worthless.
    entry_vpu = (−1*2.00*100) + (1*1.50*100) + (1*100*100) = 9950
    exit_vpu  = 0 + 0 + (1*100*100) = 10000
    Gross PnL/unit = 10000 − 9950 = 50
    Commission/unit = 0.65 × 2 × 2 = 2.60  (2 option legs)
    Net PnL/unit = 47.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 2.00), ("P95", _ENTRY): _quote(_ENTRY, 1.50),
            ("C105", _MID): _quote(_MID, 1.50), ("P95", _MID): _quote(_MID, 1.00),
            ("C105", _EXP): _quote(_EXP, 0.0), ("P95", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("collar"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 2.00 * 100) + (1 * 1.50 * 100) + (1 * 100 * 100)  # 9950
        exit_vpu = 0.0 + 0.0 + (1 * 100 * 100)  # 10000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestCoveredStranglePnl:
    """Hand-verified P&L for covered_strangle.

    Long 100 shares at 100, Short C105 at 2.00, Short P95 at 1.50.
    Underlying stays at 100 — both options expire worthless.
    entry_vpu = (−1*2.00*100) + (−1*1.50*100) + (1*100*100) = 9650
    exit_vpu  = 0 + 0 + (1*100*100) = 10000
    Gross PnL/unit = 10000 − 9650 = 350
    Commission/unit = 0.65 × 2 × 2 = 2.60
    Net PnL/unit = 347.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 2.00), ("P95", _ENTRY): _quote(_ENTRY, 1.50),
            ("C105", _MID): _quote(_MID, 1.50), ("P95", _MID): _quote(_MID, 1.00),
            ("C105", _EXP): _quote(_EXP, 0.0), ("P95", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("covered_strangle"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 2.00 * 100) + (-1 * 1.50 * 100) + (1 * 100 * 100)  # 9650
        exit_vpu = 0.0 + 0.0 + (1 * 100 * 100)  # 10000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Naked options
# =====================================================================


class TestNakedCallPnl:
    """Hand-verified P&L for naked_call.

    Short C105 at 2.00.  Underlying stays at 100, call expires worthless.
    entry_vpu = −1*2.00*100 = −200  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +200
    Commission/unit = 0.65 × 1 × 2 = 1.30
    Net PnL/unit = 198.70
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 2.00),
            ("C105", _MID): _quote(_MID, 1.00),
            ("C105", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("naked_call"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = -1 * 2.00 * 100
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 1 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestNakedPutPnl:
    """Hand-verified P&L for naked_put.

    Short P95 at 2.00.  Underlying stays at 100, put expires worthless.
    entry_vpu = −1*2.00*100 = −200  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +200
    Commission/unit = 1.30
    Net PnL/unit = 198.70
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
        }
        quotes = {
            ("P95", _ENTRY): _quote(_ENTRY, 2.00),
            ("P95", _MID): _quote(_MID, 1.00),
            ("P95", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("naked_put"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = -1 * 2.00 * 100
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 1 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Straddle / strangle
# =====================================================================


class TestShortStraddlePnl:
    """Hand-verified P&L for short_straddle.

    Short C100 at 3.00, Short P100 at 3.00.  Underlying stays at 100.
    entry_vpu = (−1*3.00 + −1*3.00)*100 = −600  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +600
    Commission/unit = 0.65 × 2 × 2 = 2.60
    Net PnL/unit = 597.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C100", "call", _EXP, 100, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P100", "put", _EXP, 100, 100)],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.00), ("P100", _ENTRY): _quote(_ENTRY, 3.00),
            ("C100", _MID): _quote(_MID, 1.50), ("P100", _MID): _quote(_MID, 1.50),
            ("C100", _EXP): _quote(_EXP, 0.0), ("P100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("short_straddle"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 3.00 + -1 * 3.00) * 100
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestShortStranglePnl:
    """Hand-verified P&L for short_strangle.

    Short C105 at 2.00, Short P95 at 2.00.  Underlying stays at 100.
    entry_vpu = (−1*2.00 + −1*2.00)*100 = −400  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +400
    Commission/unit = 2.60
    Net PnL/unit = 397.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 2.00), ("P95", _ENTRY): _quote(_ENTRY, 2.00),
            ("C105", _MID): _quote(_MID, 1.00), ("P95", _MID): _quote(_MID, 1.00),
            ("C105", _EXP): _quote(_EXP, 0.0), ("P95", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("short_strangle"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 2.00 + -1 * 2.00) * 100
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestLongStraddlePnl:
    """Hand-verified P&L for long_straddle.

    Long C100 at 3.00, Long P100 at 3.00.  Underlying moves to 110.
    entry_vpu = (1*3.00 + 1*3.00)*100 = 600  (debit)
    exit_vpu  = (10 + 0)*100 = 1000  (C100 intrinsic=10, P100=0)
    Gross PnL/unit = 1000 − 600 = 400
    Commission/unit = 2.60
    Net PnL/unit = 397.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 105), _bar(_EXP, 110)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C100", "call", _EXP, 100, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P100", "put", _EXP, 100, 100)],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.00), ("P100", _ENTRY): _quote(_ENTRY, 3.00),
            ("C100", _MID): _quote(_MID, 6.00), ("P100", _MID): _quote(_MID, 1.00),
            ("C100", _EXP): _quote(_EXP, 10.0), ("P100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("long_straddle"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (3.00 + 3.00) * 100  # 600
        exit_vpu = (10.0 + 0.0) * 100  # 1000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestLongStranglePnl:
    """Hand-verified P&L for long_strangle.

    Long C105 at 1.50, Long P95 at 1.50.  Underlying drops to 85.
    entry_vpu = (1*1.50 + 1*1.50)*100 = 300  (debit)
    exit_vpu  = (0 + 10)*100 = 1000  (C105=0, P95 intrinsic=10)
    Gross PnL/unit = 1000 − 300 = 700
    Commission/unit = 2.60
    Net PnL/unit = 697.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 101), _bar(_ENTRY, 100), _bar(_MID, 92), _bar(_EXP, 85)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C105", "call", _EXP, 105, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P95", "put", _EXP, 95, 100)],
        }
        quotes = {
            ("C105", _ENTRY): _quote(_ENTRY, 1.50), ("P95", _ENTRY): _quote(_ENTRY, 1.50),
            ("C105", _MID): _quote(_MID, 0.50), ("P95", _MID): _quote(_MID, 5.00),
            ("C105", _EXP): _quote(_EXP, 0.0), ("P95", _EXP): _quote(_EXP, 10.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("long_strangle"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1.50 + 1.50) * 100
        exit_vpu = (0.0 + 10.0) * 100
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Calendar / diagonal
# =====================================================================


class TestCalendarSpreadPnl:
    """Hand-verified P&L for calendar_spread.

    Long C100 far (exp 4/19, mid 5.00), Short C100 near (exp 4/5, mid 2.00).
    At near expiration: near leg expires worthless, far leg retains time value.
    entry_vpu = (1*5.00 − 1*2.00)*100 = 300  (debit)
    exit_vpu  = (1*3.50 − 1*0)*100 = 350  (far leg has residual value)
    Gross PnL/unit = 350 − 300 = 50
    Commission/unit = 0.65 × 2 × 2 = 2.60
    Net PnL/unit = 47.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C100_NEAR", "call", _EXP, 100, 100),
                OptionContractRecord("C100_FAR", "call", _FAR_EXP, 100, 100),
            ],
        }
        quotes = {
            ("C100_FAR", _ENTRY): _quote(_ENTRY, 5.00),
            ("C100_NEAR", _ENTRY): _quote(_ENTRY, 2.00),
            ("C100_FAR", _MID): _quote(_MID, 4.00),
            ("C100_NEAR", _MID): _quote(_MID, 1.00),
            ("C100_FAR", _EXP): _quote(_EXP, 3.50),
            ("C100_NEAR", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("calendar_spread", target_dte=3), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (5.00 - 2.00) * 100  # 300
        exit_vpu = (3.50 - 0.0) * 100  # 350
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestPoorMansCoveredCallPnl:
    """Hand-verified P&L for poor_mans_covered_call (PMCC).

    Long deep-ITM C90 far (exp 4/19, mid 12.00), Short OTM C105 near (exp 4/5, mid 1.50).
    At near expiration (underlying=100): short expires worthless, long retains value.
    entry_vpu = (1*12.00 − 1*1.50)*100 = 1050  (debit)
    exit_vpu  = (1*13.00 − 1*0)*100 = 1300  (C90 far quoted at 13.00)
    Gross PnL/unit = 1300 − 1050 = 250
    Commission/unit = 0.65 × 2 × 2 = 2.60
    Net PnL/unit = 247.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C105_NEAR", "call", _EXP, 105, 100),
                OptionContractRecord("C90_FAR", "call", _FAR_EXP, 90, 100),
                OptionContractRecord("C95_FAR", "call", _FAR_EXP, 95, 100),
                OptionContractRecord("C100_FAR", "call", _FAR_EXP, 100, 100),
            ],
        }
        quotes = {
            ("C90_FAR", _ENTRY): _quote(_ENTRY, 12.00),
            ("C105_NEAR", _ENTRY): _quote(_ENTRY, 1.50),
            ("C90_FAR", _MID): _quote(_MID, 12.50),
            ("C105_NEAR", _MID): _quote(_MID, 0.75),
            ("C90_FAR", _EXP): _quote(_EXP, 13.00),
            ("C105_NEAR", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("poor_mans_covered_call", target_dte=3), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (12.00 - 1.50) * 100  # 1050
        exit_vpu = (13.00 - 0.0) * 100  # 1300
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Ratio spreads
# =====================================================================


class TestRatioCallBackspreadPnl:
    """Hand-verified P&L for ratio_call_backspread.

    Short 1×C100 at 3.50, Long 2×C105 at 2.00.  Underlying rises to 115.
    entry_vpu = (−1*1*3.50 + 1*2*2.00)*100 = 50  (small debit)
    exit_vpu  = (−1*1*15 + 1*2*10)*100 = 500
    Gross PnL/unit = 500 − 50 = 450
    Commission/unit = 0.65 × 3 (1+2 contracts) × 2 = 3.90
    Net PnL/unit = 446.10
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 108), _bar(_EXP, 115)]
        contracts = {
            (_ENTRY, "call"): [
                OptionContractRecord("C100", "call", _EXP, 100, 100),
                OptionContractRecord("C105", "call", _EXP, 105, 100),
            ],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.50), ("C105", _ENTRY): _quote(_ENTRY, 2.00),
            ("C100", _MID): _quote(_MID, 8.50), ("C105", _MID): _quote(_MID, 5.50),
            ("C100", _EXP): _quote(_EXP, 15.0), ("C105", _EXP): _quote(_EXP, 10.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("ratio_call_backspread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 1 * 3.50 + 1 * 2 * 2.00) * 100  # 50
        exit_vpu = (-1 * 1 * 15.0 + 1 * 2 * 10.0) * 100  # 500
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 3 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestRatioPutBackspreadPnl:
    """Hand-verified P&L for ratio_put_backspread.

    Short 1×P100 at 3.50, Long 2×P95 at 2.00.  Underlying drops to 80.
    entry_vpu = (−1*1*3.50 + 1*2*2.00)*100 = 50  (small debit)
    exit_vpu  = (−1*1*20 + 1*2*15)*100 = 1000  (P100 intrinsic=20, P95=15)
    Gross PnL/unit = 1000 − 50 = 950
    Commission/unit = 0.65 × 3 × 2 = 3.90
    Net PnL/unit = 946.10
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 101), _bar(_ENTRY, 100), _bar(_MID, 90), _bar(_EXP, 80)]
        contracts = {
            (_ENTRY, "put"): [
                OptionContractRecord("P95", "put", _EXP, 95, 100),
                OptionContractRecord("P100", "put", _EXP, 100, 100),
            ],
        }
        quotes = {
            ("P100", _ENTRY): _quote(_ENTRY, 3.50), ("P95", _ENTRY): _quote(_ENTRY, 2.00),
            ("P100", _MID): _quote(_MID, 10.50), ("P95", _MID): _quote(_MID, 8.50),
            ("P100", _EXP): _quote(_EXP, 20.0), ("P95", _EXP): _quote(_EXP, 15.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("ratio_put_backspread"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 1 * 3.50 + 1 * 2 * 2.00) * 100  # 50
        exit_vpu = (-1 * 1 * 20.0 + 1 * 2 * 15.0) * 100  # 1000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 3 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


# =====================================================================
# Exotic
# =====================================================================


class TestJadeLizardPnl:
    """Hand-verified P&L for jade_lizard.

    Short P95 at 2.00 + Short C105 at 2.50 + Long C110 at 1.00.
    Underlying stays at 100 — all expire worthless.
    entry_vpu = (−1*2.00 + −1*2.50 + 1*1.00)*100 = −350  (credit)
    exit_vpu  = 0
    Gross PnL/unit = +350
    Commission/unit = 0.65 × 3 × 2 = 3.90
    Net PnL/unit = 346.10
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
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
            ("P95", _MID): _quote(_MID, 1.00),
            ("C105", _MID): _quote(_MID, 1.25), ("C110", _MID): _quote(_MID, 0.50),
            ("P95", _EXP): _quote(_EXP, 0.0),
            ("C105", _EXP): _quote(_EXP, 0.0), ("C110", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("jade_lizard"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (-1 * 2.00 + -1 * 2.50 + 1 * 1.00) * 100  # -350
        expected_gross = (0.0 - entry_vpu) * t.quantity
        expected_comm = _COMM * 3 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestSyntheticPutPnl:
    """Hand-verified P&L for synthetic_put.

    Short 100 shares at 100, Long C100 at 3.00.  Underlying drops to 90.
    entry_vpu = (1*3.00*100) + (−1*100*100) = −9700
    exit_vpu  = (1*0*100) + (−1*100*90) = −9000  (call worthless, stock at 90)
    Gross PnL/unit = −9000 − (−9700) = 700
    Commission/unit = 0.65 × 1 × 2 = 1.30  (only option leg)
    Net PnL/unit = 698.70
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 101), _bar(_ENTRY, 100), _bar(_MID, 95), _bar(_EXP, 90)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C100", "call", _EXP, 100, 100)],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 3.00),
            ("C100", _MID): _quote(_MID, 1.50),
            ("C100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("synthetic_put"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1 * 3.00 * 100) + (-1 * 100 * 100)  # -9700
        exit_vpu = (1 * 0.0 * 100) + (-1 * 100 * 90)  # -9000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 1 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01


class TestReverseConversionPnl:
    """Hand-verified P&L for reverse_conversion.

    Short 100 shares at 100, Long C100 at 2.50, Short P100 at 3.00.
    Underlying stays at 100 — options expire worthless, small net credit.
    entry_vpu = (1*2.50*100) + (−1*3.00*100) + (−1*100*100) = −10050
    exit_vpu  = 0 + 0 + (−1*100*100) = −10000
    Gross PnL/unit = −10000 − (−10050) = 50
    Commission/unit = 0.65 × 2 × 2 = 2.60  (2 option legs)
    Net PnL/unit = 47.40
    """

    def test_profitable(self):
        bars = [_bar(date(2025, 4, 1), 99), _bar(_ENTRY, 100), _bar(_MID, 100), _bar(_EXP, 100)]
        contracts = {
            (_ENTRY, "call"): [OptionContractRecord("C100", "call", _EXP, 100, 100)],
            (_ENTRY, "put"): [OptionContractRecord("P100", "put", _EXP, 100, 100)],
        }
        quotes = {
            ("C100", _ENTRY): _quote(_ENTRY, 2.50), ("P100", _ENTRY): _quote(_ENTRY, 3.00),
            ("C100", _MID): _quote(_MID, 2.00), ("P100", _MID): _quote(_MID, 2.50),
            ("C100", _EXP): _quote(_EXP, 0.0), ("P100", _EXP): _quote(_EXP, 0.0),
        }
        result = OptionsBacktestEngine().run(
            _cfg("reverse_conversion"), bars, set(),
            SimpleGateway(contracts=contracts, quotes=quotes),
        )
        assert result.summary.trade_count == 1
        t = result.trades[0]
        assert t.quantity >= 1

        entry_vpu = (1 * 2.50 * 100) + (-1 * 3.00 * 100) + (-1 * 100 * 100)  # -10050
        exit_vpu = (1 * 0.0 * 100) + (-1 * 0.0 * 100) + (-1 * 100 * 100)  # -10000
        expected_gross = (exit_vpu - entry_vpu) * t.quantity
        expected_comm = _COMM * 2 * t.quantity * 2
        assert abs(t.gross_pnl - expected_gross) < 0.01
        assert abs(t.total_commissions - expected_comm) < 0.01
        assert abs(t.net_pnl - (expected_gross - expected_comm)) < 0.01
