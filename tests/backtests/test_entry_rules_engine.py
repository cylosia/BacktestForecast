"""FIX 76-78: Entry rules, earnings avoidance, and combined slippage+commission tests."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import (
    AvoidEarningsRule,
    ComparisonOperator,
    MovingAverageCrossoverRule,
    RsiRule,
)


def _make_bars(
    closes: list[float],
    volumes: list[float] | None = None,
    start: date = date(2025, 1, 1),
) -> list[DailyBar]:
    vols = volumes or [1_000_000.0] * len(closes)
    bars = []
    for i, (c, v) in enumerate(zip(closes, vols)):
        d = start + timedelta(days=i)
        bars.append(DailyBar(trade_date=d, open_price=c, high_price=c, low_price=c, close_price=c, volume=v))
    return bars


@dataclass
class StubGateway:
    iv_values: list[float | None] | None = None

    def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
        if self.iv_values is None:
            return []
        exp = entry_date + timedelta(days=target_dte)
        return [OptionContractRecord(f"OPT_{entry_date}", contract_type, exp, 100.0, 100)]

    def select_contract(self, entry_date, strategy_type, underlying_close, target_dte, dte_tolerance_days):
        return self.list_contracts(entry_date, "call", target_dte, dte_tolerance_days)[0]

    def get_quote(self, option_ticker, trade_date):
        return OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.0, participant_timestamp=None)


def _build_evaluator(
    closes: list[float],
    rules: list,
    volumes: list[float] | None = None,
    earnings_dates: set[date] | None = None,
    target_dte: int = 30,
    gateway: StubGateway | None = None,
) -> EntryRuleEvaluator:
    bars = _make_bars(closes, volumes)
    config = BacktestConfig(
        symbol="TEST",
        strategy_type="long_call",
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        target_dte=target_dte,
        dte_tolerance_days=10,
        max_holding_days=30,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=1,
        entry_rules=rules,
    )
    return EntryRuleEvaluator(
        config=config,
        bars=bars,
        earnings_dates=earnings_dates or set(),
        option_gateway=gateway or StubGateway(),
    )


# ---------------------------------------------------------------------------
# FIX 76: RSI entry rule triggers
# ---------------------------------------------------------------------------


class TestRsiEntryRuleTriggers:
    def test_rsi_below_threshold_triggers_entry(self):
        """RSI dropping below threshold should allow entry."""
        closes = [100.0] * 5 + [100 - i * 2 for i in range(25)]
        rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("30"), period=14)
        ev = _build_evaluator(closes, [rule])
        assert ev.is_entry_allowed(len(closes) - 1) is True

    def test_rsi_above_threshold_blocks_entry(self):
        """RSI above the threshold should block entry for LTE rule."""
        closes = [100.0 + i * 2 for i in range(25)]
        rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("30"), period=14)
        ev = _build_evaluator(closes, [rule])
        assert ev.is_entry_allowed(len(closes) - 1) is False


# ---------------------------------------------------------------------------
# FIX 76: MA crossover entry rule
# ---------------------------------------------------------------------------


class TestMaCrossoverEntryRule:
    def test_sma_bullish_crossover_triggers(self):
        """Fast SMA crossing above slow SMA should trigger bullish entry."""
        down = [100 - i * 0.5 for i in range(20)]
        sharp_up = [90 + i * 3 for i in range(15)]
        closes = down + sharp_up
        rule = MovingAverageCrossoverRule(
            type="sma_crossover", fast_period=3, slow_period=10, direction="bullish",
        )
        ev = _build_evaluator(closes, [rule])
        triggered = any(ev.is_entry_allowed(i) for i in range(20, len(closes)))
        assert triggered is True

    def test_sma_bearish_crossover_not_triggered_during_uptrend(self):
        """Bullish crossover rule should not trigger during a steady downtrend."""
        closes = [100.0 - i * 0.5 for i in range(30)]
        rule = MovingAverageCrossoverRule(
            type="sma_crossover", fast_period=5, slow_period=15, direction="bullish",
        )
        ev = _build_evaluator(closes, [rule])
        triggered = any(ev.is_entry_allowed(i) for i in range(1, len(closes)))
        assert triggered is False


# ---------------------------------------------------------------------------
# FIX 77: Earnings avoidance
# ---------------------------------------------------------------------------


class TestEarningsAvoidance:
    def test_entry_blocked_near_earnings(self):
        """Entries within the earnings blackout window should be blocked."""
        closes = [100.0] * 10
        bars = _make_bars(closes)
        earnings = {bars[5].trade_date}
        rule = AvoidEarningsRule(type="avoid_earnings", days_before=2, days_after=2)
        ev = _build_evaluator(closes, [rule], earnings_dates=earnings)
        assert ev.is_entry_allowed(5) is False
        assert ev.is_entry_allowed(4) is False

    def test_entry_allowed_outside_earnings_window(self):
        """Entries outside the earnings blackout window should be allowed."""
        closes = [100.0] * 20
        bars = _make_bars(closes)
        earnings = {bars[5].trade_date}
        rule = AvoidEarningsRule(type="avoid_earnings", days_before=2, days_after=2)
        ev = _build_evaluator(closes, [rule], earnings_dates=earnings)
        assert ev.is_entry_allowed(15) is True

    def test_multiple_earnings_dates_all_respected(self):
        """All earnings dates should create blackout windows."""
        closes = [100.0] * 30
        bars = _make_bars(closes)
        earnings = {bars[5].trade_date, bars[20].trade_date}
        rule = AvoidEarningsRule(type="avoid_earnings", days_before=1, days_after=1)
        ev = _build_evaluator(closes, [rule], earnings_dates=earnings)
        assert ev.is_entry_allowed(5) is False
        assert ev.is_entry_allowed(20) is False
        assert ev.is_entry_allowed(10) is True


# ---------------------------------------------------------------------------
# FIX 78: Combined slippage and commission
# ---------------------------------------------------------------------------


@dataclass
class FakeGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]

    def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
        return self.contracts.get((entry_date, contract_type), [])

    def select_contract(self, entry_date, strategy_type, underlying_close, target_dte, dte_tolerance_days):
        contract_type = "call" if strategy_type in {"long_call", "covered_call"} else "put"
        contracts = self.list_contracts(entry_date, contract_type, target_dte, dte_tolerance_days)
        if not contracts:
            from backtestforecast.errors import DataUnavailableError
            raise DataUnavailableError("No contracts")
        return contracts[0]

    def get_quote(self, option_ticker, trade_date):
        return self.quotes.get((option_ticker, trade_date))

    def get_chain_delta_lookup(self, contracts):
        return {}


def _make_bar(trade_date: date, close_price: float) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        volume=1_000_000,
    )


def _make_quote(trade_date: date, mid: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None)


class TestCombinedSlippageAndCommission:
    """Verify that both slippage_pct and commission_per_contract are reflected in trade results."""

    def test_both_slippage_and_commission_applied(self):
        engine = OptionsBacktestEngine()
        entry_date = date(2025, 4, 2)
        expiration = date(2025, 4, 5)
        bars = [
            _make_bar(date(2025, 4, 1), 100),
            _make_bar(entry_date, 100),
            _make_bar(date(2025, 4, 3), 105),
            _make_bar(date(2025, 4, 4), 108),
            _make_bar(expiration, 110),
        ]
        contracts = {
            (entry_date, "call"): [
                OptionContractRecord("C100", "call", expiration, 100, 100),
            ]
        }
        quotes = {
            ("C100", entry_date): _make_quote(entry_date, 3.0),
            ("C100", date(2025, 4, 3)): _make_quote(date(2025, 4, 3), 5.0),
            ("C100", expiration): _make_quote(expiration, 10.0),
        }

        slippage_pct = 0.5
        commission = 0.65

        result_both = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_call",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=10_000,
                risk_per_trade_pct=5,
                commission_per_contract=commission,
                slippage_pct=slippage_pct,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        result_none = engine.run(
            BacktestConfig(
                symbol="AAPL",
                strategy_type="long_call",
                start_date=date(2025, 4, 1),
                end_date=date(2025, 4, 3),
                target_dte=30,
                dte_tolerance_days=30,
                max_holding_days=30,
                account_size=10_000,
                risk_per_trade_pct=5,
                commission_per_contract=0,
                slippage_pct=0.0,
                entry_rules=[],
            ),
            bars,
            set(),
            FakeGateway(contracts=contracts, quotes=quotes),
        )

        assert result_both.trades, "Expected at least one trade with slippage+commission"
        assert result_none.trades, "Expected at least one trade without slippage+commission"

        trade_both = result_both.trades[0]
        trade_none = result_none.trades[0]

        assert trade_both.total_commissions > 0, "Commission should be nonzero"
        assert trade_both.net_pnl < trade_none.net_pnl, (
            "Net PnL with slippage+commission should be less than without"
        )
        assert trade_both.gross_pnl <= trade_none.gross_pnl, (
            "Gross PnL with slippage should be <= gross without slippage"
        )
