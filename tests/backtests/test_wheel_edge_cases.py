"""Test wheel strategy edge cases in _open_short_put, _resolve_exit, and the run loop."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from backtestforecast.backtests.strategies.wheel import (
    OpenShortOptionPhase,
    WheelStrategyBacktestEngine,
)
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import (
    DailyBar,
    OptionContractRecord,
    OptionQuoteRecord,
)


def _make_config(
    account_size: float = 100_000.0,
    risk_per_trade_pct: float = 5.0,
    commission_per_contract: float = 0.65,
    start_date: date | None = None,
    end_date: date | None = None,
    target_dte: int = 30,
    max_holding_days: int = 45,
    slippage_pct: float = 0.0,
    profit_target_pct: float | None = None,
    stop_loss_pct: float | None = None,
) -> BacktestConfig:
    sd = start_date or date(2024, 1, 2)
    ed = end_date or date(2024, 3, 29)
    return BacktestConfig(
        symbol="AAPL",
        strategy_type="wheel",
        start_date=sd,
        end_date=ed,
        target_dte=target_dte,
        dte_tolerance_days=7,
        max_holding_days=max_holding_days,
        account_size=Decimal(str(account_size)),
        risk_per_trade_pct=Decimal(str(risk_per_trade_pct)),
        commission_per_contract=Decimal(str(commission_per_contract)),
        entry_rules=[],
        slippage_pct=slippage_pct,
        profit_target_pct=profit_target_pct,
        stop_loss_pct=stop_loss_pct,
    )


def _make_bar(trade_date: date, close_price: float) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + 1,
        low_price=close_price - 1,
        close_price=close_price,
        volume=1_000_000.0,
    )


def _make_contract(
    ticker: str, contract_type: str, strike_price: float, expiration_date: date
) -> OptionContractRecord:
    return OptionContractRecord(
        ticker=ticker,
        contract_type=contract_type,
        expiration_date=expiration_date,
        strike_price=strike_price,
        shares_per_contract=100.0,
    )


def _make_quote(trade_date: date, bid: float, ask: float) -> OptionQuoteRecord:
    return OptionQuoteRecord(
        trade_date=trade_date,
        bid_price=bid,
        ask_price=ask,
        participant_timestamp=None,
    )


class _MockGateway:
    """In-memory option data gateway for testing."""

    def __init__(
        self,
        contracts: list[OptionContractRecord] | None = None,
        quotes: dict[tuple[str, date], OptionQuoteRecord] | None = None,
    ):
        self._contracts = contracts or []
        self._quotes = quotes or {}

    def list_contracts(
        self, entry_date: date, contract_type: str, target_dte: int, dte_tolerance_days: int
    ) -> Sequence[OptionContractRecord]:
        return [
            c
            for c in self._contracts
            if c.contract_type == contract_type
            and abs((c.expiration_date - entry_date).days - target_dte) <= dte_tolerance_days
        ]

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return self._quotes.get((option_ticker, trade_date))


class TestNegativeCostPerUnit:
    """When premium exceeds collateral + commission, _open_short_put returns None."""

    def test_negative_cost_skips_entry(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config(account_size=100_000.0, commission_per_contract=0.65)
        bar = _make_bar(date(2024, 1, 15), close_price=50.0)
        expiry = date(2024, 2, 16)

        strike = 50.0
        premium_bid = 55.0
        premium_ask = 55.0
        mid = (premium_bid + premium_ask) / 2.0
        capital_per_unit = strike * 100.0
        commission_per_unit = 0.65 * 2
        total_cost = capital_per_unit + commission_per_unit - (mid * 100.0)
        assert total_cost < 0, "Setup: premium must exceed collateral"

        contract = _make_contract("O:AAPL240216P00050000", "put", strike, expiry)
        quote = _make_quote(date(2024, 1, 15), premium_bid, premium_ask)
        gw = _MockGateway(
            contracts=[contract],
            quotes={("O:AAPL240216P00050000", date(2024, 1, 15)): quote},
        )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        result = engine._open_short_put(config, bar, 0, gw, 100_000.0, warnings, warning_codes)
        assert result is None
        assert any(w["code"] == "negative_cost_per_unit" for w in warnings)

    def test_negative_cost_warning_message(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config()
        bar = _make_bar(date(2024, 1, 15), close_price=10.0)
        expiry = date(2024, 2, 16)

        contract = _make_contract("O:TEST240216P00010000", "put", 10.0, expiry)
        quote = _make_quote(date(2024, 1, 15), 15.0, 15.0)
        gw = _MockGateway(
            contracts=[contract],
            quotes={("O:TEST240216P00010000", date(2024, 1, 15)): quote},
        )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        engine._open_short_put(config, bar, 0, gw, 100_000.0, warnings, warning_codes)
        neg_warn = [w for w in warnings if w["code"] == "negative_cost_per_unit"]
        assert len(neg_warn) == 1
        assert "unbounded sizing" in neg_warn[0]["message"]


class TestCashGoesNegativeDuringPutAssignment:
    """When a put is assigned, cash decreases by strike * 100 * quantity."""

    def test_put_assignment_reduces_cash(self):
        engine = WheelStrategyBacktestEngine()
        entry_date = date(2024, 1, 2)
        expiry = date(2024, 2, 2)
        config = _make_config(
            account_size=10_000.0,
            start_date=entry_date,
            end_date=date(2024, 2, 5),
            target_dte=30,
            max_holding_days=60,
            risk_per_trade_pct=100.0,
        )

        strike = 90.0
        premium = 3.0
        contract = _make_contract("O:AAPL240202P00090000", "put", strike, expiry)

        bars = []
        quotes: dict[tuple[str, date], OptionQuoteRecord] = {}
        current = entry_date
        while current <= date(2024, 2, 5):
            price = 95.0 if current < expiry else 85.0
            bars.append(_make_bar(current, price))

            if current < expiry:
                quotes[("O:AAPL240202P00090000", current)] = _make_quote(
                    current, premium - 0.10, premium + 0.10
                )
            else:
                quotes[("O:AAPL240202P00090000", current)] = _make_quote(current, 4.5, 5.5)

            current += timedelta(days=1)
            if current.weekday() >= 5:
                current += timedelta(days=7 - current.weekday())

        gw = _MockGateway(contracts=[contract], quotes=quotes)
        result = engine.run(config, bars, set(), gw)

        assigned_trades = [t for t in result.trades if t.exit_reason == "assignment"]
        if assigned_trades:
            for eq_pt in result.equity_curve:
                if eq_pt.trade_date >= expiry:
                    assert eq_pt.cash < float(config.account_size)


class TestCoveredCallAssignment:
    """When shares are held and a covered call expires ITM, shares are called away."""

    def test_resolve_exit_at_expiration(self):
        bar = _make_bar(date(2024, 2, 16), 155.0)
        position = OpenShortOptionPhase(
            ticker="O:AAPL240216C00150000",
            contract_type="call",
            strike_price=150.0,
            expiration_date=date(2024, 2, 16),
            entry_date=date(2024, 1, 16),
            entry_index=0,
            quantity=1,
            entry_mid=3.0,
            phase="covered_call",
            last_mid=3.0,
        )
        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=45,
            backtest_end_date=date(2024, 3, 1),
            last_bar_date=date(2024, 3, 1),
            current_bar_index=22,
        )
        assert should_exit is True
        assert reason == "expiration"

    def test_resolve_exit_before_expiration_no_trigger(self):
        bar = _make_bar(date(2024, 2, 10), 155.0)
        position = OpenShortOptionPhase(
            ticker="O:AAPL240216C00150000",
            contract_type="call",
            strike_price=150.0,
            expiration_date=date(2024, 2, 16),
            entry_date=date(2024, 2, 1),
            entry_index=0,
            quantity=1,
            entry_mid=3.0,
            phase="covered_call",
            last_mid=3.0,
        )
        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=45,
            backtest_end_date=date(2024, 3, 1),
            last_bar_date=date(2024, 3, 1),
            current_bar_index=7,
        )
        assert should_exit is False
        assert reason == ""


class TestResolveExitEdgeCases:
    def test_profit_target_exit(self):
        bar = _make_bar(date(2024, 1, 20), 100.0)
        position = OpenShortOptionPhase(
            ticker="O:TEST",
            contract_type="put",
            strike_price=95.0,
            expiration_date=date(2024, 2, 16),
            entry_date=date(2024, 1, 10),
            entry_index=0,
            quantity=1,
            entry_mid=2.0,
            phase="cash_secured_put",
            last_mid=0.5,
        )
        capital_at_risk = 2.0 * 1 * 100.0
        position_pnl = (2.0 - 0.5) * 1 * 100.0
        current_value = capital_at_risk + position_pnl

        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=45,
            backtest_end_date=date(2024, 3, 1),
            last_bar_date=date(2024, 3, 1),
            current_bar_index=8,
            profit_target_pct=50.0,
            capital_at_risk=capital_at_risk,
            current_value=current_value,
        )
        assert should_exit is True
        assert reason == "profit_target"

    def test_stop_loss_exit(self):
        bar = _make_bar(date(2024, 1, 20), 90.0)
        position = OpenShortOptionPhase(
            ticker="O:TEST",
            contract_type="put",
            strike_price=95.0,
            expiration_date=date(2024, 2, 16),
            entry_date=date(2024, 1, 10),
            entry_index=0,
            quantity=1,
            entry_mid=2.0,
            phase="cash_secured_put",
            last_mid=6.0,
        )
        capital_at_risk = 2.0 * 1 * 100.0
        position_pnl = (2.0 - 6.0) * 1 * 100.0
        current_value = capital_at_risk + position_pnl

        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=45,
            backtest_end_date=date(2024, 3, 1),
            last_bar_date=date(2024, 3, 1),
            current_bar_index=8,
            stop_loss_pct=100.0,
            capital_at_risk=capital_at_risk,
            current_value=current_value,
        )
        assert should_exit is True
        assert reason == "stop_loss"

    def test_max_holding_days_exit(self):
        bar = _make_bar(date(2024, 2, 28), 100.0)
        position = OpenShortOptionPhase(
            ticker="O:TEST",
            contract_type="put",
            strike_price=95.0,
            expiration_date=date(2024, 3, 15),
            entry_date=date(2024, 1, 10),
            entry_index=0,
            quantity=1,
            entry_mid=2.0,
            phase="cash_secured_put",
            last_mid=1.0,
        )
        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=10,
            backtest_end_date=date(2024, 4, 1),
            last_bar_date=date(2024, 4, 1),
            current_bar_index=15,
        )
        assert should_exit is True
        assert reason == "max_holding_days"

    def test_no_exit_when_within_limits(self):
        bar = _make_bar(date(2024, 1, 15), 100.0)
        position = OpenShortOptionPhase(
            ticker="O:TEST",
            contract_type="put",
            strike_price=95.0,
            expiration_date=date(2024, 2, 16),
            entry_date=date(2024, 1, 10),
            entry_index=0,
            quantity=1,
            entry_mid=2.0,
            phase="cash_secured_put",
            last_mid=1.8,
        )
        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=45,
            backtest_end_date=date(2024, 3, 1),
            last_bar_date=date(2024, 3, 1),
            current_bar_index=3,
        )
        assert should_exit is False
        assert reason == ""

    def test_backtest_end_exit_on_last_bar(self):
        end_date = date(2024, 3, 1)
        bar = _make_bar(end_date, 100.0)
        position = OpenShortOptionPhase(
            ticker="O:TEST",
            contract_type="put",
            strike_price=95.0,
            expiration_date=date(2024, 3, 15),
            entry_date=date(2024, 2, 1),
            entry_index=0,
            quantity=1,
            entry_mid=2.0,
            phase="cash_secured_put",
            last_mid=1.5,
        )
        should_exit, reason = WheelStrategyBacktestEngine._resolve_exit(
            bar=bar,
            position=position,
            max_holding_days=60,
            backtest_end_date=end_date,
            last_bar_date=end_date,
            current_bar_index=20,
        )
        assert should_exit is True
        assert reason == "backtest_end"


class TestEmptyBarsProducesEmptyResult:
    def test_run_with_no_bars(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config()
        result = engine.run(config, [], set(), _MockGateway())
        assert result.trades == []
        assert result.equity_curve == []

    def test_run_with_no_bars_returns_starting_equity(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config(account_size=50_000.0)
        result = engine.run(config, [], set(), _MockGateway())
        assert result.summary.starting_equity == 50_000.0
        assert result.summary.ending_equity == 50_000.0


class TestInsufficientCashSkipsEntry:
    """When cash is too low for even one contract, _open_short_put returns None."""

    def test_insufficient_cash_returns_none(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config(account_size=100.0, risk_per_trade_pct=100.0)
        bar = _make_bar(date(2024, 1, 15), close_price=150.0)
        expiry = date(2024, 2, 16)

        contract = _make_contract("O:AAPL240216P00150000", "put", 150.0, expiry)
        quote = _make_quote(date(2024, 1, 15), 2.0, 3.0)
        gw = _MockGateway(
            contracts=[contract],
            quotes={("O:AAPL240216P00150000", date(2024, 1, 15)): quote},
        )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        result = engine._open_short_put(config, bar, 0, gw, 100.0, warnings, warning_codes)
        assert result is None


class TestMissingQuoteSkipsEntry:
    """When option gateway returns no quote, _open_short_put returns None."""

    def test_missing_quote_returns_none(self):
        engine = WheelStrategyBacktestEngine()
        config = _make_config()
        bar = _make_bar(date(2024, 1, 15), close_price=150.0)
        expiry = date(2024, 2, 16)

        contract = _make_contract("O:AAPL240216P00150000", "put", 150.0, expiry)
        gw = _MockGateway(contracts=[contract], quotes={})

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        result = engine._open_short_put(config, bar, 0, gw, 100_000.0, warnings, warning_codes)
        assert result is None
        assert any(w["code"] == "missing_entry_quote" for w in warnings)


class TestDecimalOptionalNumericInputs:
    def test_run_accepts_decimal_slippage_and_exit_thresholds(self):
        engine = WheelStrategyBacktestEngine()
        entry_date = date(2024, 1, 2)
        expiry = date(2024, 2, 2)
        config = _make_config(
            account_size=10_000.0,
            start_date=entry_date,
            end_date=date(2024, 2, 5),
            target_dte=30,
            max_holding_days=60,
            risk_per_trade_pct=100.0,
            slippage_pct=Decimal("0.50"),
            profit_target_pct=Decimal("25"),
            stop_loss_pct=Decimal("25"),
        )

        contract = _make_contract("O:AAPL240202P00090000", "put", 90.0, expiry)
        bars = [
            _make_bar(date(2024, 1, 2), 95.0),
            _make_bar(date(2024, 1, 3), 94.0),
            _make_bar(date(2024, 2, 2), 85.0),
            _make_bar(date(2024, 2, 5), 86.0),
        ]
        quotes = {
            ("O:AAPL240202P00090000", date(2024, 1, 2)): _make_quote(date(2024, 1, 2), 2.9, 3.1),
            ("O:AAPL240202P00090000", date(2024, 1, 3)): _make_quote(date(2024, 1, 3), 2.4, 2.6),
            ("O:AAPL240202P00090000", date(2024, 2, 2)): _make_quote(date(2024, 2, 2), 4.5, 5.5),
        }

        result = engine.run(config, bars, set(), _MockGateway(contracts=[contract], quotes=quotes))

        assert result.equity_curve
        assert result.trades


class TestAddWarningOnce:
    """The _add_warning_once method deduplicates warnings by code."""

    def test_only_adds_warning_once(self):
        warnings: list[dict[str, Any]] = []
        codes: set[str] = set()
        WheelStrategyBacktestEngine._add_warning_once(warnings, codes, "test_code", "Test message")
        WheelStrategyBacktestEngine._add_warning_once(warnings, codes, "test_code", "Test message again")
        assert len(warnings) == 1
        assert warnings[0]["code"] == "test_code"

    def test_different_codes_added_separately(self):
        warnings: list[dict[str, Any]] = []
        codes: set[str] = set()
        WheelStrategyBacktestEngine._add_warning_once(warnings, codes, "code_a", "Message A")
        WheelStrategyBacktestEngine._add_warning_once(warnings, codes, "code_b", "Message B")
        assert len(warnings) == 2
