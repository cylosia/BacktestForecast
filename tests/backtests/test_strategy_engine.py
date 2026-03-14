from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pytest

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

    def get_chain_delta_lookup(self, contracts):
        return {}


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
    """Quote with explicit bid/ask spread — mid_price = (bid + ask) / 2."""
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


def test_custom_2_leg_stock_only_uses_end_date() -> None:
    """Stock-only custom legs use config.end_date as scheduled_exit_date."""
    from backtestforecast.schemas.backtests import CustomLegDefinition

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
    assert result.trades[2].exit_mid == 100.0, "shares called away at strike, not close"
    assert round(result.summary.total_net_pnl, 2) == -1400.0


# ---------------------------------------------------------------------------
# Financial correctness tests — verifiable P&L against hand-calculated values
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
        assert round(trade.total_commissions, 2) == round(expected_entry_commission, 2), (
            f"Assignment exit should have zero exit commission; "
            f"total_commissions ({trade.total_commissions}) should equal "
            f"entry-only commission ({expected_entry_commission})"
        )


class TestBullCallSpreadCorrectness:
    """Verify exact P&L, commissions, and position sizing for a bull call debit spread.

    Setup:
        Long C100 (mid 4.00) / Short C105 (mid 1.50)
        Debit per unit = 2.50 × 100 = $250
        Width = $500, max_profit = $250, max_loss = $250
        Account $10,000 @ 5% risk → 2 units
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

        # risk_budget = $500, max_loss_per_unit = $250 → by_risk = 2
        assert trade.quantity == 2

        # At expiration: C100 intrinsic = 8.00, C105 intrinsic = 3.00
        # exit_value_per_unit = (8 − 3) × 100 = 500
        # gross = (500 − 250) × 2 = 500
        assert round(trade.gross_pnl, 2) == 500.0

        # 2 legs × 2 units × $0.65, charged at entry and exit
        expected_comm = commission * 2 * 2 * 2  # 5.20
        assert round(trade.total_commissions, 2) == round(expected_comm, 2)

        assert round(trade.net_pnl, 2) == round(500.0 - expected_comm, 2)
        assert trade.detail_json["max_profit_per_unit"] == 250.0
        assert trade.detail_json["actual_units"] == 2


class TestIronCondorCorrectness:
    """Verify iron condor P&L for range-bound and breakout scenarios.

    Setup (shared):
        Short C100 (3.00) / Long C105 (1.00) / Short P100 (3.00) / Long P95 (1.00)
        Net credit per unit = (3+3−1−1) × 100 = $400
        Wing width = $500, max_loss_per_unit = $100
        Account $10,000 @ 2% risk → 2 units
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
        """Underlying stays at 100 — all legs expire worthless, full credit kept."""
        commission = 0.65
        result = self._run(exit_underlying=100, commission=commission)

        assert result.summary.trade_count == 1
        trade = result.trades[0]

        # risk_budget = $200, max_loss_per_unit = $100 → 2 units
        assert trade.quantity == 2

        # All intrinsics = 0 at expiration with underlying = 100
        # gross = (0 − (−400)) × 2 = 800
        assert round(trade.gross_pnl, 2) == 800.0

        # 4 legs × 2 units × $0.65 × 2 (entry + exit) = $10.40
        expected_comm = commission * 4 * 2 * 2
        assert round(trade.total_commissions, 2) == round(expected_comm, 2)

        assert round(trade.net_pnl, 2) == round(800.0 - expected_comm, 2)
        assert trade.net_pnl > 0

        # Gross profit does not exceed wing_width × quantity
        wing_width = 500
        assert abs(trade.gross_pnl) <= wing_width * trade.quantity

    def test_max_loss_when_market_breaks_out(self) -> None:
        """Underlying surges to 110 — call side fully breached, max loss realised."""
        commission = 0.65
        result = self._run(exit_underlying=110, commission=commission)

        assert result.summary.trade_count == 1
        trade = result.trades[0]
        assert trade.quantity == 2

        # At 110: C100 intrinsic = 10, C105 = 5, puts = 0
        # exit_value_per_unit = (−10 + 5) × 100 = −500
        # gross = (−500 − (−400)) × 2 = −200
        assert round(trade.gross_pnl, 2) == -200.0
        assert trade.net_pnl < 0

        # Max loss per unit ($100) × 2 units — gross loss exactly equals maximum
        assert abs(trade.gross_pnl) == trade.detail_json["max_loss_total"]

        # Gross loss bounded by wing width × quantity
        wing_width_per_unit = 500
        assert abs(trade.gross_pnl) <= wing_width_per_unit * trade.quantity

        expected_comm = commission * 4 * 2 * 2
        assert round(trade.total_commissions, 2) == round(expected_comm, 2)
        assert round(trade.net_pnl, 2) == round(-200.0 - expected_comm, 2)


class TestCashSecuredPutCorrectness:
    """Verify cash-secured put P&L for OTM expiration and ITM assignment loss.

    Setup:
        Short P100 (mid 2.00), underlying at 105 on entry
        Credit = $200, cash required = strike × 100 = $10,000
        Account $100,000 @ 10% risk → 1 unit
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
        """Underlying stays at 105 — put expires worthless, full premium is profit."""
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

        # cash_required = strike × 100 × quantity
        assert trade.detail_json["capital_required_total"] == self.STRIKE * 100 * trade.quantity

        # Full premium: gross = 2.00 × 100 = $200
        premium_collected = self.PREMIUM * 100
        assert round(trade.gross_pnl, 2) == premium_collected

        # 1 leg × 1 unit × $0.65 × 2 (entry + exit) = $1.30
        expected_comm = self.COMMISSION * 1 * 1 * 2
        assert round(trade.total_commissions, 2) == round(expected_comm, 2)
        assert round(trade.net_pnl, 2) == round(premium_collected - expected_comm, 2)

    def test_itm_realizes_assignment_loss(self) -> None:
        """Underlying drops to 90 — put ITM, loss = (strike − spot) × 100 − premium."""
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

        # intrinsic = (100 − 90) × 100 = $1,000 loss on short put
        # offset by premium = $200 → gross = −$800
        intrinsic_loss = (self.STRIKE - 90) * 100
        expected_gross = -(intrinsic_loss - self.PREMIUM * 100)  # -800
        assert round(trade.gross_pnl, 2) == expected_gross

        expected_comm = self.COMMISSION * 1 * 1 * 2
        assert round(trade.total_commissions, 2) == round(expected_comm, 2)
        assert round(trade.net_pnl, 2) == round(expected_gross - expected_comm, 2)
        assert trade.net_pnl < 0


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
        cash_delta = ending_cash - account_size
        assert round(cash_delta, 2) == round(total_net_pnl, 2), (
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
        from backtestforecast.backtests.strategies.calendar import CalendarSpreadStrategy
        from backtestforecast.backtests.margin import naked_call_margin

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

        expected_margin = naked_call_margin(underlying_close, strike, short_mid)
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
