from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from backtestforecast.backtests.calendar_adjustments import (
    CLOSE_AT_SHORT_EXPIRATION_POLICY,
    HOLD_LONG_ONLY_IF_SHORT_OTM_POLICY,
    RECENTER_SHORT_ONCE_POLICY,
    ROLL_SAME_STRIKE_ONCE_POLICY,
    run_adjusted_calendar_backtest,
    select_calendar_roll_short_contract,
)
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import StrategyOverrides, StrikeSelection, StrikeSelectionMode


@dataclass
class AdjustmentGateway:
    contracts: dict[tuple[date, str], list[OptionContractRecord]]
    quotes: dict[tuple[str, date], OptionQuoteRecord]
    exact_calls: list[tuple[date, str, date]] = field(default_factory=list)

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        return self.contracts.get((entry_date, contract_type), [])

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
        return [
            contract
            for contract in self.contracts.get((entry_date, contract_type), [])
            if contract.expiration_date == expiration_date
            and (strike_price_gte is None or contract.strike_price >= strike_price_gte)
            and (strike_price_lte is None or contract.strike_price <= strike_price_lte)
        ]

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return self.quotes.get((option_ticker, trade_date))

    def get_quotes(self, option_tickers: list[str], trade_date: date) -> list[tuple[str, OptionQuoteRecord | None]]:
        return [(ticker, self.get_quote(ticker, trade_date)) for ticker in option_tickers]

    def get_quote_series(
        self,
        option_tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[date, OptionQuoteRecord | None]]:
        series: dict[str, dict[date, OptionQuoteRecord | None]] = {}
        for ticker in option_tickers:
            by_date: dict[date, OptionQuoteRecord | None] = {}
            for (quote_ticker, quote_date), quote in self.quotes.items():
                if quote_ticker == ticker and start_date <= quote_date <= end_date:
                    by_date[quote_date] = quote
            series[ticker] = by_date
        return series

    def get_ex_dividend_dates(self, start_date: date, end_date: date) -> set[date]:
        return set()


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
    return OptionQuoteRecord(
        trade_date=trade_date,
        bid_price=mid,
        ask_price=mid,
        participant_timestamp=None,
    )


def _calendar_config() -> BacktestConfig:
    return BacktestConfig(
        symbol="UVXY",
        strategy_type="calendar_spread",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 5),
        target_dte=2,
        dte_tolerance_days=0,
        max_holding_days=30,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        slippage_pct=0.0,
        entry_rules=[],
        strategy_overrides=StrategyOverrides(
            calendar_contract_type="put",
            calendar_far_leg_target_dte=7,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.ATM_OFFSET_STEPS,
                value=Decimal("0"),
            ),
        ),
        profit_target_pct=20.0,
    )


def test_select_calendar_roll_short_contract_same_strike_uses_next_viable_expiration() -> None:
    gateway = AdjustmentGateway(
        contracts={
            (date(2025, 1, 3), "put"): [
                OptionContractRecord("P100_0105", "put", date(2025, 1, 5), 100.0, 100.0),
                OptionContractRecord("P95_0105", "put", date(2025, 1, 5), 95.0, 100.0),
                OptionContractRecord("P100_0106", "put", date(2025, 1, 6), 100.0, 100.0),
            ]
        },
        quotes={},
    )

    contract = select_calendar_roll_short_contract(
        gateway,
        entry_date=date(2025, 1, 3),
        contract_type="put",
        target_dte=2,
        dte_tolerance_days=0,
        long_expiration=date(2025, 1, 8),
        current_short_strike=100.0,
        underlying_close=97.0,
        recenter_short=False,
    )

    assert contract is not None
    assert contract.ticker == "P100_0105"


def test_select_calendar_roll_short_contract_recenter_picks_atm_strike() -> None:
    gateway = AdjustmentGateway(
        contracts={
            (date(2025, 1, 3), "put"): [
                OptionContractRecord("P90_0105", "put", date(2025, 1, 5), 90.0, 100.0),
                OptionContractRecord("P95_0105", "put", date(2025, 1, 5), 95.0, 100.0),
                OptionContractRecord("P100_0105", "put", date(2025, 1, 5), 100.0, 100.0),
            ]
        },
        quotes={},
    )

    contract = select_calendar_roll_short_contract(
        gateway,
        entry_date=date(2025, 1, 3),
        contract_type="put",
        target_dte=2,
        dte_tolerance_days=0,
        long_expiration=date(2025, 1, 8),
        current_short_strike=100.0,
        underlying_close=96.2,
        recenter_short=True,
    )

    assert contract is not None
    assert contract.ticker == "P95_0105"


def test_hold_long_only_policy_can_recover_campaign_after_short_expiration() -> None:
    bars = [
        make_bar(date(2025, 1, 1), 100.0),
        make_bar(date(2025, 1, 2), 101.0),
        make_bar(date(2025, 1, 3), 102.0),
        make_bar(date(2025, 1, 4), 95.0),
        make_bar(date(2025, 1, 5), 94.0),
    ]
    gateway = AdjustmentGateway(
        contracts={
            (date(2025, 1, 1), "put"): [
                OptionContractRecord("SHORT_P100_0103", "put", date(2025, 1, 3), 100.0, 100.0),
                OptionContractRecord("LONG_P100_0108", "put", date(2025, 1, 8), 100.0, 100.0),
            ]
        },
        quotes={
            ("SHORT_P100_0103", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 2.0),
            ("LONG_P100_0108", date(2025, 1, 1)): make_quote(date(2025, 1, 1), 3.0),
            ("SHORT_P100_0103", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 1.0),
            ("LONG_P100_0108", date(2025, 1, 2)): make_quote(date(2025, 1, 2), 1.9),
            ("SHORT_P100_0103", date(2025, 1, 3)): make_quote(date(2025, 1, 3), 0.0),
            ("LONG_P100_0108", date(2025, 1, 3)): make_quote(date(2025, 1, 3), 0.8),
            ("LONG_P100_0108", date(2025, 1, 4)): make_quote(date(2025, 1, 4), 1.5),
            ("LONG_P100_0108", date(2025, 1, 5)): make_quote(date(2025, 1, 5), 1.3),
        },
    )
    config = _calendar_config()

    baseline = run_adjusted_calendar_backtest(
        config=config,
        bars=bars,
        earnings_dates=set(),
        option_gateway=gateway,
        policy=CLOSE_AT_SHORT_EXPIRATION_POLICY,
    )
    adjusted = run_adjusted_calendar_backtest(
        config=config,
        bars=bars,
        earnings_dates=set(),
        option_gateway=gateway,
        policy=HOLD_LONG_ONLY_IF_SHORT_OTM_POLICY,
    )

    assert baseline.summary.trade_count == 1
    assert adjusted.summary.trade_count == 1
    assert adjusted.trades[0].exit_reason == "adjustment_recovered"
    assert float(adjusted.trades[0].net_pnl) > float(baseline.trades[0].net_pnl)
    assert adjusted.trades[0].detail_json["campaign_adjustment_events"][0]["event_type"] == HOLD_LONG_ONLY_IF_SHORT_OTM_POLICY.name
