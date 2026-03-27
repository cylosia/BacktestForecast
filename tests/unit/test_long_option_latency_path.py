from __future__ import annotations

from datetime import date
from decimal import Decimal

from backtestforecast.backtests.strategies.long_options import LONG_CALL_STRATEGY
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import StrategyOverrides


class _GatewayUsingExactExpiration:
    def __init__(self) -> None:
        self.exact_calls: list[tuple[date, str, int, int, float | None, float | None]] = []
        self.list_calls: list[tuple[date, str, int, int]] = []

    def list_contracts_for_preferred_expiration(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
        *,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        self.exact_calls.append(
            (entry_date, contract_type, target_dte, dte_tolerance_days, strike_price_gte, strike_price_lte)
        )
        return [
            OptionContractRecord(
                ticker="O:AAPL250404C00200000",
                contract_type="call",
                expiration_date=date(2025, 4, 4),
                strike_price=200.0,
                shares_per_contract=100.0,
            )
        ]

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        self.list_calls.append((entry_date, contract_type, target_dte, dte_tolerance_days))
        raise AssertionError("broad list_contracts should not be used when exact expiration lookup exists")

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        return OptionQuoteRecord(
            trade_date=trade_date,
            bid_price=2.0,
            ask_price=2.2,
            participant_timestamp=None,
        )


def test_long_call_uses_preferred_expiration_lookup_when_available():
    gateway = _GatewayUsingExactExpiration()
    config = BacktestConfig(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 30),
        target_dte=7,
        dte_tolerance_days=2,
        max_holding_days=7,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
        strategy_overrides=StrategyOverrides.model_validate(
            {"long_call_strike": {"mode": "atm_offset_steps", "value": 0}}
        ),
    )
    bar = DailyBar(
        trade_date=date(2025, 4, 1),
        open_price=200.0,
        high_price=201.0,
        low_price=199.0,
        close_price=200.0,
        volume=1_000_000,
    )

    position = LONG_CALL_STRATEGY.build_position(config, bar, 0, gateway)

    assert position is not None
    assert gateway.exact_calls == [(date(2025, 4, 1), "call", 7, 2, 170.0, 230.0)]
    assert gateway.list_calls == []
    assert position.display_ticker == "O:AAPL250404C00200000"
