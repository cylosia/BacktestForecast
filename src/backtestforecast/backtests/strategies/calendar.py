from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import naked_call_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    choose_primary_expiration,
    choose_secondary_expiration,
    contracts_for_expiration,
    require_contract_for_strike,
    synthetic_ticker,
    valid_entry_mids,
)
from backtestforecast.backtests.types import (
    BacktestConfig,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OptionDataGateway,
)
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class CalendarSpreadStrategy(StrategyDefinition):
    strategy_type: str = "calendar_spread"
    margin_warning_message: str | None = None

    # TODO: Add support for put calendar spreads. Currently only call calendars
    # are supported. A `contract_type` parameter would enable put calendars for
    # bearish/neutral market views.

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        near_expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        far_expiration = choose_secondary_expiration(calls, bar.trade_date, near_expiration, min_extra_days=14)
        if far_expiration is None:
            raise DataUnavailableError("Calendar spread requires a later expiration beyond the target cycle.")

        near_calls = contracts_for_expiration(calls, near_expiration)
        far_calls = contracts_for_expiration(calls, far_expiration)
        common_strikes = sorted(
            {contract.strike_price for contract in near_calls} & {contract.strike_price for contract in far_calls}
        )
        if not common_strikes:
            raise DataUnavailableError("Calendar spread requires a common strike across near and far expirations.")
        strike = choose_atm_strike(common_strikes, bar.close_price)
        short_near = require_contract_for_strike(near_calls, strike)
        long_far = require_contract_for_strike(far_calls, strike)

        short_quote = option_gateway.get_quote(short_near.ticker, bar.trade_date)
        long_quote = option_gateway.get_quote(long_far.ticker, bar.trade_date)
        if short_quote is None or long_quote is None:
            return None
        if not valid_entry_mids(short_quote.mid_price, long_quote.mid_price):
            return None

        entry_value_per_unit = (long_quote.mid_price - short_quote.mid_price) * 100.0
        net_debit = max(entry_value_per_unit, 0.0)
        full_margin = naked_call_margin(bar.close_price, short_near.strike_price, short_quote.mid_price)
        long_leg_value = long_quote.mid_price * 100.0
        reduced_margin = max(full_margin - long_leg_value, net_debit)
        _MIN_DEBIT_FLOOR = 1.0
        if entry_value_per_unit >= 0:
            capital = max(entry_value_per_unit, _MIN_DEBIT_FLOOR)
            max_loss: float | None = max(entry_value_per_unit, _MIN_DEBIT_FLOOR)
        else:
            capital = reduced_margin
            max_loss = reduced_margin

        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": long_far.ticker,
                    "side": "long",
                    "contract_type": "call",
                    "strike_price": long_far.strike_price,
                    "expiration_date": long_far.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": long_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": short_near.ticker,
                    "side": "short",
                    "contract_type": "call",
                    "strike_price": short_near.strike_price,
                    "expiration_date": short_near.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": short_quote.mid_price,
                },
            ],
            "assumptions": [
                "Calendar spread is modeled as a call calendar in this slice.",
                "The short leg uses the expiration nearest target_dte and the long leg uses"
                " the next later expiration at least 14 days farther out when available.",
                "The package exits at the near-leg expiration, max_holding_days, or backtest end;"
                " the far leg is closed at market on that exit date.",
            ],
            "capital_required_per_unit": capital,
            "max_loss_per_unit": max_loss,
            "max_profit_per_unit": None,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_far.ticker, short_near.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(near_expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    long_far.ticker,
                    "call",
                    1,
                    long_far.strike_price,
                    long_far.expiration_date,
                    1,
                    long_quote.mid_price,
                    long_quote.mid_price,
                ),
                OpenOptionLeg(
                    short_near.ticker,
                    "call",
                    -1,
                    short_near.strike_price,
                    short_near.expiration_date,
                    1,
                    short_quote.mid_price,
                    short_quote.mid_price,
                ),
            ],
            scheduled_exit_date=near_expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            max_profit_per_unit=None,
            detail_json=detail_json,
        )


CALENDAR_SPREAD_STRATEGY = CalendarSpreadStrategy()
