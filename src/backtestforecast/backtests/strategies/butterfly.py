from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_wing_strike,
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
class ButterflyStrategy(StrategyDefinition):
    strategy_type: str = "butterfly"
    margin_warning_message: str | None = None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        call_contracts = contracts_for_expiration(calls, expiration)
        strikes = sorted({contract.strike_price for contract in call_contracts})
        center_strike = choose_atm_strike(strikes, bar.close_price)
        lower_strike = resolve_wing_strike(strikes, center_strike, -1, bar.close_price, overrides.spread_width)
        upper_strike = resolve_wing_strike(strikes, center_strike, 1, bar.close_price, overrides.spread_width)
        if lower_strike is None or upper_strike is None:
            raise DataUnavailableError("Butterfly requires one listed strike below and above the center strike.")

        lower_call = require_contract_for_strike(call_contracts, lower_strike)
        center_call = require_contract_for_strike(call_contracts, center_strike)
        upper_call = require_contract_for_strike(call_contracts, upper_strike)

        lower_quote = option_gateway.get_quote(lower_call.ticker, bar.trade_date)
        center_quote = option_gateway.get_quote(center_call.ticker, bar.trade_date)
        upper_quote = option_gateway.get_quote(upper_call.ticker, bar.trade_date)
        if lower_quote is None or center_quote is None or upper_quote is None:
            return None
        if not valid_entry_mids(lower_quote.mid_price, center_quote.mid_price, upper_quote.mid_price):
            return None

        entry_value_per_unit = (lower_quote.mid_price + upper_quote.mid_price - (2.0 * center_quote.mid_price)) * 100.0
        left_width = (center_call.strike_price - lower_call.strike_price) * 100.0
        right_width = (upper_call.strike_price - center_call.strike_price) * 100.0
        wing_width = min(left_width, right_width)
        # Long call butterfly payoff math:
        # Structure: +1 lower call, -2 center calls, +1 upper call.
        # If opened for a debit (typical): max loss = debit paid (underlying
        # moves far away from center strike). Max profit = narrower wing width
        # minus debit paid (underlying pins exactly at center strike at
        # expiration). If opened for a credit (unusual, asymmetric strikes):
        # max loss = wider wing width minus credit received,
        # max profit = narrower wing width + credit received.
        if entry_value_per_unit >= 0:
            capital_per_unit = entry_value_per_unit
            max_loss_per_unit = entry_value_per_unit
            max_profit_per_unit = max(wing_width - entry_value_per_unit, 0.0)
        else:
            credit = abs(entry_value_per_unit)
            wider_wing = max(left_width, right_width)
            capital_per_unit = max(wider_wing - credit, 0.0)
            max_loss_per_unit = max(wider_wing - credit, 0.0)
            max_profit_per_unit = wing_width + credit

        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": lower_call.ticker,
                    "side": "long",
                    "contract_type": "call",
                    "strike_price": lower_call.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": lower_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": center_call.ticker,
                    "side": "short",
                    "contract_type": "call",
                    "strike_price": center_call.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 2,
                    "entry_mid": center_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": upper_call.ticker,
                    "side": "long",
                    "contract_type": "call",
                    "strike_price": upper_call.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": upper_quote.mid_price,
                },
            ],
            "assumptions": [
                "Butterfly is modeled as a long call butterfly with the center strike nearest spot.",
                "The wings use the immediately adjacent listed strikes above and below the center.",
                "If strike spacing is asymmetric, max-profit uses the narrower wing width.",
            ],
            "capital_required_per_unit": capital_per_unit,
            "max_loss_per_unit": max_loss_per_unit,
            "max_profit_per_unit": max_profit_per_unit,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([lower_call.ticker, center_call.ticker, upper_call.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    lower_call.ticker,
                    "call",
                    1,
                    lower_call.strike_price,
                    expiration,
                    1,
                    lower_quote.mid_price,
                    lower_quote.mid_price,
                ),
                OpenOptionLeg(
                    center_call.ticker,
                    "call",
                    -1,
                    center_call.strike_price,
                    expiration,
                    2,
                    center_quote.mid_price,
                    center_quote.mid_price,
                ),
                OpenOptionLeg(
                    upper_call.ticker,
                    "call",
                    1,
                    upper_call.strike_price,
                    expiration,
                    1,
                    upper_quote.mid_price,
                    upper_quote.mid_price,
                ),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital_per_unit,
            max_loss_per_unit=max_loss_per_unit,
            max_profit_per_unit=max_profit_per_unit,
            detail_json=detail_json,
        )


BUTTERFLY_STRATEGY = ButterflyStrategy()
