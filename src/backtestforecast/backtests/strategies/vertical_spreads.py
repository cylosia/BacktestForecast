from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import credit_spread_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
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
class VerticalSpreadStrategy(StrategyDefinition):
    strategy_type: str
    contract_type: str
    is_debit: bool
    margin_warning_message: str | None = None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        contracts = option_gateway.list_contracts(
            entry_date=bar.trade_date,
            contract_type=self.contract_type,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        expiration = choose_primary_expiration(contracts, bar.trade_date, config.target_dte)
        expiration_contracts = contracts_for_expiration(contracts, expiration)
        strikes = [contract.strike_price for contract in expiration_contracts]
        dte = (expiration - bar.trade_date).days

        # Determine short leg placement override based on contract type
        short_override = overrides.short_call_strike if self.contract_type == "call" else overrides.short_put_strike

        _kw = dict(contracts=expiration_contracts, option_gateway=option_gateway, trade_date=bar.trade_date, iv_cache=getattr(option_gateway, '_iv_cache', None))
        if self.contract_type == "call":
            if self.is_debit:
                base_strike = resolve_strike(strikes, bar.close_price, "call", None, dte, **_kw)
                if short_override is not None:
                    upper_strike = resolve_strike(strikes, bar.close_price, "call", short_override, dte, **_kw)
                else:
                    upper_strike = resolve_wing_strike(
                        strikes, base_strike, 1, bar.close_price, overrides.spread_width,
                    )
            else:
                base_strike = resolve_strike(strikes, bar.close_price, "call", short_override, dte, **_kw)
                upper_strike = resolve_wing_strike(
                    strikes, base_strike, 1, bar.close_price, overrides.spread_width,
                )
            if upper_strike is None:
                raise DataUnavailableError("No higher call strike was available to build the spread.")
            lower_contract = require_contract_for_strike(expiration_contracts, base_strike)
            upper_contract = require_contract_for_strike(expiration_contracts, upper_strike)
            if self.is_debit:
                long_contract = lower_contract
                short_contract = upper_contract
            else:
                short_contract = lower_contract
                long_contract = upper_contract
        else:
            if self.is_debit:
                base_strike = resolve_strike(strikes, bar.close_price, "put", None, dte, **_kw)
                if short_override is not None:
                    lower_strike = resolve_strike(strikes, bar.close_price, "put", short_override, dte, **_kw)
                else:
                    lower_strike = resolve_wing_strike(
                        strikes, base_strike, -1, bar.close_price, overrides.spread_width,
                    )
            else:
                base_strike = resolve_strike(strikes, bar.close_price, "put", short_override, dte, **_kw)
                lower_strike = resolve_wing_strike(
                    strikes, base_strike, -1, bar.close_price, overrides.spread_width,
                )
            if lower_strike is None:
                raise DataUnavailableError("No lower put strike was available to build the spread.")
            upper_contract = require_contract_for_strike(expiration_contracts, base_strike)
            lower_contract = require_contract_for_strike(expiration_contracts, lower_strike)
            if self.is_debit:
                long_contract = upper_contract
                short_contract = lower_contract
            else:
                short_contract = upper_contract
                long_contract = lower_contract

        long_quote = option_gateway.get_quote(long_contract.ticker, bar.trade_date)
        short_quote = option_gateway.get_quote(short_contract.ticker, bar.trade_date)
        if long_quote is None or short_quote is None:
            return None
        if not valid_entry_mids(long_quote.mid_price, short_quote.mid_price):
            return None

        long_value = long_quote.mid_price * 100.0
        short_value = short_quote.mid_price * 100.0
        entry_value_per_unit = long_value - short_value
        width = abs(long_contract.strike_price - short_contract.strike_price) * 100.0

        # Vertical spread payoff math:
        # Debit spread (e.g. bull call debit, bear put debit): pay a net debit
        # upfront. Max loss = debit paid. Max profit = strike width - debit
        # (underlying moves fully through the spread at expiration).
        # Credit spread (e.g. bull put credit, bear call credit): collect a net
        # credit upfront. Max profit = credit received. Max loss = strike width
        # - credit (underlying moves fully through the spread).
        if self.is_debit:
            if entry_value_per_unit <= 0:
                return None
            debit = entry_value_per_unit
            max_loss_per_unit = debit
            max_profit_per_unit = max(width - debit, 0.0)
            capital_required_per_unit = debit
        else:
            if entry_value_per_unit > 0:
                return None
            credit = abs(entry_value_per_unit)
            max_loss_per_unit = max(width - credit, 0.0)
            max_profit_per_unit = credit
            capital_required_per_unit = credit_spread_margin(
                abs(long_contract.strike_price - short_contract.strike_price),
            )

        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": long_contract.ticker,
                    "side": "long",
                    "contract_type": self.contract_type,
                    "strike_price": long_contract.strike_price,
                    "expiration_date": long_contract.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": long_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": short_contract.ticker,
                    "side": "short",
                    "contract_type": self.contract_type,
                    "strike_price": short_contract.strike_price,
                    "expiration_date": short_contract.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": short_quote.mid_price,
                },
            ],
            "assumptions": [
                "Spreads use a single expiration nearest target_dte.",
                "Vertical width defaults to the next listed strike increment.",
                "entry_mid and exit_mid represent the net package value per 100-share option multiplier.",
            ],
            "capital_required_per_unit": capital_required_per_unit,
            "max_loss_per_unit": max_loss_per_unit,
            "max_profit_per_unit": max_profit_per_unit,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_contract.ticker, short_contract.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    ticker=long_contract.ticker,
                    contract_type=self.contract_type,
                    side=1,
                    strike_price=long_contract.strike_price,
                    expiration_date=expiration,
                    quantity_per_unit=1,
                    entry_mid=long_quote.mid_price,
                    last_mid=long_quote.mid_price,
                ),
                OpenOptionLeg(
                    ticker=short_contract.ticker,
                    contract_type=self.contract_type,
                    side=-1,
                    strike_price=short_contract.strike_price,
                    expiration_date=expiration,
                    quantity_per_unit=1,
                    entry_mid=short_quote.mid_price,
                    last_mid=short_quote.mid_price,
                ),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital_required_per_unit,
            max_loss_per_unit=max_loss_per_unit,
            max_profit_per_unit=max_profit_per_unit,
            detail_json=detail_json,
        )


BULL_CALL_DEBIT_SPREAD_STRATEGY = VerticalSpreadStrategy(
    strategy_type="bull_call_debit_spread",
    contract_type="call",
    is_debit=True,
)
BEAR_PUT_DEBIT_SPREAD_STRATEGY = VerticalSpreadStrategy(
    strategy_type="bear_put_debit_spread",
    contract_type="put",
    is_debit=True,
)
BULL_PUT_CREDIT_SPREAD_STRATEGY = VerticalSpreadStrategy(
    strategy_type="bull_put_credit_spread",
    contract_type="put",
    is_debit=False,
    margin_warning_message=(
        "Bull put credit spread sizing is constrained"
        " by spread width at risk, not premium collected."
    ),
)
BEAR_CALL_CREDIT_SPREAD_STRATEGY = VerticalSpreadStrategy(
    strategy_type="bear_call_credit_spread",
    contract_type="call",
    is_debit=False,
    margin_warning_message=(
        "Bear call credit spread sizing is constrained"
        " by spread width at risk, not premium collected."
    ),
)
