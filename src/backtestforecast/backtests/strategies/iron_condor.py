from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import iron_condor_margin
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
class IronCondorStrategy(StrategyDefinition):
    strategy_type: str = "iron_condor"
    margin_warning_message: str | None = (
        "Iron condor sizing is constrained by the widest side of the spread package at risk."
    )

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_expirations = sorted(
            {contract.expiration_date for contract in calls} & {contract.expiration_date for contract in puts}
        )
        if not common_expirations:
            raise DataUnavailableError("No common call/put expiration was available for iron condor construction.")
        expiration = choose_primary_expiration(
            [contract for contract in calls if contract.expiration_date in common_expirations],
            bar.trade_date,
            config.target_dte,
        )
        call_contracts = contracts_for_expiration(calls, expiration)
        put_contracts = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        call_short_strike = resolve_strike(
            [c.strike_price for c in call_contracts],
            bar.close_price,
            "call",
            overrides.short_call_strike,
            dte,
            contracts=call_contracts, option_gateway=option_gateway, trade_date=bar.trade_date,
        )
        put_short_strike = resolve_strike(
            [c.strike_price for c in put_contracts],
            bar.close_price,
            "put",
            overrides.short_put_strike,
            dte,
            contracts=put_contracts, option_gateway=option_gateway, trade_date=bar.trade_date,
        )
        call_long_strike = resolve_wing_strike(
            [c.strike_price for c in call_contracts],
            call_short_strike,
            1,
            bar.close_price,
            overrides.spread_width,
        )
        put_long_strike = resolve_wing_strike(
            [c.strike_price for c in put_contracts],
            put_short_strike,
            -1,
            bar.close_price,
            overrides.spread_width,
        )
        if call_long_strike is None or put_long_strike is None:
            raise DataUnavailableError("Iron condor requires one listed wing strike beyond each short strike.")

        short_call = require_contract_for_strike(call_contracts, call_short_strike)
        long_call = require_contract_for_strike(call_contracts, call_long_strike)
        short_put = require_contract_for_strike(put_contracts, put_short_strike)
        long_put = require_contract_for_strike(put_contracts, put_long_strike)

        quotes = {
            short_call.ticker: option_gateway.get_quote(short_call.ticker, bar.trade_date),
            long_call.ticker: option_gateway.get_quote(long_call.ticker, bar.trade_date),
            short_put.ticker: option_gateway.get_quote(short_put.ticker, bar.trade_date),
            long_put.ticker: option_gateway.get_quote(long_put.ticker, bar.trade_date),
        }
        if any(quote is None for quote in quotes.values()):
            return None

        sc = quotes[short_call.ticker].mid_price  # type: ignore[union-attr]
        lc = quotes[long_call.ticker].mid_price  # type: ignore[union-attr]
        sp = quotes[short_put.ticker].mid_price  # type: ignore[union-attr]
        lp = quotes[long_put.ticker].mid_price  # type: ignore[union-attr]
        if not valid_entry_mids(sc, lc, sp, lp):
            return None
        entry_value_per_unit = (lc + lp - sc - sp) * 100.0
        put_width = abs(short_put.strike_price - long_put.strike_price) * 100.0
        call_width = abs(long_call.strike_price - short_call.strike_price) * 100.0
        wider_spread = max(put_width, call_width)
        if entry_value_per_unit <= 0:
            credit = abs(entry_value_per_unit)
            max_loss_per_unit = max(wider_spread - credit, 0.0)
            max_profit_per_unit = credit
        else:
            max_loss_per_unit = wider_spread + entry_value_per_unit
            max_profit_per_unit = max(wider_spread - entry_value_per_unit, 0.0)
        margin = iron_condor_margin(
            long_call.strike_price - short_call.strike_price,
            short_put.strike_price - long_put.strike_price,
        )

        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": long_put.ticker,
                    "side": "long",
                    "contract_type": "put",
                    "strike_price": long_put.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": lp,
                },
                {
                    "asset_type": "option",
                    "ticker": short_put.ticker,
                    "side": "short",
                    "contract_type": "put",
                    "strike_price": short_put.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": sp,
                },
                {
                    "asset_type": "option",
                    "ticker": short_call.ticker,
                    "side": "short",
                    "contract_type": "call",
                    "strike_price": short_call.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": sc,
                },
                {
                    "asset_type": "option",
                    "ticker": long_call.ticker,
                    "side": "long",
                    "contract_type": "call",
                    "strike_price": long_call.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": lc,
                },
            ],
            "assumptions": [
                "Short strikes are placed one listed strike OTM on each side.",
                "Long wings are placed one additional listed strike beyond the shorts.",
                "The package is exited no later than the shared short-leg expiration.",
            ],
            "capital_required_per_unit": margin,
            "max_loss_per_unit": max_loss_per_unit,
            "max_profit_per_unit": max_profit_per_unit,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_put.ticker, short_put.ticker, short_call.ticker, long_call.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_put.ticker, "put", 1, long_put.strike_price, expiration, 1, lp, lp),
                OpenOptionLeg(short_put.ticker, "put", -1, short_put.strike_price, expiration, 1, sp, sp),
                OpenOptionLeg(short_call.ticker, "call", -1, short_call.strike_price, expiration, 1, sc, sc),
                OpenOptionLeg(long_call.ticker, "call", 1, long_call.strike_price, expiration, 1, lc, lc),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=margin,
            max_loss_per_unit=max_loss_per_unit,
            max_profit_per_unit=max_profit_per_unit,
            detail_json=detail_json,
        )


IRON_CONDOR_STRATEGY = IronCondorStrategy()
