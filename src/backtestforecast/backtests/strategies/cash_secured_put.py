from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import cash_secured_put_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    get_overrides,
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    select_preferred_expiration_contracts,
    sorted_unique_strikes,
    valid_entry_mids,
)
from backtestforecast.backtests.types import (
    BacktestConfig,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OptionDataGateway,
)
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class CashSecuredPutStrategy(StrategyDefinition):
    strategy_type: str = "cash_secured_put"
    margin_warning_message: str | None = (
        "Cash-secured put sizing is constrained by full strike collateral, not only premium received."
    )

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        primary_expiration, put_contracts = select_preferred_expiration_contracts(
            option_gateway,
            entry_date=bar.trade_date,
            contract_type="put",
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        dte = (primary_expiration - bar.trade_date).days
        delta_lookup = maybe_build_contract_delta_lookup(
            selection=overrides.short_put_strike,
            contracts=put_contracts,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=dte,
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
            dividend_yield=config.dividend_yield,
            iv_cache=getattr(option_gateway, "_iv_cache", None),
        )
        strikes = sorted_unique_strikes(put_contracts)
        strike = resolve_strike(
            strikes,
            bar.close_price,
            "put",
            overrides.short_put_strike,
            dte,
            delta_lookup=delta_lookup,
            contracts=put_contracts,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=getattr(option_gateway, '_iv_cache', None),
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
        )
        short_put = require_contract_for_strike(put_contracts, strike)
        quote = option_gateway.get_quote(short_put.ticker, bar.trade_date)
        if quote is None or not valid_entry_mids(quote.mid_price):
            return None

        entry_value_per_unit = -quote.mid_price * 100.0
        max_loss_per_unit = max((short_put.strike_price * 100.0) - abs(entry_value_per_unit), 0.0)
        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": short_put.ticker,
                    "side": "short",
                    "contract_type": "put",
                    "strike_price": short_put.strike_price,
                    "expiration_date": short_put.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": quote.mid_price,
                }
            ],
            "assumptions": [
                "The short put is one strike OTM or closest available below spot.",
                "Full strike collateral is reserved conceptually for sizing.",
                "If the put expires ITM, standalone cash-secured put P&L is realized without converting to shares.",
            ],
            "capital_required_per_unit": cash_secured_put_margin(short_put.strike_price),
            "max_loss_per_unit": max_loss_per_unit,
            "max_profit_per_unit": abs(entry_value_per_unit),
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=short_put.ticker,
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(short_put.expiration_date - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    ticker=short_put.ticker,
                    contract_type="put",
                    side=-1,
                    strike_price=short_put.strike_price,
                    expiration_date=short_put.expiration_date,
                    quantity_per_unit=1,
                    entry_mid=quote.mid_price,
                    last_mid=quote.mid_price,
                )
            ],
            scheduled_exit_date=short_put.expiration_date,
            capital_required_per_unit=cash_secured_put_margin(short_put.strike_price),
            max_loss_per_unit=max_loss_per_unit,
            max_profit_per_unit=abs(entry_value_per_unit),
            detail_json=detail_json,
        )


CASH_SECURED_PUT_STRATEGY = CashSecuredPutStrategy()
