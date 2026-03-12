from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import naked_option_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
)
from backtestforecast.backtests.types import (
    BacktestConfig,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OptionDataGateway,
)
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class NakedOptionStrategy(StrategyDefinition):
    strategy_type: str
    contract_type: str
    margin_warning_message: str | None = "Naked options have theoretically unlimited loss and require margin approval."

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        contracts = option_gateway.list_contracts(
            bar.trade_date,
            self.contract_type,
            config.target_dte,
            config.dte_tolerance_days,
        )
        expiration = choose_primary_expiration(contracts, bar.trade_date, config.target_dte)
        exp_contracts = contracts_for_expiration(contracts, expiration)
        dte = (expiration - bar.trade_date).days

        override = overrides.short_call_strike if self.contract_type == "call" else overrides.short_put_strike
        strike = resolve_strike(
            [c.strike_price for c in exp_contracts],
            bar.close_price,
            self.contract_type,
            override,
            dte,
            contracts=exp_contracts, option_gateway=option_gateway, trade_date=bar.trade_date,
        )
        contract = require_contract_for_strike(exp_contracts, strike)
        quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
        if quote is None:
            return None

        credit = quote.mid_price * 100.0
        margin = naked_option_margin(self.contract_type, bar.close_price, strike, quote.mid_price)

        return OpenMultiLegPosition(
            display_ticker=contract.ticker,
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=dte,
            option_legs=[
                OpenOptionLeg(
                    contract.ticker,
                    self.contract_type,
                    -1,
                    strike,
                    expiration,
                    1,
                    quote.mid_price,
                    quote.mid_price,
                ),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=margin,
            max_loss_per_unit=None,
            max_profit_per_unit=credit,
            detail_json={
                "legs": [
                    {
                        "asset_type": "option",
                        "ticker": contract.ticker,
                        "side": "short",
                        "contract_type": self.contract_type,
                        "strike_price": strike,
                        "entry_mid": quote.mid_price,
                    }
                ],
                "assumptions": [
                    "Naked option sold at mid-price.",
                    "Margin per Reg T: max(20% underlying − OTM amount + premium, 10% × underlying/strike + premium).",
                    "Theoretically unlimited loss on calls; loss to zero on puts.",
                ],
                "margin_per_contract": margin,
            },
        )


NAKED_CALL_STRATEGY = NakedOptionStrategy(strategy_type="naked_call", contract_type="call")
NAKED_PUT_STRATEGY = NakedOptionStrategy(strategy_type="naked_put", contract_type="put")
