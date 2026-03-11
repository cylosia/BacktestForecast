from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_common_atm_strike,
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
    synthetic_ticker,
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
class VolatilityExpansionStrategy(StrategyDefinition):
    strategy_type: str
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
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_expirations = sorted(
            {contract.expiration_date for contract in calls} & {contract.expiration_date for contract in puts}
        )
        if not common_expirations:
            raise DataUnavailableError(
                "No common call/put expiration was available for volatility strategy construction."
            )
        expiration = choose_primary_expiration(
            [contract for contract in calls if contract.expiration_date in common_expirations],
            bar.trade_date,
            config.target_dte,
        )
        call_contracts = contracts_for_expiration(calls, expiration)
        put_contracts = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        if self.strategy_type == "long_straddle":
            common_strike = choose_common_atm_strike(call_contracts, put_contracts, bar.close_price)
            call_contract = require_contract_for_strike(call_contracts, common_strike)
            put_contract = require_contract_for_strike(put_contracts, common_strike)
            assumptions = [
                "The straddle buys the nearest common ATM call and put strike at the selected expiration.",
                "Position stays open until expiration, max_holding_days, or backtest end.",
            ]
        else:
            call_strike = resolve_strike(
                [c.strike_price for c in call_contracts],
                bar.close_price,
                "call",
                overrides.long_call_strike,
                dte,
            )
            put_strike = resolve_strike(
                [c.strike_price for c in put_contracts],
                bar.close_price,
                "put",
                overrides.long_put_strike,
                dte,
            )
            call_contract = require_contract_for_strike(call_contracts, call_strike)
            put_contract = require_contract_for_strike(put_contracts, put_strike)
            assumptions = [
                "The strangle buys one OTM call and one OTM put (configurable).",
                "Position stays open until expiration, max_holding_days, or backtest end.",
            ]

        call_quote = option_gateway.get_quote(call_contract.ticker, bar.trade_date)
        put_quote = option_gateway.get_quote(put_contract.ticker, bar.trade_date)
        if call_quote is None or put_quote is None:
            return None

        entry_value_per_unit = (call_quote.mid_price + put_quote.mid_price) * 100.0
        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": call_contract.ticker,
                    "side": "long",
                    "contract_type": "call",
                    "strike_price": call_contract.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": call_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": put_contract.ticker,
                    "side": "long",
                    "contract_type": "put",
                    "strike_price": put_contract.strike_price,
                    "expiration_date": expiration.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": put_quote.mid_price,
                },
            ],
            "assumptions": assumptions,
            "capital_required_per_unit": entry_value_per_unit,
            "max_loss_per_unit": entry_value_per_unit,
            "max_profit_per_unit": None,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([call_contract.ticker, put_contract.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    call_contract.ticker,
                    "call",
                    1,
                    call_contract.strike_price,
                    expiration,
                    1,
                    call_quote.mid_price,
                    call_quote.mid_price,
                ),
                OpenOptionLeg(
                    put_contract.ticker,
                    "put",
                    1,
                    put_contract.strike_price,
                    expiration,
                    1,
                    put_quote.mid_price,
                    put_quote.mid_price,
                ),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=entry_value_per_unit,
            max_loss_per_unit=entry_value_per_unit,
            max_profit_per_unit=None,
            detail_json=detail_json,
        )


LONG_STRADDLE_STRATEGY = VolatilityExpansionStrategy(strategy_type="long_straddle")
LONG_STRANGLE_STRATEGY = VolatilityExpansionStrategy(strategy_type="long_strangle")
