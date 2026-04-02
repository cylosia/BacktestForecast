from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_common_atm_strike,
    get_entry_quotes,
    get_overrides,
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    select_preferred_common_expiration_contracts,
    sorted_unique_strikes,
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
        expiration, call_contracts, put_contracts = select_preferred_common_expiration_contracts(
            option_gateway,
            entry_date=bar.trade_date,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
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
            _iv_cache = getattr(option_gateway, '_iv_cache', None)
            risk_free_rate = config.resolve_risk_free_rate(bar.trade_date)
            call_delta_lookup = maybe_build_contract_delta_lookup(
                selection=overrides.long_call_strike,
                contracts=call_contracts,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                underlying_close=bar.close_price,
                dte_days=dte,
                risk_free_rate=risk_free_rate,
                dividend_yield=config.dividend_yield,
                iv_cache=_iv_cache,
            )
            put_delta_lookup = maybe_build_contract_delta_lookup(
                selection=overrides.long_put_strike,
                contracts=put_contracts,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                underlying_close=bar.close_price,
                dte_days=dte,
                risk_free_rate=risk_free_rate,
                dividend_yield=config.dividend_yield,
                iv_cache=_iv_cache,
            )
            call_strikes = sorted_unique_strikes(call_contracts)
            put_strikes = sorted_unique_strikes(put_contracts)
            call_strike = resolve_strike(
                call_strikes,
                bar.close_price,
                "call",
                overrides.long_call_strike,
                dte,
                delta_lookup=call_delta_lookup,
                contracts=call_contracts,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                iv_cache=_iv_cache,
                risk_free_rate=risk_free_rate,
            )
            put_strike = resolve_strike(
                put_strikes,
                bar.close_price,
                "put",
                overrides.long_put_strike,
                dte,
                delta_lookup=put_delta_lookup,
                contracts=put_contracts,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                iv_cache=_iv_cache,
                risk_free_rate=risk_free_rate,
            )
            call_contract = require_contract_for_strike(call_contracts, call_strike)
            put_contract = require_contract_for_strike(put_contracts, put_strike)
            assumptions = [
                "The strangle buys one OTM call and one OTM put (configurable).",
                "Position stays open until expiration, max_holding_days, or backtest end.",
            ]

        quotes = get_entry_quotes(
            option_gateway,
            trade_date=bar.trade_date,
            contracts=[call_contract, put_contract],
        )
        call_quote = quotes.get(call_contract.ticker)
        put_quote = quotes.get(put_contract.ticker)
        if call_quote is None or put_quote is None:
            return None
        if not valid_entry_mids(call_quote.mid_price, put_quote.mid_price):
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
