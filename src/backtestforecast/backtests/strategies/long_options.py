from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
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
class LongOptionStrategy(StrategyDefinition):
    strategy_type: str
    contract_type: str = "call"
    margin_warning_message: str | None = None

    @staticmethod
    def _preferred_strike_band(
        underlying_close: float,
        strike_override: object | None,
    ) -> tuple[float, float] | None:
        if strike_override is None:
            buffer = max(25.0, underlying_close * 0.15)
            return (max(0.5, underlying_close - buffer), underlying_close + buffer)

        mode = getattr(strike_override, "mode", None)
        value = float(getattr(strike_override, "value", 0) or 0)
        mode_value = getattr(mode, "value", mode)

        if mode_value == "nearest_otm":
            buffer = max(25.0, underlying_close * 0.15)
            return (max(0.5, underlying_close - buffer), underlying_close + buffer)

        if mode_value == "atm_offset_steps" and value <= 5:
            buffer = max(30.0, underlying_close * 0.15 + value * 10.0)
            return (max(0.5, underlying_close - buffer), underlying_close + buffer)

        return None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        strike_override = (
            overrides.long_call_strike if self.contract_type == "call" else overrides.long_put_strike
        )
        strike_band = self._preferred_strike_band(bar.close_price, strike_override)
        exact_expiration_fetch = getattr(option_gateway, "list_contracts_for_preferred_expiration", None)
        if callable(exact_expiration_fetch):
            exp_contracts = exact_expiration_fetch(
                entry_date=bar.trade_date,
                contract_type=self.contract_type,
                target_dte=config.target_dte,
                dte_tolerance_days=config.dte_tolerance_days,
                strike_price_gte=strike_band[0] if strike_band is not None else None,
                strike_price_lte=strike_band[1] if strike_band is not None else None,
            )
            primary_expiration = exp_contracts[0].expiration_date
        else:
            all_contracts = option_gateway.list_contracts(
                entry_date=bar.trade_date,
                contract_type=self.contract_type,
                target_dte=config.target_dte,
                dte_tolerance_days=config.dte_tolerance_days,
            )
            primary_expiration = choose_primary_expiration(all_contracts, bar.trade_date, config.target_dte)
            exp_contracts = contracts_for_expiration(all_contracts, primary_expiration)
        dte = (primary_expiration - bar.trade_date).days
        strike = resolve_strike(
            [c.strike_price for c in exp_contracts],
            bar.close_price,
            self.contract_type,
            strike_override,
            dte,
            contracts=exp_contracts,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=getattr(option_gateway, '_iv_cache', None),
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
        )
        contract = require_contract_for_strike(exp_contracts, strike)
        entry_quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
        if entry_quote is None or not valid_entry_mids(entry_quote.mid_price):
            return None

        entry_value_per_unit = entry_quote.mid_price * 100.0
        if self.contract_type == "put":
            max_profit = max(contract.strike_price * 100.0 - entry_value_per_unit, 0.0)
        else:
            max_profit = None
        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": contract.ticker,
                    "side": "long",
                    "contract_type": contract.contract_type,
                    "strike_price": contract.strike_price,
                    "expiration_date": contract.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": entry_quote.mid_price,
                }
            ],
            "assumptions": [
                "Nearest eligible expiration to target_dte is selected.",
                "Default strike selection is nearest OTM; overridable via strategy_overrides.",
            ],
            "capital_required_per_unit": entry_value_per_unit,
            "max_loss_per_unit": entry_value_per_unit,
            "max_profit_per_unit": max_profit,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=contract.ticker,
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(contract.expiration_date - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    ticker=contract.ticker,
                    contract_type=contract.contract_type,
                    side=1,
                    strike_price=contract.strike_price,
                    expiration_date=contract.expiration_date,
                    quantity_per_unit=1,
                    entry_mid=entry_quote.mid_price,
                    last_mid=entry_quote.mid_price,
                )
            ],
            scheduled_exit_date=contract.expiration_date,
            capital_required_per_unit=entry_value_per_unit,
            max_loss_per_unit=entry_value_per_unit,
            max_profit_per_unit=max_profit,
            detail_json=detail_json,
        )


LONG_CALL_STRATEGY = LongOptionStrategy(strategy_type="long_call", contract_type="call")
LONG_PUT_STRATEGY = LongOptionStrategy(strategy_type="long_put", contract_type="put")
