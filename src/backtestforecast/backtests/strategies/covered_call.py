from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import covered_call_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    get_overrides,
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    select_preferred_expiration_contracts,
    synthetic_ticker,
    valid_entry_mids,
)
from backtestforecast.backtests.types import (
    BacktestConfig,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OpenStockLeg,
    OptionDataGateway,
)
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class CoveredCallStrategy(StrategyDefinition):
    strategy_type: str = "covered_call"
    margin_warning_message: str | None = "Covered call sizing is constrained by 100-share stock ownership per contract."

    def estimate_minimum_capital_required_per_unit(
        self,
        config: BacktestConfig,
        bar: DailyBar,
    ) -> float | None:
        return covered_call_margin(bar.close_price)

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        primary_expiration, call_contracts = select_preferred_expiration_contracts(
            option_gateway,
            entry_date=bar.trade_date,
            contract_type="call",
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        dte = (primary_expiration - bar.trade_date).days
        delta_lookup = maybe_build_contract_delta_lookup(
            selection=overrides.short_call_strike,
            contracts=call_contracts,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=dte,
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
            dividend_yield=config.dividend_yield,
            iv_cache=getattr(option_gateway, "_iv_cache", None),
        )
        strike = resolve_strike(
            [c.strike_price for c in call_contracts],
            bar.close_price,
            "call",
            overrides.short_call_strike,
            dte,
            delta_lookup=delta_lookup,
            contracts=call_contracts,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=getattr(option_gateway, '_iv_cache', None),
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
        )
        short_call = require_contract_for_strike(call_contracts, strike)
        quote = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        if quote is None or not valid_entry_mids(quote.mid_price):
            return None

        stock_value = bar.close_price * 100.0
        call_credit = quote.mid_price * 100.0
        entry_value_per_unit = stock_value - call_credit
        max_loss_per_unit = entry_value_per_unit
        margin = covered_call_margin(bar.close_price)
        max_profit_per_unit = ((short_call.strike_price - bar.close_price) * 100.0) + call_credit
        detail_json = {
            "legs": [
                {
                    "asset_type": "stock",
                    "identifier": config.symbol,
                    "side": "long",
                    "share_quantity_per_unit": 100,
                    "entry_price": bar.close_price,
                },
                {
                    "asset_type": "option",
                    "ticker": short_call.ticker,
                    "side": "short",
                    "contract_type": "call",
                    "strike_price": short_call.strike_price,
                    "expiration_date": short_call.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": quote.mid_price,
                },
            ],
            "assumptions": [
                "Covered call enters by purchasing 100 shares and selling one OTM call.",
                "Stock commissions are assumed to be zero in this slice;"
                " only option commission_per_contract is charged.",
                "Combined position is exited at option expiration, max_holding_days, or backtest end.",
            ],
            "capital_required_per_unit": margin,
            "max_loss_per_unit": max_loss_per_unit,
            "max_profit_per_unit": max_profit_per_unit,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([config.symbol, short_call.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(short_call.expiration_date - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    ticker=short_call.ticker,
                    contract_type="call",
                    side=-1,
                    strike_price=short_call.strike_price,
                    expiration_date=short_call.expiration_date,
                    quantity_per_unit=1,
                    entry_mid=quote.mid_price,
                    last_mid=quote.mid_price,
                )
            ],
            stock_legs=[
                OpenStockLeg(
                    symbol=config.symbol,
                    side=1,
                    share_quantity_per_unit=100,
                    entry_price=bar.close_price,
                    last_price=bar.close_price,
                )
            ],
            scheduled_exit_date=short_call.expiration_date,
            capital_required_per_unit=margin,
            max_loss_per_unit=max_loss_per_unit,
            max_profit_per_unit=max_profit_per_unit,
            detail_json=detail_json,
        )


COVERED_CALL_STRATEGY = CoveredCallStrategy()
