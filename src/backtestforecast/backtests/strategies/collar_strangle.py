from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import collar_margin, covered_strangle_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    get_overrides,
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    select_preferred_common_expiration_contracts,
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
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class CollarStrategy(StrategyDefinition):
    strategy_type: str = "collar"
    margin_warning_message: str | None = "Collar sizing is constrained by 100-share stock ownership per contract."

    def estimate_minimum_capital_required_per_unit(
        self,
        config: BacktestConfig,
        bar: DailyBar,
    ) -> float | None:
        return collar_margin(bar.close_price)

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        expiration, cc, pc = select_preferred_common_expiration_contracts(
            option_gateway,
            entry_date=bar.trade_date,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        dte = (expiration - bar.trade_date).days

        _iv_cache = getattr(option_gateway, '_iv_cache', None)
        risk_free_rate = config.resolve_risk_free_rate(bar.trade_date)
        call_delta_lookup = maybe_build_contract_delta_lookup(
            selection=overrides.short_call_strike,
            contracts=cc,
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
            contracts=pc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=dte,
            risk_free_rate=risk_free_rate,
            dividend_yield=config.dividend_yield,
            iv_cache=_iv_cache,
        )
        call_strike = resolve_strike(
            [c.strike_price for c in cc],
            bar.close_price,
            "call",
            overrides.short_call_strike,
            dte,
            delta_lookup=call_delta_lookup,
            contracts=cc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=_iv_cache,
            risk_free_rate=risk_free_rate,
        )
        put_strike = resolve_strike(
            [c.strike_price for c in pc],
            bar.close_price,
            "put",
            overrides.long_put_strike,
            dte,
            delta_lookup=put_delta_lookup,
            contracts=pc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=_iv_cache,
            risk_free_rate=risk_free_rate,
        )
        short_call = require_contract_for_strike(cc, call_strike)
        long_put = require_contract_for_strike(pc, put_strike)

        cq = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        pq = option_gateway.get_quote(long_put.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None
        if not valid_entry_mids(cq.mid_price, pq.mid_price):
            return None

        net_option_cost = (pq.mid_price - cq.mid_price) * 100.0
        capital = collar_margin(bar.close_price) + max(net_option_cost, 0.0)
        max_loss = max((bar.close_price - put_strike) * 100.0 + net_option_cost, 0.0)
        max_profit = max((call_strike - bar.close_price) * 100.0 - net_option_cost, 0.0)
        entry_package_market_value = bar.close_price * 100.0 + net_option_cost

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([config.symbol, short_call.ticker, long_put.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(short_call.ticker, "call", -1, call_strike, expiration, 1, cq.mid_price, cq.mid_price),
                OpenOptionLeg(long_put.ticker, "put", 1, put_strike, expiration, 1, pq.mid_price, pq.mid_price),
            ],
            stock_legs=[OpenStockLeg(config.symbol, 1, 100, bar.close_price, bar.close_price)],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            max_profit_per_unit=max_profit,
            detail_json={
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
                        "strike_price": call_strike,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": cq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": long_put.ticker,
                        "side": "long",
                        "contract_type": "put",
                        "strike_price": put_strike,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": pq.mid_price,
                    },
                ],
                "assumptions": [
                    "Collar enters by purchasing 100 shares, selling one OTM call, and buying one OTM put.",
                    "Stock commissions are assumed to be zero in this slice;"
                    " only option commission_per_contract is charged.",
                    "Combined position is exited at option expiration, max_holding_days, or backtest end.",
                ],
                "capital_required_per_unit": capital,
                "max_loss_per_unit": max_loss,
                "max_profit_per_unit": max_profit,
                "entry_package_market_value": entry_package_market_value,
            },
        )


@dataclass(frozen=True, slots=True)
class CoveredStrangleStrategy(StrategyDefinition):
    strategy_type: str = "covered_strangle"
    margin_warning_message: str | None = "Covered strangle requires 100-share ownership and margin for the short put."

    def estimate_minimum_capital_required_per_unit(
        self,
        config: BacktestConfig,
        bar: DailyBar,
    ) -> float | None:
        return bar.close_price * 100.0

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        expiration, cc, pc = select_preferred_common_expiration_contracts(
            option_gateway,
            entry_date=bar.trade_date,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        dte = (expiration - bar.trade_date).days

        _iv_cache = getattr(option_gateway, '_iv_cache', None)
        risk_free_rate = config.resolve_risk_free_rate(bar.trade_date)
        call_delta_lookup = maybe_build_contract_delta_lookup(
            selection=overrides.short_call_strike,
            contracts=cc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=dte,
            risk_free_rate=risk_free_rate,
            dividend_yield=config.dividend_yield,
            iv_cache=_iv_cache,
        )
        put_delta_lookup = maybe_build_contract_delta_lookup(
            selection=overrides.short_put_strike,
            contracts=pc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=dte,
            risk_free_rate=risk_free_rate,
            dividend_yield=config.dividend_yield,
            iv_cache=_iv_cache,
        )
        call_strike = resolve_strike(
            [c.strike_price for c in cc],
            bar.close_price,
            "call",
            overrides.short_call_strike,
            dte,
            delta_lookup=call_delta_lookup,
            contracts=cc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=_iv_cache,
            risk_free_rate=risk_free_rate,
        )
        put_strike = resolve_strike(
            [c.strike_price for c in pc],
            bar.close_price,
            "put",
            overrides.short_put_strike,
            dte,
            delta_lookup=put_delta_lookup,
            contracts=pc,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            iv_cache=_iv_cache,
            risk_free_rate=risk_free_rate,
        )
        short_call = require_contract_for_strike(cc, call_strike)
        short_put = require_contract_for_strike(pc, put_strike)

        cq = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        pq = option_gateway.get_quote(short_put.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None
        if not valid_entry_mids(cq.mid_price, pq.mid_price):
            return None

        credit = (cq.mid_price + pq.mid_price) * 100.0
        capital = covered_strangle_margin(bar.close_price, put_strike, pq.mid_price)
        # Worst case: stock drops to $0 AND short put is exercised, forcing
        # purchase of 100 additional shares at the put strike. This represents
        # a 2x position exposure scenario and is intentionally conservative.
        max_loss = max((bar.close_price * 100.0) + (put_strike * 100.0) - credit, 0.0)
        max_profit = max((call_strike - bar.close_price) * 100.0, 0.0) + credit
        entry_package_market_value = bar.close_price * 100.0 - credit

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([config.symbol, short_call.ticker, short_put.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(short_call.ticker, "call", -1, call_strike, expiration, 1, cq.mid_price, cq.mid_price),
                OpenOptionLeg(short_put.ticker, "put", -1, put_strike, expiration, 1, pq.mid_price, pq.mid_price),
            ],
            stock_legs=[OpenStockLeg(config.symbol, 1, 100, bar.close_price, bar.close_price)],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            max_profit_per_unit=max_profit,
            detail_json={
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
                        "strike_price": call_strike,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": cq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": short_put.ticker,
                        "side": "short",
                        "contract_type": "put",
                        "strike_price": put_strike,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": pq.mid_price,
                    },
                ],
                "assumptions": [
                    "Covered strangle enters by purchasing 100 shares, selling one OTM call, and selling one OTM put.",
                    "Stock commissions are assumed to be zero in this slice;"
                    " only option commission_per_contract is charged.",
                    "Combined position is exited at option expiration, max_holding_days, or backtest end.",
                ],
                "capital_required_per_unit": capital,
                "max_loss_per_unit": max_loss,
                "max_profit_per_unit": max_profit,
                "entry_package_market_value": entry_package_market_value,
            },
        )


COLLAR_STRATEGY = CollarStrategy()
COVERED_STRANGLE_STRATEGY = CoveredStrangleStrategy()
