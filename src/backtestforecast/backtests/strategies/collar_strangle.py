from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import collar_margin, covered_strangle_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
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
    OpenStockLeg,
    OptionDataGateway,
)
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class CollarStrategy(StrategyDefinition):
    strategy_type: str = "collar"
    margin_warning_message: str | None = "Collar sizing is constrained by 100-share stock ownership per contract."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_exp:
            raise DataUnavailableError("No common expiration for collar.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_exp], bar.trade_date, config.target_dte
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        call_strike = resolve_strike(
            [c.strike_price for c in cc], bar.close_price, "call", overrides.short_call_strike, dte
        )
        put_strike = resolve_strike(
            [c.strike_price for c in pc], bar.close_price, "put", overrides.long_put_strike, dte
        )
        short_call = require_contract_for_strike(cc, call_strike)
        long_put = require_contract_for_strike(pc, put_strike)

        cq = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        pq = option_gateway.get_quote(long_put.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None

        net_option_cost = (pq.mid_price - cq.mid_price) * 100.0
        capital = collar_margin(bar.close_price)
        max_loss = (bar.close_price - put_strike) * 100.0 + net_option_cost

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
            max_loss_per_unit=max(max_loss, 0.0),
            detail_json={
                "legs": [
                    {
                        "asset_type": "stock",
                        "symbol": config.symbol,
                        "side": "long",
                        "shares": 100,
                        "entry_price": bar.close_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": short_call.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": call_strike,
                        "entry_mid": cq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": long_put.ticker,
                        "side": "long",
                        "contract_type": "put",
                        "strike_price": put_strike,
                        "entry_mid": pq.mid_price,
                    },
                ]
            },
        )


@dataclass(frozen=True, slots=True)
class CoveredStrangleStrategy(StrategyDefinition):
    strategy_type: str = "covered_strangle"
    margin_warning_message: str | None = "Covered strangle requires 100-share ownership and margin for the short put."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_exp:
            raise DataUnavailableError("No common expiration for covered strangle.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_exp], bar.trade_date, config.target_dte
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        call_strike = resolve_strike(
            [c.strike_price for c in cc], bar.close_price, "call", overrides.short_call_strike, dte
        )
        put_strike = resolve_strike(
            [c.strike_price for c in pc], bar.close_price, "put", overrides.short_put_strike, dte
        )
        short_call = require_contract_for_strike(cc, call_strike)
        short_put = require_contract_for_strike(pc, put_strike)

        cq = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        pq = option_gateway.get_quote(short_put.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None

        credit = (cq.mid_price + pq.mid_price) * 100.0
        capital = covered_strangle_margin(bar.close_price, put_strike, pq.mid_price)

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
            max_loss_per_unit=None,
            max_profit_per_unit=credit,
            detail_json={
                "legs": [
                    {"asset_type": "stock", "symbol": config.symbol, "side": "long", "shares": 100},
                    {
                        "asset_type": "option",
                        "ticker": short_call.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": call_strike,
                    },
                    {
                        "asset_type": "option",
                        "ticker": short_put.ticker,
                        "side": "short",
                        "contract_type": "put",
                        "strike_price": put_strike,
                    },
                ]
            },
        )


COLLAR_STRATEGY = CollarStrategy()
COVERED_STRANGLE_STRATEGY = CoveredStrangleStrategy()
