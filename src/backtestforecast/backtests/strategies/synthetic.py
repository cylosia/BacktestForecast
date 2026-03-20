from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import short_stock_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    choose_common_atm_strike,
    choose_primary_expiration,
    contracts_for_expiration,
    require_contract_for_strike,
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
class SyntheticPutStrategy(StrategyDefinition):
    """Short 100 shares + buy 1 ATM call. Behaves like a long put."""

    strategy_type: str = "synthetic_put"
    margin_warning_message: str | None = "Synthetic put requires margin for the short stock position."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        cc = contracts_for_expiration(calls, expiration)
        if not cc:
            return None
        strike = choose_atm_strike([c.strike_price for c in cc], bar.close_price)
        long_call = require_contract_for_strike(cc, strike)

        cq = option_gateway.get_quote(long_call.ticker, bar.trade_date)
        if cq is None or not valid_entry_mids(cq.mid_price):
            return None

        # Synthetic put payoff math:
        # Structure: short 100 shares + long 1 ATM call.
        # Behaves like a long put: profits when underlying falls, capped loss
        # when underlying rises (call offsets stock loss above the strike).
        # Max loss = call premium + (strike - entry_price)*100 if strike > entry
        # (stock loss up to the strike minus call intrinsic value at that point),
        # or just the premium if strike <= entry (call is OTM, stock shorts
        # profitably to the strike). Max profit = theoretically unlimited
        # (stock can fall to zero), set to None.
        premium = cq.mid_price * 100.0
        margin = short_stock_margin(bar.close_price)
        capital = margin + premium

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([config.symbol, long_call.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_call.ticker, "call", 1, strike, expiration, 1, cq.mid_price, cq.mid_price),
            ],
            stock_legs=[OpenStockLeg(config.symbol, -1, 100, bar.close_price, bar.close_price)],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max(premium + (strike - bar.close_price) * 100.0, 0.0) if strike > bar.close_price else max(premium - (bar.close_price - strike) * 100.0, 0.0),
            max_profit_per_unit=None,
            detail_json={
                "legs": [
                    {"asset_type": "stock", "identifier": config.symbol, "side": "short", "share_quantity_per_unit": 100, "entry_price": bar.close_price},
                    {
                        "asset_type": "option",
                        "ticker": long_call.ticker,
                        "side": "long",
                        "contract_type": "call",
                        "strike_price": strike,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": cq.mid_price,
                    },
                ]
            },
        )


@dataclass(frozen=True, slots=True)
class ReverseConversionStrategy(StrategyDefinition):
    """Short 100 shares + buy 1 ATM call + sell 1 ATM put (same strike). Arbitrage-style."""

    strategy_type: str = "reverse_conversion"
    margin_warning_message: str | None = "Reverse conversion requires margin for the short stock."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_exp:
            raise DataUnavailableError("No common expiration for reverse conversion.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_exp],
            bar.trade_date,
            config.target_dte,
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)
        strike = choose_common_atm_strike(cc, pc, bar.close_price)

        long_call = require_contract_for_strike(cc, strike)
        short_put = require_contract_for_strike(pc, strike)

        cq = option_gateway.get_quote(long_call.ticker, bar.trade_date)
        pq = option_gateway.get_quote(short_put.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None
        if not valid_entry_mids(cq.mid_price, pq.mid_price):
            return None

        # Reverse conversion payoff math:
        # Structure: short 100 shares + long 1 ATM call + short 1 ATM put
        # (same strike). Arbitrage-style: P/L is the net option cost vs the
        # stock-to-strike carry. Max loss = net option cost - (entry - strike)
        # * 100 if that quantity is positive; otherwise zero. Max profit is
        # not explicitly tracked (set to None by the framework).
        net_option = (cq.mid_price - pq.mid_price) * 100.0
        margin = short_stock_margin(bar.close_price)
        capital = margin + max(net_option, 0.0)

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([config.symbol, long_call.ticker, short_put.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_call.ticker, "call", 1, strike, expiration, 1, cq.mid_price, cq.mid_price),
                OpenOptionLeg(short_put.ticker, "put", -1, strike, expiration, 1, pq.mid_price, pq.mid_price),
            ],
            stock_legs=[OpenStockLeg(config.symbol, -1, 100, bar.close_price, bar.close_price)],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max(net_option - (bar.close_price - strike) * 100.0, 0.0),
            detail_json={
                "strike": strike,
                "net_option_cost": net_option,
                "legs": [
                    {"asset_type": "stock", "identifier": config.symbol, "side": "short", "share_quantity_per_unit": 100, "entry_price": bar.close_price},
                    {"asset_type": "option", "ticker": long_call.ticker, "side": "long", "contract_type": "call", "strike_price": strike, "expiration_date": expiration.isoformat(), "quantity_per_unit": 1, "entry_mid": cq.mid_price},
                    {"asset_type": "option", "ticker": short_put.ticker, "side": "short", "contract_type": "put", "strike_price": strike, "expiration_date": expiration.isoformat(), "quantity_per_unit": 1, "entry_mid": pq.mid_price},
                ],
            },
        )


SYNTHETIC_PUT_STRATEGY = SyntheticPutStrategy()
REVERSE_CONVERSION_STRATEGY = ReverseConversionStrategy()
