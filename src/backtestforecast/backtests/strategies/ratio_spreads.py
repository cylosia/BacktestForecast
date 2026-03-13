from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import ratio_backspread_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    offset_strike,
    require_contract_for_strike,
    resolve_strike,
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
class RatioCallBackspreadStrategy(StrategyDefinition):
    """Sell 1 lower-strike call, buy 2 higher-strike calls. Net debit or small credit."""

    strategy_type: str = "ratio_call_backspread"
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
        expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        cc = contracts_for_expiration(calls, expiration)
        strikes = sorted_unique_strikes(cc)
        dte = (expiration - bar.trade_date).days

        short_strike = resolve_strike(
            strikes, bar.close_price, "call", overrides.short_call_strike, dte,
            contracts=cc, option_gateway=option_gateway, trade_date=bar.trade_date,
        )
        long_strike = offset_strike(strikes, short_strike, 1)
        if long_strike is None:
            raise DataUnavailableError("No higher strike for ratio call backspread.")

        short_c = require_contract_for_strike(cc, short_strike)
        long_c = require_contract_for_strike(cc, long_strike)

        sq = option_gateway.get_quote(short_c.ticker, bar.trade_date)
        lq = option_gateway.get_quote(long_c.ticker, bar.trade_date)
        if sq is None or lq is None:
            return None
        if not valid_entry_mids(sq.mid_price, lq.mid_price):
            return None

        # Sell 1 × short, buy 2 × long
        entry_cost = (2 * lq.mid_price - sq.mid_price) * 100.0
        margin = ratio_backspread_margin("call", bar.close_price, short_strike, long_strike, sq.mid_price)
        capital = max(margin, max(entry_cost, 0.0))

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([short_c.ticker, long_c.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(short_c.ticker, "call", -1, short_strike, expiration, 1, sq.mid_price, sq.mid_price),
                OpenOptionLeg(long_c.ticker, "call", 1, long_strike, expiration, 2, lq.mid_price, lq.mid_price),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max((long_strike - short_strike) * 100.0 + entry_cost, 0.0),
            max_profit_per_unit=None,
            detail_json={
                "ratio": "1:2",
                "short_strike": short_strike,
                "long_strike": long_strike,
                "assumptions": ["Sell 1 lower call, buy 2 higher calls at next listed strike."],
            },
        )


@dataclass(frozen=True, slots=True)
class RatioPutBackspreadStrategy(StrategyDefinition):
    """Sell 1 higher-strike put, buy 2 lower-strike puts. Net debit or small credit."""

    strategy_type: str = "ratio_put_backspread"
    margin_warning_message: str | None = None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        expiration = choose_primary_expiration(puts, bar.trade_date, config.target_dte)
        pc = contracts_for_expiration(puts, expiration)
        strikes = sorted_unique_strikes(pc)
        dte = (expiration - bar.trade_date).days

        short_strike = resolve_strike(
            strikes, bar.close_price, "put", overrides.short_put_strike, dte,
            contracts=pc, option_gateway=option_gateway, trade_date=bar.trade_date,
        )
        long_strike = offset_strike(strikes, short_strike, -1)
        if long_strike is None:
            raise DataUnavailableError("No lower strike for ratio put backspread.")

        short_c = require_contract_for_strike(pc, short_strike)
        long_c = require_contract_for_strike(pc, long_strike)

        sq = option_gateway.get_quote(short_c.ticker, bar.trade_date)
        lq = option_gateway.get_quote(long_c.ticker, bar.trade_date)
        if sq is None or lq is None:
            return None
        if not valid_entry_mids(sq.mid_price, lq.mid_price):
            return None

        entry_cost = (2 * lq.mid_price - sq.mid_price) * 100.0
        margin = ratio_backspread_margin("put", bar.close_price, short_strike, long_strike, sq.mid_price)
        capital = max(margin, max(entry_cost, 0.0))

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([short_c.ticker, long_c.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(short_c.ticker, "put", -1, short_strike, expiration, 1, sq.mid_price, sq.mid_price),
                OpenOptionLeg(long_c.ticker, "put", 1, long_strike, expiration, 2, lq.mid_price, lq.mid_price),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max((short_strike - long_strike) * 100.0 + entry_cost, 0.0),
            max_profit_per_unit=None,
            detail_json={
                "ratio": "1:2",
                "short_strike": short_strike,
                "long_strike": long_strike,
                "assumptions": ["Sell 1 higher put, buy 2 lower puts at next listed strike."],
            },
        )


RATIO_CALL_BACKSPREAD_STRATEGY = RatioCallBackspreadStrategy()
RATIO_PUT_BACKSPREAD_STRATEGY = RatioPutBackspreadStrategy()
