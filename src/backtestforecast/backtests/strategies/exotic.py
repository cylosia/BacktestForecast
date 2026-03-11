from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import (
    iron_condor_margin,
    jade_lizard_margin,
)
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_common_atm_strike,
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
    resolve_wing_strike,
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
class JadeLizardStrategy(StrategyDefinition):
    """Short OTM put + short OTM call spread (bear call spread). No upside risk if structured right."""

    strategy_type: str = "jade_lizard"
    margin_warning_message: str | None = "Jade lizard has downside risk beyond the short put strike."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_exp:
            raise DataUnavailableError("No common expiration for jade lizard.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_exp],
            bar.trade_date,
            config.target_dte,
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        # Short put: configurable OTM
        put_strike = resolve_strike(
            [c.strike_price for c in pc], bar.close_price, "put", overrides.short_put_strike, dte
        )
        # Short call spread: configurable short call + configurable width
        call_short_strike = resolve_strike(
            [c.strike_price for c in cc], bar.close_price, "call", overrides.short_call_strike, dte
        )
        call_long_strike = resolve_wing_strike(
            [c.strike_price for c in cc],
            call_short_strike,
            1,
            bar.close_price,
            overrides.spread_width,
        )
        if call_long_strike is None:
            raise DataUnavailableError("No higher strike for jade lizard call wing.")

        sp = require_contract_for_strike(pc, put_strike)
        sc = require_contract_for_strike(cc, call_short_strike)
        lc = require_contract_for_strike(cc, call_long_strike)

        spq = option_gateway.get_quote(sp.ticker, bar.trade_date)
        scq = option_gateway.get_quote(sc.ticker, bar.trade_date)
        lcq = option_gateway.get_quote(lc.ticker, bar.trade_date)
        if spq is None or scq is None or lcq is None:
            return None

        total_credit = (spq.mid_price + scq.mid_price - lcq.mid_price) * 100.0
        call_width = (call_long_strike - call_short_strike) * 100.0
        # No upside loss if credit > call spread width
        upside_risk = max(call_width - total_credit, 0.0)
        margin = jade_lizard_margin(
            bar.close_price,
            put_strike,
            spq.mid_price,
            call_long_strike - call_short_strike,
            total_credit / 100.0,
        )

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([sp.ticker, sc.ticker, lc.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(sp.ticker, "put", -1, put_strike, expiration, 1, spq.mid_price, spq.mid_price),
                OpenOptionLeg(sc.ticker, "call", -1, call_short_strike, expiration, 1, scq.mid_price, scq.mid_price),
                OpenOptionLeg(lc.ticker, "call", 1, call_long_strike, expiration, 1, lcq.mid_price, lcq.mid_price),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=margin,
            max_loss_per_unit=None,
            max_profit_per_unit=total_credit,
            detail_json={
                "put_strike": put_strike,
                "call_short_strike": call_short_strike,
                "call_long_strike": call_long_strike,
                "total_credit": total_credit,
                "upside_risk": upside_risk,
            },
        )


@dataclass(frozen=True, slots=True)
class IronButterflyStrategy(StrategyDefinition):
    """Sell ATM straddle + buy OTM wings. Defined-risk credit strategy."""

    strategy_type: str = "iron_butterfly"
    margin_warning_message: str | None = "Iron butterfly risk is the wider wing minus net credit."

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_exp:
            raise DataUnavailableError("No common expiration for iron butterfly.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_exp],
            bar.trade_date,
            config.target_dte,
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)

        # ATM strike for short straddle
        center_strike = choose_common_atm_strike(cc, pc, bar.close_price)
        # Wings: configurable width
        call_wing = resolve_wing_strike(
            [c.strike_price for c in cc],
            center_strike,
            1,
            bar.close_price,
            overrides.spread_width,
        )
        put_wing = resolve_wing_strike(
            [c.strike_price for c in pc],
            center_strike,
            -1,
            bar.close_price,
            overrides.spread_width,
        )
        if call_wing is None or put_wing is None:
            raise DataUnavailableError("Wing strikes unavailable for iron butterfly.")

        short_call = require_contract_for_strike(cc, center_strike)
        short_put = require_contract_for_strike(pc, center_strike)
        long_call = require_contract_for_strike(cc, call_wing)
        long_put = require_contract_for_strike(pc, put_wing)

        scq = option_gateway.get_quote(short_call.ticker, bar.trade_date)
        spq = option_gateway.get_quote(short_put.ticker, bar.trade_date)
        lcq = option_gateway.get_quote(long_call.ticker, bar.trade_date)
        lpq = option_gateway.get_quote(long_put.ticker, bar.trade_date)
        if any(q is None for q in [scq, spq, lcq, lpq]):
            return None

        credit = (scq.mid_price + spq.mid_price - lcq.mid_price - lpq.mid_price) * 100.0  # type: ignore[union-attr]
        call_width = (call_wing - center_strike) * 100.0
        put_width = (center_strike - put_wing) * 100.0
        max_loss = max(call_width, put_width) - credit
        margin = iron_condor_margin(
            call_wing - center_strike,
            center_strike - put_wing,
        )

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_put.ticker, short_put.ticker, short_call.ticker, long_call.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_put.ticker, "put", 1, put_wing, expiration, 1, lpq.mid_price, lpq.mid_price),  # type: ignore[union-attr]
                OpenOptionLeg(short_put.ticker, "put", -1, center_strike, expiration, 1, spq.mid_price, spq.mid_price),  # type: ignore[union-attr]
                OpenOptionLeg(
                    short_call.ticker, "call", -1, center_strike, expiration, 1, scq.mid_price, scq.mid_price
                ),  # type: ignore[union-attr]
                OpenOptionLeg(long_call.ticker, "call", 1, call_wing, expiration, 1, lcq.mid_price, lcq.mid_price),  # type: ignore[union-attr]
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=margin,
            max_loss_per_unit=max(max_loss, 0.0),
            max_profit_per_unit=credit,
            detail_json={
                "center_strike": center_strike,
                "call_wing": call_wing,
                "put_wing": put_wing,
                "credit": credit,
                "max_loss": max_loss,
            },
        )


JADE_LIZARD_STRATEGY = JadeLizardStrategy()
IRON_BUTTERFLY_STRATEGY = IronButterflyStrategy()
