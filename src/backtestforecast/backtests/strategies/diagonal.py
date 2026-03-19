from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import naked_call_margin, short_straddle_strangle_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    choose_secondary_expiration,
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


def _deep_itm_call_strike(strikes: list[float], underlying_close: float) -> float:
    """Select a deep ITM call strike — 2 increments below spot, or the lowest available.

    Raises DataUnavailableError when no in-the-money strikes exist, because
    a PMCC with an OTM long leg is structurally invalid (unlimited risk).
    """
    below = sorted([s for s in strikes if s < underlying_close])
    if len(below) >= 2:
        return below[-2]
    if below:
        return below[-1]
    raise DataUnavailableError("No deep ITM call strike available for PMCC — all strikes are at or above the underlying price.")


@dataclass(frozen=True, slots=True)
class PMCCStrategy(StrategyDefinition):
    strategy_type: str = "poor_mans_covered_call"
    margin_warning_message: str | None = None

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        near_exp = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        far_exp = choose_secondary_expiration(calls, bar.trade_date, near_exp)
        if far_exp is None:
            raise DataUnavailableError("No longer-dated expiration available for PMCC.")
        near_cc = contracts_for_expiration(calls, near_exp)
        far_cc = contracts_for_expiration(calls, far_exp)
        dte = (near_exp - bar.trade_date).days

        short_strike = resolve_strike(
            [c.strike_price for c in near_cc], bar.close_price, "call", overrides.short_call_strike, dte,
            contracts=near_cc, option_gateway=option_gateway, trade_date=bar.trade_date, iv_cache=getattr(option_gateway, '_iv_cache', None),
        )
        long_strike = _deep_itm_call_strike([c.strike_price for c in far_cc], bar.close_price)
        short_c = require_contract_for_strike(near_cc, short_strike)
        long_c = require_contract_for_strike(far_cc, long_strike)

        sq = option_gateway.get_quote(short_c.ticker, bar.trade_date)
        lq = option_gateway.get_quote(long_c.ticker, bar.trade_date)
        if sq is None or lq is None:
            return None
        if not valid_entry_mids(sq.mid_price, lq.mid_price):
            return None

        # PMCC (Poor Man's Covered Call) payoff math:
        # Structure: long deep-ITM far-dated call + short OTM near-dated call.
        # If opened for a debit (typical): max loss = debit paid (far-dated
        # call expires worthless while short call decays). Max profit is
        # theoretically large but path-dependent (the far-dated call retains
        # time value), so it is not capped here (set to None by the framework).
        # If opened for a credit (unusual, aggressive strikes): capital =
        # naked call margin on the short leg, max loss = None (uncapped if
        # underlying rises sharply beyond the short strike and the long call
        # cannot fully offset due to different expirations).
        entry_value = (lq.mid_price - sq.mid_price) * 100.0
        if entry_value >= 0:
            capital = entry_value
            max_loss: float | None = entry_value
        else:
            capital = naked_call_margin(bar.close_price, short_strike, sq.mid_price)
            max_loss = None

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_c.ticker, short_c.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(near_exp - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_c.ticker, "call", 1, long_strike, far_exp, 1, lq.mid_price, lq.mid_price),
                OpenOptionLeg(short_c.ticker, "call", -1, short_strike, near_exp, 1, sq.mid_price, sq.mid_price),
            ],
            scheduled_exit_date=near_exp,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            detail_json={
                "long_expiration": far_exp.isoformat(),
                "short_expiration": near_exp.isoformat(),
                "legs": [
                    {
                        "asset_type": "option",
                        "ticker": long_c.ticker,
                        "side": "long",
                        "contract_type": "call",
                        "strike_price": long_strike,
                        "expiration_date": far_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": lq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": short_c.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": short_strike,
                        "expiration_date": near_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": sq.mid_price,
                    },
                ],
            },
        )


@dataclass(frozen=True, slots=True)
class DiagonalSpreadStrategy(StrategyDefinition):
    strategy_type: str = "diagonal_spread"
    margin_warning_message: str | None = None

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        near_exp = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        far_exp = choose_secondary_expiration(calls, bar.trade_date, near_exp)
        if far_exp is None:
            raise DataUnavailableError("No longer-dated expiration available for diagonal spread.")
        near_cc = contracts_for_expiration(calls, near_exp)
        far_cc = contracts_for_expiration(calls, far_exp)
        dte = (near_exp - bar.trade_date).days

        near_strike = resolve_strike(
            [c.strike_price for c in near_cc], bar.close_price, "call", overrides.short_call_strike, dte,
            contracts=near_cc, option_gateway=option_gateway, trade_date=bar.trade_date, iv_cache=getattr(option_gateway, '_iv_cache', None),
        )
        far_strikes = sorted_unique_strikes(far_cc)
        far_strike = offset_strike(far_strikes, near_strike, -1)
        if far_strike is None:
            raise DataUnavailableError(
                "No lower strike available in far-dated chain for diagonal spread."
            )
        short_c = require_contract_for_strike(near_cc, near_strike)
        long_c = require_contract_for_strike(far_cc, far_strike)

        sq = option_gateway.get_quote(short_c.ticker, bar.trade_date)
        lq = option_gateway.get_quote(long_c.ticker, bar.trade_date)
        if sq is None or lq is None:
            return None
        if not valid_entry_mids(sq.mid_price, lq.mid_price):
            return None

        # Diagonal spread payoff math:
        # Structure: long far-dated call (lower strike) + short near-dated call
        # (higher strike, closer expiration). If opened for a debit: max loss =
        # debit paid (both options expire worthless). Max profit is path-
        # dependent because the legs have different expirations, so it cannot
        # be precisely capped at entry — set to None. If opened for a credit:
        # capital = naked call margin, max loss = None (uncapped risk if the
        # underlying moves sharply and the near-dated short call loses more
        # than the far-dated long call gains due to different time decay).
        entry_value = (lq.mid_price - sq.mid_price) * 100.0
        if entry_value >= 0:
            capital = entry_value
            max_loss: float | None = entry_value
        else:
            capital = naked_call_margin(bar.close_price, near_strike, sq.mid_price)
            max_loss = None

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_c.ticker, short_c.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(near_exp - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(long_c.ticker, "call", 1, far_strike, far_exp, 1, lq.mid_price, lq.mid_price),
                OpenOptionLeg(short_c.ticker, "call", -1, near_strike, near_exp, 1, sq.mid_price, sq.mid_price),
            ],
            scheduled_exit_date=near_exp,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            detail_json={
                "long_expiration": far_exp.isoformat(),
                "short_expiration": near_exp.isoformat(),
                "legs": [
                    {
                        "asset_type": "option",
                        "ticker": long_c.ticker,
                        "side": "long",
                        "contract_type": "call",
                        "strike_price": far_strike,
                        "expiration_date": far_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": lq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": short_c.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": near_strike,
                        "expiration_date": near_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": sq.mid_price,
                    },
                ],
            },
        )


@dataclass(frozen=True, slots=True)
class DoubleDiagonalStrategy(StrategyDefinition):
    strategy_type: str = "double_diagonal"
    margin_warning_message: str | None = None

    def build_position(
        self, config: BacktestConfig, bar: DailyBar, bar_index: int, option_gateway: OptionDataGateway
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_near_exp = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_near_exp:
            raise DataUnavailableError("No common expiration for double diagonal.")
        near_exp = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_near_exp], bar.trade_date, config.target_dte
        )
        common_far_exps = sorted(
            {c.expiration_date for c in calls if c.expiration_date > near_exp}
            & {c.expiration_date for c in puts if c.expiration_date > near_exp}
        )
        if not common_far_exps:
            raise DataUnavailableError("No common far expiration for double diagonal.")
        far_exp = choose_secondary_expiration(
            [c for c in calls if c.expiration_date in set(common_far_exps)],
            bar.trade_date,
            near_exp,
        )
        if far_exp is None:
            raise DataUnavailableError("No longer-dated expiration for double diagonal.")

        near_cc = contracts_for_expiration(calls, near_exp)
        near_pc = contracts_for_expiration(puts, near_exp)
        far_cc = contracts_for_expiration(calls, far_exp)
        far_pc = contracts_for_expiration(puts, far_exp)
        dte = (near_exp - bar.trade_date).days

        _iv_cache = getattr(option_gateway, '_iv_cache', None)
        near_call_strike = resolve_strike(
            [c.strike_price for c in near_cc], bar.close_price, "call", overrides.short_call_strike, dte,
            contracts=near_cc, option_gateway=option_gateway, trade_date=bar.trade_date, iv_cache=_iv_cache,
        )
        near_put_strike = resolve_strike(
            [c.strike_price for c in near_pc], bar.close_price, "put", overrides.short_put_strike, dte,
            contracts=near_pc, option_gateway=option_gateway, trade_date=bar.trade_date, iv_cache=_iv_cache,
        )
        far_call_strike = offset_strike(sorted_unique_strikes(far_cc), near_call_strike, -1)
        far_put_strike = offset_strike(sorted_unique_strikes(far_pc), near_put_strike, 1)
        if far_call_strike is None or far_put_strike is None:
            raise DataUnavailableError("No adjacent strike available in far-dated chain for double diagonal spread.")

        sc = require_contract_for_strike(near_cc, near_call_strike)
        sp = require_contract_for_strike(near_pc, near_put_strike)
        lc = require_contract_for_strike(far_cc, far_call_strike)
        lp = require_contract_for_strike(far_pc, far_put_strike)

        scq = option_gateway.get_quote(sc.ticker, bar.trade_date)
        spq = option_gateway.get_quote(sp.ticker, bar.trade_date)
        lcq = option_gateway.get_quote(lc.ticker, bar.trade_date)
        lpq = option_gateway.get_quote(lp.ticker, bar.trade_date)
        if any(q is None for q in [scq, spq, lcq, lpq]):
            return None
        if not valid_entry_mids(scq.mid_price, spq.mid_price, lcq.mid_price, lpq.mid_price):  # type: ignore[union-attr]
            return None

        entry_value = (lcq.mid_price + lpq.mid_price - scq.mid_price - spq.mid_price) * 100.0  # type: ignore[union-attr]
        if entry_value >= 0:
            capital = entry_value
            max_loss: float | None = entry_value
        else:
            capital = short_straddle_strangle_margin(
                bar.close_price,
                near_call_strike,
                near_put_strike,
                scq.mid_price,  # type: ignore[union-attr]
                spq.mid_price,  # type: ignore[union-attr]
            )
            # The long far-dated legs reduce risk vs a naked short straddle.
            # Apply a 50% reduction to approximate the margin benefit of the
            # protective long legs. A precise calculation would require
            # modeling the long legs' delta/gamma offset.
            capital = round(capital * 0.50, 2)
            max_loss = None

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([lc.ticker, sc.ticker, lp.ticker, sp.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(near_exp - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(lc.ticker, "call", 1, far_call_strike, far_exp, 1, lcq.mid_price, lcq.mid_price),  # type: ignore[union-attr]
                OpenOptionLeg(sc.ticker, "call", -1, near_call_strike, near_exp, 1, scq.mid_price, scq.mid_price),  # type: ignore[union-attr]
                OpenOptionLeg(lp.ticker, "put", 1, far_put_strike, far_exp, 1, lpq.mid_price, lpq.mid_price),  # type: ignore[union-attr]
                OpenOptionLeg(sp.ticker, "put", -1, near_put_strike, near_exp, 1, spq.mid_price, spq.mid_price),  # type: ignore[union-attr]
            ],
            scheduled_exit_date=near_exp,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            detail_json={
                "near_expiration": near_exp.isoformat(),
                "far_expiration": far_exp.isoformat(),
                "legs": [
                    {
                        "asset_type": "option",
                        "ticker": lc.ticker,
                        "side": "long",
                        "contract_type": "call",
                        "strike_price": far_call_strike,
                        "expiration_date": far_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": lcq.mid_price,  # type: ignore[union-attr]
                    },
                    {
                        "asset_type": "option",
                        "ticker": sc.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": near_call_strike,
                        "expiration_date": near_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": scq.mid_price,  # type: ignore[union-attr]
                    },
                    {
                        "asset_type": "option",
                        "ticker": lp.ticker,
                        "side": "long",
                        "contract_type": "put",
                        "strike_price": far_put_strike,
                        "expiration_date": far_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": lpq.mid_price,  # type: ignore[union-attr]
                    },
                    {
                        "asset_type": "option",
                        "ticker": sp.ticker,
                        "side": "short",
                        "contract_type": "put",
                        "strike_price": near_put_strike,
                        "expiration_date": near_exp.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": spq.mid_price,  # type: ignore[union-attr]
                    },
                ],
            },
        )


PMCC_STRATEGY = PMCCStrategy()
DIAGONAL_SPREAD_STRATEGY = DiagonalSpreadStrategy()
DOUBLE_DIAGONAL_STRATEGY = DoubleDiagonalStrategy()
