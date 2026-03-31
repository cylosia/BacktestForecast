from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import credit_spread_margin, naked_option_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    choose_primary_expiration,
    choose_secondary_expiration,
    contracts_for_expiration,
    maybe_build_contract_delta_lookup,
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
    OpenStockLeg,
    OptionDataGateway,
)
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CustomLegDefinition


@dataclass(frozen=True, slots=True)
class CustomNLegStrategy(StrategyDefinition):
    strategy_type: str
    margin_warning_message: str | None = "Custom strategies may require margin. Capital requirements are estimated."

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
        *,
        custom_legs: list[CustomLegDefinition] | None = None,
    ) -> OpenMultiLegPosition | None:
        if not custom_legs:
            raise DataUnavailableError("custom_legs definitions are required for custom strategies.")

        option_leg_defs = [leg for leg in custom_legs if leg.asset_type == "option"]
        explicit_expiration_dates = sorted(
            {
                leg.expiration_date
                for leg in option_leg_defs
                if leg.expiration_date is not None
            }
        )
        uses_explicit_expirations = bool(option_leg_defs) and all(
            leg.expiration_date is not None for leg in option_leg_defs
        )

        calls: list = []
        puts: list = []
        all_contracts: list = []
        expirations: dict[int, object] = {}
        contracts_by_type_and_expiration: dict[tuple[str, object], list] = {}

        def _fetch_exact_contracts(contract_type: str, expiration_date: object) -> list:
            cache_key = (contract_type, expiration_date)
            cached = contracts_by_type_and_expiration.get(cache_key)
            if cached is not None:
                return cached

            exact_fetch = getattr(option_gateway, "list_contracts_for_expiration", None)
            chain: list
            if callable(exact_fetch):
                chain = list(
                    exact_fetch(
                        entry_date=bar.trade_date,
                        contract_type=contract_type,
                        expiration_date=expiration_date,
                    )
                )
            else:
                pool = calls if contract_type == "call" else puts
                chain = contracts_for_expiration(pool, expiration_date)
            contracts_by_type_and_expiration[cache_key] = chain
            return chain

        if option_leg_defs:
            if uses_explicit_expirations:
                earliest_expiration = min(explicit_expiration_dates)
                furthest_expiration = max(explicit_expiration_dates)
                exact_fetch = getattr(option_gateway, "list_contracts_for_expiration", None)
                if not callable(exact_fetch):
                    lower_bound = max(1, (earliest_expiration - bar.trade_date).days)
                    upper_bound = max(1, (furthest_expiration - bar.trade_date).days)
                    fetch_target_dte = max(1, (lower_bound + upper_bound) // 2)
                    fetch_tolerance = max(fetch_target_dte - lower_bound, upper_bound - fetch_target_dte)
                    calls = list(
                        option_gateway.list_contracts(
                            bar.trade_date, "call", fetch_target_dte, fetch_tolerance,
                        )
                    )
                    puts = list(
                        option_gateway.list_contracts(
                            bar.trade_date, "put", fetch_target_dte, fetch_tolerance,
                        )
                    )
                for expiration_date in explicit_expiration_dates:
                    if any(leg.contract_type == "call" and leg.expiration_date == expiration_date for leg in option_leg_defs):
                        _fetch_exact_contracts("call", expiration_date)
                    if any(leg.contract_type == "put" and leg.expiration_date == expiration_date for leg in option_leg_defs):
                        _fetch_exact_contracts("put", expiration_date)
            else:
                max_expiration_offset = max((leg.expiration_offset for leg in option_leg_defs), default=0)
                minimum_dte = max(1, config.target_dte - config.dte_tolerance_days)
                maximum_dte = config.target_dte + config.dte_tolerance_days + (14 * max_expiration_offset)
                fetch_target_dte = max(1, (minimum_dte + maximum_dte) // 2)
                fetch_tolerance = max(fetch_target_dte - minimum_dte, maximum_dte - fetch_target_dte)

                calls = list(
                    option_gateway.list_contracts(
                        bar.trade_date, "call", fetch_target_dte, fetch_tolerance,
                    )
                )
                puts = list(
                    option_gateway.list_contracts(
                        bar.trade_date, "put", fetch_target_dte, fetch_tolerance,
                    )
                )
                all_contracts = calls + puts
                if not all_contracts:
                    raise DataUnavailableError("No option contracts available.")

                primary_exp = choose_primary_expiration(all_contracts, bar.trade_date, config.target_dte)
                expirations = {0: primary_exp}
                if any(leg.expiration_offset >= 1 for leg in option_leg_defs):
                    sec = choose_secondary_expiration(all_contracts, bar.trade_date, primary_exp)
                    if sec is None:
                        raise DataUnavailableError("No secondary expiration available.")
                    expirations[1] = sec
                if any(leg.expiration_offset >= 2 for leg in option_leg_defs):
                    sec1 = expirations.get(1, primary_exp)
                    third = choose_secondary_expiration(all_contracts, bar.trade_date, sec1)
                    if third is None:
                        raise DataUnavailableError("No third expiration available.")
                    expirations[2] = third

        option_legs: list[OpenOptionLeg] = []
        stock_legs: list[OpenStockLeg] = []
        total_debit = 0.0
        total_credit = 0.0
        tickers: list[str] = []

        for leg_def in custom_legs:
            side_sign = 1 if leg_def.side == "long" else -1

            if leg_def.asset_type == "stock":
                share_qty = round(float(100 * leg_def.quantity_ratio))
                if share_qty == 0:
                    raise DataUnavailableError(
                        f"Stock leg quantity_ratio {leg_def.quantity_ratio} rounds to 0 shares."
                    )
                stock_legs.append(
                    OpenStockLeg(
                        symbol=config.symbol,
                        side=side_sign,
                        share_quantity_per_unit=share_qty,
                        entry_price=bar.close_price,
                        last_price=bar.close_price,
                    )
                )
                cost = bar.close_price * share_qty
                if side_sign == 1:
                    total_debit += cost
                else:
                    total_credit += cost
                tickers.append(config.symbol)
                continue

            # Option leg
            if leg_def.contract_type is None:
                raise DataUnavailableError("contract_type is required for option legs.")
            exp = leg_def.expiration_date if uses_explicit_expirations else expirations.get(leg_def.expiration_offset)
            if exp is None:
                raise DataUnavailableError(
                    f"Expiration offset {leg_def.expiration_offset} is not available. "
                    f"Only offsets 0-2 are supported and the requested expiration must exist in the chain."
                )
            chain = _fetch_exact_contracts(leg_def.contract_type, exp)

            if not chain:
                if uses_explicit_expirations:
                    raise DataUnavailableError(
                        f"No {leg_def.contract_type} contracts were available for expiration {exp}."
                    )
                raise DataUnavailableError(
                    f"No {leg_def.contract_type} contracts at expiration offset {leg_def.expiration_offset}."
                )

            strikes = sorted_unique_strikes(chain)
            if leg_def.strike_selection is not None:
                dte_days = max(1, (exp - bar.trade_date).days)
                risk_free_rate = config.resolve_risk_free_rate(bar.trade_date)
                delta_lookup = maybe_build_contract_delta_lookup(
                    selection=leg_def.strike_selection,
                    contracts=chain,
                    option_gateway=option_gateway,
                    trade_date=bar.trade_date,
                    underlying_close=bar.close_price,
                    dte_days=dte_days,
                    risk_free_rate=risk_free_rate,
                    dividend_yield=config.dividend_yield,
                )
                strike = resolve_strike(
                    strikes,
                    bar.close_price,
                    leg_def.contract_type,
                    leg_def.strike_selection,
                    dte_days,
                    delta_lookup=delta_lookup,
                    contracts=chain,
                    option_gateway=option_gateway,
                    trade_date=bar.trade_date,
                    expiration_date=exp,
                    risk_free_rate=risk_free_rate,
                )
            elif leg_def.strike_offset == 0:
                strike = choose_atm_strike(strikes, bar.close_price)
            else:
                base = choose_atm_strike(strikes, bar.close_price)
                resolved = offset_strike(strikes, base, leg_def.strike_offset)
                if resolved is None:
                    raise DataUnavailableError(
                        f"Strike offset {leg_def.strike_offset} out of range for {leg_def.contract_type}."
                    )
                strike = resolved

            contract = require_contract_for_strike(chain, strike)
            quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
            if quote is None:
                return None
            if not valid_entry_mids(quote.mid_price):
                return None

            option_qty = round(float(leg_def.quantity_ratio))
            if option_qty == 0:
                raise DataUnavailableError(
                    f"Option leg quantity_ratio {leg_def.quantity_ratio} rounds to 0 contracts."
                )
            option_legs.append(
                OpenOptionLeg(
                    ticker=contract.ticker,
                    contract_type=leg_def.contract_type,
                    side=side_sign,
                    strike_price=strike,
                    expiration_date=exp,
                    quantity_per_unit=option_qty,
                    entry_mid=quote.mid_price,
                    last_mid=quote.mid_price,
                )
            )
            cost = quote.mid_price * 100.0 * option_qty
            if side_sign == 1:
                total_debit += cost
            else:
                total_credit += cost
            tickers.append(contract.ticker)

        if not option_legs and not stock_legs:
            return None

        net_cost = total_debit - total_credit
        capital = max(net_cost, 0.0)
        furthest_exp = max(
            (leg.expiration_date for leg in option_legs),
            default=config.end_date,
        )

        if capital <= 0:
            short_leg_margin = self._estimate_credit_margin(option_legs, bar.close_price)
            capital = max(short_leg_margin, abs(net_cost), 1.0)

        max_loss = self._estimate_max_loss(option_legs, net_cost, bar.close_price)

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker(tickers),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=max(0, (furthest_exp - bar.trade_date).days),
            option_legs=option_legs,
            stock_legs=stock_legs,
            scheduled_exit_date=furthest_exp,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            max_profit_per_unit=None,
            detail_json={
                "legs": [
                    *[
                        {
                            "asset_type": "option",
                            "ticker": leg.ticker,
                            "side": "long" if leg.side == 1 else "short",
                            "contract_type": leg.contract_type,
                            "strike_price": leg.strike_price,
                            "expiration_date": leg.expiration_date.isoformat(),
                            "quantity_per_unit": leg.quantity_per_unit,
                            "entry_mid": leg.entry_mid,
                        }
                        for leg in option_legs
                    ],
                    *[
                        {
                            "asset_type": "stock",
                            "identifier": leg.symbol,
                            "side": "long" if leg.side == 1 else "short",
                            "share_quantity_per_unit": leg.share_quantity_per_unit,
                            "entry_price": leg.entry_price,
                        }
                        for leg in stock_legs
                    ],
                ],
                "custom_legs": [ld.model_dump(mode="json") for ld in custom_legs],
                "net_cost_per_unit": net_cost,
                "resolved_option_expirations": sorted({leg.expiration_date.isoformat() for leg in option_legs}),
                "assumptions": [
                    (
                        "Custom option legs use explicit expiration_date values and exact-expiration contract resolution."
                        if uses_explicit_expirations
                        else "Strike offsets are relative to the ATM strike at the selected expiration."
                    ),
                    (
                        "The position remains open until the furthest option expiration unless another exit rule closes it earlier."
                        if option_legs
                        else "Stock-only custom positions use the backtest end date as the scheduled exit."
                    ),
                    (
                        "Expiration offsets: 0=nearest to target_dte, 1=next available, 2=second-next."
                        if not uses_explicit_expirations
                        else "Per-leg strike_selection values override strike_offset for contract selection."
                    ),
                    "Capital requirement is estimated; actual margin requirements may differ.",
                ],
            },
        )


    @staticmethod
    def _estimate_credit_margin(
        option_legs: list[OpenOptionLeg],
        underlying_price: float,
    ) -> float:
        """Estimate margin for a multi-leg credit position.

        Uses a globally-optimal greedy pairing: enumerate all valid
        (short, long) pairs, sort by spread width ascending, and assign
        the tightest pairs first.  This avoids the suboptimal results of
        the per-short greedy approach where pairing short1 with the
        nearest long could leave short2 unpaired when a global
        rearrangement would pair both.
        """
        short_legs = [leg for leg in option_legs if leg.side == -1]
        long_legs = [leg for leg in option_legs if leg.side == 1]
        short_remaining = [leg.quantity_per_unit for leg in short_legs]
        long_remaining = [leg.quantity_per_unit for leg in long_legs]
        margin = 0.0

        candidates: list[tuple[float, int, int]] = []
        for si, short in enumerate(short_legs):
            for li, long in enumerate(long_legs):
                if long.contract_type != short.contract_type:
                    continue
                if long.expiration_date != short.expiration_date:
                    continue
                width = abs(short.strike_price - long.strike_price)
                candidates.append((width, si, li))
        candidates.sort()

        for width, si, li in candidates:
            if short_remaining[si] <= 0 or long_remaining[li] <= 0:
                continue
            paired_qty = min(short_remaining[si], long_remaining[li])
            margin += credit_spread_margin(width) * paired_qty
            short_remaining[si] -= paired_qty
            long_remaining[li] -= paired_qty

        for si, short in enumerate(short_legs):
            if short_remaining[si] <= 0:
                continue
            margin += naked_option_margin(
                short.contract_type, underlying_price, short.strike_price, short.entry_mid,
            ) * short_remaining[si]

        return margin

    @staticmethod
    def _estimate_max_loss(
        option_legs: list[OpenOptionLeg],
        net_cost: float,
        underlying_price: float,
    ) -> float | None:
        """Estimate worst-case max loss for position sizing.

        For fully-hedged positions (all shorts paired with longs of same type
        and expiration), max loss is the widest spread width per pair.
        For positions with naked shorts, returns None (unlimited risk).

        Pairs are sorted by width ascending (tightest first) to maximize the
        chance of covering all shorts - same strategy as _estimate_credit_margin.
        """
        short_legs = [leg for leg in option_legs if leg.side == -1]
        long_legs = [leg for leg in option_legs if leg.side == 1]
        if not short_legs:
            return max(net_cost, 0.0) if net_cost > 0 else 0.0

        short_remaining = [leg.quantity_per_unit for leg in short_legs]
        long_remaining = [leg.quantity_per_unit for leg in long_legs]

        candidates: list[tuple[float, int, int]] = []
        for si, short in enumerate(short_legs):
            for li, long in enumerate(long_legs):
                if long.contract_type != short.contract_type:
                    continue
                if long.expiration_date != short.expiration_date:
                    continue
                width = abs(short.strike_price - long.strike_price)
                candidates.append((width, si, li))
        candidates.sort()

        max_spread_risk = 0.0
        for width, si, li in candidates:
            if short_remaining[si] <= 0 or long_remaining[li] <= 0:
                continue
            paired_qty = min(short_remaining[si], long_remaining[li])
            max_spread_risk = max(max_spread_risk, width * 100.0 * paired_qty)
            short_remaining[si] -= paired_qty
            long_remaining[li] -= paired_qty

        has_naked = any(r > 0 for r in short_remaining)
        if has_naked:
            return None
        return max_spread_risk + max(net_cost, 0.0)


CUSTOM_2_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_2_leg")
CUSTOM_3_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_3_leg")
CUSTOM_4_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_4_leg")
CUSTOM_5_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_5_leg")
CUSTOM_6_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_6_leg")
CUSTOM_7_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_7_leg")
CUSTOM_8_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_8_leg")
