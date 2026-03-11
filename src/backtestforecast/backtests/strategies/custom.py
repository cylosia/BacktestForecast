from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    choose_primary_expiration,
    choose_secondary_expiration,
    contracts_for_expiration,
    offset_strike,
    require_contract_for_strike,
    sorted_unique_strikes,
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

        # Gather all calls and puts for expiration selection
        calls = list(
            option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        )
        puts = list(option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days))
        all_contracts = calls + puts
        if not all_contracts:
            raise DataUnavailableError("No option contracts available.")

        # Resolve expirations: offset 0 = primary, 1/2 = successively later
        primary_exp = choose_primary_expiration(all_contracts, bar.trade_date, config.target_dte)
        expirations = {0: primary_exp}
        if any(leg.expiration_offset >= 1 for leg in custom_legs):
            sec = choose_secondary_expiration(all_contracts, bar.trade_date, primary_exp)
            if sec is None:
                raise DataUnavailableError("No secondary expiration available.")
            expirations[1] = sec
        if any(leg.expiration_offset >= 2 for leg in custom_legs):
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
                stock_legs.append(
                    OpenStockLeg(
                        symbol=config.symbol,
                        side=side_sign,
                        share_quantity_per_unit=100 * leg_def.quantity_ratio,
                        entry_price=bar.close_price,
                        last_price=bar.close_price,
                    )
                )
                cost = bar.close_price * 100.0 * leg_def.quantity_ratio
                if side_sign == 1:
                    total_debit += cost
                tickers.append(config.symbol)
                continue

            # Option leg
            assert leg_def.contract_type is not None
            exp = expirations.get(leg_def.expiration_offset, primary_exp)
            if leg_def.contract_type == "call":
                chain = contracts_for_expiration(calls, exp)
            else:
                chain = contracts_for_expiration(puts, exp)

            if not chain:
                raise DataUnavailableError(
                    f"No {leg_def.contract_type} contracts at expiration offset {leg_def.expiration_offset}."
                )

            strikes = sorted_unique_strikes(chain)
            if leg_def.strike_offset == 0:
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

            option_legs.append(
                OpenOptionLeg(
                    ticker=contract.ticker,
                    contract_type=leg_def.contract_type,
                    side=side_sign,
                    strike_price=strike,
                    expiration_date=exp,
                    quantity_per_unit=leg_def.quantity_ratio,
                    entry_mid=quote.mid_price,
                    last_mid=quote.mid_price,
                )
            )
            cost = quote.mid_price * 100.0 * leg_def.quantity_ratio
            if side_sign == 1:
                total_debit += cost
            else:
                total_credit += cost
            tickers.append(contract.ticker)

        if not option_legs and not stock_legs:
            return None

        net_cost = total_debit - total_credit
        capital = max(net_cost, 0.0)
        earliest_exp = min(
            (leg.expiration_date for leg in option_legs),
            default=config.end_date,
        )

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker(tickers),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(earliest_exp - bar.trade_date).days,
            option_legs=option_legs,
            stock_legs=stock_legs,
            scheduled_exit_date=earliest_exp,
            capital_required_per_unit=capital if capital > 0 else abs(net_cost) * 0.2 + total_credit * 0.15,
            max_loss_per_unit=None,
            max_profit_per_unit=None,
            detail_json={
                "custom_legs": [
                    {
                        "asset_type": ld.asset_type,
                        "contract_type": ld.contract_type,
                        "side": ld.side,
                        "strike_offset": ld.strike_offset,
                        "expiration_offset": ld.expiration_offset,
                        "quantity_ratio": ld.quantity_ratio,
                    }
                    for ld in custom_legs
                ],
                "net_cost_per_unit": net_cost,
                "assumptions": [
                    "Strike offsets are relative to the ATM strike at the selected expiration.",
                    "Expiration offsets: 0=nearest to target_dte, 1=next available, 2=second-next.",
                    "Capital requirement is estimated; actual margin requirements may differ.",
                ],
            },
        )


CUSTOM_2_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_2_leg")
CUSTOM_3_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_3_leg")
CUSTOM_4_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_4_leg")
CUSTOM_5_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_5_leg")
CUSTOM_6_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_6_leg")
CUSTOM_8_LEG_STRATEGY = CustomNLegStrategy(strategy_type="custom_8_leg")
