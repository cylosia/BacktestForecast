from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import naked_call_margin, naked_put_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    choose_secondary_expiration,
    contracts_for_expiration,
    get_overrides,
    maybe_build_contract_delta_lookup,
    preferred_expiration_dates,
    require_contract_for_strike,
    resolve_strike,
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

CALENDAR_MIN_FAR_LEG_EXTRA_DAYS = 1
CALENDAR_MIN_DTE_TOLERANCE_DAYS = 8


def resolve_calendar_contract_groups(
    option_gateway: OptionDataGateway,
    *,
    entry_date: date,
    contract_type: str,
    target_dte: int,
    dte_tolerance_days: int,
    strike_price_gte: float | None = None,
    strike_price_lte: float | None = None,
) -> tuple[date, list[OptionContractRecord], date, list[OptionContractRecord]]:
    effective_tolerance_days = max(dte_tolerance_days, CALENDAR_MIN_DTE_TOLERANCE_DAYS)
    ordered_expirations = preferred_expiration_dates(
        entry_date,
        target_dte,
        effective_tolerance_days,
    )
    batch_fetch = getattr(option_gateway, "list_contracts_for_expirations", None)
    if callable(batch_fetch):
        contracts_by_expiration = batch_fetch(
            entry_date=entry_date,
            contract_type=contract_type,
            expiration_dates=ordered_expirations,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
        )
        near_expiration: date | None = None
        near_contracts: list[OptionContractRecord] = []
        for expiration_date in ordered_expirations:
            contracts = list(contracts_by_expiration.get(expiration_date, []))
            if contracts:
                near_expiration = expiration_date
                near_contracts = contracts
                break
        if near_expiration is None:
            raise DataUnavailableError("No eligible option expirations were available.")
        minimum_target = (near_expiration - entry_date).days + CALENDAR_MIN_FAR_LEG_EXTRA_DAYS
        for expiration_date in ordered_expirations:
            if expiration_date <= near_expiration:
                continue
            if (expiration_date - entry_date).days < minimum_target:
                continue
            contracts = list(contracts_by_expiration.get(expiration_date, []))
            if contracts:
                return near_expiration, near_contracts, expiration_date, contracts
        raise DataUnavailableError("Calendar spread requires a later expiration beyond the target cycle.")

    exact_fetch = getattr(option_gateway, "list_contracts_for_expiration", None)
    if callable(exact_fetch):
        near_expiration: date | None = None
        near_contracts: list[OptionContractRecord] = []
        for expiration_date in ordered_expirations:
            contracts = list(
                exact_fetch(
                    entry_date=entry_date,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    strike_price_gte=strike_price_gte,
                    strike_price_lte=strike_price_lte,
                )
            )
            if contracts:
                near_expiration = expiration_date
                near_contracts = contracts
                break
        if near_expiration is None:
            raise DataUnavailableError("No eligible option expirations were available.")

        minimum_target = (near_expiration - entry_date).days + CALENDAR_MIN_FAR_LEG_EXTRA_DAYS
        later_expirations = sorted(
            expiration_date
            for expiration_date in ordered_expirations
            if expiration_date > near_expiration and (expiration_date - entry_date).days >= minimum_target
        )
        for expiration_date in later_expirations:
            contracts = list(
                exact_fetch(
                    entry_date=entry_date,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    strike_price_gte=strike_price_gte,
                    strike_price_lte=strike_price_lte,
                )
            )
            if contracts:
                return near_expiration, near_contracts, expiration_date, contracts
        raise DataUnavailableError("Calendar spread requires a later expiration beyond the target cycle.")

    contracts = option_gateway.list_contracts(
        entry_date,
        contract_type,
        target_dte,
        effective_tolerance_days,
    )
    near_expiration = choose_primary_expiration(contracts, entry_date, target_dte)
    far_expiration = choose_secondary_expiration(
        contracts,
        entry_date,
        near_expiration,
        min_extra_days=CALENDAR_MIN_FAR_LEG_EXTRA_DAYS,
    )
    if far_expiration is None:
        raise DataUnavailableError("Calendar spread requires a later expiration beyond the target cycle.")
    return (
        near_expiration,
        contracts_for_expiration(contracts, near_expiration),
        far_expiration,
        contracts_for_expiration(contracts, far_expiration),
    )


@dataclass(frozen=True, slots=True)
class CalendarSpreadStrategy(StrategyDefinition):
    strategy_type: str = "calendar_spread"
    margin_warning_message: str | None = None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        contract_type = overrides.calendar_contract_type or "call"
        strike_selection = (
            overrides.short_put_strike or overrides.long_put_strike
            if contract_type == "put"
            else overrides.short_call_strike or overrides.long_call_strike
        )
        near_expiration, near_calls, far_expiration, far_calls = resolve_calendar_contract_groups(
            option_gateway,
            entry_date=bar.trade_date,
            contract_type=contract_type,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
        )
        common_strikes = sorted(
            {contract.strike_price for contract in near_calls} & {contract.strike_price for contract in far_calls}
        )
        if not common_strikes:
            raise DataUnavailableError("Calendar spread requires a common strike across near and far expirations.")
        near_dte = (near_expiration - bar.trade_date).days
        delta_lookup = maybe_build_contract_delta_lookup(
            selection=strike_selection,
            contracts=near_calls,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            underlying_close=bar.close_price,
            dte_days=near_dte,
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
            dividend_yield=config.dividend_yield,
            iv_cache=getattr(option_gateway, "_iv_cache", None),
        )
        strike = resolve_strike(
            common_strikes,
            bar.close_price,
            contract_type,
            strike_selection,
            near_dte,
            delta_lookup=delta_lookup,
            contracts=near_calls,
            option_gateway=option_gateway,
            trade_date=bar.trade_date,
            expiration_date=near_expiration,
            iv_cache=getattr(option_gateway, "_iv_cache", None),
            risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
        )
        short_near = require_contract_for_strike(near_calls, strike)
        long_far = require_contract_for_strike(far_calls, strike)

        short_quote = option_gateway.get_quote(short_near.ticker, bar.trade_date)
        long_quote = option_gateway.get_quote(long_far.ticker, bar.trade_date)
        if short_quote is None or long_quote is None:
            return None
        if not valid_entry_mids(short_quote.mid_price, long_quote.mid_price):
            return None

        entry_value_per_unit = (long_quote.mid_price - short_quote.mid_price) * 100.0
        net_debit = max(entry_value_per_unit, 0.0)
        if contract_type == "put":
            full_margin = naked_put_margin(bar.close_price, short_near.strike_price, short_quote.mid_price)
        else:
            full_margin = naked_call_margin(bar.close_price, short_near.strike_price, short_quote.mid_price)
        long_leg_value = long_quote.mid_price * 100.0
        reduced_margin = max(full_margin - long_leg_value, net_debit)
        _MIN_DEBIT_FLOOR = 1.0
        if entry_value_per_unit >= 0:
            capital = max(entry_value_per_unit, _MIN_DEBIT_FLOOR)
            max_loss: float | None = max(entry_value_per_unit, _MIN_DEBIT_FLOOR)
        else:
            capital = reduced_margin
            # Approximation: credit calendars can theoretically lose more than
            # reduced_margin if the underlying moves sharply, but exact max
            # loss requires modeling both expirations' decay. Using
            # reduced_margin as a practical upper-bound estimate.
            max_loss = reduced_margin

        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": long_far.ticker,
                    "side": "long",
                    "contract_type": contract_type,
                    "strike_price": long_far.strike_price,
                    "expiration_date": long_far.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": long_quote.mid_price,
                },
                {
                    "asset_type": "option",
                    "ticker": short_near.ticker,
                    "side": "short",
                    "contract_type": contract_type,
                    "strike_price": short_near.strike_price,
                    "expiration_date": short_near.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": short_quote.mid_price,
                },
            ],
            "assumptions": [
                f"Calendar spread is modeled as a {contract_type} calendar in this slice.",
                "The short leg uses the expiration nearest target_dte and the long leg uses"
                " the next later expiration at least 1 day farther out when available."
                f" Expiration search uses at least {CALENDAR_MIN_DTE_TOLERANCE_DAYS} DTE tolerance days.",
                "The package exits at the near-leg expiration, max_holding_days, or backtest end;"
                " the far leg is closed at market on that exit date.",
            ],
            "capital_required_per_unit": capital,
            "max_loss_per_unit": max_loss,
            "max_profit_per_unit": None,
            "entry_package_market_value": entry_value_per_unit,
        }
        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([long_far.ticker, short_near.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(near_expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    long_far.ticker,
                    contract_type,
                    1,
                    long_far.strike_price,
                    long_far.expiration_date,
                    1,
                    long_quote.mid_price,
                    long_quote.mid_price,
                ),
                OpenOptionLeg(
                    short_near.ticker,
                    contract_type,
                    -1,
                    short_near.strike_price,
                    short_near.expiration_date,
                    1,
                    short_quote.mid_price,
                    short_quote.mid_price,
                ),
            ],
            scheduled_exit_date=near_expiration,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
            max_profit_per_unit=None,
            detail_json=detail_json,
        )


CALENDAR_SPREAD_STRATEGY = CalendarSpreadStrategy()
