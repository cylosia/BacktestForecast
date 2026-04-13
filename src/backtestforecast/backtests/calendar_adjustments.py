from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.margin import naked_call_margin, naked_put_margin
from backtestforecast.backtests.rules import EntryRuleComputationCache, EntryRuleEvaluator
from backtestforecast.backtests.strategies.common import (
    choose_atm_strike,
    preferred_expiration_dates,
    require_contract_for_strike,
    sorted_unique_strikes,
)
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OptionDataGateway,
    TradeResult,
)
from backtestforecast.errors import AppValidationError, DataUnavailableError
from backtestforecast.market_data.historical_store import parse_option_ticker_metadata
from backtestforecast.market_data.types import DailyBar, OptionContractRecord
from backtestforecast.schemas.backtests import is_calendar_strategy_type

_D0 = Decimal("0")
_D100 = Decimal("100")


def _d(value: Decimal | float | int) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class CalendarAdjustmentPolicy:
    name: str
    mode: str
    max_rolls: int = 0
    min_long_dte_remaining: int = 2
    hold_long_trading_days: int = 0
    exit_long_on_breakeven: bool = False
    require_net_credit: bool = True


CLOSE_AT_SHORT_EXPIRATION_POLICY = CalendarAdjustmentPolicy(
    name="close_at_short_expiration",
    mode="close",
)

ROLL_SAME_STRIKE_ONCE_POLICY = CalendarAdjustmentPolicy(
    name="roll_same_strike_once",
    mode="roll_same_strike_once",
    max_rolls=1,
    min_long_dte_remaining=2,
    require_net_credit=True,
)

RECENTER_SHORT_ONCE_POLICY = CalendarAdjustmentPolicy(
    name="recenter_short_once",
    mode="recenter_short_once",
    max_rolls=1,
    min_long_dte_remaining=2,
    require_net_credit=True,
)

HOLD_LONG_ONLY_IF_SHORT_OTM_POLICY = CalendarAdjustmentPolicy(
    name="hold_long_only_if_short_otm",
    mode="hold_long_only_if_short_otm",
    hold_long_trading_days=3,
    exit_long_on_breakeven=True,
)


def default_calendar_adjustment_policies() -> list[CalendarAdjustmentPolicy]:
    return [
        CLOSE_AT_SHORT_EXPIRATION_POLICY,
        ROLL_SAME_STRIKE_ONCE_POLICY,
        RECENTER_SHORT_ONCE_POLICY,
        HOLD_LONG_ONLY_IF_SHORT_OTM_POLICY,
    ]


@dataclass(slots=True)
class _CalendarCampaign:
    policy: CalendarAdjustmentPolicy
    initial_display_ticker: str
    position: OpenMultiLegPosition
    initial_entry_value_per_unit: Decimal
    initial_entry_commission_total: Decimal
    gross_cash_balance: Decimal
    net_cash_balance: Decimal
    total_commissions: Decimal
    total_slippage: Decimal
    max_capital_at_risk: float
    initial_legs: list[dict[str, Any]]
    initial_assumptions: list[str]
    adjustment_events: list[dict[str, Any]] = field(default_factory=list)
    roll_count: int = 0
    forced_exit_bar_index: int | None = None


def select_calendar_roll_short_contract(
    option_gateway: OptionDataGateway,
    *,
    entry_date: date,
    contract_type: str,
    target_dte: int,
    dte_tolerance_days: int,
    long_expiration: date,
    current_short_strike: float,
    underlying_close: float,
    recenter_short: bool,
) -> OptionContractRecord | None:
    max_roll_dte = (long_expiration - entry_date).days - 1
    if max_roll_dte < 1:
        return None
    effective_target_dte = min(target_dte, max_roll_dte)
    search_tolerance = max(dte_tolerance_days, max_roll_dte)
    ordered_expirations = [
        expiration_date
        for expiration_date in preferred_expiration_dates(entry_date, effective_target_dte, search_tolerance)
        if entry_date < expiration_date < long_expiration
    ]
    ordered_expirations = list(dict.fromkeys(ordered_expirations))
    if not ordered_expirations:
        return None

    batch_fetch = getattr(option_gateway, "list_contracts_for_expirations", None)
    if callable(batch_fetch):
        contracts_by_expiration = batch_fetch(
            entry_date=entry_date,
            contract_type=contract_type,
            expiration_dates=ordered_expirations,
        )
        for expiration_date in ordered_expirations:
            candidate = _choose_roll_contract_from_expiration(
                contracts=list(contracts_by_expiration.get(expiration_date, [])),
                current_short_strike=current_short_strike,
                underlying_close=underlying_close,
                recenter_short=recenter_short,
            )
            if candidate is not None:
                return candidate

    exact_fetch = getattr(option_gateway, "list_contracts_for_expiration", None)
    if callable(exact_fetch):
        for expiration_date in ordered_expirations:
            contracts = exact_fetch(
                entry_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
            )
            candidate = _choose_roll_contract_from_expiration(
                contracts=list(contracts),
                current_short_strike=current_short_strike,
                underlying_close=underlying_close,
                recenter_short=recenter_short,
            )
            if candidate is not None:
                return candidate

    list_fetch = getattr(option_gateway, "list_contracts", None)
    if callable(list_fetch):
        contracts = list(
            list_fetch(
                entry_date,
                contract_type,
                effective_target_dte,
                search_tolerance,
            )
        )
        contracts = [
            contract
            for contract in contracts
            if entry_date < contract.expiration_date < long_expiration
        ]
        grouped: dict[date, list[OptionContractRecord]] = {}
        for contract in contracts:
            grouped.setdefault(contract.expiration_date, []).append(contract)
        for expiration_date in ordered_expirations:
            candidate = _choose_roll_contract_from_expiration(
                contracts=grouped.get(expiration_date, []),
                current_short_strike=current_short_strike,
                underlying_close=underlying_close,
                recenter_short=recenter_short,
            )
            if candidate is not None:
                return candidate
    return None


def run_adjusted_calendar_backtest(
    *,
    config: BacktestConfig,
    bars: list[DailyBar],
    earnings_dates: set[date],
    option_gateway: OptionDataGateway,
    policy: CalendarAdjustmentPolicy,
    ex_dividend_dates: set[date] | None = None,
    shared_entry_rule_cache: EntryRuleComputationCache | None = None,
    force_single_contract: bool = True,
) -> BacktestExecutionResult:
    if not is_calendar_strategy_type(config.strategy_type):
        raise AppValidationError("Calendar adjustment runner only supports calendar_spread or put_calendar_spread.")
    if config.start_date > config.end_date:
        raise AppValidationError("start_date must not be after end_date")
    if config.account_size <= 0:
        raise AppValidationError("account_size must be positive")

    strategy = STRATEGY_REGISTRY.get(config.strategy_type)
    if strategy is None:
        raise AppValidationError(f"Unsupported strategy_type: {config.strategy_type}")

    engine = OptionsBacktestEngine()
    warnings: list[dict[str, Any]] = []
    warning_codes: set[str] = set()
    sorted_bars = sorted((bar for bar in bars if bar.close_price > 0), key=lambda bar: bar.trade_date)
    if not sorted_bars:
        summary = build_summary(
            float(config.account_size),
            float(config.account_size),
            [],
            [],
            risk_free_rate=config.risk_free_rate,
            risk_free_rate_curve=config.risk_free_rate_curve,
            warnings=warnings,
        )
        return BacktestExecutionResult(summary=summary, trades=[], equity_curve=[], warnings=warnings)

    evaluator = EntryRuleEvaluator(
        config=config,
        bars=sorted_bars,
        earnings_dates=earnings_dates,
        option_gateway=option_gateway,
        shared_cache=shared_entry_rule_cache,
    )
    entry_allowed_mask = evaluator.build_entry_allowed_mask()
    last_bar_date = sorted_bars[-1].trade_date

    cash = _d(config.account_size)
    peak_equity = _d(config.account_size)
    trades: list[TradeResult] = []
    equity_curve: list[EquityPointResult] = []
    campaign: _CalendarCampaign | None = None

    for index, bar in enumerate(sorted_bars):
        if bar.trade_date < config.start_date:
            continue

        just_closed_this_bar = False
        position_value = _D0

        if campaign is not None:
            snapshot = engine._mark_position(
                campaign.position,
                bar,
                option_gateway,
                warnings,
                warning_codes,
                ex_dividend_dates,
            )
            position_value = snapshot.position_value
            exit_prices = {leg.ticker: leg.last_mid for leg in campaign.position.option_legs}
            for stock_leg in campaign.position.stock_legs:
                exit_prices[stock_leg.symbol] = stock_leg.last_price

            if snapshot.assignment_exit_reason is not None:
                trade, cash_delta = _close_campaign(
                    engine=engine,
                    campaign=campaign,
                    config=config,
                    exit_value=position_value,
                    exit_date=bar.trade_date,
                    exit_underlying_close=bar.close_price,
                    exit_prices=exit_prices,
                    exit_reason=snapshot.assignment_exit_reason,
                    current_bar_index=index,
                    assignment_detail=snapshot.assignment_detail,
                    trade_warnings=snapshot.warnings,
                )
                cash += cash_delta
                trades.append(trade)
                campaign = None
                position_value = _D0
                just_closed_this_bar = True
            else:
                should_exit, exit_reason = _resolve_campaign_exit(
                    campaign=campaign,
                    config=config,
                    bar=bar,
                    current_bar_index=index,
                    position_value=position_value,
                    last_bar_date=last_bar_date,
                )
                if should_exit and exit_reason == "expiration":
                    adjusted, adjustment_cash_delta = _apply_expiration_adjustment(
                        engine=engine,
                        campaign=campaign,
                        config=config,
                        bar=bar,
                        current_bar_index=index,
                        option_gateway=option_gateway,
                        position_value=position_value,
                        last_bar_date=last_bar_date,
                    )
                    if adjusted:
                        cash += adjustment_cash_delta
                        position_value = engine._current_position_value(campaign.position, bar.close_price)
                    else:
                        trade, cash_delta = _close_campaign(
                            engine=engine,
                            campaign=campaign,
                            config=config,
                            exit_value=position_value,
                            exit_date=bar.trade_date,
                            exit_underlying_close=bar.close_price,
                            exit_prices=exit_prices,
                            exit_reason=exit_reason,
                            current_bar_index=index,
                            trade_warnings=snapshot.warnings,
                        )
                        cash += cash_delta
                        trades.append(trade)
                        campaign = None
                        position_value = _D0
                        just_closed_this_bar = True
                elif should_exit:
                    trade, cash_delta = _close_campaign(
                        engine=engine,
                        campaign=campaign,
                        config=config,
                        exit_value=position_value,
                        exit_date=bar.trade_date,
                        exit_underlying_close=bar.close_price,
                        exit_prices=exit_prices,
                        exit_reason=exit_reason,
                        current_bar_index=index,
                        trade_warnings=snapshot.warnings,
                    )
                    cash += cash_delta
                    trades.append(trade)
                    campaign = None
                    position_value = _D0
                    just_closed_this_bar = True

        if campaign is None and just_closed_this_bar:
            engine._add_warning_once(
                warnings,
                warning_codes,
                "same_day_reentry_blocked",
                "One or more entry signals were suppressed because a position was closed on the same trading day. The adjustment runner does not re-enter on the same bar.",
            )

        if campaign is None and not just_closed_this_bar and bar.trade_date <= config.end_date:
            entry_allowed = index < len(entry_allowed_mask) and entry_allowed_mask[index]
            if entry_allowed and not engine._can_afford_minimum_strategy_package(strategy, config, bar, cash):
                engine._add_warning_once(
                    warnings,
                    warning_codes,
                    "capital_requirement_exceeded",
                    "One or more signals were skipped because available cash or configured risk budget could not support the strategy package.",
                )
                entry_allowed = False
            if entry_allowed:
                try:
                    candidate = strategy.build_position(config, bar, index, option_gateway)
                except DataUnavailableError:
                    engine._add_warning_once(
                        warnings,
                        warning_codes,
                        "missing_contract_chain",
                        "One or more entry dates could not be evaluated because no eligible option contract chain was returned.",
                    )
                    candidate = None
                if candidate is None:
                    engine._add_warning_once(
                        warnings,
                        warning_codes,
                        "missing_entry_quote",
                        "One or more entry dates were skipped because no valid same-day option quote was returned.",
                    )
                else:
                    engine._enrich_position_option_legs(position=candidate, option_gateway=option_gateway)
                    entry_value_per_unit = engine._entry_value_per_unit(candidate)
                    contracts_per_unit = sum(leg.quantity_per_unit for leg in candidate.option_legs)
                    commission_per_unit = float(config.commission_per_contract) * contracts_per_unit
                    gross_notional_per_unit = (
                        sum(abs(leg.entry_mid * getattr(leg, "contract_multiplier", 100.0)) * leg.quantity_per_unit for leg in candidate.option_legs)
                        + sum(abs(leg.entry_price * leg.share_quantity_per_unit) for leg in candidate.stock_legs)
                    )
                    quantity = engine._resolve_position_size(
                        available_cash=cash,
                        account_size=float(config.account_size),
                        risk_per_trade_pct=float(config.risk_per_trade_pct),
                        capital_required_per_unit=candidate.capital_required_per_unit,
                        max_loss_per_unit=candidate.max_loss_per_unit,
                        entry_cost_per_unit=float(abs(entry_value_per_unit)),
                        commission_per_unit=commission_per_unit,
                        slippage_pct=config.slippage_pct,
                        gross_notional_per_unit=gross_notional_per_unit,
                    )
                    if force_single_contract:
                        quantity = 1 if quantity >= 1 else 0
                    if quantity <= 0:
                        engine._add_warning_once(
                            warnings,
                            warning_codes,
                            "capital_requirement_exceeded",
                            "One or more signals were skipped because available cash or configured risk budget could not support the strategy package.",
                        )
                    else:
                        candidate.quantity = quantity
                        candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
                        entry_commission = engine._option_commission_total(candidate, config.commission_per_contract)
                        candidate.entry_commission_total = entry_commission
                        entry_slippage = _d(gross_notional_per_unit) * _d(quantity) * (_d(config.slippage_pct) / _D100)
                        total_entry_cost = (entry_value_per_unit * _d(quantity)) + entry_commission + entry_slippage
                        if cash - total_entry_cost < 0:
                            engine._add_warning_once(
                                warnings,
                                warning_codes,
                                "negative_cash_rejected",
                                "One or more entries were skipped because the total cost (including slippage) would have exceeded available cash.",
                            )
                        else:
                            _attach_quote_series_for_open_position(
                                engine=engine,
                                position=candidate,
                                option_gateway=option_gateway,
                                start_date=bar.trade_date,
                                last_bar_date=last_bar_date,
                            )
                            cash -= total_entry_cost
                            campaign = _CalendarCampaign(
                                policy=policy,
                                initial_display_ticker=candidate.display_ticker,
                                position=candidate,
                                initial_entry_value_per_unit=entry_value_per_unit,
                                initial_entry_commission_total=entry_commission,
                                gross_cash_balance=-(entry_value_per_unit * _d(quantity)),
                                net_cash_balance=-total_entry_cost,
                                total_commissions=entry_commission,
                                total_slippage=entry_slippage,
                                max_capital_at_risk=candidate.capital_required_per_unit * quantity,
                                initial_legs=[dict(leg) for leg in candidate.detail_json.get("legs", [])],
                                initial_assumptions=list(candidate.detail_json.get("assumptions", [])),
                            )
                            position_value = engine._current_position_value(candidate, bar.close_price)

        if campaign is not None:
            position_value = engine._current_position_value(campaign.position, bar.close_price)
            if not position_value.is_finite():
                position_value = _D0

        equity = cash + position_value
        if equity < _D0:
            engine._add_warning_once(
                warnings,
                warning_codes,
                "negative_equity",
                "Account equity went negative. This indicates a margin call scenario where losses exceeded the account balance.",
            )
        peak_equity = max(peak_equity, equity)
        drawdown_pct = ((peak_equity - equity) / peak_equity * _D100) if peak_equity > _D0 else _D0
        equity_curve.append(
            EquityPointResult(
                trade_date=bar.trade_date,
                equity=equity,
                cash=cash,
                position_value=position_value,
                drawdown_pct=drawdown_pct,
            )
        )

        if campaign is None and bar.trade_date > config.end_date:
            break

    if campaign is not None:
        snapshot = engine._mark_position(
            campaign.position,
            sorted_bars[-1],
            option_gateway,
            warnings,
            warning_codes,
            ex_dividend_dates,
        )
        final_position_value = snapshot.position_value
        if not final_position_value.is_finite():
            final_position_value = engine._entry_value_per_unit(campaign.position) * _d(campaign.position.quantity)
            if not final_position_value.is_finite():
                final_position_value = _D0
        exit_prices = {leg.ticker: leg.last_mid for leg in campaign.position.option_legs}
        trade, cash_delta = _close_campaign(
            engine=engine,
            campaign=campaign,
            config=config,
            exit_value=final_position_value,
            exit_date=sorted_bars[-1].trade_date,
            exit_underlying_close=sorted_bars[-1].close_price,
            exit_prices=exit_prices,
            exit_reason="data_exhausted",
            current_bar_index=len(sorted_bars) - 1,
            trade_warnings=snapshot.warnings,
        )
        cash += cash_delta
        trades.append(trade)
        equity = cash
        peak_equity = max(peak_equity, equity)
        drawdown_pct = ((peak_equity - equity) / peak_equity * _D100) if peak_equity > _D0 else _D0
        final_point = EquityPointResult(
            trade_date=sorted_bars[-1].trade_date,
            equity=equity,
            cash=cash,
            position_value=_D0,
            drawdown_pct=drawdown_pct,
        )
        if equity_curve and equity_curve[-1].trade_date == sorted_bars[-1].trade_date:
            equity_curve[-1] = final_point
        else:
            equity_curve.append(final_point)
        engine._add_warning_once(
            warnings,
            warning_codes,
            "position_force_closed",
            "An open position was force-closed because no more market data was available.",
        )

    summary = build_summary(
        float(config.account_size),
        float(equity_curve[-1].equity) if equity_curve else float(config.account_size),
        trades,
        equity_curve,
        risk_free_rate=config.risk_free_rate,
        risk_free_rate_curve=config.risk_free_rate_curve,
        warnings=warnings,
    )
    return BacktestExecutionResult(summary=summary, trades=trades, equity_curve=equity_curve, warnings=warnings)


def _choose_roll_contract_from_expiration(
    *,
    contracts: list[OptionContractRecord],
    current_short_strike: float,
    underlying_close: float,
    recenter_short: bool,
) -> OptionContractRecord | None:
    if not contracts:
        return None
    try:
        if recenter_short:
            strike = choose_atm_strike(sorted_unique_strikes(contracts), underlying_close)
        else:
            strike = current_short_strike
        return require_contract_for_strike(contracts, strike)
    except DataUnavailableError:
        return None


def _attach_quote_series_for_open_position(
    *,
    engine: OptionsBacktestEngine,
    position: OpenMultiLegPosition,
    option_gateway: OptionDataGateway,
    start_date: date,
    last_bar_date: date,
) -> None:
    if not position.option_legs:
        return
    end_date = min(last_bar_date, max(leg.expiration_date for leg in position.option_legs))
    engine._attach_position_quote_series(
        position,
        option_gateway=option_gateway,
        start_date=start_date,
        end_date=end_date,
    )


def _attach_quote_series_for_tickers(
    *,
    position: OpenMultiLegPosition,
    option_gateway: OptionDataGateway,
    tickers: list[str],
    start_date: date,
    end_date: date,
) -> None:
    if not tickers or end_date < start_date:
        return
    fetch_series = getattr(option_gateway, "get_quote_series", None)
    if not callable(fetch_series):
        return
    try:
        fetched = fetch_series(tickers, start_date, end_date)
    except Exception:
        return
    for ticker, quotes_by_date in fetched.items():
        position.quote_series_lookup.setdefault(ticker, {}).update(dict(quotes_by_date))
        position.quote_series_loaded_tickers.add(ticker)


def _resolve_campaign_exit(
    *,
    campaign: _CalendarCampaign,
    config: BacktestConfig,
    bar: DailyBar,
    current_bar_index: int,
    position_value: Decimal,
    last_bar_date: date,
) -> tuple[bool, str]:
    if campaign.policy.mode == "hold_long_only_if_short_otm" and campaign.forced_exit_bar_index is not None:
        if campaign.policy.exit_long_on_breakeven and _campaign_mark_to_market_pnl(campaign, position_value) >= _D0:
            return True, "adjustment_recovered"
        if current_bar_index >= campaign.forced_exit_bar_index:
            return True, "adjustment_timeout"

    if campaign.position.option_legs:
        exit_date = campaign.position.scheduled_exit_date or max(leg.expiration_date for leg in campaign.position.option_legs)
    else:
        exit_date = campaign.position.scheduled_exit_date or (campaign.position.entry_date + timedelta(days=config.max_holding_days))
    if bar.trade_date >= exit_date:
        return True, "expiration"

    capital_at_risk = max(campaign.max_capital_at_risk, 0.0)
    if capital_at_risk > 0 and (config.stop_loss_pct is not None or config.profit_target_pct is not None):
        unrealized_pnl_pct = float(_campaign_mark_to_market_pnl(campaign, position_value) / _d(capital_at_risk) * _D100)
        if config.stop_loss_pct is not None and unrealized_pnl_pct <= -config.stop_loss_pct:
            return True, "stop_loss"
        if config.profit_target_pct is not None and unrealized_pnl_pct >= config.profit_target_pct:
            return True, "profit_target"

    if current_bar_index - campaign.position.entry_index >= config.max_holding_days:
        return True, "max_holding_days"
    if bar.trade_date >= config.end_date and bar.trade_date == last_bar_date:
        return True, "backtest_end"
    return False, ""


def _campaign_mark_to_market_pnl(campaign: _CalendarCampaign, position_value: Decimal) -> Decimal:
    return campaign.net_cash_balance + position_value


def _apply_expiration_adjustment(
    *,
    engine: OptionsBacktestEngine,
    campaign: _CalendarCampaign,
    config: BacktestConfig,
    bar: DailyBar,
    current_bar_index: int,
    option_gateway: OptionDataGateway,
    position_value: Decimal,
    last_bar_date: date,
) -> tuple[bool, Decimal]:
    if campaign.policy.mode == "close":
        return False, _D0
    if _campaign_mark_to_market_pnl(campaign, position_value) >= _D0:
        return False, _D0

    short_leg = next((leg for leg in campaign.position.option_legs if leg.side < 0), None)
    long_leg = next((leg for leg in campaign.position.option_legs if leg.side > 0), None)
    if short_leg is None or long_leg is None:
        return False, _D0
    if short_leg.expiration_date > bar.trade_date:
        return False, _D0

    if campaign.policy.mode in {"roll_same_strike_once", "recenter_short_once"}:
        if campaign.roll_count >= campaign.policy.max_rolls:
            return False, _D0
        long_remaining_dte = (long_leg.expiration_date - bar.trade_date).days
        if long_remaining_dte < campaign.policy.min_long_dte_remaining:
            return False, _D0
        new_short_contract = select_calendar_roll_short_contract(
            option_gateway,
            entry_date=bar.trade_date,
            contract_type=short_leg.contract_type,
            target_dte=config.target_dte,
            dte_tolerance_days=config.dte_tolerance_days,
            long_expiration=long_leg.expiration_date,
            current_short_strike=short_leg.strike_price,
            underlying_close=bar.close_price,
            recenter_short=(campaign.policy.mode == "recenter_short_once"),
        )
        if new_short_contract is None:
            return False, _D0
        quote = option_gateway.get_quote(new_short_contract.ticker, bar.trade_date)
        if quote is None or quote.mid_price is None or quote.mid_price <= 0:
            return False, _D0

        old_short_mid = short_leg.last_mid
        new_short_mid = quote.mid_price
        old_short_notional = _per_leg_notional(short_leg, old_short_mid, campaign.position.quantity)
        new_short_notional = _per_leg_notional(short_leg, new_short_mid, campaign.position.quantity)
        gross_adjustment_cash = new_short_notional - old_short_notional
        commission_delta = _d(config.commission_per_contract) * _d(abs(short_leg.quantity_per_unit) * campaign.position.quantity * 2)
        slippage_delta = (old_short_notional + new_short_notional) * (_d(config.slippage_pct) / _D100)
        net_adjustment_cash = gross_adjustment_cash - commission_delta - slippage_delta
        if campaign.policy.require_net_credit and net_adjustment_cash <= _D0:
            return False, _D0

        campaign.gross_cash_balance += gross_adjustment_cash
        campaign.net_cash_balance += net_adjustment_cash
        campaign.total_commissions += commission_delta
        campaign.total_slippage += slippage_delta
        campaign.roll_count += 1

        rolled_leg = _build_replacement_short_leg(
            template=short_leg,
            contract=new_short_contract,
            entry_mid=new_short_mid,
            underlying_symbol=config.symbol,
        )
        for idx, leg in enumerate(campaign.position.option_legs):
            if leg is short_leg:
                campaign.position.option_legs[idx] = rolled_leg
                break
        campaign.position.display_ticker = _compose_display_ticker(campaign.position)
        campaign.position.scheduled_exit_date = rolled_leg.expiration_date
        campaign.position.capital_required_per_unit = _estimate_position_capital_required(
            position=campaign.position,
            underlying_close=bar.close_price,
        )
        campaign.max_capital_at_risk = max(
            campaign.max_capital_at_risk,
            campaign.position.capital_required_per_unit * campaign.position.quantity,
        )
        _sync_position_detail_legs(campaign.position)
        campaign.adjustment_events.append(
            {
                "event_type": campaign.policy.mode,
                "trade_date": bar.trade_date.isoformat(),
                "closed_short_ticker": short_leg.ticker,
                "closed_short_mid": old_short_mid,
                "opened_short_ticker": rolled_leg.ticker,
                "opened_short_mid": new_short_mid,
                "gross_adjustment_cash": float(gross_adjustment_cash),
                "net_adjustment_cash": float(net_adjustment_cash),
            }
        )
        _attach_quote_series_for_tickers(
            position=campaign.position,
            option_gateway=option_gateway,
            tickers=[rolled_leg.ticker],
            start_date=bar.trade_date,
            end_date=min(last_bar_date, max(leg.expiration_date for leg in campaign.position.option_legs)),
        )
        return True, net_adjustment_cash

    if campaign.policy.mode == "hold_long_only_if_short_otm":
        if short_leg.contract_type != "put":
            return False, _D0
        if bar.close_price <= short_leg.strike_price:
            return False, _D0
        settlement_mid = float(
            engine._intrinsic_value(
                short_leg.contract_type,
                short_leg.strike_price,
                bar.close_price,
                deliverable_shares_per_contract=getattr(short_leg, "deliverable_shares_per_contract", 100.0),
            )
        )
        short_leg.last_mid = settlement_mid
        campaign.position.option_legs = [leg for leg in campaign.position.option_legs if leg.side > 0]
        if not campaign.position.option_legs:
            return False, _D0
        campaign.position.display_ticker = _compose_display_ticker(campaign.position)
        campaign.position.scheduled_exit_date = long_leg.expiration_date
        campaign.position.capital_required_per_unit = _estimate_position_capital_required(
            position=campaign.position,
            underlying_close=bar.close_price,
        )
        campaign.forced_exit_bar_index = current_bar_index + max(campaign.policy.hold_long_trading_days, 1)
        _sync_position_detail_legs(campaign.position)
        campaign.adjustment_events.append(
            {
                "event_type": campaign.policy.mode,
                "trade_date": bar.trade_date.isoformat(),
                "expired_short_ticker": short_leg.ticker,
                "expired_short_mid": settlement_mid,
                "remaining_long_ticker": long_leg.ticker,
                "forced_exit_bar_index": campaign.forced_exit_bar_index,
            }
        )
        return True, _D0

    return False, _D0


def _per_leg_notional(leg: OpenOptionLeg, mid_price: float, quantity: int) -> Decimal:
    multiplier = getattr(leg, "contract_multiplier", 100.0)
    return _d(abs(mid_price) * multiplier * abs(leg.quantity_per_unit) * quantity)


def _build_replacement_short_leg(
    *,
    template: OpenOptionLeg,
    contract: OptionContractRecord,
    entry_mid: float,
    underlying_symbol: str,
) -> OpenOptionLeg:
    parsed = parse_option_ticker_metadata(contract.ticker)
    return OpenOptionLeg(
        ticker=contract.ticker,
        contract_type=contract.contract_type,
        side=template.side,
        strike_price=contract.strike_price,
        expiration_date=contract.expiration_date,
        quantity_per_unit=template.quantity_per_unit,
        entry_mid=entry_mid,
        last_mid=entry_mid,
        contract_multiplier=getattr(template, "contract_multiplier", 100.0),
        deliverable_shares_per_contract=contract.shares_per_contract,
        contract_root_symbol=parsed[0] if parsed is not None else None,
        reference_underlying_symbol=contract.underlying_symbol or underlying_symbol,
        mark_ticker=contract.ticker,
        is_nonstandard=not abs(contract.shares_per_contract - 100.0) < 1e-9,
    )


def _estimate_position_capital_required(
    *,
    position: OpenMultiLegPosition,
    underlying_close: float,
) -> float:
    if not position.option_legs:
        return 1.0
    package_value = float(
        sum(
            _d(leg.last_mid) * _d(leg.side) * _d(leg.quantity_per_unit) * _d(getattr(leg, "contract_multiplier", 100.0))
            for leg in position.option_legs
        )
    )
    short_legs = [leg for leg in position.option_legs if leg.side < 0]
    long_legs = [leg for leg in position.option_legs if leg.side > 0]
    if not short_legs:
        return max(abs(package_value), 1.0)
    short_margin = 0.0
    for leg in short_legs:
        if leg.contract_type == "put":
            short_margin += naked_put_margin(underlying_close, leg.strike_price, max(leg.last_mid, 0.0))
        else:
            short_margin += naked_call_margin(underlying_close, leg.strike_price, max(leg.last_mid, 0.0))
    long_value = sum(
        max(leg.last_mid, 0.0) * getattr(leg, "contract_multiplier", 100.0) * leg.quantity_per_unit
        for leg in long_legs
    )
    return max(max(short_margin - long_value, max(package_value, 0.0)), 1.0)


def _sync_position_detail_legs(position: OpenMultiLegPosition) -> None:
    position.detail_json["legs"] = [
        {
            "asset_type": "option",
            "ticker": leg.ticker,
            "side": "long" if leg.side > 0 else "short",
            "contract_type": leg.contract_type,
            "strike_price": leg.strike_price,
            "expiration_date": leg.expiration_date.isoformat(),
            "quantity_per_unit": leg.quantity_per_unit,
            "entry_mid": leg.entry_mid,
        }
        for leg in position.option_legs
    ]


def _compose_display_ticker(position: OpenMultiLegPosition) -> str:
    identifiers = [leg.ticker for leg in position.option_legs]
    identifiers.extend(leg.symbol for leg in position.stock_legs)
    return "|".join(identifiers)


def _close_campaign(
    *,
    engine: OptionsBacktestEngine,
    campaign: _CalendarCampaign,
    config: BacktestConfig,
    exit_value: Decimal,
    exit_date: date,
    exit_underlying_close: float,
    exit_prices: dict[str, float],
    exit_reason: str,
    current_bar_index: int,
    assignment_detail: dict[str, Any] | None = None,
    trade_warnings: tuple[str, ...] = (),
) -> tuple[TradeResult, Decimal]:
    position = campaign.position
    exit_commission, exit_slippage_notional, commission_waivers = engine._option_exit_cost_profile(
        position=position,
        commission_per_contract=config.commission_per_contract,
        exit_date=exit_date,
        exit_prices=exit_prices,
        assignment_detail=assignment_detail,
    )
    stock_exit_notional = (
        sum(abs(_d(leg.last_price) * _d(leg.share_quantity_per_unit)) for leg in position.stock_legs) * _d(position.quantity)
        if position.stock_legs
        else _D0
    )
    exit_gross_notional = exit_slippage_notional + stock_exit_notional
    exit_slippage = exit_gross_notional * (_d(config.slippage_pct) / _D100)
    cash_delta = exit_value - exit_commission - exit_slippage

    campaign.gross_cash_balance += exit_value
    campaign.net_cash_balance += cash_delta
    campaign.total_commissions += exit_commission
    campaign.total_slippage += exit_slippage

    exit_value_per_unit = exit_value / _d(position.quantity) if position.quantity else _D0
    expiration_date = position.scheduled_exit_date or max((leg.expiration_date for leg in position.option_legs), default=exit_date)

    detail_json = engine._build_trade_detail_json(
        position,
        exit_prices,
        float(exit_value_per_unit),
        assignment_detail,
    )
    detail_json["campaign_policy"] = campaign.policy.name
    detail_json["campaign_initial_display_ticker"] = campaign.initial_display_ticker
    detail_json["campaign_current_display_ticker"] = position.display_ticker
    detail_json["campaign_initial_legs"] = campaign.initial_legs
    detail_json["campaign_initial_assumptions"] = campaign.initial_assumptions
    detail_json["campaign_adjustment_events"] = campaign.adjustment_events
    detail_json["campaign_roll_count"] = campaign.roll_count
    detail_json["campaign_total_commissions"] = float(campaign.total_commissions)
    detail_json["campaign_total_slippage"] = float(campaign.total_slippage)
    detail_json["campaign_gross_cash_balance"] = float(campaign.gross_cash_balance)
    detail_json["campaign_net_cash_balance"] = float(campaign.net_cash_balance)
    detail_json["commission_waivers"] = commission_waivers
    detail_json["entry_commissions"] = float(campaign.initial_entry_commission_total)
    detail_json["exit_commissions"] = float(exit_commission)
    detail_json["adjustment_commissions"] = float(campaign.total_commissions - campaign.initial_entry_commission_total - exit_commission)
    detail_json["total_slippage"] = float(campaign.total_slippage)

    trade = TradeResult(
        option_ticker=campaign.initial_display_ticker,
        strategy_type=config.strategy_type,
        underlying_symbol=config.symbol,
        entry_date=position.entry_date,
        exit_date=exit_date,
        expiration_date=expiration_date,
        quantity=position.quantity,
        dte_at_open=position.dte_at_open,
        holding_period_days=(exit_date - position.entry_date).days,
        holding_period_trading_days=current_bar_index - position.entry_index,
        entry_underlying_close=_d(engine._entry_underlying_close(position)),
        exit_underlying_close=_d(exit_underlying_close),
        entry_mid=campaign.initial_entry_value_per_unit / _D100,
        exit_mid=exit_value_per_unit / _D100,
        gross_pnl=campaign.gross_cash_balance,
        net_pnl=campaign.net_cash_balance,
        total_commissions=campaign.total_commissions,
        entry_reason=position.entry_reason,
        exit_reason=exit_reason,
        detail_json=detail_json,
        warnings=trade_warnings,
    )
    return trade, cash_delta
