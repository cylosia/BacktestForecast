from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import Any

import structlog

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.strategies.common import (
    choose_call_otm_strike,
    choose_primary_expiration,
    choose_put_otm_strike,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
    valid_entry_mids,
)
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OptionDataGateway,
    TradeResult,
)
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar

logger = structlog.get_logger(__name__)


def _D(v: float | int) -> Decimal:
    return Decimal(str(v))


@dataclass(slots=True)
class OpenShortOptionPhase:
    ticker: str
    contract_type: str
    strike_price: float
    expiration_date: date
    entry_date: date
    entry_index: int
    quantity: int
    entry_mid: float
    phase: str
    last_mid: float


@dataclass(slots=True)
class HeldShares:
    quantity: int
    entry_date: date
    entry_price: float


class WheelStrategyBacktestEngine:
    def run(
        self,
        config: BacktestConfig,
        bars: list[DailyBar],
        earnings_dates: set[date],
        option_gateway: OptionDataGateway,
    ) -> BacktestExecutionResult:
        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        if not sorted_bars:
            return BacktestExecutionResult(
                summary=build_summary(float(config.account_size), float(config.account_size), [], [], risk_free_rate=config.risk_free_rate),
                trades=[], equity_curve=[]
            )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        # Known inconsistency: config.account_size is Decimal but the engine
        # converts to float here because all downstream arithmetic (option mids,
        # strike prices, close prices, commissions, slippage) uses float.  Python
        # raises TypeError on Decimal+float, so keeping cash as Decimal would
        # require wrapping every interacting value in Decimal(str(...)) — too
        # invasive for the marginal precision gain on a value that's already
        # rounded to cents.  If sub-cent precision becomes important (e.g. for
        # regulatory reporting), convert the entire engine to Decimal.
        cash = float(config.account_size)
        peak_equity = cash
        active_option: OpenShortOptionPhase | None = None
        held_shares: HeldShares | None = None
        trades: list[TradeResult] = []
        equity_curve: list[EquityPointResult] = []
        evaluator = EntryRuleEvaluator(
            config=config, bars=sorted_bars, earnings_dates=earnings_dates, option_gateway=option_gateway
        )

        for index, bar in enumerate(sorted_bars):
            if bar.trade_date < config.start_date:
                continue

            option_value = 0.0
            if active_option is not None:
                quote = option_gateway.get_quote(active_option.ticker, bar.trade_date)
                if quote is None:
                    if bar.trade_date >= active_option.expiration_date:
                        current_mid = (
                            max(0.0, bar.close_price - active_option.strike_price)
                            if active_option.contract_type == "call"
                            else max(0.0, active_option.strike_price - bar.close_price)
                        )
                    else:
                        current_mid = active_option.last_mid
                        self._add_warning_once(
                            warnings,
                            warning_codes,
                            "missing_option_mark_quote",
                            "One or more daily option marks were missing;"
                            " the engine carried forward the previous mid-price.",
                        )
                else:
                    if math.isfinite(quote.mid_price):
                        current_mid = quote.mid_price
                    else:
                        current_mid = active_option.last_mid
                        self._add_warning_once(
                            warnings,
                            warning_codes,
                            "invalid_option_mid_price",
                            "One or more option mid-prices were NaN or Inf;"
                            " the engine carried forward the previous mid-price.",
                        )
                active_option.last_mid = current_mid
                option_value = -current_mid * 100.0 * active_option.quantity

                capital_at_risk = active_option.entry_mid * active_option.quantity * 100.0
                position_pnl = (active_option.entry_mid - current_mid) * active_option.quantity * 100.0
                should_exit, exit_reason = self._resolve_exit(
                    bar=bar,
                    position=active_option,
                    max_holding_days=config.max_holding_days,
                    backtest_end_date=config.end_date,
                    last_bar_date=sorted_bars[-1].trade_date,
                    current_bar_index=index,
                    profit_target_pct=config.profit_target_pct,
                    stop_loss_pct=config.stop_loss_pct,
                    capital_at_risk=capital_at_risk,
                    current_value=capital_at_risk + position_pnl,
                )
                if should_exit:
                    exit_mid = current_mid
                    exit_commission = float(config.commission_per_contract) * active_option.quantity
                    entry_commission = float(config.commission_per_contract) * active_option.quantity
                    option_gross_pnl = (active_option.entry_mid - exit_mid) * 100.0 * active_option.quantity
                    entry_slippage = active_option.entry_mid * 100.0 * active_option.quantity * (config.slippage_pct / 100.0)
                    exit_slippage = abs(exit_mid) * 100.0 * active_option.quantity * (config.slippage_pct / 100.0)
                    option_net_pnl = option_gross_pnl - (entry_commission + exit_commission) - (entry_slippage + exit_slippage)
                    option_detail = {
                        "phase": active_option.phase,
                        "legs": [
                            {
                                "asset_type": "option",
                                "ticker": active_option.ticker,
                                "side": "short",
                                "contract_type": active_option.contract_type,
                                "strike_price": active_option.strike_price,
                                "expiration_date": active_option.expiration_date.isoformat(),
                                "quantity_per_unit": 1,
                                "entry_mid": active_option.entry_mid,
                                "exit_mid": exit_mid,
                            }
                        ],
                        "assumptions": [
                            "Wheel phases are recorded separately so share inventory can persist across cycles.",
                            "Put assignment converts option liability into long shares at strike;"
                            " covered-call assignment converts shares to cash at strike.",
                        ],
                    }

                    if (
                        active_option.phase == "cash_secured_put"
                        and exit_reason == "expiration"
                        and bar.close_price < active_option.strike_price
                    ):
                        exit_mid = 0.0
                        option_detail["legs"][0]["exit_mid"] = exit_mid
                        option_gross_pnl = active_option.entry_mid * 100.0 * active_option.quantity
                        option_net_pnl = option_gross_pnl - entry_commission - entry_slippage
                        cash -= active_option.strike_price * 100.0 * active_option.quantity
                        if cash < 0:
                            self._add_warning_once(
                                warnings, warning_codes, "implicit_margin",
                                "Cash balance went negative during put assignment. Returns may be "
                                "overstated because margin interest is not modeled.",
                            )
                        held_shares = HeldShares(
                            quantity=active_option.quantity,
                            entry_date=bar.trade_date,
                            entry_price=active_option.strike_price,
                        )
                        trades.append(
                            TradeResult(
                                option_ticker=active_option.ticker,
                                strategy_type=config.strategy_type,
                                underlying_symbol=config.symbol,
                                entry_date=active_option.entry_date,
                                exit_date=bar.trade_date,
                                expiration_date=active_option.expiration_date,
                                quantity=active_option.quantity,
                                dte_at_open=(active_option.expiration_date - active_option.entry_date).days,
                                holding_period_days=(bar.trade_date - active_option.entry_date).days,
                                entry_underlying_close=_D(sorted_bars[active_option.entry_index].close_price),
                                exit_underlying_close=_D(bar.close_price),
                                entry_mid=_D(active_option.entry_mid),
                                exit_mid=_D(exit_mid),
                                gross_pnl=_D(option_gross_pnl),
                                net_pnl=_D(option_net_pnl),
                                total_commissions=_D(float(config.commission_per_contract) * active_option.quantity),
                                entry_reason="entry_rules_met",
                                exit_reason="assignment",
                                detail_json={**option_detail, "assignment": True, "unit_convention": "per_share_option_premium"},
                            )
                        )
                    elif (
                        active_option.phase == "covered_call"
                        and exit_reason == "expiration"
                        and bar.close_price > active_option.strike_price
                        and held_shares is not None
                    ):
                        if held_shares.quantity != active_option.quantity:
                            logger.warning(
                                "wheel.quantity_mismatch_at_assignment",
                                held_shares_qty=held_shares.quantity,
                                option_qty=active_option.quantity,
                                bar_date=str(bar.trade_date),
                            )
                        exit_mid = 0.0
                        option_detail["legs"][0]["exit_mid"] = exit_mid
                        option_gross_pnl = active_option.entry_mid * 100.0 * active_option.quantity
                        option_net_pnl = option_gross_pnl - entry_commission - entry_slippage
                        cash += active_option.strike_price * 100.0 * active_option.quantity
                        trades.append(
                            TradeResult(
                                option_ticker=active_option.ticker,
                                strategy_type=config.strategy_type,
                                underlying_symbol=config.symbol,
                                entry_date=active_option.entry_date,
                                exit_date=bar.trade_date,
                                expiration_date=active_option.expiration_date,
                                quantity=active_option.quantity,
                                dte_at_open=(active_option.expiration_date - active_option.entry_date).days,
                                holding_period_days=(bar.trade_date - active_option.entry_date).days,
                                entry_underlying_close=_D(sorted_bars[active_option.entry_index].close_price),
                                exit_underlying_close=_D(bar.close_price),
                                entry_mid=_D(active_option.entry_mid),
                                exit_mid=_D(exit_mid),
                                gross_pnl=_D(option_gross_pnl),
                                net_pnl=_D(option_net_pnl),
                                total_commissions=_D(float(config.commission_per_contract) * active_option.quantity),
                                entry_reason="entry_rules_met",
                                exit_reason="call_assignment",
                                detail_json={**option_detail, "assignment": True, "unit_convention": "per_share_option_premium"},
                            )
                        )
                        called_away_price = active_option.strike_price
                        stock_gross = (called_away_price - held_shares.entry_price) * 100.0 * held_shares.quantity
                        trades.append(
                            TradeResult(
                                option_ticker=f"stock:{config.symbol}",
                                strategy_type=config.strategy_type,
                                underlying_symbol=config.symbol,
                                entry_date=held_shares.entry_date,
                                exit_date=bar.trade_date,
                                expiration_date=bar.trade_date,
                                quantity=held_shares.quantity,
                                dte_at_open=0,
                                holding_period_days=max((bar.trade_date - held_shares.entry_date).days, 0),
                                entry_underlying_close=_D(held_shares.entry_price),
                                exit_underlying_close=_D(called_away_price),
                                entry_mid=_D(held_shares.entry_price),
                                exit_mid=_D(called_away_price),
                                gross_pnl=_D(stock_gross),
                                net_pnl=_D(stock_gross),
                                total_commissions=_D(0.0),
                                entry_reason="put_assignment",
                                exit_reason="called_away",
                                detail_json={
                                    "phase": "stock_inventory",
                                    "share_quantity": held_shares.quantity * 100,
                                    "unit_convention": "per_share_option_premium",
                                    "assumptions": [
                                        "Share P&L is realized separately from short-call"
                                        " premium in the wheel strategy."
                                    ],
                                },
                            )
                        )
                        held_shares = None
                    else:
                        cash += (-exit_mid * 100.0 * active_option.quantity) - exit_commission - exit_slippage
                        trades.append(
                            TradeResult(
                                option_ticker=active_option.ticker,
                                strategy_type=config.strategy_type,
                                underlying_symbol=config.symbol,
                                entry_date=active_option.entry_date,
                                exit_date=bar.trade_date,
                                expiration_date=active_option.expiration_date,
                                quantity=active_option.quantity,
                                dte_at_open=(active_option.expiration_date - active_option.entry_date).days,
                                holding_period_days=(bar.trade_date - active_option.entry_date).days,
                                entry_underlying_close=_D(sorted_bars[active_option.entry_index].close_price),
                                exit_underlying_close=_D(bar.close_price),
                                entry_mid=_D(active_option.entry_mid),
                                exit_mid=_D(exit_mid),
                                gross_pnl=_D(option_gross_pnl),
                                net_pnl=_D(option_net_pnl),
                                total_commissions=_D((float(config.commission_per_contract) * active_option.quantity)
                                + exit_commission),
                                entry_reason="entry_rules_met",
                                exit_reason=exit_reason,
                                detail_json={**option_detail, "assignment": False, "unit_convention": "per_share_option_premium"},
                            )
                        )
                    active_option = None
                    option_value = 0.0

            shares_value = 0.0 if held_shares is None else bar.close_price * 100.0 * held_shares.quantity

            entry_allowed = False
            if active_option is None and bar.trade_date <= config.end_date:
                try:
                    entry_allowed = evaluator.is_entry_allowed(index)
                except Exception:
                    logger.warning("entry_rule_evaluation_error", bar_index=index, exc_info=True)
                    self._add_warning_once(
                        warnings, warning_codes, "entry_rule_evaluation_error",
                        "One or more entry rule evaluations failed and were treated as not-allowed.",
                    )
            if entry_allowed:
                if held_shares is None:
                    position = self._open_short_put(config, bar, index, option_gateway, cash, warnings, warning_codes)
                    if position is not None:
                        active_option = position
                        entry_slip = position.entry_mid * 100.0 * position.quantity * (config.slippage_pct / 100.0)
                        cash += (position.entry_mid * 100.0 * position.quantity) - (
                            float(config.commission_per_contract) * position.quantity
                        ) - entry_slip
                else:
                    position = self._open_covered_call(
                        config, bar, index, option_gateway, held_shares.quantity, warnings, warning_codes
                    )
                    if position is not None:
                        active_option = position
                        entry_slip = position.entry_mid * 100.0 * position.quantity * (config.slippage_pct / 100.0)
                        cash += (position.entry_mid * 100.0 * position.quantity) - (
                            float(config.commission_per_contract) * position.quantity
                        ) - entry_slip

            option_value = 0.0
            if active_option is not None:
                option_value = -active_option.last_mid * 100.0 * active_option.quantity
            shares_value = 0.0 if held_shares is None else bar.close_price * 100.0 * held_shares.quantity

            if not math.isfinite(option_value):
                option_value = 0.0
            if not math.isfinite(shares_value):
                shares_value = 0.0

            equity = cash + shares_value + option_value
            peak_equity = max(peak_equity, equity)
            drawdown_pct = 0.0 if peak_equity == 0 else ((peak_equity - equity) / peak_equity) * 100.0
            equity_curve.append(
                EquityPointResult(
                    trade_date=bar.trade_date,
                    equity=_D(equity),
                    cash=_D(cash),
                    position_value=_D(shares_value + option_value),
                    drawdown_pct=_D(drawdown_pct),
                )
            )

            if active_option is None and held_shares is None and bar.trade_date > config.end_date:
                break

        if active_option is not None:
            final_bar = sorted_bars[-1]
            exit_mid = active_option.last_mid
            entry_commission = float(config.commission_per_contract) * active_option.quantity
            exit_commission = float(config.commission_per_contract) * active_option.quantity
            option_gross = (active_option.entry_mid - exit_mid) * 100.0 * active_option.quantity
            liq_entry_slip = active_option.entry_mid * 100.0 * active_option.quantity * (config.slippage_pct / 100.0)
            liq_exit_slip = abs(exit_mid) * 100.0 * active_option.quantity * (config.slippage_pct / 100.0)
            option_net = option_gross - (entry_commission + exit_commission) - (liq_entry_slip + liq_exit_slip)
            cash += (-exit_mid * 100.0 * active_option.quantity) - exit_commission - liq_exit_slip
            trades.append(
                TradeResult(
                    option_ticker=active_option.ticker,
                    strategy_type=config.strategy_type,
                    underlying_symbol=config.symbol,
                    entry_date=active_option.entry_date,
                    exit_date=final_bar.trade_date,
                    expiration_date=active_option.expiration_date,
                    quantity=active_option.quantity,
                    dte_at_open=(active_option.expiration_date - active_option.entry_date).days,
                    holding_period_days=max((final_bar.trade_date - active_option.entry_date).days, 0),
                    entry_underlying_close=_D(sorted_bars[active_option.entry_index].close_price),
                    exit_underlying_close=_D(final_bar.close_price),
                    entry_mid=_D(active_option.entry_mid),
                    exit_mid=_D(exit_mid),
                    gross_pnl=_D(option_gross),
                    net_pnl=_D(option_net),
                    total_commissions=_D(entry_commission + exit_commission),
                    entry_reason="entry_rules_met",
                    exit_reason="backtest_end_option_liquidation",
                    detail_json={
                        "phase": active_option.phase,
                        "unit_convention": "per_share_option_premium",
                        "assumptions": [
                            "Open short option is liquidated at last available mid-price on the final bar."
                        ],
                    },
                )
            )
            active_option = None

        if held_shares is not None:
            final_bar = sorted_bars[-1]
            cash += final_bar.close_price * 100.0 * held_shares.quantity
            stock_gross = (final_bar.close_price - held_shares.entry_price) * 100.0 * held_shares.quantity
            trades.append(
                TradeResult(
                    option_ticker=f"stock:{config.symbol}",
                    strategy_type=config.strategy_type,
                    underlying_symbol=config.symbol,
                    entry_date=held_shares.entry_date,
                    exit_date=final_bar.trade_date,
                    expiration_date=final_bar.trade_date,
                    quantity=held_shares.quantity,
                    dte_at_open=0,
                    holding_period_days=max((final_bar.trade_date - held_shares.entry_date).days, 0),
                    entry_underlying_close=_D(held_shares.entry_price),
                    exit_underlying_close=_D(final_bar.close_price),
                    entry_mid=_D(held_shares.entry_price),
                    exit_mid=_D(final_bar.close_price),
                    gross_pnl=_D(stock_gross),
                    net_pnl=_D(stock_gross),
                    total_commissions=_D(0.0),
                    entry_reason="put_assignment",
                    exit_reason="backtest_end_share_liquidation",
                    detail_json={
                        "phase": "stock_inventory",
                        "share_quantity": held_shares.quantity * 100,
                        "unit_convention": "per_share_option_premium",
                        "assumptions": ["Remaining wheel share inventory is liquidated on the final available bar."],
                    },
                )
            )

        ending_equity = cash

        if equity_curve and _D(ending_equity) != equity_curve[-1].equity:
            last_td = equity_curve[-1].trade_date
            peak_equity = max(peak_equity, ending_equity)
            dd = 0.0 if peak_equity == 0 else ((peak_equity - ending_equity) / peak_equity) * 100.0
            equity_curve[-1] = EquityPointResult(
                trade_date=last_td,
                equity=_D(ending_equity),
                cash=_D(cash),
                position_value=_D(0.0),
                drawdown_pct=_D(dd),
            )

        summary = build_summary(
            starting_equity=float(config.account_size),
            ending_equity=ending_equity,
            trades=trades,
            equity_curve=equity_curve,
            risk_free_rate=config.risk_free_rate,
            warnings=warnings,
        )
        return BacktestExecutionResult(summary=summary, trades=trades, equity_curve=equity_curve, warnings=warnings)

    def _open_short_put(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
        cash: float,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
    ) -> OpenShortOptionPhase | None:
        try:
            puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
            expiration = choose_primary_expiration(puts, bar.trade_date, config.target_dte)
            put_contracts = contracts_for_expiration(puts, expiration)
            dte = (expiration - bar.trade_date).days
            overrides = get_overrides(config.strategy_overrides)
            strike = resolve_strike(
                [contract.strike_price for contract in put_contracts], bar.close_price, "put",
                overrides.short_put_strike, dte,
                contracts=put_contracts, option_gateway=option_gateway,
                trade_date=bar.trade_date,
            )
            contract = require_contract_for_strike(put_contracts, strike)
        except DataUnavailableError:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_contract_chain",
                "One or more entry dates could not be evaluated because"
                " no eligible option contract chain was returned.",
            )
            return None

        quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
        if quote is None:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_entry_quote",
                "One or more entry dates were skipped because no valid same-day option quote was returned.",
            )
            return None
        if not valid_entry_mids(quote.mid_price):
            self._add_warning_once(
                warnings,
                warning_codes,
                "invalid_entry_mid",
                "Entry quote had invalid mid price and was skipped.",
            )
            return None

        capital_required_per_unit = contract.strike_price * 100.0
        commission_per_unit = float(config.commission_per_contract) * 2
        total_cost_per_unit = capital_required_per_unit + commission_per_unit - (quote.mid_price * 100.0)
        if total_cost_per_unit <= 0:
            self._add_warning_once(
                warnings,
                warning_codes,
                "negative_cost_per_unit",
                "Premium exceeds collateral + commission; skipped to avoid unbounded sizing.",
            )
            return None
        max_loss_per_unit = max((contract.strike_price - quote.mid_price) * 100.0, 0.0)
        risk_budget = float(config.account_size) * (float(config.risk_per_trade_pct) / 100.0)
        by_risk = int(risk_budget // max_loss_per_unit) if max_loss_per_unit > 0 else 0
        by_cash = int(cash // total_cost_per_unit)
        quantity = max(0, min(by_risk, by_cash))
        if quantity <= 0:
            self._add_warning_once(
                warnings,
                warning_codes,
                "capital_requirement_exceeded",
                "One or more wheel entries were skipped because cash-secured collateral"
                " or risk budget was insufficient.",
            )
            return None

        self._add_warning_once(
            warnings,
            warning_codes,
            "margin_reserved",
            "Wheel strategy sizing is constrained by cash-secured put collateral and covered-call share inventory.",
        )
        return OpenShortOptionPhase(
            ticker=contract.ticker,
            contract_type="put",
            strike_price=contract.strike_price,
            expiration_date=contract.expiration_date,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=quantity,
            entry_mid=quote.mid_price,
            phase="cash_secured_put",
            last_mid=quote.mid_price,
        )

    def _open_covered_call(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
        quantity: int,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
    ) -> OpenShortOptionPhase | None:
        try:
            calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
            expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
            call_contracts = contracts_for_expiration(calls, expiration)
            dte = (expiration - bar.trade_date).days
            overrides = get_overrides(config.strategy_overrides)
            strike = resolve_strike(
                [contract.strike_price for contract in call_contracts], bar.close_price, "call",
                overrides.short_call_strike, dte,
                contracts=call_contracts, option_gateway=option_gateway,
                trade_date=bar.trade_date,
            )
            contract = require_contract_for_strike(call_contracts, strike)
        except DataUnavailableError:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_contract_chain",
                "One or more entry dates could not be evaluated because"
                " no eligible option contract chain was returned.",
            )
            return None

        quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
        if quote is None:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_entry_quote",
                "One or more entry dates were skipped because no valid same-day option quote was returned.",
            )
            return None
        if not valid_entry_mids(quote.mid_price):
            self._add_warning_once(
                warnings,
                warning_codes,
                "invalid_entry_mid",
                "Entry quote had invalid mid price and was skipped.",
            )
            return None
        return OpenShortOptionPhase(
            ticker=contract.ticker,
            contract_type="call",
            strike_price=contract.strike_price,
            expiration_date=contract.expiration_date,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=quantity,
            entry_mid=quote.mid_price,
            phase="covered_call",
            last_mid=quote.mid_price,
        )

    @staticmethod
    def _resolve_exit(
        bar: DailyBar,
        position: OpenShortOptionPhase,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
        current_bar_index: int | None = None,
        *,
        profit_target_pct: float | None = None,
        stop_loss_pct: float | None = None,
        capital_at_risk: float | None = None,
        current_value: float | None = None,
    ) -> tuple[bool, str]:
        if bar.trade_date >= position.expiration_date:
            return True, "expiration"

        if (
            capital_at_risk is not None
            and current_value is not None
            and capital_at_risk > 0
        ):
            unrealised_pnl = current_value - capital_at_risk
            pnl_pct = (unrealised_pnl / capital_at_risk) * 100.0
            if profit_target_pct is not None and pnl_pct >= profit_target_pct:
                return True, "profit_target"
            if stop_loss_pct is not None and pnl_pct <= -stop_loss_pct:
                return True, "stop_loss"
        # Count trading days (bars) instead of calendar days to avoid
        # premature exits over weekends and holidays.
        if current_bar_index is not None:
            trading_days_held = current_bar_index - position.entry_index
            if trading_days_held >= max_holding_days:
                return True, "max_holding_days"
        else:
            if (bar.trade_date - position.entry_date).days >= max_holding_days:
                return True, "max_holding_days"
        if bar.trade_date >= backtest_end_date and bar.trade_date == last_bar_date:
            return True, "backtest_end"
        return False, ""

    @staticmethod
    def _add_warning_once(
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
        code: str,
        message: str,
    ) -> None:
        if code in warning_codes:
            return
        warning_codes.add(code)
        warnings.append({"code": code, "message": message})
