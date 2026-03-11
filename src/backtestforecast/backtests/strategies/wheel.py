from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.strategies.common import (
    choose_call_otm_strike,
    choose_primary_expiration,
    choose_put_otm_strike,
    contracts_for_expiration,
    require_contract_for_strike,
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
        earnings_dates: set,
        option_gateway: OptionDataGateway,
    ) -> BacktestExecutionResult:
        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        if not sorted_bars:
            return BacktestExecutionResult(
                summary=build_summary(config.account_size, config.account_size, [], []), trades=[], equity_curve=[]
            )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        cash = config.account_size
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
                    current_mid = quote.mid_price
                active_option.last_mid = current_mid
                option_value = -current_mid * 100.0 * active_option.quantity

                should_exit, exit_reason = self._resolve_exit(
                    bar_index=index,
                    bar=bar,
                    position=active_option,
                    max_holding_days=config.max_holding_days,
                    backtest_end_date=config.end_date,
                    last_bar_date=sorted_bars[-1].trade_date,
                )
                if should_exit:
                    exit_mid = current_mid
                    exit_commission = config.commission_per_contract * active_option.quantity
                    option_gross_pnl = (active_option.entry_mid - exit_mid) * 100.0 * active_option.quantity
                    option_net_pnl = option_gross_pnl - (
                        (config.commission_per_contract * active_option.quantity) + exit_commission
                    )
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
                        cash -= (active_option.strike_price * 100.0 * active_option.quantity) + exit_commission
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
                                entry_underlying_close=sorted_bars[active_option.entry_index].close_price,
                                exit_underlying_close=bar.close_price,
                                entry_mid=active_option.entry_mid,
                                exit_mid=exit_mid,
                                gross_pnl=option_gross_pnl,
                                net_pnl=option_net_pnl,
                                total_commissions=(config.commission_per_contract * active_option.quantity)
                                + exit_commission,
                                entry_reason="entry_rules_met",
                                exit_reason="assignment",
                                detail_json={**option_detail, "assignment": True},
                            )
                        )
                    elif (
                        active_option.phase == "covered_call"
                        and exit_reason == "expiration"
                        and bar.close_price > active_option.strike_price
                        and held_shares is not None
                    ):
                        cash += (active_option.strike_price * 100.0 * active_option.quantity) - exit_commission
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
                                entry_underlying_close=sorted_bars[active_option.entry_index].close_price,
                                exit_underlying_close=bar.close_price,
                                entry_mid=active_option.entry_mid,
                                exit_mid=exit_mid,
                                gross_pnl=option_gross_pnl,
                                net_pnl=option_net_pnl,
                                total_commissions=(config.commission_per_contract * active_option.quantity)
                                + exit_commission,
                                entry_reason="entry_rules_met",
                                exit_reason="call_assignment",
                                detail_json={**option_detail, "assignment": True},
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
                                entry_underlying_close=held_shares.entry_price,
                                exit_underlying_close=called_away_price,
                                entry_mid=held_shares.entry_price,
                                exit_mid=called_away_price,
                                gross_pnl=stock_gross,
                                net_pnl=stock_gross,
                                total_commissions=0.0,
                                entry_reason="put_assignment",
                                exit_reason="called_away",
                                detail_json={
                                    "phase": "stock_inventory",
                                    "share_quantity": held_shares.quantity * 100,
                                    "assumptions": [
                                        "Share P&L is realized separately from short-call"
                                        " premium in the wheel strategy."
                                    ],
                                },
                            )
                        )
                        held_shares = None
                    else:
                        cash += (-exit_mid * 100.0 * active_option.quantity) - exit_commission
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
                                entry_underlying_close=sorted_bars[active_option.entry_index].close_price,
                                exit_underlying_close=bar.close_price,
                                entry_mid=active_option.entry_mid,
                                exit_mid=exit_mid,
                                gross_pnl=option_gross_pnl,
                                net_pnl=option_net_pnl,
                                total_commissions=(config.commission_per_contract * active_option.quantity)
                                + exit_commission,
                                entry_reason="entry_rules_met",
                                exit_reason=exit_reason,
                                detail_json={**option_detail, "assignment": False},
                            )
                        )
                    active_option = None
                    option_value = 0.0

            shares_value = 0.0 if held_shares is None else bar.close_price * 100.0 * held_shares.quantity

            if active_option is None and bar.trade_date <= config.end_date and evaluator.is_entry_allowed(index):
                if held_shares is None:
                    position = self._open_short_put(config, bar, index, option_gateway, cash, warnings, warning_codes)
                    if position is not None:
                        active_option = position
                        cash += (position.entry_mid * 100.0 * position.quantity) - (
                            config.commission_per_contract * position.quantity
                        )
                else:
                    position = self._open_covered_call(config, bar, index, option_gateway, held_shares.quantity)
                    if position is not None:
                        active_option = position
                        cash += (position.entry_mid * 100.0 * position.quantity) - (
                            config.commission_per_contract * position.quantity
                        )

            option_value = 0.0
            if active_option is not None:
                option_value = -active_option.last_mid * 100.0 * active_option.quantity
            shares_value = 0.0 if held_shares is None else bar.close_price * 100.0 * held_shares.quantity

            equity = cash + shares_value + option_value
            peak_equity = max(peak_equity, equity)
            drawdown_pct = 0.0 if peak_equity == 0 else ((peak_equity - equity) / peak_equity) * 100.0
            equity_curve.append(
                EquityPointResult(
                    trade_date=bar.trade_date,
                    equity=equity,
                    cash=cash,
                    position_value=shares_value + option_value,
                    drawdown_pct=drawdown_pct,
                )
            )

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
                    entry_underlying_close=held_shares.entry_price,
                    exit_underlying_close=final_bar.close_price,
                    entry_mid=held_shares.entry_price,
                    exit_mid=final_bar.close_price,
                    gross_pnl=stock_gross,
                    net_pnl=stock_gross,
                    total_commissions=0.0,
                    entry_reason="put_assignment",
                    exit_reason="backtest_end_share_liquidation",
                    detail_json={
                        "phase": "stock_inventory",
                        "share_quantity": held_shares.quantity * 100,
                        "assumptions": ["Remaining wheel share inventory is liquidated on the final available bar."],
                    },
                )
            )

        ending_equity = cash if not equity_curve else (cash if active_option is None else equity_curve[-1].equity)
        summary = build_summary(
            starting_equity=config.account_size,
            ending_equity=ending_equity,
            trades=trades,
            equity_curve=equity_curve,
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
            strike = choose_put_otm_strike([contract.strike_price for contract in put_contracts], bar.close_price)
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

        capital_required_per_unit = contract.strike_price * 100.0
        max_loss_per_unit = max((contract.strike_price - quote.mid_price) * 100.0, 0.0)
        risk_budget = config.account_size * (config.risk_per_trade_pct / 100.0)
        by_risk = int(risk_budget // max_loss_per_unit) if max_loss_per_unit > 0 else 0
        by_cash = int(cash // capital_required_per_unit) if capital_required_per_unit > 0 else 0
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
    ) -> OpenShortOptionPhase | None:
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        expiration = choose_primary_expiration(calls, bar.trade_date, config.target_dte)
        call_contracts = contracts_for_expiration(calls, expiration)
        strike = choose_call_otm_strike([contract.strike_price for contract in call_contracts], bar.close_price)
        contract = require_contract_for_strike(call_contracts, strike)
        quote = option_gateway.get_quote(contract.ticker, bar.trade_date)
        if quote is None:
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
        bar_index: int,
        bar: DailyBar,
        position: OpenShortOptionPhase,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
    ) -> tuple[bool, str]:
        if bar.trade_date >= position.expiration_date:
            return True, "expiration"
        if (bar.trade_date - position.entry_date).days >= max_holding_days:
            return True, "max_holding_days"
        if bar.trade_date > backtest_end_date and bar.trade_date == last_bar_date:
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
