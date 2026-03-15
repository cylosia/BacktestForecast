from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import structlog

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.strategies.wheel import WheelStrategyBacktestEngine
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OptionDataGateway,
    PositionSnapshot,
    TradeResult,
)
from backtestforecast.errors import DataUnavailableError, ValidationError
from backtestforecast.market_data.types import DailyBar

logger = structlog.get_logger(__name__)


class OptionsBacktestEngine:
    def __init__(self) -> None:
        self._wheel_engine: WheelStrategyBacktestEngine | None = None

    @property
    def wheel_engine(self) -> WheelStrategyBacktestEngine:
        if self._wheel_engine is None:
            self._wheel_engine = WheelStrategyBacktestEngine()
        return self._wheel_engine

    def run(
        self,
        config: BacktestConfig,
        bars: list[DailyBar],
        earnings_dates: set[date],
        option_gateway: OptionDataGateway,
    ) -> BacktestExecutionResult:
        if config.strategy_type == "wheel_strategy":
            return self.wheel_engine.run(
                config=config, bars=bars, earnings_dates=earnings_dates, option_gateway=option_gateway
            )

        strategy = STRATEGY_REGISTRY.get(config.strategy_type)
        if strategy is None:
            raise ValidationError(f"Unsupported strategy_type: {config.strategy_type}")

        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        if not sorted_bars:
            return BacktestExecutionResult(
                summary=build_summary(config.account_size, config.account_size, [], [], risk_free_rate=config.risk_free_rate),
                trades=[], equity_curve=[]
            )

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        cash = config.account_size
        peak_equity = cash
        position: OpenMultiLegPosition | None = None
        trades: list[TradeResult] = []
        equity_curve: list[EquityPointResult] = []
        evaluator = EntryRuleEvaluator(
            config=config, bars=sorted_bars, earnings_dates=earnings_dates, option_gateway=option_gateway
        )

        for index, bar in enumerate(sorted_bars):
            if bar.trade_date < config.start_date:
                continue

            position_value = 0.0
            exit_prices: dict[str, float] = {}

            if position is not None:
                snapshot = self._mark_position(position, bar, option_gateway, warnings, warning_codes)
                position_value = snapshot.position_value
                exit_prices = {leg.ticker: leg.last_mid for leg in position.option_legs}
                for stock_leg in position.stock_legs:
                    exit_prices[stock_leg.symbol] = stock_leg.last_price

                should_exit, exit_reason = self._resolve_exit(
                    bar=bar,
                    position=position,
                    max_holding_days=config.max_holding_days,
                    backtest_end_date=config.end_date,
                    last_bar_date=sorted_bars[-1].trade_date,
                )
                if should_exit:
                    trade, cash_delta = self._close_position(
                        position, config, snapshot.position_value, bar.trade_date, bar.close_price,
                        exit_prices, exit_reason,
                    )
                    cash += cash_delta
                    trades.append(trade)
                    position = None
                    position_value = 0.0

            entry_allowed = False
            if position is None and bar.trade_date <= config.end_date:
                try:
                    entry_allowed = evaluator.is_entry_allowed(index)
                except Exception:
                    logger.warning("entry_rule_evaluation_error", bar_index=index, exc_info=True)
                    self._add_warning_once(
                        warnings, warning_codes, "entry_rule_evaluation_error",
                        "One or more entry rule evaluations failed and were treated as not-allowed.",
                    )
            if entry_allowed:
                try:
                    build_kwargs: dict = {}
                    if config.custom_legs is not None:
                        build_kwargs["custom_legs"] = list(config.custom_legs)
                    candidate = strategy.build_position(
                        config,
                        bar,
                        index,
                        option_gateway,
                        **build_kwargs,
                    )
                except DataUnavailableError:
                    self._add_warning_once(
                        warnings,
                        warning_codes,
                        "missing_contract_chain",
                        "One or more entry dates could not be evaluated because no eligible"
                        " option contract chain was returned.",
                    )
                else:
                    if candidate is None:
                        self._add_warning_once(
                            warnings,
                            warning_codes,
                            "missing_entry_quote",
                            "One or more entry dates were skipped because no valid same-day option quote was returned.",
                        )
                    else:
                        ev_per_unit = self._entry_value_per_unit(candidate)
                        contracts_per_unit = sum(leg.quantity_per_unit for leg in candidate.option_legs)
                        commission_per_unit = config.commission_per_contract * contracts_per_unit
                        gross_notional_per_unit = (
                            sum(abs(leg.entry_mid * 100.0) * leg.quantity_per_unit for leg in candidate.option_legs)
                            + sum(abs(leg.entry_price * leg.share_quantity_per_unit) for leg in candidate.stock_legs)
                        )
                        quantity = self._resolve_position_size(
                            available_cash=cash,
                            account_size=config.account_size,
                            risk_per_trade_pct=config.risk_per_trade_pct,
                            capital_required_per_unit=candidate.capital_required_per_unit,
                            max_loss_per_unit=candidate.max_loss_per_unit,
                            entry_cost_per_unit=abs(ev_per_unit),
                            commission_per_unit=commission_per_unit,
                            slippage_pct=config.slippage_pct,
                            gross_notional_per_unit=gross_notional_per_unit,
                        )
                        if quantity <= 0:
                            self._add_warning_once(
                                warnings,
                                warning_codes,
                                "capital_requirement_exceeded",
                                "One or more signals were skipped because available cash or"
                                " configured risk budget could not support the strategy package.",
                            )
                        else:
                            candidate.quantity = quantity
                            candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
                            entry_commission = self._option_commission_total(candidate, config.commission_per_contract)
                            candidate.entry_commission_total = entry_commission
                            slippage_cost = gross_notional_per_unit * quantity * (config.slippage_pct / 100.0)
                            cash -= (ev_per_unit * quantity) + entry_commission + slippage_cost
                            position = candidate
                            if strategy.margin_warning_message and candidate.capital_required_per_unit > abs(
                                ev_per_unit
                            ):
                                self._add_warning_once(
                                    warnings, warning_codes, "margin_reserved", strategy.margin_warning_message
                                )

            if position is not None:
                position_value = self._current_position_value(position, bar.close_price)
                if not math.isfinite(position_value):
                    logger.warning("engine.nan_position_value", ticker=position.display_ticker, bar_date=str(bar.trade_date))
                    position_value = 0.0

            equity = cash + position_value
            peak_equity = max(peak_equity, equity)
            drawdown_pct = 0.0 if peak_equity == 0 else ((peak_equity - equity) / peak_equity) * 100.0
            equity_curve.append(
                EquityPointResult(
                    trade_date=bar.trade_date,
                    equity=equity,
                    cash=cash,
                    position_value=position_value,
                    drawdown_pct=drawdown_pct,
                )
            )

            if position is None and bar.trade_date > config.end_date:
                break

        if position is not None:
            snapshot = self._mark_position(position, sorted_bars[-1], option_gateway, warnings, warning_codes)
            exit_prices_fc = {leg.ticker: leg.last_mid for leg in position.option_legs}
            for stock_leg in position.stock_legs:
                exit_prices_fc[stock_leg.symbol] = stock_leg.last_price
            trade, cash_delta = self._close_position(
                position, config, snapshot.position_value, sorted_bars[-1].trade_date,
                sorted_bars[-1].close_price, exit_prices_fc, "data_exhausted",
            )
            cash += cash_delta
            trades.append(trade)
            position = None
            equity = cash
            peak_equity = max(peak_equity, equity)
            drawdown_pct = 0.0 if peak_equity == 0 else ((peak_equity - equity) / peak_equity) * 100.0
            force_close_point = EquityPointResult(
                trade_date=sorted_bars[-1].trade_date,
                equity=equity,
                cash=cash,
                position_value=0.0,
                drawdown_pct=drawdown_pct,
            )
            if equity_curve and equity_curve[-1].trade_date == sorted_bars[-1].trade_date:
                equity_curve[-1] = force_close_point
            else:
                equity_curve.append(force_close_point)
            self._add_warning_once(
                warnings, warning_codes, "position_force_closed",
                "An open position was force-closed because no more market data was available.",
            )

        ending_equity = equity_curve[-1].equity if equity_curve else config.account_size
        summary = build_summary(
            config.account_size, ending_equity, trades, equity_curve, risk_free_rate=config.risk_free_rate,
            warnings=warnings,
        )
        return BacktestExecutionResult(summary=summary, trades=trades, equity_curve=equity_curve, warnings=warnings)

    def _mark_position(
        self,
        position: OpenMultiLegPosition,
        bar: DailyBar,
        option_gateway: OptionDataGateway,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
    ) -> PositionSnapshot:
        option_value = 0.0
        missing_quote_tickers: list[str] = []
        for leg in position.option_legs:
            quote = option_gateway.get_quote(leg.ticker, bar.trade_date)
            if quote is None:
                if bar.trade_date >= leg.expiration_date:
                    current_mid = self._intrinsic_value(leg.contract_type, leg.strike_price, bar.close_price)
                else:
                    current_mid = leg.last_mid
                    missing_quote_tickers.append(leg.ticker)
            else:
                current_mid = quote.mid_price
                if not math.isfinite(current_mid):
                    current_mid = leg.last_mid
                    missing_quote_tickers.append(leg.ticker)
            leg.last_mid = current_mid
            option_value += leg.side * leg.quantity_per_unit * current_mid * 100.0 * position.quantity

        if missing_quote_tickers:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_option_mark_quote",
                "One or more daily option marks were missing; the engine carried forward the previous mid-price.",
            )

        stock_value = 0.0
        for leg in position.stock_legs:
            leg.last_price = bar.close_price
            stock_value += leg.side * leg.share_quantity_per_unit * bar.close_price * position.quantity

        return PositionSnapshot(
            position_value=option_value + stock_value,
            position_missing_quote=bool(missing_quote_tickers),
            missing_quote_tickers=tuple(missing_quote_tickers),
        )

    @staticmethod
    def _current_position_value(position: OpenMultiLegPosition, underlying_close: float) -> float:
        option_value = sum(
            leg.side * leg.quantity_per_unit * leg.last_mid * 100.0 * position.quantity for leg in position.option_legs
        )
        stock_value = sum(
            leg.side * leg.share_quantity_per_unit * underlying_close * position.quantity for leg in position.stock_legs
        )
        return option_value + stock_value

    @staticmethod
    def _resolve_position_size(
        available_cash: float,
        account_size: float,
        risk_per_trade_pct: float,
        capital_required_per_unit: float,
        max_loss_per_unit: float | None,
        entry_cost_per_unit: float = 0.0,
        commission_per_unit: float = 0.0,
        slippage_pct: float = 0.0,
        gross_notional_per_unit: float = 0.0,
    ) -> int:
        if capital_required_per_unit <= 0:
            return 0
        risk_budget = account_size * (risk_per_trade_pct / 100.0)
        effective_risk = (
            max_loss_per_unit if max_loss_per_unit is not None and max_loss_per_unit > 0 else capital_required_per_unit
        )
        effective_risk = max(effective_risk, 1.0)
        by_risk = int(risk_budget // effective_risk)
        slippage_per_unit = (gross_notional_per_unit if gross_notional_per_unit > 0 else entry_cost_per_unit) * (slippage_pct / 100.0)
        cash_per_unit = max(capital_required_per_unit, entry_cost_per_unit) + commission_per_unit + slippage_per_unit
        if cash_per_unit <= 0:
            return 0
        by_cash = int(available_cash // cash_per_unit)
        return max(0, min(by_risk, by_cash))

    def _close_position(
        self,
        position: OpenMultiLegPosition,
        config: BacktestConfig,
        exit_value: float,
        exit_date: date,
        exit_underlying_close: float,
        exit_prices: dict[str, float],
        exit_reason: str,
    ) -> tuple[TradeResult, float]:
        """Close a position and return the TradeResult and cash change (exit proceeds minus exit commission)."""
        exit_commission = self._option_commission_total(position, config.commission_per_contract)
        option_exit_notional = sum(abs(leg.last_mid * 100.0) * leg.quantity_per_unit for leg in position.option_legs) * position.quantity
        stock_exit_notional = sum(abs(leg.last_price * leg.share_quantity_per_unit) for leg in position.stock_legs) * position.quantity if position.stock_legs else 0.0
        exit_gross_notional = option_exit_notional + stock_exit_notional
        exit_slippage = exit_gross_notional * (config.slippage_pct / 100.0)
        entry_value_per_unit = self._entry_value_per_unit(position)
        option_entry_notional = sum(abs(leg.entry_mid * 100.0) * leg.quantity_per_unit for leg in position.option_legs) * position.quantity
        stock_entry_notional = sum(abs(leg.entry_price * leg.share_quantity_per_unit) for leg in position.stock_legs) * position.quantity if position.stock_legs else 0.0
        entry_gross_notional = option_entry_notional + stock_entry_notional
        entry_slippage = entry_gross_notional * (config.slippage_pct / 100.0)
        cash_delta = exit_value - exit_commission - exit_slippage
        exit_value_per_unit = exit_value / position.quantity if position.quantity else 0.0
        gross_pnl = (exit_value_per_unit - entry_value_per_unit) * position.quantity
        total_commissions = position.entry_commission_total + exit_commission
        total_slippage = entry_slippage + exit_slippage
        net_pnl = gross_pnl - total_commissions - total_slippage
        expiration_date = position.scheduled_exit_date or (
            max(leg.expiration_date for leg in position.option_legs)
            if position.option_legs
            else exit_date
        )
        trade = TradeResult(
            option_ticker=position.display_ticker,
            strategy_type=config.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=position.entry_date,
            exit_date=exit_date,
            expiration_date=expiration_date,
            quantity=position.quantity,
            dte_at_open=position.dte_at_open,
            holding_period_days=(exit_date - position.entry_date).days,
            entry_underlying_close=self._entry_underlying_close(position),
            exit_underlying_close=exit_underlying_close,
            # entry_mid and exit_mid are the per-contract net cost (not a single-leg
            # price). They are derived by dividing the total per-unit package value
            # by 100 (the contract multiplier) so that downstream consumers see a
            # per-share equivalent suitable for display and reporting.
            entry_mid=entry_value_per_unit / 100.0,
            exit_mid=exit_value_per_unit / 100.0,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            total_commissions=total_commissions,
            entry_reason=position.entry_reason,
            exit_reason=exit_reason,
            detail_json={
                **self._build_trade_detail_json(position, exit_prices, exit_value_per_unit),
                "unit_convention": "per_unit_divided_by_100",
                "total_slippage": total_slippage,
                "entry_slippage": entry_slippage,
                "exit_slippage": exit_slippage,
            },
        )
        return trade, cash_delta

    @staticmethod
    def _entry_value_per_unit(position: OpenMultiLegPosition) -> float:
        option_value = sum(leg.side * leg.quantity_per_unit * leg.entry_mid * 100.0 for leg in position.option_legs)
        stock_value = sum(leg.side * leg.share_quantity_per_unit * leg.entry_price for leg in position.stock_legs)
        return option_value + stock_value

    @staticmethod
    def _entry_underlying_close(position: OpenMultiLegPosition) -> float:
        if position.stock_legs:
            return position.stock_legs[0].entry_price
        # `or 0.0` handles the case where the stored value is explicitly None.
        return position.detail_json.get("entry_underlying_close", 0.0) or 0.0

    @staticmethod
    def _option_commission_total(position: OpenMultiLegPosition, commission_per_contract: float) -> float:
        contracts_per_unit = sum(leg.quantity_per_unit for leg in position.option_legs)
        return commission_per_contract * contracts_per_unit * position.quantity

    @staticmethod
    def _build_trade_detail_json(
        position: OpenMultiLegPosition,
        exit_prices: dict[str, float],
        exit_value_per_unit: float,
    ) -> dict[str, Any]:
        detail = dict(position.detail_json)
        legs = [dict(leg) for leg in detail.get("legs", [])]
        for leg in legs:
            ticker = leg.get("ticker")
            if isinstance(ticker, str) and ticker in exit_prices:
                leg["exit_mid"] = exit_prices[ticker]
        for leg in legs:
            if leg.get("asset_type") == "stock":
                identifier = leg.get("identifier")
                if isinstance(identifier, str) and identifier in exit_prices:
                    leg["exit_price"] = exit_prices[identifier]
        detail["legs"] = legs
        detail["actual_units"] = position.quantity
        detail["capital_required_total"] = position.capital_required_per_unit * position.quantity
        detail["max_loss_total"] = (
            None if position.max_loss_per_unit is None else position.max_loss_per_unit * position.quantity
        )
        detail["max_profit_total"] = (
            None if position.max_profit_per_unit is None else position.max_profit_per_unit * position.quantity
        )
        detail["exit_package_market_value"] = exit_value_per_unit
        return detail

    @staticmethod
    def _resolve_exit(
        bar: DailyBar,
        position: OpenMultiLegPosition,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
    ) -> tuple[bool, str]:
        if position.option_legs:
            exit_date = position.scheduled_exit_date or max(leg.expiration_date for leg in position.option_legs)
        else:
            exit_date = position.scheduled_exit_date or (position.entry_date + timedelta(days=max_holding_days))
        if bar.trade_date >= exit_date:
            return True, "expiration"
        if (bar.trade_date - position.entry_date).days >= max_holding_days:
            return True, "max_holding_days"
        if bar.trade_date >= backtest_end_date and bar.trade_date == last_bar_date:
            return True, "backtest_end"
        return False, ""

    @staticmethod
    def _intrinsic_value(contract_type: str, strike_price: float, underlying_close: float) -> float:
        if contract_type == "call":
            return max(0.0, underlying_close - strike_price)
        return max(0.0, strike_price - underlying_close)

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
