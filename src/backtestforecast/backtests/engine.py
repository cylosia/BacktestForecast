from __future__ import annotations

import inspect
import math
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import structlog

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.strategies.wheel import WheelStrategyBacktestEngine
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    DEFAULT_CONTRACT_MULTIPLIER,
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OptionDataGateway,
    PositionSnapshot,
    TradeResult,
)
from backtestforecast.errors import AppValidationError, DataUnavailableError
from backtestforecast.market_data.types import DailyBar

logger = structlog.get_logger(__name__)

CONTRACT_MULTIPLIER = DEFAULT_CONTRACT_MULTIPLIER


def _leg_multiplier(leg: object) -> float:
    """Return the contract multiplier for a leg, defaulting to 100."""
    return getattr(leg, "contract_multiplier", CONTRACT_MULTIPLIER)


_D0 = Decimal("0")
_D100 = Decimal("100")
_D365 = Decimal("365")


_D_CACHE: dict[int | float, Decimal] = {
    0: _D0, 1: Decimal("1"), -1: Decimal("-1"), 100: _D100,
}

_D_CACHE_MAX = 4096


def _D(v: float | int) -> Decimal:
    """Convert a float/int to Decimal via string to avoid IEEE 754 artefacts.

    Results are cached (up to 4096 entries) because the same prices recur
    across bars during mark-to-market and commission calculations.
    """
    cached = _D_CACHE.get(v)
    if cached is not None:
        return cached
    result = Decimal(str(v))
    if len(_D_CACHE) < _D_CACHE_MAX:
        _D_CACHE[v] = result
    return result


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
        *,
        ex_dividend_dates: set[date] | None = None,
    ) -> BacktestExecutionResult:
        if config.strategy_type == "wheel_strategy":
            return self.wheel_engine.run(
                config=config, bars=bars, earnings_dates=earnings_dates, option_gateway=option_gateway
            )

        strategy = STRATEGY_REGISTRY.get(config.strategy_type)
        if strategy is None:
            raise AppValidationError(f"Unsupported strategy_type: {config.strategy_type}")

        # Decimal precision: Phase 1 & 2 complete.
        #
        # Phase 1: `cash` uses Decimal throughout.
        # Phase 2: `position_value`, `entry_cost`, `gross_pnl`, `net_pnl`,
        # commissions, slippage, and all P&L math use Decimal. TradeResult
        # and EquityPointResult financial fields are Decimal. Float inputs
        # from strategy legs/quotes are converted at the computation boundary
        # via _D(). build_summary converts to float at the statistics boundary.
        #
        # Remaining: OpenOptionLeg/OpenStockLeg price fields are still float
        # (they come from external API data). Conversion happens at the point
        # of financial computation, not at storage.

        if config.start_date > config.end_date:
            raise AppValidationError("start_date must not be after end_date")
        if config.account_size <= 0:
            raise AppValidationError("account_size must be positive")

        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        _pre_filter_len = len(sorted_bars)
        sorted_bars = [b for b in sorted_bars if b.close_price > 0]
        if not sorted_bars:
            return BacktestExecutionResult(
                summary=build_summary(
                    float(config.account_size),
                    float(config.account_size),
                    [],
                    [],
                    risk_free_rate=config.risk_free_rate,
                    risk_free_rate_curve=config.risk_free_rate_curve,
                ),
                trades=[], equity_curve=[]
            )
        ex_dividend_dates = set(ex_dividend_dates or ())
        if not ex_dividend_dates and hasattr(option_gateway, "get_ex_dividend_dates"):
            try:
                ex_dividend_dates = option_gateway.get_ex_dividend_dates(
                    sorted_bars[0].trade_date, sorted_bars[-1].trade_date,
                )
            except Exception:
                logger.warning("engine.ex_dividend_dates_unavailable", exc_info=True)

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        if len(sorted_bars) < _pre_filter_len:
            self._add_warning_once(
                warnings, warning_codes, "non_positive_close_filtered",
                f"{_pre_filter_len - len(sorted_bars)} bar(s) with non-positive close price were excluded.",
            )
        cash = Decimal(str(config.account_size))
        peak_equity = cash
        position: OpenMultiLegPosition | None = None
        realized_vol = self._estimate_realized_vol(sorted_bars)
        trades: list[TradeResult] = []
        equity_curve: list[EquityPointResult] = []
        evaluator = EntryRuleEvaluator(
            config=config, bars=sorted_bars, earnings_dates=earnings_dates, option_gateway=option_gateway
        )

        for index, bar in enumerate(sorted_bars):
            if bar.trade_date < config.start_date:
                continue

            position_value = _D0
            exit_prices: dict[str, float] = {}

            if position is not None:
                snapshot = self._mark_position(
                    position, bar, option_gateway, warnings, warning_codes, ex_dividend_dates,
                )
                position_value = snapshot.position_value
                exit_prices = {leg.ticker: leg.last_mid for leg in position.option_legs}
                for stock_leg in position.stock_legs:
                    exit_prices[stock_leg.symbol] = stock_leg.last_price

                entry_cost = self._entry_value_per_unit(position) * _D(position.quantity)
                if not position_value.is_finite():
                    logger.warning("engine.nan_position_value_exit_guard", bar_date=str(bar.trade_date))
                    position_value = entry_cost
                if not entry_cost.is_finite():
                    logger.warning("engine.nan_entry_cost", bar_date=str(bar.trade_date))
                    entry_cost = _D0
                    position_value = _D0
                if math.isnan(position.capital_required_per_unit):
                    logger.warning(
                        "engine.nan_capital_required_per_unit",
                        ticker=position.display_ticker,
                        bar_date=str(bar.trade_date),
                    )
                    self._add_warning_once(
                        warnings, warning_codes, "nan_capital_required",
                        "Skipped stop/profit check: capital_required_per_unit is NaN.",
                    )
                    capital_at_risk = 0.0
                else:
                    capital_at_risk = position.capital_required_per_unit * position.quantity
                should_exit, exit_reason = self._resolve_exit(
                    bar=bar,
                    position=position,
                    max_holding_days=config.max_holding_days,
                    backtest_end_date=config.end_date,
                    last_bar_date=sorted_bars[-1].trade_date,
                    position_value=float(position_value),
                    entry_cost=float(entry_cost),
                    capital_at_risk=capital_at_risk,
                    profit_target_pct=config.profit_target_pct,
                    stop_loss_pct=config.stop_loss_pct,
                    current_bar_index=index,
                )
                assignment_detail = snapshot.assignment_detail
                if snapshot.assignment_exit_reason is not None:
                    should_exit = True
                    exit_reason = snapshot.assignment_exit_reason
                if should_exit:
                    trade, cash_delta = self._close_position(
                        position, config, position_value, bar.trade_date, bar.close_price,
                        exit_prices, exit_reason, warnings, warning_codes,
                        current_bar_index=index,
                        assignment_detail=assignment_detail,
                        trade_warnings=snapshot.warnings,
                    )
                    cash += cash_delta
                    trades.append(trade)
                    position = None
                    position_value = _D0

            entry_allowed = False
            # Prevent re-entry on the same bar where a position was just closed
            # during *this* iteration. This avoids infinite open/close loops when
            # entry rules remain satisfied after a stop-loss or expiration exit.
            # Note: this blocks all same-day re-entry, which may suppress
            # legitimate signals on high-frequency strategies.
            just_closed_this_bar = (
                position is None
                and len(trades) > 0
                and trades[-1].exit_date == bar.trade_date
            )
            if just_closed_this_bar:
                self._add_warning_once(
                    warnings, warning_codes, "same_day_reentry_blocked",
                    "One or more entry signals were suppressed because a position was "
                    "closed on the same trading day. The engine does not re-enter on "
                    "the same bar to avoid infinite open/close loops.",
                )
            if position is None and not just_closed_this_bar and bar.trade_date <= config.end_date:
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
                    build_position_params = inspect.signature(strategy.build_position).parameters
                    if config.custom_legs is not None and "custom_legs" in build_position_params:
                        build_kwargs["custom_legs"] = list(config.custom_legs)
                    if realized_vol is not None and "realized_vol" in build_position_params:
                        build_kwargs["realized_vol"] = realized_vol
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
                        commission_per_unit = float(config.commission_per_contract) * contracts_per_unit
                        gross_notional_per_unit = (
                            sum(abs(leg.entry_mid * _leg_multiplier(leg)) * leg.quantity_per_unit for leg in candidate.option_legs)
                            + sum(abs(leg.entry_price * leg.share_quantity_per_unit) for leg in candidate.stock_legs)
                        )
                        quantity = self._resolve_position_size(
                            available_cash=cash,
                            account_size=float(config.account_size),
                            risk_per_trade_pct=float(config.risk_per_trade_pct),
                            capital_required_per_unit=candidate.capital_required_per_unit,
                            max_loss_per_unit=candidate.max_loss_per_unit,
                            entry_cost_per_unit=float(abs(ev_per_unit)),
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
                            slippage_cost_d = _D(gross_notional_per_unit) * _D(quantity) * (_D(config.slippage_pct) / _D100)
                            total_entry_cost = (ev_per_unit * _D(quantity)) + entry_commission + slippage_cost_d
                            if cash - total_entry_cost < 0:
                                self._add_warning_once(
                                    warnings, warning_codes, "negative_cash_rejected",
                                    "One or more entries were skipped because the total cost "
                                    "(including slippage) would have exceeded available cash.",
                                )
                            else:
                                cash -= total_entry_cost
                                position = candidate
                                if strategy.margin_warning_message and candidate.capital_required_per_unit > float(
                                    abs(ev_per_unit)
                                ):
                                    self._add_warning_once(
                                        warnings, warning_codes, "margin_reserved", strategy.margin_warning_message
                                    )

            if position is not None:
                position_value = self._current_position_value(position, bar.close_price)
                if not position_value.is_finite():
                    logger.warning("engine.nan_position_value", ticker=position.display_ticker, bar_date=str(bar.trade_date))
                    position_value = _D0

            equity = cash + position_value
            if equity < _D0:
                self._add_warning_once(
                    warnings, warning_codes, "negative_equity",
                    "Account equity went negative. This indicates a margin call scenario "
                    "where losses exceeded the account balance. Drawdown percentages above "
                    "100% may occur.",
                )
            peak_equity = max(peak_equity, equity)
            drawdown_pct = (peak_equity - equity) / peak_equity * _D100 if peak_equity > _D0 else _D0
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
            snapshot = self._mark_position(
                position, sorted_bars[-1], option_gateway, warnings, warning_codes, ex_dividend_dates,
            )
            final_position_value = snapshot.position_value
            if not final_position_value.is_finite():
                logger.warning(
                    "engine.nan_position_value_force_close_guard",
                    bar_date=str(sorted_bars[-1].trade_date),
                )
                final_position_value = self._entry_value_per_unit(position) * _D(position.quantity)
                if not final_position_value.is_finite():
                    final_position_value = _D0
            exit_prices_fc = {leg.ticker: leg.last_mid for leg in position.option_legs}
            for stock_leg in position.stock_legs:
                exit_prices_fc[stock_leg.symbol] = stock_leg.last_price
            trade, cash_delta = self._close_position(
                position, config, final_position_value, sorted_bars[-1].trade_date,
                sorted_bars[-1].close_price, exit_prices_fc, "data_exhausted",
                warnings, warning_codes,
                current_bar_index=len(sorted_bars) - 1,
                trade_warnings=snapshot.warnings,
            )
            cash += cash_delta
            trades.append(trade)
            position = None
            equity = cash
            peak_equity = max(peak_equity, equity)
            drawdown_pct = (peak_equity - equity) / peak_equity * _D100 if peak_equity > _D0 else _D0
            force_close_point = EquityPointResult(
                trade_date=sorted_bars[-1].trade_date,
                equity=equity,
                cash=cash,
                position_value=_D0,
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
            self._add_warning_once(
                warnings, warning_codes, "data_exhausted_pricing",
                "Position force-closed at last available bar price. Actual settlement price may differ significantly.",
            )

        ending_equity_f = float(equity_curve[-1].equity) if equity_curve else float(config.account_size)
        summary = build_summary(
            float(config.account_size),
            ending_equity_f,
            trades,
            equity_curve,
            risk_free_rate=config.risk_free_rate,
            risk_free_rate_curve=config.risk_free_rate_curve,
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
        ex_dividend_dates: set[date] | None = None,
    ) -> PositionSnapshot:
        """Re-price all legs at the current bar and return a value snapshot.

        IMPORTANT: This method intentionally mutates ``leg.last_mid`` and
        ``leg.last_price`` in-place.  The engine loop relies on these updated
        values for carry-forward pricing when a quote is missing on a
        subsequent day.  Do not replace with a copy-on-write pattern unless
        you also update the carry-forward logic in ``get_quote is None``
        branches above and in ``_close_position``.
        """
        option_value = _D0
        missing_quote_tickers: list[str] = []
        for leg in position.option_legs:
            current_mid = self._resolve_option_mid(
                leg, bar, option_gateway, missing_quote_tickers,
            )
            leg.last_mid = current_mid
            multiplier = _leg_multiplier(leg)
            option_value += _D(leg.side) * _D(leg.quantity_per_unit) * _D(current_mid) * _D(multiplier) * _D(position.quantity)

        if missing_quote_tickers:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_option_mark_quote",
                "One or more daily option marks were missing; the engine carried forward the previous mid-price.",
            )

        stock_value = _D0
        for leg in position.stock_legs:
            leg.last_price = bar.close_price
            stock_value += _D(leg.side) * _D(leg.share_quantity_per_unit) * _D(bar.close_price) * _D(position.quantity)

        assignment_exit_reason, assignment_detail = self._check_early_assignment(
            position=position,
            bar=bar,
            ex_dividend_dates=ex_dividend_dates or set(),
        )
        if assignment_detail is not None:
            assigned_leg = assignment_detail["assigned_leg"]
            exit_mid = assignment_detail["settlement_price"]
            for leg in position.option_legs:
                if leg.ticker == assigned_leg:
                    leg.last_mid = exit_mid
            option_value = self._assignment_position_value(position, assignment_detail)

        return PositionSnapshot(
            position_value=option_value + stock_value,
            position_missing_quote=bool(missing_quote_tickers),
            missing_quote_tickers=tuple(missing_quote_tickers),
            assignment_exit_reason=assignment_exit_reason,
            assignment_detail=assignment_detail,
            warnings=((assignment_detail["warning_message"],) if assignment_detail is not None else ()),
        )

    @classmethod
    def _assignment_position_value(
        cls,
        position: OpenMultiLegPosition,
        assignment_detail: dict[str, Any],
    ) -> Decimal:
        assigned_ticker = assignment_detail["assigned_leg"]
        settlement_price = _D(assignment_detail["settlement_price"])
        option_value = _D0
        for leg in position.option_legs:
            leg_mid = settlement_price if leg.ticker == assigned_ticker else _D(leg.last_mid)
            option_value += (
                _D(leg.side)
                * _D(leg.quantity_per_unit)
                * leg_mid
                * _D(_leg_multiplier(leg))
                * _D(position.quantity)
            )
        return option_value

    @classmethod
    def _check_early_assignment(
        cls,
        *,
        position: OpenMultiLegPosition,
        bar: DailyBar,
        ex_dividend_dates: set[date],
    ) -> tuple[str | None, dict[str, Any] | None]:
        next_day = bar.trade_date + timedelta(days=1)
        for leg in position.option_legs:
            if leg.side >= 0:
                continue
            intrinsic = float(cls._intrinsic_value(leg.contract_type, leg.strike_price, bar.close_price))
            if intrinsic <= 0:
                continue
            time_value = max(0.0, leg.last_mid - intrinsic)
            dte = max(0, (leg.expiration_date - bar.trade_date).days)
            moneyness = (bar.close_price / leg.strike_price) if leg.contract_type == "call" else (leg.strike_price / bar.close_price)

            if leg.contract_type == "call" and next_day in ex_dividend_dates and intrinsic >= 2.0 and time_value <= 0.25:
                return "early_assignment_call_ex_div", {
                    "assignment": True,
                    "assignment_trigger": "ex_dividend",
                    "assigned_leg": leg.ticker,
                    "contract_type": leg.contract_type,
                    "intrinsic_value": intrinsic,
                    "time_value": time_value,
                    "days_to_expiration": dte,
                    "next_ex_dividend_date": next_day.isoformat(),
                    "settlement_price": intrinsic,
                    "warning_message": (
                        f"Short call {leg.ticker} was treated as early-assigned on "
                        f"{bar.trade_date.isoformat()} before the {next_day.isoformat()} ex-dividend date."
                    ),
                }

            if leg.contract_type == "put" and dte <= 3 and moneyness >= 1.05 and time_value <= 0.1:
                return "early_assignment_put_deep_itm", {
                    "assignment": True,
                    "assignment_trigger": "deep_itm_put",
                    "assigned_leg": leg.ticker,
                    "contract_type": leg.contract_type,
                    "intrinsic_value": intrinsic,
                    "time_value": time_value,
                    "days_to_expiration": dte,
                    "moneyness_ratio": moneyness,
                    "settlement_price": intrinsic,
                    "warning_message": (
                        f"Short put {leg.ticker} was treated as early-assigned on "
                        f"{bar.trade_date.isoformat()} due to deep ITM intrinsic value near expiry."
                    ),
                }

        return None, None

    def _resolve_option_mid(
        self,
        leg: Any,
        bar: DailyBar,
        option_gateway: OptionDataGateway,
        missing_quote_tickers: list[str],
    ) -> float:
        """Determine the current mid-price for a single option leg.

        Returns the best available price: live quote > intrinsic value (if
        expired) > carry-forward from last known mid.  Appends the ticker to
        *missing_quote_tickers* when a live quote was unavailable.
        """
        quote = option_gateway.get_quote(leg.ticker, bar.trade_date)
        if quote is None:
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        if quote.mid_price is None or not math.isfinite(quote.mid_price):
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        if quote.mid_price <= 0:
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        return quote.mid_price

    @staticmethod
    def _current_position_value(position: OpenMultiLegPosition, underlying_close: float) -> Decimal:
        option_value = sum(
            (_D(leg.side) * _D(leg.quantity_per_unit) * _D(leg.last_mid)
             * _D(_leg_multiplier(leg)) * _D(position.quantity))
            for leg in position.option_legs
        ) or _D0
        stock_value = sum(
            (_D(leg.side) * _D(leg.share_quantity_per_unit) * _D(underlying_close) * _D(position.quantity))
            for leg in position.stock_legs
        ) or _D0
        result = option_value + stock_value
        if not result.is_finite():
            return _D0
        return result

    @staticmethod
    def _resolve_position_size(
        available_cash: Decimal | float,
        account_size: float,
        risk_per_trade_pct: float,
        capital_required_per_unit: float,
        max_loss_per_unit: float | None,
        entry_cost_per_unit: float = 0.0,
        commission_per_unit: float = 0.0,
        slippage_pct: float = 0.0,
        gross_notional_per_unit: float = 0.0,
    ) -> int:
        d_cash = _D(available_cash) if not isinstance(available_cash, Decimal) else available_cash
        d_account = _D(account_size)
        d_risk_pct = _D(risk_per_trade_pct)
        d_cap_req = _D(capital_required_per_unit)
        if d_cap_req < _D0:
            return 0
        _MIN_CAPITAL_PER_UNIT = _D("50")
        if d_cap_req < _MIN_CAPITAL_PER_UNIT:
            d_cap_req = _MIN_CAPITAL_PER_UNIT
        risk_budget = d_account * (d_risk_pct / _D100)
        d_max_loss = _D(max_loss_per_unit) if max_loss_per_unit is not None and max_loss_per_unit > 0 else d_cap_req
        effective_risk = max(d_max_loss, _D("1"))
        by_risk = int(risk_budget // effective_risk)
        d_gross_notional = _D(gross_notional_per_unit)
        d_entry_cost = _D(entry_cost_per_unit)
        d_slip_pct = _D(slippage_pct) / _D100
        slippage_base = d_gross_notional if d_gross_notional > _D0 else d_entry_cost
        slippage_per_unit = slippage_base * d_slip_pct
        d_commission = _D(commission_per_unit)
        cash_per_unit = max(d_cap_req, d_entry_cost) + d_commission + slippage_per_unit
        if cash_per_unit <= _D0:
            return 0
        by_cash = int(d_cash // cash_per_unit)
        return max(0, min(by_risk, by_cash))

    def _close_position(
        self,
        position: OpenMultiLegPosition,
        config: BacktestConfig,
        exit_value: Decimal | float | int,
        exit_date: date,
        exit_underlying_close: float,
        exit_prices: dict[str, float],
        exit_reason: str,
        warnings: list[dict[str, Any]] | None = None,
        warning_codes: set[str] | None = None,
        current_bar_index: int | None = None,
        assignment_detail: dict[str, Any] | None = None,
        trade_warnings: tuple[str, ...] = (),
    ) -> tuple[TradeResult, Decimal]:
        """Close a position and return the TradeResult and cash change (exit proceeds minus exit commission)."""
        exit_value_d = exit_value if isinstance(exit_value, Decimal) else _D(exit_value)
        exit_commission = self._option_commission_total(position, config.commission_per_contract)
        option_exit_notional: Decimal = sum(
            (abs(_D(leg.last_mid) * _D(_leg_multiplier(leg)))
             * _D(leg.quantity_per_unit))
            for leg in position.option_legs
        ) or _D0
        option_exit_notional *= _D(position.quantity)
        stock_exit_notional: Decimal = (
            sum(abs(_D(leg.last_price) * _D(leg.share_quantity_per_unit)) for leg in position.stock_legs) * _D(position.quantity)
            if position.stock_legs else _D0
        )
        exit_gross_notional = option_exit_notional + stock_exit_notional
        slippage_pct_d = _D(config.slippage_pct) / _D100
        exit_slippage = exit_gross_notional * slippage_pct_d
        entry_value_per_unit = self._entry_value_per_unit(position)
        option_entry_notional: Decimal = sum(
            (abs(_D(leg.entry_mid) * _D(_leg_multiplier(leg)))
             * _D(leg.quantity_per_unit))
            for leg in position.option_legs
        ) or _D0
        option_entry_notional *= _D(position.quantity)
        stock_entry_notional: Decimal = (
            sum(abs(_D(leg.entry_price) * _D(leg.share_quantity_per_unit)) for leg in position.stock_legs) * _D(position.quantity)
            if position.stock_legs else _D0
        )
        entry_gross_notional = option_entry_notional + stock_entry_notional
        entry_slippage = entry_gross_notional * slippage_pct_d
        cash_delta = exit_value_d - exit_commission - exit_slippage
        exit_value_per_unit = exit_value_d / _D(position.quantity) if position.quantity else _D0
        gross_pnl = (exit_value_per_unit - entry_value_per_unit) * _D(position.quantity)
        dividends_received = self._estimate_dividends_received(position, config, exit_date)
        if dividends_received:
            gross_pnl += dividends_received
            cash_delta += dividends_received
        entry_commission_total = (
            position.entry_commission_total
            if isinstance(position.entry_commission_total, Decimal)
            else _D(position.entry_commission_total)
        )
        total_commissions = entry_commission_total + exit_commission
        total_slippage = entry_slippage + exit_slippage
        net_pnl = gross_pnl - total_commissions - total_slippage

        nan_fields: list[str] = []
        for name, val in [("gross_pnl", gross_pnl), ("net_pnl", net_pnl), ("exit_value_per_unit", exit_value_per_unit)]:
            if not val.is_finite():
                logger.warning("engine.non_finite_trade_value", field=name, value=str(val), ticker=position.display_ticker)
                nan_fields.append(name)
        if not gross_pnl.is_finite():
            gross_pnl = _D0
        if not net_pnl.is_finite():
            net_pnl = _D0
        if not exit_value_per_unit.is_finite():
            exit_value_per_unit = _D0
        if not total_commissions.is_finite():
            nan_fields.append("total_commissions")
            total_commissions = _D0
        if not total_slippage.is_finite():
            nan_fields.append("total_slippage")
            total_slippage = _D0
        if not cash_delta.is_finite():
            nan_fields.append("cash_delta")
            cash_delta = _D0
        if nan_fields and warnings is not None and warning_codes is not None:
            self._add_warning_once(
                warnings, warning_codes, "nan_in_trade_values",
                f"One or more trade values ({', '.join(nan_fields)}) were NaN/Inf and "
                f"were zeroed. This may indicate poor data quality for {position.display_ticker}.",
            )
        expiration_date = position.scheduled_exit_date or (
            max(leg.expiration_date for leg in position.option_legs)
            if position.option_legs
            else exit_date
        )
        trading_days_held = (
            (current_bar_index - position.entry_index) if current_bar_index is not None else None
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
            holding_period_trading_days=trading_days_held,
            entry_underlying_close=_D(self._entry_underlying_close(position)),
            exit_underlying_close=_D(exit_underlying_close),
            entry_mid=entry_value_per_unit / _D100,
            exit_mid=exit_value_per_unit / _D100,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            total_commissions=total_commissions,
            entry_reason=position.entry_reason,
            exit_reason=exit_reason,
            detail_json={
                **self._build_trade_detail_json(position, exit_prices, float(exit_value_per_unit), assignment_detail),
                "unit_convention": "per_unit_divided_by_100",
                "total_slippage": float(total_slippage),
                "entry_slippage": float(entry_slippage),
                "exit_slippage": float(exit_slippage),
                "dividends_received": float(dividends_received),
            },
            warnings=trade_warnings,
        )
        if assignment_detail is not None and warnings is not None and warning_codes is not None:
            self._add_warning_once(
                warnings,
                warning_codes,
                assignment_detail["assignment_trigger"],
                assignment_detail["warning_message"],
            )
        return trade, cash_delta

    def _estimate_dividends_received(
        self,
        position: OpenMultiLegPosition,
        config: BacktestConfig,
        exit_date: date,
    ) -> Decimal:
        """Approximate dividends for stock legs using annualized dividend_yield.

        This is a pragmatic correction for stock-holding strategies until
        per-ex-dividend cash amounts are available from market-data sources.
        """
        if not position.stock_legs or config.dividend_yield <= 0:
            return _D0
        holding_days = max((exit_date - position.entry_date).days, 0)
        if holding_days == 0:
            return _D0
        annual_yield = _D(config.dividend_yield)
        proration = _D(holding_days) / _D365
        dividends = _D0
        for leg in position.stock_legs:
            notional = _D(leg.entry_price) * _D(leg.share_quantity_per_unit) * _D(position.quantity)
            dividends += notional * annual_yield * proration * _D(leg.side)
        return dividends

    @staticmethod
    def _entry_value_per_unit(position: OpenMultiLegPosition) -> Decimal:
        option_value = sum(
            (_D(leg.side) * _D(leg.quantity_per_unit) * _D(leg.entry_mid)
             * _D(_leg_multiplier(leg)))
            for leg in position.option_legs
        ) or _D0
        stock_value = sum(
            (_D(leg.side) * _D(leg.share_quantity_per_unit) * _D(leg.entry_price))
            for leg in position.stock_legs
        ) or _D0
        return option_value + stock_value

    @staticmethod
    def _entry_underlying_close(position: OpenMultiLegPosition) -> float:
        if position.stock_legs:
            return position.stock_legs[0].entry_price
        value = position.detail_json.get("entry_underlying_close")
        if value is not None and value != 0.0:
            return float(value)
        if position.option_legs:
            strikes = [leg.strike_price for leg in position.option_legs if leg.strike_price > 0]
            if strikes:
                fallback = sum(strikes) / len(strikes)
                logger.warning(
                    "engine.missing_entry_underlying_close",
                    ticker=position.display_ticker,
                    fallback=fallback,
                )
                return fallback
        return 0.0

    @staticmethod
    def _option_commission_total(
        position: OpenMultiLegPosition,
        commission_per_contract: Decimal | float | int,
    ) -> Decimal:
        contracts_per_unit = sum(leg.quantity_per_unit for leg in position.option_legs)
        commission = (
            commission_per_contract
            if isinstance(commission_per_contract, Decimal)
            else _D(commission_per_contract)
        )
        return commission * _D(contracts_per_unit) * _D(position.quantity)

    @staticmethod
    def _build_trade_detail_json(
        position: OpenMultiLegPosition,
        exit_prices: dict[str, float],
        exit_value_per_unit: float,
        assignment_detail: dict[str, Any] | None = None,
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

        cap_req = position.capital_required_per_unit * position.quantity
        detail["capital_required_total"] = None if (isinstance(cap_req, float) and not math.isfinite(cap_req)) else cap_req

        if position.max_loss_per_unit is None:
            detail["max_loss_total"] = None
        else:
            ml = position.max_loss_per_unit * position.quantity
            detail["max_loss_total"] = None if (isinstance(ml, float) and not math.isfinite(ml)) else ml

        if position.max_profit_per_unit is None:
            detail["max_profit_total"] = None
        else:
            mp = position.max_profit_per_unit * position.quantity
            detail["max_profit_total"] = None if (isinstance(mp, float) and not math.isfinite(mp)) else mp
        detail["exit_package_market_value"] = exit_value_per_unit
        if assignment_detail is not None:
            detail["assignment"] = True
            detail["assignment_detail"] = {
                key: value for key, value in assignment_detail.items() if key != "warning_message"
            }
        return detail

    @staticmethod
    def _resolve_exit(
        bar: DailyBar,
        position: OpenMultiLegPosition,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
        *,
        position_value: float = 0.0,
        entry_cost: float = 0.0,
        capital_at_risk: float = 0.0,
        profit_target_pct: float | None = None,
        stop_loss_pct: float | None = None,
        current_bar_index: int | None = None,
    ) -> tuple[bool, str]:
        if max_holding_days < 1:
            logger.warning("engine.max_holding_days_clamped", original=max_holding_days)
            max_holding_days = 1
        # NOTE: The wheel strategy has its own exit logic (assignment-based
        # rolling) that does not go through this method. See
        # backtests/strategies/wheel.py for that path.
        if position.option_legs:
            exit_date = position.scheduled_exit_date or max(leg.expiration_date for leg in position.option_legs)
        else:
            exit_date = position.scheduled_exit_date or (position.entry_date + timedelta(days=max_holding_days))
        if bar.trade_date >= exit_date:
            return True, "expiration"

        if capital_at_risk > 0 and (stop_loss_pct is not None or profit_target_pct is not None):
            unrealized_pnl = position_value - entry_cost
            unrealized_pnl_pct = (unrealized_pnl / capital_at_risk) * 100.0

            if stop_loss_pct is not None and unrealized_pnl_pct <= -stop_loss_pct:
                return True, "stop_loss"
            if profit_target_pct is not None and unrealized_pnl_pct >= profit_target_pct:
                return True, "profit_target"

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
    def _intrinsic_value(contract_type: str, strike_price: float, underlying_close: float) -> Decimal:
        if contract_type == "call":
            return max(_D0, _D(underlying_close) - _D(strike_price))
        return max(_D0, _D(strike_price) - _D(underlying_close))

    @staticmethod
    def _estimate_realized_vol(bars: list, lookback: int = 60) -> float | None:
        """Estimate annualized realized volatility from recent close prices."""
        closes = [b.close_price for b in bars[-lookback:] if b.close_price > 0]
        if len(closes) < 20:
            return None
        log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        if len(log_returns) < 10:
            return None
        mean_r = sum(log_returns) / len(log_returns)
        variance = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
        return math.sqrt(variance * 252)

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
