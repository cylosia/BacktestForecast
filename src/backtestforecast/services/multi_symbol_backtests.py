from __future__ import annotations

import inspect
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import structlog
from sqlalchemy import desc, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestConfig, EquityPointResult, OpenMultiLegPosition, TradeResult
from backtestforecast.config import get_settings
from backtestforecast.errors import AppValidationError, DataUnavailableError, ExternalServiceError, NotFoundError
from backtestforecast.indicators.calculations import ema, rsi, sma
from backtestforecast.market_data.service import HistoricalDataBundle, MassiveOptionGateway
from backtestforecast.market_data.types import DailyBar
from backtestforecast.models import (
    MultiSymbolEquityPoint,
    MultiSymbolRun,
    MultiSymbolRunSymbol,
    MultiSymbolSymbolEquityPoint,
    MultiSymbolTrade,
    MultiSymbolTradeGroup,
    User,
)
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.schemas.backtests import BacktestSummaryResponse, EquityCurvePointResponse
from backtestforecast.schemas.multi_symbol_backtests import (
    CreateMultiSymbolRunRequest,
    MultiSymbolDefinition,
    MultiSymbolLegDefinition,
    MultiSymbolRunDetailResponse,
    MultiSymbolRunHistoryItemResponse,
    MultiSymbolRunListResponse,
    MultiSymbolRunStatusResponse,
    MultiSymbolRunSymbolSummaryResponse,
    MultiSymbolTradeGroupResponse,
    MultiSymbolTradeResponse,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.backtest_workflow_access import enforce_backtest_workflow_quota
from backtestforecast.services.dispatch_recovery import redispatch_if_stale_queued

logger = structlog.get_logger("services.multi_symbol_backtests")

_QUEUE = "multi_symbol_backtests"


def _zero_summary() -> BacktestSummaryResponse:
    zero = Decimal("0")
    return BacktestSummaryResponse(
        trade_count=0,
        decided_trades=0,
        win_rate=zero,
        total_roi_pct=zero,
        average_win_amount=zero,
        average_loss_amount=zero,
        average_holding_period_days=zero,
        average_dte_at_open=zero,
        max_drawdown_pct=zero,
        total_commissions=zero,
        total_net_pnl=zero,
        starting_equity=zero,
        ending_equity=zero,
        expectancy=zero,
    )


def _summary_from_run(run: MultiSymbolRun) -> BacktestSummaryResponse:
    return BacktestSummaryResponse(
        trade_count=run.trade_count,
        decided_trades=run.trade_count,
        win_rate=run.win_rate,
        total_roi_pct=run.total_roi_pct,
        average_win_amount=run.average_win_amount,
        average_loss_amount=run.average_loss_amount,
        average_holding_period_days=run.average_holding_period_days,
        average_dte_at_open=run.average_dte_at_open,
        max_drawdown_pct=run.max_drawdown_pct,
        total_commissions=run.total_commissions,
        total_net_pnl=run.total_net_pnl,
        starting_equity=run.starting_equity,
        ending_equity=run.ending_equity,
        profit_factor=run.profit_factor,
        payoff_ratio=run.payoff_ratio,
        expectancy=run.expectancy,
        sharpe_ratio=run.sharpe_ratio,
        sortino_ratio=run.sortino_ratio,
        cagr_pct=run.cagr_pct,
        calmar_ratio=run.calmar_ratio,
        max_consecutive_wins=run.max_consecutive_wins,
        max_consecutive_losses=run.max_consecutive_losses,
        recovery_factor=run.recovery_factor,
    )


def _to_warning(code: str, message: str, *, severity: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "message": message}
    if severity is not None:
        payload["severity"] = severity
    return payload


def _indicator_lookback(name: str, explicit_lookback: int | None) -> int:
    indicator = name.strip().lower()
    if explicit_lookback is not None:
        return explicit_lookback
    if indicator in {"close", "volume"}:
        return 1
    if "_" not in indicator:
        return 30
    _prefix, raw_period = indicator.split("_", 1)
    if raw_period.isdigit():
        return max(1, int(raw_period))
    return 30


def _merge_warnings(*warning_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for warning_set in warning_sets:
        for warning in warning_set:
            code = str(warning.get("code") or "")
            if code and code in seen:
                continue
            if code:
                seen.add(code)
            merged.append(warning)
    return merged


@dataclass(slots=True)
class _PreparedSymbolData:
    definition: MultiSymbolDefinition
    bars: list[DailyBar]
    bundle: HistoricalDataBundle
    bars_by_date: dict[date, DailyBar]
    bar_index_by_date: dict[date, int]
    starting_cash: Decimal


@dataclass(slots=True)
class _OpenGroupedPosition:
    group_id: UUID
    positions: dict[str, OpenMultiLegPosition]
    leg_configs: dict[str, BacktestConfig]
    group_name: str


class _CrossSymbolRuleEvaluator:
    def __init__(self, prepared: dict[str, _PreparedSymbolData]) -> None:
        self._prepared = prepared
        self._sma_cache: dict[tuple[str, int], list[float | None]] = {}
        self._ema_cache: dict[tuple[str, int], list[float | None]] = {}
        self._rsi_cache: dict[tuple[str, int], list[float | None]] = {}

    def evaluate(self, rules: list[Any], trade_date: date) -> bool:
        for rule in rules:
            left = self._value_for(rule.left_symbol, rule.left_indicator, trade_date)
            if left is None:
                return False
            if rule.right_symbol is not None:
                right = self._value_for(rule.right_symbol, rule.right_indicator or "", trade_date)
                if right is None:
                    return False
            elif rule.threshold is not None:
                right = float(rule.threshold)
            else:
                return False
            if not self._compare(left, right, rule.operator):
                return False
        return True

    def _value_for(self, symbol: str, indicator: str, trade_date: date) -> float | None:
        prepared = self._prepared[symbol]
        bar = prepared.bars_by_date.get(trade_date)
        index = prepared.bar_index_by_date.get(trade_date)
        if bar is None or index is None:
            return None
        normalized = indicator.strip().lower()
        if normalized == "close":
            return bar.close_price
        if normalized == "volume":
            return bar.volume
        if "_" not in normalized:
            return None
        prefix, raw_period = normalized.split("_", 1)
        if not raw_period.isdigit():
            return None
        period = int(raw_period)
        closes = [item.close_price for item in prepared.bars]
        if prefix == "sma":
            series = self._sma_cache.setdefault((symbol, period), sma(closes, period))
        elif prefix == "ema":
            series = self._ema_cache.setdefault((symbol, period), ema(closes, period))
        elif prefix == "rsi":
            series = self._rsi_cache.setdefault((symbol, period), rsi(closes, period))
        else:
            return None
        return series[index]

    @staticmethod
    def _compare(left: float, right: float, operator: str) -> bool:
        if operator == "lt":
            return left < right
        if operator == "lte":
            return left <= right
        if operator == "gt":
            return left > right
        if operator == "gte":
            return left >= right
        if operator == "eq":
            return abs(left - right) <= 1e-9
        if operator == "neq":
            return abs(left - right) > 1e-9
        return False


class MultiSymbolBacktestService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self._engine = OptionsBacktestEngine()

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> MultiSymbolBacktestService:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc:
            self.session.rollback()
        self.close()

    def create_and_dispatch(
        self,
        user: User,
        request: CreateMultiSymbolRunRequest,
        *,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> MultiSymbolRun:
        from apps.api.app.dispatch import dispatch_celery_task

        run = self.enqueue(user, request)
        dispatch_celery_task(
            db=self.session,
            job=run,
            task_name="multi_symbol_backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue=_QUEUE,
            log_event="multi_symbol_backtest",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(run)
        return run

    def enqueue(self, user: User, request: CreateMultiSymbolRunRequest) -> MultiSymbolRun:
        if request.idempotency_key:
            existing = self.session.scalar(
                select(MultiSymbolRun).where(
                    MultiSymbolRun.user_id == user.id,
                    MultiSymbolRun.idempotency_key == request.idempotency_key,
                )
            )
            if existing is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing,
                    model_name="MultiSymbolRun",
                    task_name="multi_symbol_backtests.run",
                    task_kwargs={"run_id": str(existing.id)},
                    queue=_QUEUE,
                    log_event="multi_symbol_backtest",
                    logger=logger,
                )

        enforce_backtest_workflow_quota(self.session, user)

        run = MultiSymbolRun(
            id=uuid4(),
            user_id=user.id,
            status="queued",
            name=request.name,
            start_date=request.start_date,
            end_date=request.end_date,
            account_size=request.account_size,
            capital_allocation_mode=request.capital_allocation_mode,
            commission_per_contract=request.commission_per_contract,
            slippage_pct=request.slippage_pct,
            input_snapshot_json=request.model_dump(mode="json"),
            warnings_json=[],
            idempotency_key=request.idempotency_key,
            starting_equity=request.account_size,
            ending_equity=request.account_size,
        )
        self.session.add(run)
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if request.idempotency_key:
                existing = self.session.scalar(
                    select(MultiSymbolRun).where(
                        MultiSymbolRun.user_id == user.id,
                        MultiSymbolRun.idempotency_key == request.idempotency_key,
                    )
                )
                if existing is not None:
                    return existing
            raise

        for symbol_def in request.symbols:
            allocated_cash = self._allocated_cash_for_symbol(request, symbol_def)
            self.session.add(
                MultiSymbolRunSymbol(
                    run_id=run.id,
                    symbol=symbol_def.symbol,
                    risk_per_trade_pct=symbol_def.risk_per_trade_pct,
                    max_open_positions=symbol_def.max_open_positions,
                    capital_allocation_pct=symbol_def.capital_allocation_pct,
                    starting_equity=allocated_cash,
                    ending_equity=allocated_cash,
                )
            )
        return run

    def execute_run_by_id(self, run_id: UUID) -> MultiSymbolRun:
        run = self.session.get(MultiSymbolRun, run_id)
        if run is None:
            raise NotFoundError("Multi-symbol backtest run not found.")
        if run.status not in ("queued", "running"):
            return run

        started_at = datetime.now(UTC)
        rows = self.session.execute(
            update(MultiSymbolRun)
            .where(MultiSymbolRun.id == run_id, MultiSymbolRun.status == "queued")
            .values(status="running", started_at=started_at, updated_at=started_at)
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(run)
            return run

        self.session.refresh(run)
        try:
            request = CreateMultiSymbolRunRequest.model_validate(run.input_snapshot_json or {})
            prepared = self._prepare_symbol_data(request)
            result = self._execute_request(request, prepared)
            self._persist_success(run, request, prepared, result)
        except ExternalServiceError:
            self.session.rollback()
            raise
        except Exception as exc:
            logger.exception("multi_symbol_backtest.execution_failed", run_id=str(run_id))
            run.status = "failed"
            run.error_code = "execution_failed"
            run.error_message = str(exc)
            run.completed_at = datetime.now(UTC)
            run.warnings_json = _merge_warnings(
                list(run.warnings_json or []),
                [_to_warning("multi_symbol_execution_failed", "Multi-symbol execution failed before completion.")],
            )
            self.session.commit()
        return run

    def list_runs(
        self,
        user: User,
        *,
        limit: int,
        offset: int,
        cursor: str | None,
    ) -> MultiSymbolRunListResponse:
        _parsed_cursor, parsed_offset = parse_cursor_param(cursor)
        effective_offset = parsed_offset if cursor else offset
        total = self.session.scalar(select(func.count()).select_from(MultiSymbolRun).where(MultiSymbolRun.user_id == user.id)) or 0
        runs = list(
            self.session.scalars(
                select(MultiSymbolRun)
                .where(MultiSymbolRun.user_id == user.id)
                .order_by(desc(MultiSymbolRun.created_at), desc(MultiSymbolRun.id))
                .offset(effective_offset)
                .limit(limit + 1)
            )
        )
        page = finalize_cursor_page(runs, total=total or 0, offset=effective_offset, limit=limit)
        items = [self._to_history_item(run) for run in page.items]
        return MultiSymbolRunListResponse(items=items, total=page.total, offset=page.offset, limit=page.limit, next_cursor=page.next_cursor)

    def get_run_for_owner(self, *, user_id: UUID, run_id: UUID) -> MultiSymbolRunDetailResponse:
        run = self.session.scalar(select(MultiSymbolRun).where(MultiSymbolRun.id == run_id, MultiSymbolRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-symbol backtest run not found.")
        return self._to_detail_response(run)

    def get_run_status_for_owner(self, *, user_id: UUID, run_id: UUID) -> MultiSymbolRunStatusResponse:
        run = self.session.scalar(select(MultiSymbolRun).where(MultiSymbolRun.id == run_id, MultiSymbolRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-symbol backtest run not found.")
        return MultiSymbolRunStatusResponse(
            id=run.id,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_code=run.error_code,
            error_message=run.error_message,
        )

    def _prepare_symbol_data(self, request: CreateMultiSymbolRunRequest) -> dict[str, _PreparedSymbolData]:
        market_data_service = self.execution_service.market_data_service
        max_lookback = max(
            (_indicator_lookback(rule.left_indicator, rule.lookback_period) for rule in [*request.entry_rules, *request.exit_rules]),
            default=30,
        )
        for rule in [*request.entry_rules, *request.exit_rules]:
            if rule.right_indicator is not None:
                max_lookback = max(max_lookback, _indicator_lookback(rule.right_indicator, rule.lookback_period))

        prepared: dict[str, _PreparedSymbolData] = {}
        for symbol_def in request.symbols:
            symbol = symbol_def.symbol
            symbol_legs = [leg for group in request.strategy_groups for leg in group.legs if leg.symbol == symbol]
            max_target_dte = max((leg.target_dte for leg in symbol_legs), default=30)
            max_holding_days = max((leg.max_holding_days for leg in symbol_legs), default=30)
            max_tolerance = max((leg.dte_tolerance_days for leg in symbol_legs), default=5)
            extended_start = request.start_date - timedelta(days=max(max_lookback * 4, 180))
            extended_end = request.end_date + timedelta(days=max(max_holding_days, max_target_dte + max_tolerance) + 45)
            raw_bars = market_data_service._fetch_bars_coalesced(symbol, extended_start, extended_end)
            bars = market_data_service._validate_bars(raw_bars, symbol)
            if not bars:
                raise DataUnavailableError(f"No daily bar data was returned for {symbol}.")
            ex_dividend_dates = market_data_service._load_ex_dividend_dates(symbol, start_date=bars[0].trade_date, end_date=bars[-1].trade_date)
            option_gateway = MassiveOptionGateway(
                market_data_service.client,
                symbol,
                redis_cache=getattr(market_data_service, "_redis_cache", None),
            )
            option_gateway.set_ex_dividend_dates(ex_dividend_dates)
            bundle = HistoricalDataBundle(
                bars=bars,
                earnings_dates=set(),
                ex_dividend_dates=ex_dividend_dates,
                option_gateway=option_gateway,
            )
            prepared[symbol] = _PreparedSymbolData(
                definition=symbol_def,
                bars=bars,
                bundle=bundle,
                bars_by_date={bar.trade_date: bar for bar in bars},
                bar_index_by_date={bar.trade_date: idx for idx, bar in enumerate(bars)},
                starting_cash=self._allocated_cash_for_symbol(request, symbol_def),
            )
        return prepared

    def _execute_request(
        self,
        request: CreateMultiSymbolRunRequest,
        prepared: dict[str, _PreparedSymbolData],
    ) -> dict[str, Any]:
        settings = get_settings()
        warnings: list[dict[str, Any]] = [
            _to_warning(
                "multi_symbol_alpha_v1",
                "Multi-Symbol v1 executes grouped synchronous entries and per-symbol P&L, but currently treats any symbol-level exit as a grouped liquidation event.",
                severity="warning",
            )
        ]
        symbol_cash = {symbol: data.starting_cash for symbol, data in prepared.items()}
        symbol_curves: dict[str, list[EquityPointResult]] = {symbol: [] for symbol in prepared}
        symbol_trades: dict[str, list[TradeResult]] = {symbol: [] for symbol in prepared}
        combined_curve: list[EquityPointResult] = []
        grouped_trades: list[dict[str, Any]] = []
        group_rows: list[dict[str, Any]] = []
        open_group: _OpenGroupedPosition | None = None
        rule_evaluator = _CrossSymbolRuleEvaluator(prepared)
        common_dates = sorted(
            set.intersection(
                *[
                    {bar.trade_date for bar in data.bars if request.start_date <= bar.trade_date <= request.end_date}
                    for data in prepared.values()
                ]
            )
        )
        if not common_dates:
            raise DataUnavailableError("No shared trading dates were available across the selected symbols.")

        peak_combined = sum(symbol_cash.values())
        peak_by_symbol = {symbol: cash for symbol, cash in symbol_cash.items()}

        for trade_date in common_dates:
            symbol_position_values = {symbol: Decimal("0") for symbol in prepared}
            if open_group is not None:
                should_group_exit = False
                group_exit_reason = ""
                marked_values: dict[str, Decimal] = {}
                exit_prices_by_symbol: dict[str, dict[str, float]] = {}
                assignment_details: dict[str, dict[str, Any] | None] = {}
                trade_warning_map: dict[str, tuple[str, ...]] = {}
                for symbol, position in open_group.positions.items():
                    data = prepared[symbol]
                    bar = data.bars_by_date[trade_date]
                    snapshot = self._engine._mark_position(
                        position,
                        bar,
                        data.bundle.option_gateway,
                        warnings,
                        {warning["code"] for warning in warnings if "code" in warning},
                        data.bundle.ex_dividend_dates,
                    )
                    marked_values[symbol] = snapshot.position_value
                    symbol_position_values[symbol] = snapshot.position_value
                    exit_prices_by_symbol[symbol] = {leg.ticker: leg.last_mid for leg in position.option_legs}
                    exit_prices_by_symbol[symbol].update({leg.symbol: leg.last_price for leg in position.stock_legs})
                    assignment_details[symbol] = snapshot.assignment_detail
                    trade_warning_map[symbol] = snapshot.warnings
                    config = open_group.leg_configs[symbol]
                    entry_cost = self._engine._entry_value_per_unit(position) * Decimal(position.quantity)
                    capital_at_risk = position.capital_required_per_unit * position.quantity
                    leg_should_exit, leg_exit_reason = self._engine._resolve_exit(
                        bar=bar,
                        position=position,
                        max_holding_days=config.max_holding_days,
                        backtest_end_date=request.end_date,
                        last_bar_date=common_dates[-1],
                        position_value=float(snapshot.position_value),
                        entry_cost=float(entry_cost),
                        capital_at_risk=capital_at_risk,
                        current_bar_index=data.bar_index_by_date[trade_date],
                    )
                    if snapshot.assignment_exit_reason is not None:
                        leg_should_exit = True
                        leg_exit_reason = snapshot.assignment_exit_reason
                    if leg_should_exit and not should_group_exit:
                        should_group_exit = True
                        group_exit_reason = f"group_{leg_exit_reason}"
                if request.exit_rules and rule_evaluator.evaluate(request.exit_rules, trade_date):
                    should_group_exit = True
                    group_exit_reason = group_exit_reason or "group_exit_rules"
                if should_group_exit:
                    group_trade_rows: list[TradeResult] = []
                    for symbol, position in list(open_group.positions.items()):
                        data = prepared[symbol]
                        bar = data.bars_by_date[trade_date]
                        config = open_group.leg_configs[symbol]
                        trade, cash_delta = self._engine._close_position(
                            position=position,
                            config=config,
                            exit_value=marked_values[symbol],
                            exit_date=trade_date,
                            exit_underlying_close=bar.close_price,
                            exit_prices=exit_prices_by_symbol[symbol],
                            exit_reason=group_exit_reason,
                            warnings=warnings,
                            warning_codes={warning["code"] for warning in warnings if "code" in warning},
                            current_bar_index=data.bar_index_by_date[trade_date],
                            assignment_detail=assignment_details[symbol],
                            trade_warnings=trade_warning_map[symbol],
                        )
                        symbol_cash[symbol] += cash_delta
                        symbol_trades[symbol].append(trade)
                        group_trade_rows.append(trade)
                        symbol_position_values[symbol] = Decimal("0")
                    grouped_trades.append({"group_id": open_group.group_id, "group_name": open_group.group_name, "trades": group_trade_rows})
                    group_rows.append(
                        {
                            "id": open_group.group_id,
                            "entry_date": min(trade.entry_date for trade in group_trade_rows),
                            "exit_date": max(trade.exit_date for trade in group_trade_rows),
                            "status": "closed",
                            "detail_json": {"group_name": open_group.group_name},
                        }
                    )
                    open_group = None

            if open_group is None and rule_evaluator.evaluate(request.entry_rules, trade_date):
                entry = self._attempt_group_entry(request, prepared, symbol_cash, trade_date, warnings)
                if entry is not None:
                    open_group, entry_position_values = entry
                    symbol_position_values.update(entry_position_values)

            combined_equity = Decimal("0")
            combined_cash = Decimal("0")
            combined_position_value = Decimal("0")
            for symbol in prepared:
                equity = symbol_cash[symbol] + symbol_position_values[symbol]
                peak_by_symbol[symbol] = max(peak_by_symbol[symbol], equity)
                drawdown = Decimal("0")
                if peak_by_symbol[symbol] > 0:
                    drawdown = (peak_by_symbol[symbol] - equity) / peak_by_symbol[symbol] * Decimal("100")
                point = EquityPointResult(
                    trade_date=trade_date,
                    equity=equity,
                    cash=symbol_cash[symbol],
                    position_value=symbol_position_values[symbol],
                    drawdown_pct=drawdown,
                )
                symbol_curves[symbol].append(point)
                combined_equity += equity
                combined_cash += symbol_cash[symbol]
                combined_position_value += symbol_position_values[symbol]
            peak_combined = max(peak_combined, combined_equity)
            combined_drawdown = Decimal("0")
            if peak_combined > 0:
                combined_drawdown = (peak_combined - combined_equity) / peak_combined * Decimal("100")
            combined_curve.append(
                EquityPointResult(
                    trade_date=trade_date,
                    equity=combined_equity,
                    cash=combined_cash,
                    position_value=combined_position_value,
                    drawdown_pct=combined_drawdown,
                )
            )

        if open_group is not None:
            final_date = common_dates[-1]
            group_trade_rows: list[TradeResult] = []
            for symbol, position in list(open_group.positions.items()):
                data = prepared[symbol]
                bar = data.bars_by_date[final_date]
                snapshot = self._engine._mark_position(
                    position,
                    bar,
                    data.bundle.option_gateway,
                    warnings,
                    {warning["code"] for warning in warnings if "code" in warning},
                    data.bundle.ex_dividend_dates,
                )
                exit_prices = {leg.ticker: leg.last_mid for leg in position.option_legs}
                exit_prices.update({leg.symbol: leg.last_price for leg in position.stock_legs})
                trade, cash_delta = self._engine._close_position(
                    position=position,
                    config=open_group.leg_configs[symbol],
                    exit_value=snapshot.position_value,
                    exit_date=final_date,
                    exit_underlying_close=bar.close_price,
                    exit_prices=exit_prices,
                    exit_reason="data_exhausted",
                    warnings=warnings,
                    warning_codes={warning["code"] for warning in warnings if "code" in warning},
                    current_bar_index=data.bar_index_by_date[final_date],
                    assignment_detail=snapshot.assignment_detail,
                    trade_warnings=snapshot.warnings,
                )
                symbol_cash[symbol] += cash_delta
                symbol_trades[symbol].append(trade)
                group_trade_rows.append(trade)
            grouped_trades.append({"group_id": open_group.group_id, "group_name": open_group.group_name, "trades": group_trade_rows})
            group_rows.append(
                {
                    "id": open_group.group_id,
                    "entry_date": min(trade.entry_date for trade in group_trade_rows),
                    "exit_date": max(trade.exit_date for trade in group_trade_rows),
                    "status": "closed",
                    "detail_json": {"group_name": open_group.group_name, "forced_close": True},
                }
            )

        combined_summary = build_summary(
            float(request.account_size),
            float(combined_curve[-1].equity) if combined_curve else float(request.account_size),
            [trade for trades in symbol_trades.values() for trade in trades],
            combined_curve,
            risk_free_rate=float(settings.risk_free_rate),
            warnings=warnings,
        )
        symbol_summaries = {
            symbol: build_summary(
                float(data.starting_cash),
                float(symbol_curves[symbol][-1].equity) if symbol_curves[symbol] else float(data.starting_cash),
                symbol_trades[symbol],
                symbol_curves[symbol],
                risk_free_rate=float(settings.risk_free_rate),
            )
            for symbol, data in prepared.items()
        }
        return {
            "warnings": warnings,
            "combined_summary": combined_summary,
            "symbol_summaries": symbol_summaries,
            "combined_curve": combined_curve,
            "symbol_curves": symbol_curves,
            "group_rows": group_rows,
            "grouped_trades": grouped_trades,
        }

    def _attempt_group_entry(
        self,
        request: CreateMultiSymbolRunRequest,
        prepared: dict[str, _PreparedSymbolData],
        symbol_cash: dict[str, Decimal],
        trade_date: date,
        warnings: list[dict[str, Any]],
    ) -> tuple[_OpenGroupedPosition, dict[str, Decimal]] | None:
        warning_codes = {warning["code"] for warning in warnings if "code" in warning}
        for group in request.strategy_groups:
            positions: dict[str, OpenMultiLegPosition] = {}
            leg_configs: dict[str, BacktestConfig] = {}
            entry_costs: dict[str, Decimal] = {}
            position_values: dict[str, Decimal] = {}
            build_failed = False
            for leg in group.legs:
                data = prepared[leg.symbol]
                bar = data.bars_by_date.get(trade_date)
                if bar is None:
                    build_failed = True
                    break
                config = self._build_leg_config(request, data.definition, leg)
                strategy = STRATEGY_REGISTRY.get(config.strategy_type)
                if strategy is None:
                    raise AppValidationError(f"Unsupported multi-symbol strategy_type: {config.strategy_type}")
                build_kwargs: dict[str, Any] = {}
                params = inspect.signature(strategy.build_position).parameters
                if leg.custom_legs is not None and "custom_legs" in params:
                    build_kwargs["custom_legs"] = list(leg.custom_legs)
                realized_vol = self._engine._estimate_realized_vol(data.bars[: data.bar_index_by_date[trade_date] + 1])
                if realized_vol is not None and "realized_vol" in params:
                    build_kwargs["realized_vol"] = realized_vol
                candidate = strategy.build_position(
                    config,
                    bar,
                    data.bar_index_by_date[trade_date],
                    data.bundle.option_gateway,
                    **build_kwargs,
                )
                if candidate is None:
                    build_failed = True
                    break
                quantity = self._resolve_quantity(candidate, leg, config, symbol_cash[leg.symbol], data.starting_cash)
                if quantity <= 0:
                    build_failed = True
                    break
                candidate.quantity = quantity
                candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
                entry_commission = self._engine._option_commission_total(candidate, config.commission_per_contract)
                candidate.entry_commission_total = entry_commission
                gross_notional_per_unit = (
                    sum(abs(Decimal(str(opt.entry_mid)) * Decimal(str(getattr(opt, "contract_multiplier", 100.0)))) * Decimal(opt.quantity_per_unit) for opt in candidate.option_legs)
                    + sum(abs(Decimal(str(stock.entry_price))) * Decimal(stock.share_quantity_per_unit) for stock in candidate.stock_legs)
                )
                slippage_cost = gross_notional_per_unit * Decimal(quantity) * (Decimal(str(config.slippage_pct)) / Decimal("100"))
                entry_cost = (self._engine._entry_value_per_unit(candidate) * Decimal(quantity)) + entry_commission + slippage_cost
                if symbol_cash[leg.symbol] - entry_cost < 0:
                    build_failed = True
                    break
                positions[leg.symbol] = candidate
                leg_configs[leg.symbol] = config
                entry_costs[leg.symbol] = entry_cost
                position_values[leg.symbol] = self._engine._entry_value_per_unit(candidate) * Decimal(quantity)
            if build_failed or len(positions) != len(group.legs):
                self._engine._add_warning_once(
                    warnings,
                    warning_codes,
                    "synchronous_entry_skipped",
                    "One or more grouped entries were skipped because not every leg could be constructed or funded on the same date.",
                )
                continue
            for symbol, entry_cost in entry_costs.items():
                symbol_cash[symbol] -= entry_cost
            return (
                _OpenGroupedPosition(
                    group_id=uuid4(),
                    positions=positions,
                    leg_configs=leg_configs,
                    group_name=group.name,
                ),
                position_values,
            )
        return None

    def _resolve_quantity(
        self,
        candidate: OpenMultiLegPosition,
        leg: MultiSymbolLegDefinition,
        config: BacktestConfig,
        available_cash: Decimal,
        symbol_account_size: Decimal,
    ) -> int:
        if leg.quantity_mode == "fixed_contracts":
            return leg.fixed_contracts or 0
        entry_value = self._engine._entry_value_per_unit(candidate)
        contracts_per_unit = sum(opt.quantity_per_unit for opt in candidate.option_legs)
        commission_per_unit = float(config.commission_per_contract) * contracts_per_unit
        gross_notional_per_unit = (
            sum(abs(opt.entry_mid * getattr(opt, "contract_multiplier", 100.0)) * opt.quantity_per_unit for opt in candidate.option_legs)
            + sum(abs(stock.entry_price * stock.share_quantity_per_unit) for stock in candidate.stock_legs)
        )
        return self._engine._resolve_position_size(
            available_cash=available_cash,
            account_size=float(symbol_account_size),
            risk_per_trade_pct=float(config.risk_per_trade_pct),
            capital_required_per_unit=candidate.capital_required_per_unit,
            max_loss_per_unit=candidate.max_loss_per_unit,
            entry_cost_per_unit=float(abs(entry_value)),
            commission_per_unit=commission_per_unit,
            slippage_pct=config.slippage_pct,
            gross_notional_per_unit=gross_notional_per_unit,
        )

    def _build_leg_config(
        self,
        request: CreateMultiSymbolRunRequest,
        symbol_def: MultiSymbolDefinition,
        leg: MultiSymbolLegDefinition,
    ) -> BacktestConfig:
        return BacktestConfig(
            symbol=leg.symbol,
            strategy_type=leg.strategy_type.value,
            start_date=request.start_date,
            end_date=request.end_date,
            target_dte=leg.target_dte,
            dte_tolerance_days=leg.dte_tolerance_days,
            max_holding_days=leg.max_holding_days,
            account_size=self._allocated_cash_for_symbol(request, symbol_def),
            risk_per_trade_pct=symbol_def.risk_per_trade_pct,
            commission_per_contract=request.commission_per_contract,
            entry_rules=[],
            risk_free_rate=float(get_settings().risk_free_rate),
            slippage_pct=float(request.slippage_pct),
            strategy_overrides=leg.strategy_overrides,
            custom_legs=leg.custom_legs,
        )

    def _allocated_cash_for_symbol(self, request: CreateMultiSymbolRunRequest, symbol_def: MultiSymbolDefinition) -> Decimal:
        if request.capital_allocation_mode == "explicit":
            return request.account_size * (Decimal(symbol_def.capital_allocation_pct or Decimal("0")) / Decimal("100"))
        return request.account_size / Decimal(len(request.symbols))

    def _persist_success(
        self,
        run: MultiSymbolRun,
        request: CreateMultiSymbolRunRequest,
        prepared: dict[str, _PreparedSymbolData],
        result: dict[str, Any],
    ) -> None:
        run.status = "succeeded"
        run.completed_at = datetime.now(UTC)
        run.error_code = None
        run.error_message = None
        run.warnings_json = result["warnings"]
        self._apply_summary_to_run(run, result["combined_summary"], request.account_size)

        symbol_rows = list(self.session.scalars(select(MultiSymbolRunSymbol).where(MultiSymbolRunSymbol.run_id == run.id)))
        rows_by_symbol = {row.symbol: row for row in symbol_rows}
        for symbol, summary in result["symbol_summaries"].items():
            row = rows_by_symbol[symbol]
            row.trade_count = summary.trade_count
            row.win_rate = Decimal(str(summary.win_rate))
            row.total_roi_pct = Decimal(str(summary.total_roi_pct))
            row.max_drawdown_pct = Decimal(str(summary.max_drawdown_pct))
            row.total_commissions = Decimal(str(summary.total_commissions))
            row.total_net_pnl = Decimal(str(summary.total_net_pnl))
            row.starting_equity = prepared[symbol].starting_cash
            row.ending_equity = Decimal(str(summary.ending_equity))

        for group_row in result["group_rows"]:
            self.session.add(
                MultiSymbolTradeGroup(
                    id=group_row["id"],
                    run_id=run.id,
                    entry_date=group_row["entry_date"],
                    exit_date=group_row["exit_date"],
                    status=group_row["status"],
                    detail_json=group_row["detail_json"],
                )
            )
        self.session.flush()

        for group_bundle in result["grouped_trades"]:
            for trade in group_bundle["trades"]:
                self.session.add(
                    MultiSymbolTrade(
                        run_id=run.id,
                        trade_group_id=group_bundle["group_id"],
                        symbol=trade.underlying_symbol,
                        option_ticker=trade.option_ticker,
                        strategy_type=trade.strategy_type,
                        entry_date=trade.entry_date,
                        exit_date=trade.exit_date,
                        expiration_date=trade.expiration_date,
                        quantity=trade.quantity,
                        dte_at_open=trade.dte_at_open,
                        holding_period_days=trade.holding_period_days,
                        entry_underlying_close=trade.entry_underlying_close,
                        exit_underlying_close=trade.exit_underlying_close,
                        entry_mid=trade.entry_mid,
                        exit_mid=trade.exit_mid,
                        gross_pnl=trade.gross_pnl,
                        net_pnl=trade.net_pnl,
                        total_commissions=trade.total_commissions,
                        entry_reason=trade.entry_reason,
                        exit_reason=trade.exit_reason,
                        detail_json=trade.detail_json,
                    )
                )
        self.session.flush()

        for point in result["combined_curve"]:
            self.session.add(
                MultiSymbolEquityPoint(
                    run_id=run.id,
                    trade_date=point.trade_date,
                    equity=point.equity,
                    cash=point.cash,
                    position_value=point.position_value,
                    drawdown_pct=point.drawdown_pct,
                )
            )
        for symbol, curve in result["symbol_curves"].items():
            run_symbol_id = rows_by_symbol[symbol].id
            for point in curve:
                self.session.add(
                    MultiSymbolSymbolEquityPoint(
                        run_symbol_id=run_symbol_id,
                        trade_date=point.trade_date,
                        equity=point.equity,
                        cash=point.cash,
                        position_value=point.position_value,
                        drawdown_pct=point.drawdown_pct,
                    )
                )
        self.session.commit()

    def _apply_summary_to_run(self, run: MultiSymbolRun, summary: Any, starting_equity: Decimal) -> None:
        run.trade_count = summary.trade_count
        run.win_rate = Decimal(str(summary.win_rate))
        run.total_roi_pct = Decimal(str(summary.total_roi_pct))
        run.average_win_amount = Decimal(str(summary.average_win_amount))
        run.average_loss_amount = Decimal(str(summary.average_loss_amount))
        run.average_holding_period_days = Decimal(str(summary.average_holding_period_days))
        run.average_dte_at_open = Decimal(str(summary.average_dte_at_open))
        run.max_drawdown_pct = Decimal(str(summary.max_drawdown_pct))
        run.total_commissions = Decimal(str(summary.total_commissions))
        run.total_net_pnl = Decimal(str(summary.total_net_pnl))
        run.starting_equity = starting_equity
        run.ending_equity = Decimal(str(summary.ending_equity))
        run.profit_factor = None if summary.profit_factor is None else Decimal(str(summary.profit_factor))
        run.payoff_ratio = None if summary.payoff_ratio is None else Decimal(str(summary.payoff_ratio))
        run.expectancy = Decimal(str(summary.expectancy))
        run.sharpe_ratio = None if summary.sharpe_ratio is None else Decimal(str(summary.sharpe_ratio))
        run.sortino_ratio = None if summary.sortino_ratio is None else Decimal(str(summary.sortino_ratio))
        run.cagr_pct = None if summary.cagr_pct is None else Decimal(str(summary.cagr_pct))
        run.calmar_ratio = None if summary.calmar_ratio is None else Decimal(str(summary.calmar_ratio))
        run.max_consecutive_wins = summary.max_consecutive_wins
        run.max_consecutive_losses = summary.max_consecutive_losses
        run.recovery_factor = None if summary.recovery_factor is None else Decimal(str(summary.recovery_factor))

    def _to_history_item(self, run: MultiSymbolRun) -> MultiSymbolRunHistoryItemResponse:
        symbol_rows = list(
            self.session.scalars(select(MultiSymbolRunSymbol).where(MultiSymbolRunSymbol.run_id == run.id).order_by(MultiSymbolRunSymbol.symbol))
        )
        return MultiSymbolRunHistoryItemResponse(
            id=run.id,
            name=run.name,
            status=run.status,
            created_at=run.created_at,
            completed_at=run.completed_at,
            symbols=[row.symbol for row in symbol_rows],
            summary=_summary_from_run(run),
        )

    def _to_detail_response(self, run: MultiSymbolRun) -> MultiSymbolRunDetailResponse:
        symbol_rows = list(
            self.session.scalars(select(MultiSymbolRunSymbol).where(MultiSymbolRunSymbol.run_id == run.id).order_by(MultiSymbolRunSymbol.symbol))
        )
        group_rows = list(
            self.session.scalars(select(MultiSymbolTradeGroup).where(MultiSymbolTradeGroup.run_id == run.id).order_by(MultiSymbolTradeGroup.entry_date))
        )
        trade_rows = list(
            self.session.scalars(select(MultiSymbolTrade).where(MultiSymbolTrade.run_id == run.id).order_by(MultiSymbolTrade.entry_date))
        )
        equity_rows = list(
            self.session.scalars(select(MultiSymbolEquityPoint).where(MultiSymbolEquityPoint.run_id == run.id).order_by(MultiSymbolEquityPoint.trade_date))
        )
        symbol_equity_rows = list(
            self.session.execute(
                select(MultiSymbolRunSymbol.symbol, MultiSymbolSymbolEquityPoint)
                .join(MultiSymbolSymbolEquityPoint, MultiSymbolSymbolEquityPoint.run_symbol_id == MultiSymbolRunSymbol.id)
                .where(MultiSymbolRunSymbol.run_id == run.id)
                .order_by(MultiSymbolRunSymbol.symbol, MultiSymbolSymbolEquityPoint.trade_date)
            )
        )
        trades_by_group: dict[UUID, list[MultiSymbolTradeResponse]] = {}
        for trade in trade_rows:
            trades_by_group.setdefault(trade.trade_group_id, []).append(MultiSymbolTradeResponse.model_validate(trade))
        groups = [
            MultiSymbolTradeGroupResponse(
                id=group.id,
                entry_date=group.entry_date,
                exit_date=group.exit_date,
                status=group.status,
                trades=trades_by_group.get(group.id, []),
            )
            for group in group_rows
        ]
        snapshot_symbols = run.input_snapshot_json.get("symbols") if isinstance(run.input_snapshot_json, dict) else []
        symbols = [MultiSymbolDefinition.model_validate(item) for item in snapshot_symbols] if isinstance(snapshot_symbols, list) else []
        symbol_summaries = [
            MultiSymbolRunSymbolSummaryResponse(
                symbol=row.symbol,
                summary=BacktestSummaryResponse(
                    trade_count=row.trade_count,
                    decided_trades=row.trade_count,
                    win_rate=row.win_rate,
                    total_roi_pct=row.total_roi_pct,
                    average_win_amount=Decimal("0"),
                    average_loss_amount=Decimal("0"),
                    average_holding_period_days=Decimal("0"),
                    average_dte_at_open=Decimal("0"),
                    max_drawdown_pct=row.max_drawdown_pct,
                    total_commissions=row.total_commissions,
                    total_net_pnl=row.total_net_pnl,
                    starting_equity=row.starting_equity,
                    ending_equity=row.ending_equity,
                    expectancy=Decimal("0"),
                ),
            )
            for row in symbol_rows
        ]
        symbol_equity_curves: dict[str, list[EquityCurvePointResponse]] = defaultdict(list)
        for symbol, point in symbol_equity_rows:
            symbol_equity_curves[symbol].append(EquityCurvePointResponse.model_validate(point))
        return MultiSymbolRunDetailResponse(
            id=run.id,
            name=run.name,
            status=run.status,
            start_date=run.start_date,
            end_date=run.end_date,
            created_at=run.created_at,
            started_at=run.started_at,
            completed_at=run.completed_at,
            warnings=run.warnings_json,
            error_code=run.error_code,
            error_message=run.error_message,
            symbols=symbols,
            summary=_summary_from_run(run),
            symbol_summaries=symbol_summaries,
            trade_groups=groups,
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity_rows],
            symbol_equity_curves=dict(symbol_equity_curves),
        )
