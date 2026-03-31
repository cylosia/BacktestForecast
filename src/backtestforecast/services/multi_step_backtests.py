from __future__ import annotations

import inspect
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
from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.strategies.common import (
    choose_primary_expiration,
    contracts_for_expiration,
    require_contract_for_strike,
)
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
    TradeResult,
)
from backtestforecast.config import get_settings
from backtestforecast.errors import AppValidationError, ConflictError, DataUnavailableError, ExternalServiceError, NotFoundError
from backtestforecast.market_data.service import (
    HistoricalDataBundle,
    MassiveOptionGateway,
    historical_flatfile_pricing_warning,
)
from backtestforecast.models import (
    MultiStepEquityPoint,
    MultiStepRun,
    MultiStepRunStep,
    MultiStepStepEvent,
    MultiStepTrade,
    User,
)
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.schemas.backtests import BacktestSummaryResponse, EquityCurvePointResponse
from backtestforecast.schemas.multi_step_backtests import (
    CreateMultiStepRunRequest,
    MultiStepEventResponse,
    MultiStepRunDetailResponse,
    MultiStepRunHistoryItemResponse,
    MultiStepRunListResponse,
    MultiStepRunStatusResponse,
    MultiStepStepOutcomeResponse,
    MultiStepTradeResponse,
    WorkflowStepDefinition,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.backtest_workflow_access import enforce_backtest_workflow_quota
from backtestforecast.services.dispatch_recovery import redispatch_if_stale_queued
from backtestforecast.services.job_cancellation import mark_job_cancelled, publish_cancellation_event, revoke_celery_task
from backtestforecast.services.job_transitions import cancellation_blocked_message, deletion_blocked_message

logger = structlog.get_logger("services.multi_step_backtests")

_QUEUE = "multi_step_backtests"
_RUNNING_DELETE_CONFLICT = deletion_blocked_message("multi-step backtest run")


def _persistable_ratio_metric(value: Any) -> Decimal | None:
    if value is None:
        return None
    metric = Decimal(str(value))
    return metric if metric.is_finite() else None


@dataclass(slots=True)
class _WorkflowLot:
    step_number: int
    position: OpenMultiLegPosition
    config: BacktestConfig


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


def _summary_from_run(run: MultiStepRun) -> BacktestSummaryResponse:
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


class MultiStepBacktestService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self._owns_execution_service = execution_service is None
        self._engine = OptionsBacktestEngine()

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None and self._owns_execution_service:
            self._execution_service.close()

    def __enter__(self) -> MultiStepBacktestService:
        return self

    def __exit__(self, exc_type, exc, _tb) -> None:
        if exc:
            self.session.rollback()
        self.close()

    def create_and_dispatch(
        self,
        user: User,
        request: CreateMultiStepRunRequest,
        *,
        request_id: str | None = None,
        traceparent: str | None = None,
        dispatch_logger: Any | None = None,
    ) -> MultiStepRun:
        from apps.api.app.dispatch import dispatch_celery_task

        run = self.enqueue(user, request)
        dispatch_celery_task(
            db=self.session,
            job=run,
            task_name="multi_step_backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue=_QUEUE,
            log_event="multi_step_backtest",
            logger=dispatch_logger or logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        self.session.refresh(run)
        return run

    def enqueue(self, user: User, request: CreateMultiStepRunRequest) -> MultiStepRun:
        if request.idempotency_key:
            existing = self.session.scalar(
                select(MultiStepRun).where(
                    MultiStepRun.user_id == user.id,
                    MultiStepRun.idempotency_key == request.idempotency_key,
                )
            )
            if existing is not None:
                return redispatch_if_stale_queued(
                    self.session,
                    existing,
                    model_name="MultiStepRun",
                    task_name="multi_step_backtests.run",
                    task_kwargs={"run_id": str(existing.id)},
                    queue=_QUEUE,
                    log_event="multi_step_backtest",
                    logger=logger,
                )

        enforce_backtest_workflow_quota(self.session, user)

        run = MultiStepRun(
            id=uuid4(),
            user_id=user.id,
            status="queued",
            name=request.name,
            symbol=request.symbol,
            workflow_type=request.workflow_type,
            start_date=request.start_date,
            end_date=request.end_date,
            account_size=request.account_size,
            risk_per_trade_pct=request.risk_per_trade_pct,
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
                    select(MultiStepRun).where(
                        MultiStepRun.user_id == user.id,
                        MultiStepRun.idempotency_key == request.idempotency_key,
                    )
                )
                if existing is not None:
                    return existing
            raise

        for step in request.steps:
            self.session.add(
                MultiStepRunStep(
                    run_id=run.id,
                    step_number=step.step_number,
                    name=step.name,
                    action=step.action,
                    trigger_json=step.trigger.model_dump(mode="json"),
                    contract_selection_json=step.contract_selection.model_dump(mode="json"),
                    failure_policy=step.failure_policy,
                )
            )
        return run

    def execute_run_by_id(self, run_id: UUID) -> MultiStepRun:
        run = self.session.get(MultiStepRun, run_id)
        if run is None:
            raise NotFoundError("Multi-step backtest run not found.")
        if run.status not in ("queued", "running"):
            return run

        started_at = datetime.now(UTC)
        rows = self.session.execute(
            update(MultiStepRun)
            .where(MultiStepRun.id == run_id, MultiStepRun.status == "queued")
            .values(status="running", started_at=started_at, updated_at=started_at)
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(run)
            return run

        self.session.refresh(run)
        try:
            request = CreateMultiStepRunRequest.model_validate(run.input_snapshot_json or {})
            result = self._execute_request(request)
            self._persist_success(run, result)
        except ExternalServiceError:
            self.session.rollback()
            raise
        except Exception as exc:
            logger.exception("multi_step_backtest.execution_failed", run_id=str(run_id))
            run.status = "failed"
            run.error_code = "execution_failed"
            run.error_message = str(exc)
            run.completed_at = datetime.now(UTC)
            run.warnings_json = [_to_warning("multi_step_execution_failed", "Multi-step execution failed before completion.")]
            self.session.commit()
        return run

    def list_runs(
        self,
        user: User,
        *,
        limit: int,
        offset: int,
        cursor: str | None,
    ) -> MultiStepRunListResponse:
        _parsed_cursor, parsed_offset = parse_cursor_param(cursor)
        effective_offset = parsed_offset if cursor else offset
        total = self.session.scalar(select(func.count()).select_from(MultiStepRun).where(MultiStepRun.user_id == user.id)) or 0
        runs = list(
            self.session.scalars(
                select(MultiStepRun)
                .where(MultiStepRun.user_id == user.id)
                .order_by(desc(MultiStepRun.created_at), desc(MultiStepRun.id))
                .offset(effective_offset)
                .limit(limit + 1)
            )
        )
        page = finalize_cursor_page(runs, total=total, offset=effective_offset, limit=limit)
        items = [self._to_history_item(run) for run in page.items]
        return MultiStepRunListResponse(items=items, total=page.total, offset=page.offset, limit=page.limit, next_cursor=page.next_cursor)

    def get_run_for_owner(self, *, user_id: UUID, run_id: UUID) -> MultiStepRunDetailResponse:
        run = self.session.scalar(select(MultiStepRun).where(MultiStepRun.id == run_id, MultiStepRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-step backtest run not found.")
        return self._to_detail_response(run)

    def get_run_status_for_owner(self, *, user_id: UUID, run_id: UUID) -> MultiStepRunStatusResponse:
        run = self.session.scalar(select(MultiStepRun).where(MultiStepRun.id == run_id, MultiStepRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-step backtest run not found.")
        return MultiStepRunStatusResponse(
            id=run.id,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_code=run.error_code,
            error_message=run.error_message,
        )

    def delete_for_user(self, *, run_id: UUID, user_id: UUID) -> None:
        run = self.session.scalar(select(MultiStepRun).where(MultiStepRun.id == run_id, MultiStepRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-step backtest run not found.")
        if run.status in ("queued", "running"):
            raise ConflictError(_RUNNING_DELETE_CONFLICT)
        self.session.delete(run)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def cancel_for_user(self, *, run_id: UUID, user_id: UUID) -> MultiStepRunStatusResponse:
        run = self.session.scalar(select(MultiStepRun).where(MultiStepRun.id == run_id, MultiStepRun.user_id == user_id))
        if run is None:
            raise NotFoundError("Multi-step backtest run not found.")
        if run.status not in ("queued", "running"):
            raise ConflictError(cancellation_blocked_message("multi-step backtest run"))
        task_id = mark_job_cancelled(run)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        revoke_celery_task(task_id, job_type="multi_step_backtest", job_id=run.id)
        publish_cancellation_event(job_type="multi_step_backtest", job_id=run.id)
        refreshed = self.session.scalar(select(MultiStepRun).where(MultiStepRun.id == run_id, MultiStepRun.user_id == user_id))
        if refreshed is None:
            raise NotFoundError("Multi-step backtest run not found.")
        return MultiStepRunStatusResponse(
            id=refreshed.id,
            status=refreshed.status,
            started_at=refreshed.started_at,
            completed_at=refreshed.completed_at,
            error_code=refreshed.error_code,
            error_message=refreshed.error_message,
        )

    def _execute_request(self, request: CreateMultiStepRunRequest) -> dict[str, Any]:
        market_data_service = self.execution_service.market_data_service
        max_target_dte = max(step.contract_selection.target_dte for step in request.steps)
        max_holding_days = max(step.contract_selection.max_holding_days for step in request.steps)
        max_tolerance = max(step.contract_selection.dte_tolerance_days for step in request.steps)
        extended_start = request.start_date - timedelta(days=240)
        extended_end = request.end_date + timedelta(days=max(max_holding_days, max_target_dte + max_tolerance) + 45)
        raw_bars = market_data_service._fetch_bars_coalesced(request.symbol, extended_start, extended_end)
        bars = market_data_service._validate_bars(raw_bars, request.symbol)
        if not bars:
            raise DataUnavailableError(f"No daily bar data was returned for {request.symbol}.")
        ex_dividend_result = market_data_service._load_ex_dividend_data(
            request.symbol,
            start_date=bars[0].trade_date,
            end_date=bars[-1].trade_date,
        )
        ex_dividend_dates = ex_dividend_result.dates
        earnings_dates = market_data_service.load_earnings_dates_for_rules(
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            rule_groups=[
                request.initial_entry_rules,
                *[step.trigger.rules for step in request.steps],
            ],
        )
        prefer_local = market_data_service._prefer_local_history(request.end_date)
        option_gateway = market_data_service.build_option_gateway(request.symbol, prefer_local=prefer_local)
        option_gateway.set_ex_dividend_dates(ex_dividend_dates)
        bundle = HistoricalDataBundle(
            bars=bars,
            earnings_dates=earnings_dates,
            ex_dividend_dates=ex_dividend_dates,
            option_gateway=option_gateway,
            data_source="historical_flatfile" if prefer_local else "massive",
            warnings=list(ex_dividend_result.warnings or []),
        )
        if prefer_local:
            bundle = HistoricalDataBundle(
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                ex_dividend_dates=bundle.ex_dividend_dates,
                option_gateway=bundle.option_gateway,
                data_source=bundle.data_source,
                warnings=[*(bundle.warnings or []), historical_flatfile_pricing_warning()],
            )
        warnings: list[dict[str, Any]] = [
            _to_warning(
                "multi_step_alpha_v1",
                "Multi-Step v1 uses inventory reconciliation across steps. Strategy-family breadth should still be treated as alpha until more transition coverage is added.",
                severity="warning",
            )
        ]
        warnings.extend(bundle.warnings or [])
        step_definitions = sorted(request.steps, key=lambda item: item.step_number)
        step_statuses = {
            step.step_number: {
                "step_number": step.step_number,
                "name": step.name,
                "action": step.action,
                "status": "pending",
                "triggered_at": None,
                "executed_at": None,
                "failure_reason": None,
            }
            for step in step_definitions
        }
        step_events: list[dict[str, Any]] = []
        trades_by_step: list[tuple[int, TradeResult]] = []
        equity_curve: list[EquityPointResult] = []
        cash = request.account_size
        peak_equity = cash
        active_lots: list[_WorkflowLot] = []
        previous_trade: TradeResult | None = None
        current_step_index = 0
        warning_codes = {warning["code"] for warning in warnings}
        step_execution_dates: dict[int, date] = {}
        step_completion_trades: dict[int, TradeResult] = {}

        trigger_evaluators = {
            step.step_number: self._build_rule_evaluator(
                symbol=request.symbol,
                bars=bars,
                target_dte=step.contract_selection.target_dte,
                dte_tolerance_days=step.contract_selection.dte_tolerance_days,
                rules=(request.initial_entry_rules if step.step_number == 1 else step.trigger.rules),
                earnings_dates=bundle.earnings_dates,
                option_gateway=option_gateway,
            )
            for step in step_definitions
        }

        tradable_bars = [bar for bar in bars if request.start_date <= bar.trade_date <= request.end_date]
        if not tradable_bars:
            raise DataUnavailableError(f"No tradable daily bars were returned for {request.symbol} in the requested workflow window.")

        for bar in tradable_bars:
            current_index = bars.index(bar)
            position_value = Decimal("0")
            next_active_lots: list[_WorkflowLot] = []
            for lot in active_lots:
                snapshot = self._engine._mark_position(
                    lot.position,
                    bar,
                    bundle.option_gateway,
                    warnings,
                    warning_codes,
                    bundle.ex_dividend_dates,
                )
                should_exit, exit_reason = self._engine._resolve_exit(
                    bar=bar,
                    position=lot.position,
                    max_holding_days=lot.config.max_holding_days,
                    backtest_end_date=request.end_date,
                    last_bar_date=tradable_bars[-1].trade_date,
                    position_value=float(snapshot.position_value),
                    entry_cost=float(self._engine._entry_value_per_unit(lot.position) * Decimal(lot.position.quantity)),
                    capital_at_risk=lot.position.capital_required_per_unit * lot.position.quantity,
                    current_bar_index=current_index,
                )
                if snapshot.assignment_exit_reason is not None:
                    should_exit = True
                    exit_reason = snapshot.assignment_exit_reason

                if exit_reason == "expiration" and current_step_index < len(step_definitions) - 1:
                    step_trade, cash_delta, survivor = self._expire_lot_and_preserve_survivors(
                        lot=lot,
                        request=request,
                        bar=bar,
                        snapshot=snapshot,
                        warning_codes=warning_codes,
                        warnings=warnings,
                        current_bar_index=current_index,
                    )
                    if step_trade is not None:
                        trades_by_step.append((lot.step_number, step_trade))
                        previous_trade = step_trade
                        step_completion_trades[lot.step_number] = step_trade
                        cash += cash_delta
                        position_value += Decimal("0")
                        current_step_index = min(current_step_index + 1, len(step_definitions) - 1)
                    if survivor is not None:
                        marked_survivor = self._engine._mark_position(
                            survivor.position,
                            bar,
                            bundle.option_gateway,
                            warnings,
                            warning_codes,
                            bundle.ex_dividend_dates,
                        )
                        position_value += marked_survivor.position_value
                        next_active_lots.append(survivor)
                    continue

                if should_exit:
                    exit_prices = {leg.ticker: leg.last_mid for leg in lot.position.option_legs}
                    exit_prices.update({leg.symbol: leg.last_price for leg in lot.position.stock_legs})
                    trade, cash_delta = self._engine._close_position(
                        position=lot.position,
                        config=lot.config,
                        exit_value=snapshot.position_value,
                        exit_date=bar.trade_date,
                        exit_underlying_close=bar.close_price,
                        exit_prices=exit_prices,
                        exit_reason=exit_reason,
                        warnings=warnings,
                        warning_codes=warning_codes,
                        current_bar_index=current_index,
                        assignment_detail=snapshot.assignment_detail,
                        trade_warnings=snapshot.warnings,
                    )
                    trades_by_step.append((lot.step_number, trade))
                    cash += cash_delta
                    previous_trade = trade
                    step_completion_trades[lot.step_number] = trade
                    current_step_index = min(lot.step_number, len(step_definitions) - 1)
                else:
                    position_value += snapshot.position_value
                    next_active_lots.append(lot)
            active_lots = next_active_lots

            step = step_definitions[current_step_index]
            if current_step_index < len(step_definitions):
                trigger_ready = self._is_step_triggered(
                    request=request,
                    step=step,
                    bar=bar,
                    bar_index=current_index,
                    evaluator=trigger_evaluators[step.step_number],
                    previous_trade=previous_trade,
                    step_execution_dates=step_execution_dates,
                    step_completion_trades=step_completion_trades,
                )
                if trigger_ready:
                    step_statuses[step.step_number]["status"] = "waiting"
                    step_statuses[step.step_number]["triggered_at"] = datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC)
                    step_events.append(
                        {
                            "step_number": step.step_number,
                            "event_type": "triggered",
                            "event_at": datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC),
                            "message": f"Step {step.step_number} triggered on {bar.trade_date.isoformat()}",
                            "payload_json": {"action": step.action},
                        }
                    )
                    candidate = self._build_step_position(request, step, bar, current_index, bundle.option_gateway, active_lots)
                    if candidate is None:
                        if step.step_number > 1:
                            failure_reason = "No valid contract or quote was available for the triggered step."
                            step_statuses[step.step_number]["status"] = "failed"
                            step_statuses[step.step_number]["failure_reason"] = failure_reason
                            step_events.append(
                                {
                                    "step_number": step.step_number,
                                    "event_type": "failed",
                                    "event_at": datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC),
                                    "message": failure_reason,
                                    "payload_json": {"failure_policy": step.failure_policy},
                                }
                            )
                            step_events.append(
                                {
                                    "step_number": step.step_number,
                                    "event_type": "liquidated",
                                    "event_at": datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC),
                                    "message": "Workflow liquidated because a later step could not be filled.",
                                    "payload_json": {},
                                }
                            )
                            if active_lots:
                                liquidation = self._liquidate_active_lots(
                                    active_lots=active_lots,
                                    bar=bar,
                                    option_gateway=bundle.option_gateway,
                                    ex_dividend_dates=bundle.ex_dividend_dates,
                                    current_bar_index=current_index,
                                    warnings=warnings,
                                    warning_codes=warning_codes,
                                )
                                for step_number, trade in liquidation["trades"]:
                                    trades_by_step.append((step_number, trade))
                                cash += liquidation["cash_delta"]
                                active_lots = []
                            current_step_index = 0
                            previous_trade = None
                        continue
                    transition = self._execute_step_transition(
                        request=request,
                        step=step,
                        candidate=candidate,
                        active_lots=active_lots,
                        available_cash=cash,
                        bar=bar,
                        option_gateway=bundle.option_gateway,
                        ex_dividend_dates=bundle.ex_dividend_dates,
                        current_bar_index=current_index,
                        warnings=warnings,
                        warning_codes=warning_codes,
                    )
                    if transition is None:
                        continue
                    active_lots = transition["active_lots"]
                    cash += transition["cash_delta"]
                    for step_number, trade in transition["closed_trades"]:
                        trades_by_step.append((step_number, trade))
                    step_statuses[step.step_number]["status"] = "executed"
                    step_statuses[step.step_number]["executed_at"] = datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC)
                    step_execution_dates[step.step_number] = bar.trade_date
                    step_events.append(
                        {
                            "step_number": step.step_number,
                            "event_type": "filled",
                            "event_at": datetime.combine(bar.trade_date, datetime.min.time(), tzinfo=UTC),
                            "message": f"Step {step.step_number} entered on {bar.trade_date.isoformat()}",
                            "payload_json": {"strategy_type": step.contract_selection.strategy_type.value},
                        }
                    )
                    if current_step_index < len(step_definitions) - 1:
                        current_step_index += 1
                    position_value = sum(
                        self._engine._current_position_value(lot.position, bar.close_price)
                        for lot in active_lots
                    ) or Decimal("0")

            equity = cash + position_value
            peak_equity = max(peak_equity, equity)
            drawdown = Decimal("0")
            if peak_equity > 0:
                drawdown = (peak_equity - equity) / peak_equity * Decimal("100")
            equity_curve.append(
                EquityPointResult(
                    trade_date=bar.trade_date,
                    equity=equity,
                    cash=cash,
                    position_value=position_value,
                    drawdown_pct=drawdown,
                )
            )

        if active_lots:
            final_bar = tradable_bars[-1]
            liquidation = self._liquidate_active_lots(
                active_lots=active_lots,
                bar=final_bar,
                option_gateway=bundle.option_gateway,
                ex_dividend_dates=bundle.ex_dividend_dates,
                current_bar_index=bars.index(final_bar),
                warnings=warnings,
                warning_codes=warning_codes,
                exit_reason="data_exhausted",
            )
            for step_number, trade in liquidation["trades"]:
                trades_by_step.append((step_number, trade))
            cash += liquidation["cash_delta"]
            step_events.append(
                {
                    "step_number": active_lots[-1].step_number,
                    "event_type": "liquidated",
                    "event_at": datetime.combine(final_bar.trade_date, datetime.min.time(), tzinfo=UTC),
                    "message": "Open workflow step was force-closed at the last available bar.",
                    "payload_json": {},
                }
            )

        summary = build_summary(
            float(request.account_size),
            float(equity_curve[-1].equity) if equity_curve else float(request.account_size),
            [trade for _, trade in trades_by_step],
            equity_curve,
            risk_free_rate=float(get_settings().risk_free_rate),
            warnings=warnings,
        )
        return {
            "summary": summary,
            "warnings": warnings,
            "steps": step_statuses,
            "events": step_events,
            "trades": trades_by_step,
            "equity_curve": equity_curve,
        }

    def _build_rule_evaluator(
        self,
        *,
        symbol: str,
        bars: list[Any],
        target_dte: int,
        dte_tolerance_days: int,
        rules: list[Any],
        earnings_dates: set[date],
        option_gateway: MassiveOptionGateway,
    ) -> EntryRuleEvaluator:
        config = BacktestConfig(
            symbol=symbol,
            strategy_type="long_call",
            start_date=bars[0].trade_date,
            end_date=bars[-1].trade_date,
            target_dte=target_dte,
            dte_tolerance_days=dte_tolerance_days,
            max_holding_days=30,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("1"),
            commission_per_contract=Decimal("0"),
            entry_rules=rules,
        )
        return EntryRuleEvaluator(
            config=config,
            bars=bars,
            earnings_dates=set(earnings_dates),
            option_gateway=option_gateway,
        )

    def _is_step_triggered(
        self,
        *,
        request: CreateMultiStepRunRequest,
        step: WorkflowStepDefinition,
        bar: Any,
        bar_index: int,
        evaluator: EntryRuleEvaluator,
        previous_trade: TradeResult | None,
        step_execution_dates: dict[int, date],
        step_completion_trades: dict[int, TradeResult],
    ) -> bool:
        if step.step_number == 1:
            initial_eval = self._build_rule_evaluator(
                symbol=request.symbol,
                bars=evaluator.bars,
                target_dte=step.contract_selection.target_dte,
                dte_tolerance_days=step.contract_selection.dte_tolerance_days,
                rules=request.initial_entry_rules,
                earnings_dates=set(evaluator.earnings_dates),
                option_gateway=evaluator.option_gateway,
            )
            if request.initial_entry_rules and not initial_eval.is_entry_allowed(bar_index):
                return False
            if step.trigger.mode in {"rule_match", "event_and_rule"} and step.trigger.rules:
                return evaluator.is_entry_allowed(bar_index)
            return True
        previous_step_number = step.step_number - 1
        previous_executed = step_execution_dates.get(previous_step_number)
        previous_completion = step_completion_trades.get(previous_step_number)
        if previous_executed is None and previous_completion is None:
            return False
        if step.trigger.mode == "after_expiration":
            return (
                previous_completion is not None
                and previous_completion.exit_reason == "expiration"
                and bar.trade_date >= previous_completion.exit_date
            )
        if step.trigger.mode == "date_offset":
            if step.trigger.days_after_prior_step is None:
                return False
            if previous_executed is None:
                return False
            return bar.trade_date >= previous_executed + timedelta(days=step.trigger.days_after_prior_step)
        if step.trigger.mode in {"rule_match", "event_and_rule"}:
            anchor_date = previous_completion.exit_date if previous_completion is not None else previous_executed
            if anchor_date is None or bar.trade_date < anchor_date:
                return False
            return evaluator.is_entry_allowed(bar_index)
        return False

    def _build_step_config(self, request: CreateMultiStepRunRequest, step: WorkflowStepDefinition) -> BacktestConfig:
        return BacktestConfig(
            symbol=request.symbol,
            strategy_type=step.contract_selection.strategy_type.value,
            start_date=request.start_date,
            end_date=request.end_date,
            target_dte=step.contract_selection.target_dte,
            dte_tolerance_days=step.contract_selection.dte_tolerance_days,
            max_holding_days=step.contract_selection.max_holding_days,
            account_size=request.account_size,
            risk_per_trade_pct=request.risk_per_trade_pct,
            commission_per_contract=request.commission_per_contract,
            entry_rules=[],
            risk_free_rate=float(get_settings().risk_free_rate),
            slippage_pct=float(request.slippage_pct),
            strategy_overrides=step.contract_selection.strategy_overrides,
            custom_legs=step.contract_selection.custom_legs,
        )

    def _build_step_position(
        self,
        request: CreateMultiStepRunRequest,
        step: WorkflowStepDefinition,
        bar: Any,
        bar_index: int,
        option_gateway: MassiveOptionGateway,
        active_lots: list[_WorkflowLot],
    ) -> OpenMultiLegPosition | None:
        if step.action == "close_position":
            return OpenMultiLegPosition(
                display_ticker="workflow_close",
                strategy_type="workflow_close",
                underlying_symbol=request.symbol,
                entry_date=bar.trade_date,
                entry_index=bar_index,
                quantity=1,
                dte_at_open=0,
                option_legs=[],
                stock_legs=[],
                scheduled_exit_date=bar.trade_date,
                capital_required_per_unit=0.0,
                max_loss_per_unit=0.0,
                detail_json={},
            )
        if step.action == "sell_premium":
            candidate = self._build_sell_premium_position(request, step, bar, bar_index, option_gateway, active_lots)
            if candidate is not None:
                return candidate
        config = self._build_step_config(request, step)
        strategy = STRATEGY_REGISTRY.get(config.strategy_type)
        if strategy is None:
            raise AppValidationError(f"Unsupported multi-step strategy_type: {config.strategy_type}")
        build_kwargs: dict[str, Any] = {}
        params = inspect.signature(strategy.build_position).parameters
        if step.contract_selection.custom_legs is not None and "custom_legs" in params:
            build_kwargs["custom_legs"] = list(step.contract_selection.custom_legs)
        return strategy.build_position(config, bar, bar_index, option_gateway, **build_kwargs)

    def _build_sell_premium_position(
        self,
        request: CreateMultiStepRunRequest,
        step: WorkflowStepDefinition,
        bar: Any,
        bar_index: int,
        option_gateway: MassiveOptionGateway,
        active_lots: list[_WorkflowLot],
    ) -> OpenMultiLegPosition | None:
        long_options = [
            leg
            for lot in active_lots
            for leg in lot.position.option_legs
            if leg.side > 0 and leg.expiration_date > bar.trade_date
        ]
        if not long_options:
            return None
        anchor_long = max(long_options, key=lambda leg: (leg.expiration_date, -leg.strike_price))
        if step.contract_selection.strategy_type.value != "calendar_spread":
            return None
        contracts = option_gateway.list_contracts(
            bar.trade_date,
            anchor_long.contract_type,
            step.contract_selection.target_dte,
            step.contract_selection.dte_tolerance_days,
        )
        near_expiration = choose_primary_expiration(contracts, bar.trade_date, step.contract_selection.target_dte)
        exp_contracts = contracts_for_expiration(contracts, near_expiration)
        short_contract = require_contract_for_strike(exp_contracts, anchor_long.strike_price)
        quote = option_gateway.get_quote(short_contract.ticker, bar.trade_date)
        if quote is None or quote.mid_price is None or quote.mid_price <= 0:
            return None
        detail_json = {
            "legs": [
                {
                    "asset_type": "option",
                    "ticker": short_contract.ticker,
                    "side": "short",
                    "contract_type": short_contract.contract_type,
                    "strike_price": short_contract.strike_price,
                    "expiration_date": short_contract.expiration_date.isoformat(),
                    "quantity_per_unit": 1,
                    "entry_mid": quote.mid_price,
                }
            ],
            "carry_forward_reference_ticker": anchor_long.ticker,
            "entry_package_market_value": -(quote.mid_price * 100.0),
            "capital_required_per_unit": max(anchor_long.strike_price * 100.0 * 0.2, 100.0),
            "max_loss_per_unit": None,
        }
        return OpenMultiLegPosition(
            display_ticker=short_contract.ticker,
            strategy_type=step.contract_selection.strategy_type.value,
            underlying_symbol=request.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(short_contract.expiration_date - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    ticker=short_contract.ticker,
                    contract_type=short_contract.contract_type,
                    side=-1,
                    strike_price=short_contract.strike_price,
                    expiration_date=short_contract.expiration_date,
                    quantity_per_unit=1,
                    entry_mid=quote.mid_price,
                    last_mid=quote.mid_price,
                )
            ],
            scheduled_exit_date=short_contract.expiration_date,
            capital_required_per_unit=max(anchor_long.strike_price * 100.0 * 0.2, 100.0),
            max_loss_per_unit=None,
            detail_json=detail_json,
        )

    def _resolve_step_quantity(
        self,
        candidate: OpenMultiLegPosition,
        request: CreateMultiStepRunRequest,
        available_cash: Decimal,
    ) -> int:
        entry_value = self._engine._entry_value_per_unit(candidate)
        contracts_per_unit = sum(opt.quantity_per_unit for opt in candidate.option_legs)
        commission_per_unit = float(request.commission_per_contract) * contracts_per_unit
        gross_notional_per_unit = (
            sum(abs(opt.entry_mid * getattr(opt, "contract_multiplier", 100.0)) * opt.quantity_per_unit for opt in candidate.option_legs)
            + sum(abs(stock.entry_price * stock.share_quantity_per_unit) for stock in candidate.stock_legs)
        )
        return self._engine._resolve_position_size(
            available_cash=available_cash,
            account_size=float(request.account_size),
            risk_per_trade_pct=float(request.risk_per_trade_pct),
            capital_required_per_unit=candidate.capital_required_per_unit,
            max_loss_per_unit=candidate.max_loss_per_unit,
            entry_cost_per_unit=float(abs(entry_value)),
            commission_per_unit=commission_per_unit,
            slippage_pct=float(request.slippage_pct),
            gross_notional_per_unit=gross_notional_per_unit,
        )

    def _execute_step_transition(
        self,
        *,
        request: CreateMultiStepRunRequest,
        step: WorkflowStepDefinition,
        candidate: OpenMultiLegPosition,
        active_lots: list[_WorkflowLot],
        available_cash: Decimal,
        bar: Any,
        option_gateway: MassiveOptionGateway,
        ex_dividend_dates: set[date],
        current_bar_index: int,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
    ) -> dict[str, Any] | None:
        config = self._build_step_config(request, step)
        close_unmatched = step.action in {"open_position", "roll", "close_position"}
        next_lots: list[_WorkflowLot] = []
        closed_trades: list[tuple[int, TradeResult]] = []
        cash_delta = Decimal("0")

        if step.action == "close_position":
            reusable_option_ids: set[int] = set()
            reusable_stock_ids: set[int] = set()
        else:
            quantity = self._resolve_step_quantity(candidate, request, available_cash)
            if quantity <= 0:
                return None
            candidate.quantity = quantity
            candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
            reusable_option_ids, reusable_stock_ids = self._match_reusable_inventory(candidate, active_lots, step.action)

        for lot in active_lots:
            lot_result = self._transition_existing_lot(
                lot=lot,
                reusable_option_ids=reusable_option_ids,
                reusable_stock_ids=reusable_stock_ids,
                close_unmatched=close_unmatched,
                bar=bar,
                option_gateway=option_gateway,
                ex_dividend_dates=ex_dividend_dates,
                current_bar_index=current_bar_index,
                warnings=warnings,
                warning_codes=warning_codes,
            )
            next_lots.extend(lot_result["kept_lots"])
            closed_trades.extend(lot_result["closed_trades"])
            cash_delta += lot_result["cash_delta"]

        if step.action == "close_position":
            return {"active_lots": next_lots, "closed_trades": closed_trades, "cash_delta": cash_delta}

        new_position = self._candidate_without_reused_inventory(candidate, active_lots, step.action)
        if new_position is not None:
            entry_commission = self._engine._option_commission_total(new_position, request.commission_per_contract)
            new_position.entry_commission_total = entry_commission
            entry_cost = (self._engine._entry_value_per_unit(new_position) * Decimal(new_position.quantity)) + entry_commission
            if available_cash + cash_delta - entry_cost < 0:
                return None
            cash_delta -= entry_cost
            next_lots.append(_WorkflowLot(step_number=step.step_number, position=new_position, config=config))

        return {"active_lots": next_lots, "closed_trades": closed_trades, "cash_delta": cash_delta}

    def _match_reusable_inventory(
        self,
        candidate: OpenMultiLegPosition,
        active_lots: list[_WorkflowLot],
        action: str,
    ) -> tuple[set[int], set[int]]:
        reusable_option_ids: set[int] = set()
        reusable_stock_ids: set[int] = set()
        used_inventory_ids: set[int] = set()
        short_anchor = max((leg.expiration_date for leg in candidate.option_legs if leg.side < 0), default=None)

        def option_inventory():
            for lot in active_lots:
                yield from lot.position.option_legs

        def stock_inventory():
            for lot in active_lots:
                yield from lot.position.stock_legs

        for leg in candidate.option_legs:
            match = None
            for existing in option_inventory():
                if id(existing) in used_inventory_ids:
                    continue
                if existing.side != leg.side or existing.contract_type != leg.contract_type:
                    continue
                if abs(existing.strike_price - leg.strike_price) > max(0.005, leg.strike_price * 0.0001):
                    continue
                if leg.side > 0:
                    if existing.ticker == leg.ticker or (short_anchor is not None and existing.expiration_date > short_anchor):
                        match = existing
                elif existing.ticker == leg.ticker:
                    match = existing
                if match is not None:
                    reusable_option_ids.add(id(match))
                    used_inventory_ids.add(id(match))
                    break

        for leg in candidate.stock_legs:
            for existing in stock_inventory():
                if id(existing) in used_inventory_ids:
                    continue
                if existing.side == leg.side and existing.share_quantity_per_unit == leg.share_quantity_per_unit and existing.symbol == leg.symbol:
                    reusable_stock_ids.add(id(existing))
                    used_inventory_ids.add(id(existing))
                    break
        return reusable_option_ids, reusable_stock_ids

    def _transition_existing_lot(
        self,
        *,
        lot: _WorkflowLot,
        reusable_option_ids: set[int],
        reusable_stock_ids: set[int],
        close_unmatched: bool,
        bar: Any,
        option_gateway: MassiveOptionGateway,
        ex_dividend_dates: set[date],
        current_bar_index: int,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
    ) -> dict[str, Any]:
        keep_option_legs = [leg for leg in lot.position.option_legs if id(leg) in reusable_option_ids or not close_unmatched]
        keep_stock_legs = [leg for leg in lot.position.stock_legs if id(leg) in reusable_stock_ids or not close_unmatched]
        close_option_legs = [leg for leg in lot.position.option_legs if leg not in keep_option_legs]
        close_stock_legs = [leg for leg in lot.position.stock_legs if leg not in keep_stock_legs]

        kept_lots: list[_WorkflowLot] = []
        closed_trades: list[tuple[int, TradeResult]] = []
        cash_delta = Decimal("0")

        if close_option_legs or close_stock_legs:
            closing_position = OpenMultiLegPosition(
                display_ticker=lot.position.display_ticker,
                strategy_type=lot.position.strategy_type,
                underlying_symbol=lot.position.underlying_symbol,
                entry_date=lot.position.entry_date,
                entry_index=lot.position.entry_index,
                quantity=lot.position.quantity,
                dte_at_open=lot.position.dte_at_open,
                option_legs=close_option_legs,
                stock_legs=close_stock_legs,
                scheduled_exit_date=bar.trade_date,
                capital_required_per_unit=max(abs(float(self._engine._entry_value_per_unit(lot.position))), 1.0),
                max_loss_per_unit=lot.position.max_loss_per_unit,
                max_profit_per_unit=lot.position.max_profit_per_unit,
                entry_reason=lot.position.entry_reason,
                entry_commission_total=(lot.position.entry_commission_total / max(len(lot.position.option_legs), 1)) * len(close_option_legs),
                detail_json={**lot.position.detail_json, "workflow_transition": "inventory_reconciled"},
            )
            snapshot = self._engine._mark_position(
                closing_position,
                bar,
                option_gateway,
                warnings,
                warning_codes,
                ex_dividend_dates,
            )
            exit_prices = {leg.ticker: leg.last_mid for leg in closing_position.option_legs}
            exit_prices.update({leg.symbol: leg.last_price for leg in closing_position.stock_legs})
            trade, delta = self._engine._close_position(
                position=closing_position,
                config=lot.config,
                exit_value=snapshot.position_value,
                exit_date=bar.trade_date,
                exit_underlying_close=bar.close_price,
                exit_prices=exit_prices,
                exit_reason="workflow_transition",
                warnings=warnings,
                warning_codes=warning_codes,
                current_bar_index=current_bar_index,
                assignment_detail=snapshot.assignment_detail,
                trade_warnings=snapshot.warnings,
            )
            closed_trades.append((lot.step_number, trade))
            cash_delta += delta

        if keep_option_legs or keep_stock_legs:
            kept_position = OpenMultiLegPosition(
                display_ticker=lot.position.display_ticker,
                strategy_type=lot.position.strategy_type,
                underlying_symbol=lot.position.underlying_symbol,
                entry_date=lot.position.entry_date,
                entry_index=lot.position.entry_index,
                quantity=lot.position.quantity,
                dte_at_open=lot.position.dte_at_open,
                option_legs=keep_option_legs,
                stock_legs=keep_stock_legs,
                scheduled_exit_date=lot.position.scheduled_exit_date,
                capital_required_per_unit=max(abs(float(self._engine._entry_value_per_unit(lot.position))), 1.0),
                max_loss_per_unit=lot.position.max_loss_per_unit,
                max_profit_per_unit=lot.position.max_profit_per_unit,
                entry_reason=lot.position.entry_reason,
                entry_commission_total=(lot.position.entry_commission_total / max(len(lot.position.option_legs), 1)) * len(keep_option_legs),
                detail_json=lot.position.detail_json,
            )
            kept_lots.append(_WorkflowLot(step_number=lot.step_number, position=kept_position, config=lot.config))
        return {"kept_lots": kept_lots, "closed_trades": closed_trades, "cash_delta": cash_delta}

    def _candidate_without_reused_inventory(
        self,
        candidate: OpenMultiLegPosition,
        active_lots: list[_WorkflowLot],
        action: str,
    ) -> OpenMultiLegPosition | None:
        reusable_long_exists = False
        if action in {"sell_premium", "hedge", "roll"}:
            candidate_short_anchor = max((leg.expiration_date for leg in candidate.option_legs if leg.side < 0), default=None)
            for lot in active_lots:
                for existing in lot.position.option_legs:
                    if existing.side <= 0:
                        continue
                    if candidate_short_anchor is not None and existing.expiration_date <= candidate_short_anchor:
                        continue
                    for leg in candidate.option_legs:
                        if leg.side <= 0:
                            continue
                        if existing.contract_type == leg.contract_type and abs(existing.strike_price - leg.strike_price) <= max(0.005, leg.strike_price * 0.0001):
                            reusable_long_exists = True
                            break
                    if reusable_long_exists:
                        break
                if reusable_long_exists:
                    break
        option_legs = [
            leg for leg in candidate.option_legs
            if not (reusable_long_exists and action in {"sell_premium", "hedge", "roll"} and leg.side > 0)
        ]
        stock_legs = list(candidate.stock_legs)
        if not option_legs and not stock_legs:
            return None
        return OpenMultiLegPosition(
            display_ticker=candidate.display_ticker,
            strategy_type=candidate.strategy_type,
            underlying_symbol=candidate.underlying_symbol,
            entry_date=candidate.entry_date,
            entry_index=candidate.entry_index,
            quantity=candidate.quantity,
            dte_at_open=candidate.dte_at_open,
            option_legs=option_legs,
            stock_legs=stock_legs,
            scheduled_exit_date=candidate.scheduled_exit_date,
            capital_required_per_unit=candidate.capital_required_per_unit,
            max_loss_per_unit=candidate.max_loss_per_unit,
            max_profit_per_unit=candidate.max_profit_per_unit,
            entry_reason=candidate.entry_reason,
            detail_json=candidate.detail_json,
        )

    def _expire_lot_and_preserve_survivors(
        self,
        *,
        lot: _WorkflowLot,
        request: CreateMultiStepRunRequest,
        bar: Any,
        snapshot: Any,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
        current_bar_index: int,
    ) -> tuple[TradeResult | None, Decimal, _WorkflowLot | None]:
        expired_option_legs = [leg for leg in lot.position.option_legs if leg.expiration_date <= bar.trade_date]
        surviving_option_legs = [leg for leg in lot.position.option_legs if leg.expiration_date > bar.trade_date]
        surviving_stock_legs = list(lot.position.stock_legs)
        if not expired_option_legs:
            return None, Decimal("0"), lot

        expired_position = OpenMultiLegPosition(
            display_ticker="|".join(leg.ticker for leg in expired_option_legs),
            strategy_type=lot.position.strategy_type,
            underlying_symbol=lot.position.underlying_symbol,
            entry_date=lot.position.entry_date,
            entry_index=lot.position.entry_index,
            quantity=lot.position.quantity,
            dte_at_open=lot.position.dte_at_open,
            option_legs=expired_option_legs,
            stock_legs=[],
            scheduled_exit_date=bar.trade_date,
            capital_required_per_unit=max(sum(abs(leg.entry_mid * 100.0) * leg.quantity_per_unit for leg in expired_option_legs), 1.0),
            max_loss_per_unit=None,
            max_profit_per_unit=None,
            entry_reason=lot.position.entry_reason,
            entry_commission_total=(lot.position.entry_commission_total / max(len(lot.position.option_legs), 1)) * len(expired_option_legs),
            detail_json={**lot.position.detail_json, "workflow_transition": "expired_legs_realized"},
        )
        for leg in expired_position.option_legs:
            leg.last_mid = float(self._engine._intrinsic_value(leg.contract_type, leg.strike_price, bar.close_price))
        exit_prices = {leg.ticker: leg.last_mid for leg in expired_position.option_legs}
        trade, cash_delta = self._engine._close_position(
            position=expired_position,
            config=lot.config,
            exit_value=self._engine._current_position_value(expired_position, bar.close_price),
            exit_date=bar.trade_date,
            exit_underlying_close=bar.close_price,
            exit_prices=exit_prices,
            exit_reason="expiration",
            warnings=warnings,
            warning_codes=warning_codes,
            current_bar_index=current_bar_index,
        )
        if not surviving_option_legs and not surviving_stock_legs:
            return trade, cash_delta, None

        survivor = OpenMultiLegPosition(
            display_ticker="|".join([*(leg.ticker for leg in surviving_option_legs), *(leg.symbol for leg in surviving_stock_legs)]),
            strategy_type=lot.position.strategy_type,
            underlying_symbol=lot.position.underlying_symbol,
            entry_date=lot.position.entry_date,
            entry_index=lot.position.entry_index,
            quantity=lot.position.quantity,
            dte_at_open=max((leg.expiration_date - lot.position.entry_date).days for leg in surviving_option_legs) if surviving_option_legs else 0,
            option_legs=surviving_option_legs,
            stock_legs=surviving_stock_legs,
            scheduled_exit_date=min((leg.expiration_date for leg in surviving_option_legs), default=bar.trade_date + timedelta(days=lot.config.max_holding_days)),
            capital_required_per_unit=max(sum(abs(leg.entry_mid * 100.0) * leg.quantity_per_unit for leg in surviving_option_legs), 1.0),
            max_loss_per_unit=None,
            max_profit_per_unit=None,
            entry_reason=lot.position.entry_reason,
            entry_commission_total=(lot.position.entry_commission_total / max(len(lot.position.option_legs), 1)) * len(surviving_option_legs),
            detail_json={**lot.position.detail_json, "workflow_transition": "surviving_legs_carried_forward"},
        )
        return trade, cash_delta, _WorkflowLot(step_number=lot.step_number, position=survivor, config=lot.config)

    def _liquidate_active_lots(
        self,
        *,
        active_lots: list[_WorkflowLot],
        bar: Any,
        option_gateway: MassiveOptionGateway,
        ex_dividend_dates: set[date],
        current_bar_index: int,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
        exit_reason: str = "workflow_liquidated",
    ) -> dict[str, Any]:
        trades: list[tuple[int, TradeResult]] = []
        cash_delta = Decimal("0")
        for lot in active_lots:
            exit_prices = {leg.ticker: leg.last_mid for leg in lot.position.option_legs}
            exit_prices.update({leg.symbol: leg.last_price for leg in lot.position.stock_legs})
            snapshot = self._engine._mark_position(
                lot.position,
                bar,
                option_gateway,
                warnings,
                warning_codes,
                ex_dividend_dates,
            )
            trade, delta = self._engine._close_position(
                position=lot.position,
                config=lot.config,
                exit_value=snapshot.position_value,
                exit_date=bar.trade_date,
                exit_underlying_close=bar.close_price,
                exit_prices=exit_prices,
                exit_reason=exit_reason,
                warnings=warnings,
                warning_codes=warning_codes,
                current_bar_index=current_bar_index,
                assignment_detail=snapshot.assignment_detail,
                trade_warnings=snapshot.warnings,
            )
            trades.append((lot.step_number, trade))
            cash_delta += delta
        return {"trades": trades, "cash_delta": cash_delta}

    def _persist_success(self, run: MultiStepRun, result: dict[str, Any]) -> None:
        summary = result["summary"]
        run.status = "succeeded"
        run.completed_at = datetime.now(UTC)
        run.error_code = None
        run.error_message = None
        run.warnings_json = result["warnings"]
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
        run.ending_equity = Decimal(str(summary.ending_equity))
        run.expectancy = Decimal(str(summary.expectancy))
        run.profit_factor = _persistable_ratio_metric(summary.profit_factor)
        run.payoff_ratio = _persistable_ratio_metric(summary.payoff_ratio)
        run.sharpe_ratio = _persistable_ratio_metric(summary.sharpe_ratio)
        run.sortino_ratio = _persistable_ratio_metric(summary.sortino_ratio)
        run.cagr_pct = _persistable_ratio_metric(summary.cagr_pct)
        run.calmar_ratio = _persistable_ratio_metric(summary.calmar_ratio)
        run.max_consecutive_wins = summary.max_consecutive_wins
        run.max_consecutive_losses = summary.max_consecutive_losses
        run.recovery_factor = _persistable_ratio_metric(summary.recovery_factor)

        step_rows = list(
            self.session.scalars(select(MultiStepRunStep).where(MultiStepRunStep.run_id == run.id).order_by(MultiStepRunStep.step_number))
        )
        for step_row in step_rows:
            state = result["steps"][step_row.step_number]
            step_row.status = state["status"]
            step_row.triggered_at = state["triggered_at"]
            step_row.executed_at = state["executed_at"]
            step_row.failure_reason = state["failure_reason"]

        for event in result["events"]:
            self.session.add(
                MultiStepStepEvent(
                    run_id=run.id,
                    step_number=event["step_number"],
                    event_type=event["event_type"],
                    event_at=event["event_at"],
                    message=event["message"],
                    payload_json=event["payload_json"],
                )
            )
        for step_number, trade in result["trades"]:
            self.session.add(
                MultiStepTrade(
                    run_id=run.id,
                    step_number=step_number,
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
        for point in result["equity_curve"]:
            self.session.add(
                MultiStepEquityPoint(
                    run_id=run.id,
                    trade_date=point.trade_date,
                    equity=point.equity,
                    cash=point.cash,
                    position_value=point.position_value,
                    drawdown_pct=point.drawdown_pct,
                )
            )
        self.session.commit()

    def _to_history_item(self, run: MultiStepRun) -> MultiStepRunHistoryItemResponse:
        return MultiStepRunHistoryItemResponse(
            id=run.id,
            name=run.name,
            symbol=run.symbol,
            workflow_type=run.workflow_type,
            status=run.status,
            created_at=run.created_at,
            completed_at=run.completed_at,
            summary=_summary_from_run(run),
        )

    def _to_detail_response(self, run: MultiStepRun) -> MultiStepRunDetailResponse:
        steps = list(
            self.session.scalars(select(MultiStepRunStep).where(MultiStepRunStep.run_id == run.id).order_by(MultiStepRunStep.step_number))
        )
        events = list(
            self.session.scalars(select(MultiStepStepEvent).where(MultiStepStepEvent.run_id == run.id).order_by(MultiStepStepEvent.event_at))
        )
        trades = list(
            self.session.scalars(select(MultiStepTrade).where(MultiStepTrade.run_id == run.id).order_by(MultiStepTrade.entry_date))
        )
        equity = list(
            self.session.scalars(select(MultiStepEquityPoint).where(MultiStepEquityPoint.run_id == run.id).order_by(MultiStepEquityPoint.trade_date))
        )
        return MultiStepRunDetailResponse(
            id=run.id,
            name=run.name,
            symbol=run.symbol,
            workflow_type=run.workflow_type,
            status=run.status,
            start_date=run.start_date,
            end_date=run.end_date,
            created_at=run.created_at,
            started_at=run.started_at,
            completed_at=run.completed_at,
            warnings=run.warnings_json,
            error_code=run.error_code,
            error_message=run.error_message,
            summary=_summary_from_run(run),
            steps=[
                MultiStepStepOutcomeResponse(
                    step_number=step.step_number,
                    name=step.name,
                    action=step.action,
                    status=step.status,
                    triggered_at=step.triggered_at,
                    executed_at=step.executed_at,
                    failure_reason=step.failure_reason,
                )
                for step in steps
            ],
            events=[
                MultiStepEventResponse(
                    step_number=event.step_number,
                    event_type=event.event_type,
                    event_at=event.event_at,
                    message=event.message,
                    payload_json=event.payload_json,
                )
                for event in events
            ],
            trades=[MultiStepTradeResponse.model_validate(trade) for trade in trades],
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity],
        )
