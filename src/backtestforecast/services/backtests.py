from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Self
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.backtests.types import BacktestExecutionResult
from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import (
    AppError,
    FeatureLockedError,
    NotFoundError,
    QuotaExceededError,
)
from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade, User
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    BacktestRunHistoryItemResponse,
    BacktestRunListResponse,
    BacktestSummaryResponse,
    BacktestTradeResponse,
    CompareBacktestsRequest,
    CompareBacktestsResponse,
    CreateBacktestRunRequest,
    CurrentUserResponse,
    EquityCurvePointResponse,
    FeatureAccessResponse,
    UsageSummaryResponse,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService

DECIMAL_QUANT = Decimal("0.0001")


def to_decimal(value: float | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


class BacktestService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self.run_repository = BacktestRunRepository(session)
        self._execution_service = execution_service

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def enqueue(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Create a queued backtest run. The caller is responsible for dispatching to Celery."""
        self._enforce_backtest_quota(user)

        # Idempotency: return existing run if key matches
        if request.idempotency_key:
            existing = self.run_repository.get_by_idempotency_key(user.id, request.idempotency_key)
            if existing is not None:
                return existing

        run = BacktestRun(
            user_id=user.id,
            status="queued",
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            date_from=request.start_date,
            date_to=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=to_decimal(request.account_size),
            risk_per_trade_pct=to_decimal(request.risk_per_trade_pct),
            commission_per_contract=to_decimal(request.commission_per_contract),
            input_snapshot_json=request.model_dump(mode="json"),
            idempotency_key=request.idempotency_key,
            warnings_json=[],
            engine_version="options-multileg-v2",
            data_source="massive",
            trade_count=0,
            win_rate=Decimal("0"),
            total_roi_pct=Decimal("0"),
            average_win_amount=Decimal("0"),
            average_loss_amount=Decimal("0"),
            average_holding_period_days=Decimal("0"),
            average_dte_at_open=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            total_commissions=Decimal("0"),
            total_net_pnl=Decimal("0"),
            starting_equity=to_decimal(request.account_size),
            ending_equity=to_decimal(request.account_size),
        )
        self.run_repository.add(run)
        self.session.commit()
        self.session.refresh(run)
        return run

    def set_celery_task_id(self, run_id: UUID, task_id: str) -> None:
        """Attach the Celery task ID after dispatch."""
        run = self.run_repository.get_by_id(run_id)
        if run is not None and run.status == "queued":
            run.celery_task_id = task_id
            self.session.commit()

    def execute_run_by_id(self, run_id: UUID) -> BacktestRun:
        """Execute the backtest for a previously enqueued run. Called by the Celery worker."""
        run = self.run_repository.get_by_id(run_id, for_update=True)
        if run is None:
            raise NotFoundError("Backtest run not found.")

        if run.status not in ("queued", "running"):
            # Already completed or failed — return as-is (idempotent)
            return run

        run.status = "running"
        run.started_at = datetime.now(UTC)
        self.session.commit()

        request = CreateBacktestRunRequest.model_validate(run.input_snapshot_json)

        try:
            execution_result = self.execution_service.execute_request(request)
            self._apply_execution_result(run, execution_result)
            run.status = "succeeded"
            run.completed_at = datetime.now(UTC)
            self.session.commit()
        except AppError as exc:
            run.status = "failed"
            run.error_code = exc.code
            run.error_message = exc.message
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        except Exception:
            run.status = "failed"
            run.error_code = "internal_error"
            run.error_message = "An internal error occurred during backtest execution."
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        finally:
            self.session.expire_all()

        stored = self.run_repository.get_by_id(run.id)
        if stored is None:
            raise NotFoundError("Backtest run was executed but could not be reloaded.")
        return stored

    def create_and_run(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Synchronous create-and-run. Preserved for tests or fallback."""
        self._enforce_backtest_quota(user)
        run = BacktestRun(
            user_id=user.id,
            status="running",
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            date_from=request.start_date,
            date_to=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=to_decimal(request.account_size),
            risk_per_trade_pct=to_decimal(request.risk_per_trade_pct),
            commission_per_contract=to_decimal(request.commission_per_contract),
            input_snapshot_json=request.model_dump(mode="json"),
            idempotency_key=request.idempotency_key,
            warnings_json=[],
            engine_version="options-multileg-v2",
            data_source="massive",
            trade_count=0,
            win_rate=Decimal("0"),
            total_roi_pct=Decimal("0"),
            average_win_amount=Decimal("0"),
            average_loss_amount=Decimal("0"),
            average_holding_period_days=Decimal("0"),
            average_dte_at_open=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            total_commissions=Decimal("0"),
            total_net_pnl=Decimal("0"),
            starting_equity=to_decimal(request.account_size),
            ending_equity=to_decimal(request.account_size),
        )
        self.run_repository.add(run)

        try:
            execution_result = self.execution_service.execute_request(request)
            self._apply_execution_result(run, execution_result)
            run.status = "succeeded"
            run.completed_at = datetime.now(UTC)
            self.session.commit()
        except AppError as exc:
            run.status = "failed"
            run.error_code = exc.code
            run.error_message = exc.message
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        except Exception:
            run.status = "failed"
            run.error_code = "internal_error"
            run.error_message = "An internal error occurred during backtest execution."
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        finally:
            self.session.expire_all()

        stored = self.run_repository.get_for_user(run.id, user.id)
        if stored is None:
            raise NotFoundError("Backtest run was created but could not be reloaded.")
        return stored

    def list_runs(self, user: User, limit: int = 50) -> BacktestRunListResponse:
        feature_policy = resolve_feature_policy(user.plan_tier, user.subscription_status)
        created_since = None
        if feature_policy.history_days is not None:
            created_since = datetime.now(UTC) - timedelta(days=feature_policy.history_days)
        effective_limit = min(limit, feature_policy.history_item_limit)
        runs = self.run_repository.list_for_user(user.id, limit=effective_limit, created_since=created_since)
        return BacktestRunListResponse(items=[self._to_history_item(run) for run in runs])

    def get_run(self, user: User, run_id: UUID) -> BacktestRunDetailResponse:
        return self.get_run_for_owner(user_id=user.id, run_id=run_id)

    def get_run_for_owner(self, *, user_id: UUID, run_id: UUID) -> BacktestRunDetailResponse:
        run = self.run_repository.get_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        return self._to_detail_response(run)

    def compare_runs(self, user: User, request: CompareBacktestsRequest) -> CompareBacktestsResponse:
        feature_policy = resolve_feature_policy(user.plan_tier, user.subscription_status)
        limit = feature_policy.side_by_side_comparison_limit

        if len(request.run_ids) > limit:
            raise FeatureLockedError(
                f"Your {feature_policy.tier.value} plan allows comparing up to {limit} runs at a time. "
                f"You requested {len(request.run_ids)}. Upgrade for more.",
                required_tier="pro" if feature_policy.tier.value == "free" else "premium",
            )

        runs = self.run_repository.get_many_for_user(request.run_ids, user.id)

        # Preserve the requested order and report missing IDs
        run_map = {run.id: run for run in runs}
        found_ids = set(run_map.keys())
        missing_ids = [rid for rid in request.run_ids if rid not in found_ids]
        if missing_ids:
            raise NotFoundError(f"One or more runs could not be found: {', '.join(str(rid) for rid in missing_ids)}")

        ordered = [run_map[rid] for rid in request.run_ids]
        return CompareBacktestsResponse(
            items=[self._to_detail_response(run) for run in ordered],
            comparison_limit=limit,
        )

    def to_current_user_response(self, user: User) -> CurrentUserResponse:
        feature_policy = resolve_feature_policy(user.plan_tier, user.subscription_status)
        used_this_month = self._current_month_backtest_count(user)
        remaining = None
        if feature_policy.monthly_backtest_quota is not None:
            remaining = max(feature_policy.monthly_backtest_quota - used_this_month, 0)
        scanner_modes: list[str] = []
        if feature_policy.basic_scanner_access:
            scanner_modes.append("basic")
        if feature_policy.advanced_scanner_access:
            scanner_modes.append("advanced")
        return CurrentUserResponse(
            id=user.id,
            clerk_user_id=user.clerk_user_id,
            email=user.email,
            plan_tier=feature_policy.tier.value,
            subscription_status=user.subscription_status,
            subscription_billing_interval=user.subscription_billing_interval,
            subscription_current_period_end=user.subscription_current_period_end,
            cancel_at_period_end=user.cancel_at_period_end,
            created_at=user.created_at,
            features=FeatureAccessResponse(
                plan_tier=feature_policy.tier.value,
                monthly_backtest_quota=feature_policy.monthly_backtest_quota,
                history_days=feature_policy.history_days,
                history_item_limit=feature_policy.history_item_limit,
                side_by_side_comparison_limit=feature_policy.side_by_side_comparison_limit,
                forecasting_access=feature_policy.forecasting_access,
                export_formats=[
                    fmt.value for fmt in sorted(feature_policy.export_formats, key=lambda item: item.value)
                ],
                scanner_modes=scanner_modes,
            ),
            usage=UsageSummaryResponse(
                backtests_used_this_month=used_this_month,
                backtests_remaining_this_month=remaining,
            ),
        )

    def _enforce_backtest_quota(self, user: User) -> None:
        # Lock the user row to serialize concurrent quota checks (prevents TOCTOU)
        self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        ).scalar_one()

        feature_policy = resolve_feature_policy(user.plan_tier, user.subscription_status)
        if feature_policy.monthly_backtest_quota is None:
            return
        used_this_month = self._current_month_backtest_count(user)
        if used_this_month >= feature_policy.monthly_backtest_quota:
            raise QuotaExceededError(
                f"The {feature_policy.tier.value} plan allows "
                f"{feature_policy.monthly_backtest_quota} backtests per month. "
                f"You have used {used_this_month}. Upgrade your plan for more.",
                current_tier=feature_policy.tier.value,
            )

    def _current_month_backtest_count(self, user: User) -> int:
        now = datetime.now(UTC)
        month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        if now.month == 12:
            next_month_start = datetime(now.year + 1, 1, 1, tzinfo=UTC)
        else:
            next_month_start = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
        return self.run_repository.count_for_user_created_between(
            user.id,
            start_inclusive=month_start,
            end_exclusive=next_month_start,
        )

    def _apply_execution_result(self, run: BacktestRun, execution_result: BacktestExecutionResult) -> None:
        summary = execution_result.summary

        run.warnings_json = execution_result.warnings
        run.trade_count = summary.trade_count
        run.win_rate = to_decimal(summary.win_rate)
        run.total_roi_pct = to_decimal(summary.total_roi_pct)
        run.average_win_amount = to_decimal(summary.average_win_amount)
        run.average_loss_amount = to_decimal(summary.average_loss_amount)
        run.average_holding_period_days = to_decimal(summary.average_holding_period_days)
        run.average_dte_at_open = to_decimal(summary.average_dte_at_open)
        run.max_drawdown_pct = to_decimal(summary.max_drawdown_pct)
        run.total_commissions = to_decimal(summary.total_commissions)
        run.total_net_pnl = to_decimal(summary.total_net_pnl)
        run.starting_equity = to_decimal(summary.starting_equity)
        run.ending_equity = to_decimal(summary.ending_equity)

        for trade in execution_result.trades:
            run.trades.append(
                BacktestTrade(
                    option_ticker=trade.option_ticker,
                    strategy_type=trade.strategy_type,
                    underlying_symbol=trade.underlying_symbol,
                    entry_date=trade.entry_date,
                    exit_date=trade.exit_date,
                    expiration_date=trade.expiration_date,
                    quantity=trade.quantity,
                    dte_at_open=trade.dte_at_open,
                    holding_period_days=trade.holding_period_days,
                    entry_underlying_close=to_decimal(trade.entry_underlying_close),
                    exit_underlying_close=to_decimal(trade.exit_underlying_close),
                    entry_mid=to_decimal(trade.entry_mid),
                    exit_mid=to_decimal(trade.exit_mid),
                    gross_pnl=to_decimal(trade.gross_pnl),
                    net_pnl=to_decimal(trade.net_pnl),
                    total_commissions=to_decimal(trade.total_commissions),
                    entry_reason=trade.entry_reason,
                    exit_reason=trade.exit_reason,
                    detail_json=trade.detail_json,
                )
            )

        for point in execution_result.equity_curve:
            run.equity_points.append(
                BacktestEquityPoint(
                    trade_date=point.trade_date,
                    equity=to_decimal(point.equity),
                    cash=to_decimal(point.cash),
                    position_value=to_decimal(point.position_value),
                    drawdown_pct=to_decimal(point.drawdown_pct),
                )
            )

    @staticmethod
    def _summary_response(run: BacktestRun) -> BacktestSummaryResponse:
        return BacktestSummaryResponse(
            trade_count=run.trade_count,
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
        )

    def _to_history_item(self, run: BacktestRun) -> BacktestRunHistoryItemResponse:
        return BacktestRunHistoryItemResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            date_from=run.date_from,
            date_to=run.date_to,
            target_dte=run.target_dte,
            max_holding_days=run.max_holding_days,
            created_at=run.created_at,
            completed_at=run.completed_at,
            summary=self._summary_response(run),
        )

    def _to_detail_response(self, run: BacktestRun) -> BacktestRunDetailResponse:
        return BacktestRunDetailResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            date_from=run.date_from,
            date_to=run.date_to,
            target_dte=run.target_dte,
            dte_tolerance_days=run.dte_tolerance_days,
            max_holding_days=run.max_holding_days,
            account_size=run.account_size,
            risk_per_trade_pct=run.risk_per_trade_pct,
            commission_per_contract=run.commission_per_contract,
            engine_version=run.engine_version,
            data_source=run.data_source,
            created_at=run.created_at,
            started_at=run.started_at,
            completed_at=run.completed_at,
            warnings=run.warnings_json,
            error_code=run.error_code,
            error_message=run.error_message,
            summary=self._summary_response(run),
            trades=[BacktestTradeResponse.model_validate(trade) for trade in run.trades],
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in run.equity_points],
        )
