from __future__ import annotations

import time as _time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Self
from uuid import UUID, uuid4

import structlog
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.backtests.types import BacktestExecutionResult
from backtestforecast.config import get_settings
from backtestforecast.schemas.json_shapes import _TRADE_DETAIL_REQUIRED_KEYS, validate_json_shape
from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import (
    AppError,
    AppValidationError,
    ConflictError,
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
    BacktestRunStatusResponse,
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
from backtestforecast.observability.metrics import BACKTEST_EXECUTION_DURATION_SECONDS
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.utils import to_decimal

logger = structlog.get_logger("services.backtests")

EQUITY_CURVE_LIMIT = 10_000

from backtestforecast.utils import decode_cursor as _decode_cursor, encode_cursor as _encode_cursor


class BacktestService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
    ) -> None:
        self.session = session
        self.run_repository = BacktestRunRepository(session)
        self.audit = AuditService(session)
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

    @staticmethod
    def _build_initial_run(
        user_id: UUID,
        request: CreateBacktestRunRequest,
        *,
        status: str = "queued",
        started_at: datetime | None = None,
    ) -> BacktestRun:
        return BacktestRun(
            user_id=user_id,
            status=status,
            started_at=started_at,
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
            risk_free_rate=to_decimal(request.risk_free_rate if request.risk_free_rate is not None else get_settings().risk_free_rate),
            input_snapshot_json={
                **request.model_dump(mode="json"),
                "risk_free_rate": float(request.risk_free_rate) if request.risk_free_rate is not None else get_settings().risk_free_rate,
                "dividend_yield": float(request.dividend_yield) if request.dividend_yield is not None else 0.0,
            },
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

    def enqueue(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Create a queued backtest run. The caller is responsible for dispatching to Celery."""
        if request.idempotency_key:
            existing = self.run_repository.get_by_idempotency_key(user.id, request.idempotency_key)
            if existing is not None:
                return existing

        self._enforce_backtest_quota(user)

        run = self._build_initial_run(user.id, request, status="queued")
        self.run_repository.add(run)
        self.audit.record(
            event_type="backtest.created",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user.id,
            metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
        )
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            if request.idempotency_key:
                stmt = select(BacktestRun).where(
                    BacktestRun.user_id == user.id,
                    BacktestRun.idempotency_key == request.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise
        self.session.refresh(run)
        return run

    def execute_run_by_id(self, run_id: UUID) -> BacktestRun:
        """Execute the backtest for a previously enqueued run. Called by the Celery worker."""
        run = self.run_repository.get_by_id_unfiltered(run_id, for_update=True)
        if run is None:
            raise NotFoundError("Backtest run not found.")

        if run.status not in ("queued", "running"):
            return run

        user = self.session.get(User, run.user_id)
        if user is None:
            run.status = "failed"
            run.error_code = "user_not_found"
            run.error_message = "User account not found."
            run.completed_at = datetime.now(UTC)
            self.session.commit()
            return run

        _transition_ts = datetime.now(UTC)
        rows = self.session.execute(
            update(BacktestRun)
            .where(BacktestRun.id == run_id, BacktestRun.status == "queued")
            .values(status="running", updated_at=_transition_ts, started_at=_transition_ts)
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(run)
            return run
        self.session.refresh(run)

        request = CreateBacktestRunRequest.model_validate(run.input_snapshot_json)

        _exec_start = _time.monotonic()
        try:
            execution_result = self.execution_service.execute_request(request)
            with self.session.no_autoflush:
                self._apply_execution_result(run, execution_result)
                completed_at = datetime.now(UTC)
                # CAS guard: only set status to "succeeded" if the reaper has not
                # concurrently marked this run as "failed".  The FOR UPDATE lock
                # was released by the commit on the queued→running transition, so
                # the reaper can legitimately change status while execution is in
                # progress.  no_autoflush prevents the ORM dirty state from being
                # flushed (and acquiring a row lock) before the CAS UPDATE, ensuring
                # the reaper can win the race if it changed status first.
                success_rows = self.session.execute(
                    update(BacktestRun)
                    .where(BacktestRun.id == run.id, BacktestRun.status == "running")
                    .values(
                        status="succeeded",
                        completed_at=completed_at,
                        updated_at=datetime.now(UTC),
                    )
                )
            if success_rows.rowcount == 0:
                self.session.rollback()
                logger.warning(
                    "backtest.success_overwrite_prevented",
                    run_id=str(run.id),
                    msg="Concurrent status change detected; success commit skipped.",
                )
            else:
                run.status = "succeeded"
                run.completed_at = completed_at
                self.audit.record_always(
                    event_type="backtest.completed",
                    subject_type="backtest_run",
                    subject_id=run.id,
                    user_id=run.user_id,
                    metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
                )
                self.session.commit()
        except AppError as exc:
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status.notin_(["succeeded", "cancelled"]))
                .values(
                    status="failed",
                    error_code=exc.code,
                    error_message="Backtest execution failed. Please try again.",
                    updated_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except Exception:
            logger.exception("backtest.execution_failed", run_id=str(run.id))
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status.notin_(["succeeded", "cancelled"]))
                .values(
                    status="failed",
                    error_code="internal_error",
                    error_message="An internal error occurred during backtest execution.",
                    updated_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        finally:
            BACKTEST_EXECUTION_DURATION_SECONDS.observe(_time.monotonic() - _exec_start)
            self.session.expire_all()

        stored = self.session.get(BacktestRun, run.id)
        if stored is None:
            raise NotFoundError("Backtest run was executed but could not be reloaded.")
        return stored

    def create_and_run(self, user: User, request: CreateBacktestRunRequest) -> BacktestRun:
        """Synchronous create-and-run for tests only.

        WARNING: Do not call from production code paths. Use ``enqueue``
        followed by the Celery task instead. This method bypasses the
        dispatch layer and holds a DB connection for the entire execution.
        """
        settings = get_settings()
        if settings.app_env not in ("test", "development"):
            raise RuntimeError(
                "create_and_run is for tests only; use enqueue + Celery in production"
            )
        if request.idempotency_key:
            existing = self.run_repository.get_by_idempotency_key(user.id, request.idempotency_key)
            if existing is not None:
                return existing

        self._enforce_backtest_quota(user)

        run = self._build_initial_run(user.id, request, status="running", started_at=datetime.now(UTC))
        self.run_repository.add(run)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            if request.idempotency_key:
                stmt = select(BacktestRun).where(
                    BacktestRun.user_id == user.id,
                    BacktestRun.idempotency_key == request.idempotency_key,
                )
                existing = self.session.scalar(stmt)
                if existing is not None:
                    return existing
            raise

        try:
            execution_result = self.execution_service.execute_request(request)
            self._apply_execution_result(run, execution_result)
            run.status = "succeeded"
            run.completed_at = datetime.now(UTC)
            self.session.commit()
        except AppError as exc:
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
                .values(
                    status="failed",
                    error_code=exc.code,
                    error_message="Backtest execution failed. Please try again.",
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except Exception:
            logger.exception("backtest.execution_failed", run_id=str(run.id))
            self.session.rollback()
            self.session.execute(
                update(BacktestRun)
                .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
                .values(
                    status="failed",
                    error_code="internal_error",
                    error_message="An internal error occurred during backtest execution.",
                    completed_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        finally:
            self.session.expire_all()

        stored = self.run_repository.get_for_user(run.id, user.id)
        if stored is None:
            raise NotFoundError("Backtest run was created but could not be reloaded.")
        return stored

    def list_runs(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        cursor: str | None = None,
    ) -> BacktestRunListResponse:
        """List backtest runs for the user. Returns BacktestRunHistoryItemResponse items
        without equity curve data; equity curves are only included in detail/compare responses.

        Supports both offset-based and cursor-based pagination.  When
        ``cursor`` is provided, ``offset`` is ignored and keyset pagination
        is used (no scan-and-discard overhead at high page numbers).
        """
        if limit < 1:
            raise AppValidationError("limit must be >= 1")
        if offset < 0:
            raise AppValidationError("offset must be >= 0")
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
        created_since = None
        if feature_policy.history_days is not None:
            created_since = datetime.now(UTC) - timedelta(days=feature_policy.history_days)
        effective_limit = min(limit, feature_policy.history_item_limit, 200)

        cursor_before = None
        if cursor:
            cursor_before = _decode_cursor(cursor)
            if cursor_before is None:
                raise AppValidationError("Invalid pagination cursor.")
            offset = 0

        runs, total = self.run_repository.list_for_user_with_count(
            user.id,
            limit=effective_limit + 1,
            offset=offset,
            created_since=created_since,
            cursor_before=cursor_before,
        )

        has_next = len(runs) > effective_limit
        if has_next:
            runs = runs[:effective_limit]

        capped_total = min(total, feature_policy.history_item_limit)

        next_cursor = None
        if has_next and runs:
            next_cursor = _encode_cursor(runs[-1].created_at)

        return BacktestRunListResponse(
            items=[self._to_history_item(run) for run in runs],
            total=capped_total,
            offset=offset,
            limit=effective_limit,
            next_cursor=next_cursor,
        )

    def get_run_status(self, user: User, run_id: UUID) -> BacktestRunStatusResponse:
        """Lightweight status check without loading trades/equity."""
        run = self.run_repository.get_lightweight_for_user(run_id, user.id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        return BacktestRunStatusResponse(
            id=run.id,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_code=run.error_code,
            error_message=run.error_message,
        )

    def get_run_for_owner(self, *, user_id: UUID, run_id: UUID, trade_limit: int = 10_000) -> BacktestRunDetailResponse:
        run = self.run_repository.get_lightweight_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        trades = self.run_repository.get_trades_for_run(run_id, limit=trade_limit, user_id=user_id)
        equity = self.run_repository.get_equity_points_for_run(run_id, limit=EQUITY_CURVE_LIMIT, user_id=user_id)
        return self._to_detail_response(run, trades=trades, equity_points=equity)

    def delete_for_user(self, run_id: UUID, user_id: UUID) -> None:
        run = self.run_repository.get_lightweight_for_user(run_id, user_id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        if run.status in ("queued", "running"):
            raise ConflictError(
                "Cannot delete a job that is currently queued or running. Cancel it first."
            )
        self.audit.record(
            event_type="backtest.deleted",
            subject_type="backtest_run",
            subject_id=run.id,
            user_id=user_id,
            metadata={"symbol": run.symbol, "strategy_type": run.strategy_type},
        )
        self.session.delete(run)
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def compare_runs(self, user: User, request: CompareBacktestsRequest) -> CompareBacktestsResponse:
        if len(request.run_ids) != len(set(request.run_ids)):
            raise AppValidationError("Duplicate run IDs are not allowed in comparison requests.")
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
        limit = feature_policy.side_by_side_comparison_limit

        if len(request.run_ids) > limit:
            tier_name = feature_policy.tier.value
            if tier_name == "premium":
                raise QuotaExceededError(
                    f"You can compare up to {limit} runs at a time. "
                    f"You requested {len(request.run_ids)}.",
                    current_tier="premium",
                )
            required = "pro" if tier_name == "free" else "premium"
            raise FeatureLockedError(
                f"Your {tier_name} plan allows comparing up to {limit} runs at a time. "
                f"You requested {len(request.run_ids)}. "
                f"Upgrade to the {required} plan to compare more runs side-by-side.",
                required_tier=required,
            )

        runs = self.run_repository.get_many_for_user(request.run_ids, user.id)

        # Preserve the requested order and report missing IDs
        run_map = {run.id: run for run in runs}
        found_ids = set(run_map.keys())
        missing_ids = [rid for rid in request.run_ids if rid not in found_ids]
        if missing_ids:
            raise NotFoundError(f"One or more runs could not be found: {', '.join(str(rid) for rid in missing_ids)}")

        non_succeeded = [rid for rid in request.run_ids if run_map[rid].status != "succeeded"]
        if non_succeeded:
            raise AppValidationError(
                f"All runs must have status 'succeeded' to compare. "
                f"Non-succeeded: {', '.join(str(rid) for rid in non_succeeded)}"
            )

        ordered = [run_map[rid] for rid in request.run_ids]
        _MAX_TOTAL_COMPARE_TRADES = 8_000
        _DEFAULT_COMPARE_TRADE_LIMIT = 2_000
        num_runs = len(ordered)
        trade_limit = min(_DEFAULT_COMPARE_TRADE_LIMIT, _MAX_TOTAL_COMPARE_TRADES // max(num_runs, 1))
        all_run_ids = [r.id for r in ordered]
        trade_batches_by_run = self.run_repository.get_trades_for_runs(
            all_run_ids, limit_per_run=trade_limit, user_id=user.id,
        )
        equity_by_run = self.run_repository.get_equity_points_for_runs(all_run_ids, limit_per_run=EQUITY_CURVE_LIMIT, user_id=user.id)
        truncated = any(
            trade_batches_by_run.get(run.id) is not None
            and trade_batches_by_run[run.id].exceeded_limit
            for run in ordered
        )
        return CompareBacktestsResponse(
            items=[
                self._to_detail_response(
                    run,
                    trades=(trade_batches_by_run[run.id].trades if run.id in trade_batches_by_run else []),
                    equity_points=equity_by_run.get(run.id, []),
                )
                for run in ordered
            ],
            comparison_limit=limit,
            trade_limit_per_run=trade_limit,
            trades_truncated=truncated,
        )

    def to_current_user_response(self, user: User) -> CurrentUserResponse:
        feature_policy = resolve_feature_policy(
            user.plan_tier, user.subscription_status, user.subscription_current_period_end,
        )
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
                cancel_at_period_end=user.cancel_at_period_end,
            ),
            usage=UsageSummaryResponse(
                backtests_used_this_month=used_this_month,
                backtests_remaining_this_month=remaining,
            ),
        )

    def _enforce_backtest_quota(self, user: User) -> None:
        locked_user = self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        ).scalar_one_or_none()
        if locked_user is None:
            raise NotFoundError("User not found.")

        feature_policy = resolve_feature_policy(
            locked_user.plan_tier, locked_user.subscription_status, locked_user.subscription_current_period_end,
        )
        if feature_policy.monthly_backtest_quota is None:
            return
        used_this_month = self._current_month_backtest_count(locked_user)
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
        run.win_rate = to_decimal(summary.win_rate) or Decimal("0")
        run.total_roi_pct = to_decimal(summary.total_roi_pct) or Decimal("0")
        run.average_win_amount = to_decimal(summary.average_win_amount) or Decimal("0")
        run.average_loss_amount = to_decimal(summary.average_loss_amount) or Decimal("0")
        run.average_holding_period_days = to_decimal(summary.average_holding_period_days) or Decimal("0")
        run.average_dte_at_open = to_decimal(summary.average_dte_at_open) or Decimal("0")
        run.max_drawdown_pct = to_decimal(summary.max_drawdown_pct) or Decimal("0")
        run.total_commissions = to_decimal(summary.total_commissions) or Decimal("0")
        run.total_net_pnl = to_decimal(summary.total_net_pnl) or Decimal("0")
        run.starting_equity = to_decimal(summary.starting_equity) or Decimal("0")
        run.ending_equity = to_decimal(summary.ending_equity) or Decimal("0")
        run.profit_factor = to_decimal(summary.profit_factor, allow_infinite=True) if summary.profit_factor is not None else None
        run.payoff_ratio = to_decimal(summary.payoff_ratio, allow_infinite=True) if summary.payoff_ratio is not None else None
        run.expectancy = to_decimal(summary.expectancy) or Decimal("0")
        run.sharpe_ratio = to_decimal(summary.sharpe_ratio, allow_infinite=True) if summary.sharpe_ratio is not None else None
        run.sortino_ratio = to_decimal(summary.sortino_ratio, allow_infinite=True) if summary.sortino_ratio is not None else None
        run.cagr_pct = to_decimal(summary.cagr_pct, allow_infinite=True) if summary.cagr_pct is not None else None
        run.calmar_ratio = to_decimal(summary.calmar_ratio, allow_infinite=True) if summary.calmar_ratio is not None else None
        run.max_consecutive_wins = summary.max_consecutive_wins
        run.max_consecutive_losses = summary.max_consecutive_losses
        run.recovery_factor = to_decimal(summary.recovery_factor, allow_infinite=True) if summary.recovery_factor is not None else None

        _MAX_TRADES = 10_000
        _MAX_EQUITY_POINTS = 10_000
        _BATCH_SIZE = 2_000

        with self.session.no_autoflush:
            if execution_result.trades:
                trades_to_insert = execution_result.trades[:_MAX_TRADES]
                if len(execution_result.trades) > _MAX_TRADES:
                    logger.warning(
                        "backtests.trades_capped",
                        run_id=str(run.id),
                        total=len(execution_result.trades),
                        cap=_MAX_TRADES,
                    )
                trade_dicts: list[dict] = []
                for trade in trades_to_insert:
                    if not validate_json_shape(trade.detail_json, "BacktestTrade.detail_json", required_keys=_TRADE_DETAIL_REQUIRED_KEYS):
                        logger.warning("backtests.malformed_trade_detail_json", option_ticker=trade.option_ticker, keys=list(trade.detail_json.keys()) if trade.detail_json else [])
                    trade_dicts.append({
                        "id": uuid4(),
                        "run_id": run.id,
                        "option_ticker": trade.option_ticker,
                        "strategy_type": trade.strategy_type,
                        "underlying_symbol": trade.underlying_symbol,
                        "entry_date": trade.entry_date,
                        "exit_date": trade.exit_date,
                        "expiration_date": trade.expiration_date,
                        "quantity": trade.quantity,
                        "dte_at_open": trade.dte_at_open,
                        "holding_period_days": trade.holding_period_days,
                        "holding_period_trading_days": trade.holding_period_trading_days,
                        "entry_underlying_close": to_decimal(trade.entry_underlying_close),
                        "exit_underlying_close": to_decimal(trade.exit_underlying_close),
                        "entry_mid": to_decimal(trade.entry_mid),
                        "exit_mid": to_decimal(trade.exit_mid),
                        "gross_pnl": to_decimal(trade.gross_pnl),
                        "net_pnl": to_decimal(trade.net_pnl),
                        "total_commissions": to_decimal(trade.total_commissions),
                        "entry_reason": trade.entry_reason,
                        "exit_reason": trade.exit_reason,
                        "detail_json": trade.detail_json,
                    })
                for batch_start in range(0, len(trade_dicts), _BATCH_SIZE):
                    self.session.execute(insert(BacktestTrade).values(trade_dicts[batch_start:batch_start + _BATCH_SIZE]))

            if execution_result.equity_curve:
                curve_to_insert = execution_result.equity_curve[:_MAX_EQUITY_POINTS]
                if len(execution_result.equity_curve) > _MAX_EQUITY_POINTS:
                    logger.warning(
                        "backtests.equity_points_capped",
                        run_id=str(run.id),
                        total=len(execution_result.equity_curve),
                        cap=_MAX_EQUITY_POINTS,
                    )
                equity_dicts: list[dict] = []
                for point in curve_to_insert:
                    equity_dicts.append({
                        "id": uuid4(),
                        "run_id": run.id,
                        "trade_date": point.trade_date,
                        "equity": to_decimal(point.equity),
                        "cash": to_decimal(point.cash),
                        "position_value": to_decimal(point.position_value),
                        "drawdown_pct": to_decimal(point.drawdown_pct),
                    })
                for batch_start in range(0, len(equity_dicts), _BATCH_SIZE):
                    self.session.execute(insert(BacktestEquityPoint).values(equity_dicts[batch_start:batch_start + _BATCH_SIZE]))

        self.session.expire(run, ["trades", "equity_points"])

    @staticmethod
    def _summary_response(
        run: BacktestRun,
        *,
        trades: list[BacktestTrade] | None = None,
    ) -> BacktestSummaryResponse:
        decided: int | None = None
        if trades is not None:
            decided = sum(1 for t in trades if t.net_pnl != 0)
        return BacktestSummaryResponse(
            trade_count=run.trade_count,
            decided_trades=decided,
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

    @staticmethod
    def _resolve_risk_free_rate(run: BacktestRun) -> float:
        """Return the risk-free rate used for this run.

        Prefers the persisted column (added in migration 0031).  For older
        runs where the column is NULL, reads the value from
        ``input_snapshot_json`` (stored at creation time).  Falls back to
        the current settings value only as a last resort.
        """
        if run.risk_free_rate is not None:
            return float(run.risk_free_rate)
        snapshot = run.input_snapshot_json or {}
        snapshot_rate = snapshot.get("risk_free_rate")
        if snapshot_rate is not None:
            try:
                return float(snapshot_rate)
            except (TypeError, ValueError):
                pass
        return get_settings().risk_free_rate

    def _to_history_item(self, run: BacktestRun) -> BacktestRunHistoryItemResponse:
        return BacktestRunHistoryItemResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            start_date=run.date_from,
            end_date=run.date_to,
            target_dte=run.target_dte,
            max_holding_days=run.max_holding_days,
            created_at=run.created_at,
            completed_at=run.completed_at,
            summary=self._summary_response(run),
        )

    def _to_detail_response(
        self,
        run: BacktestRun,
        *,
        trade_limit: int = 10_000,
        trades: list[BacktestTrade] | None = None,
        equity_points: list[BacktestEquityPoint] | None = None,
    ) -> BacktestRunDetailResponse:
        if trades is None:
            trades = self.run_repository.get_trades_for_run(run.id, limit=trade_limit, user_id=run.user_id)
        if equity_points is None:
            equity_points = self.run_repository.get_equity_points_for_run(run.id, limit=EQUITY_CURVE_LIMIT, user_id=run.user_id)
        return BacktestRunDetailResponse(
            id=run.id,
            symbol=run.symbol,
            strategy_type=run.strategy_type,
            status=run.status,
            start_date=run.date_from,
            end_date=run.date_to,
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
            summary=self._summary_response(run, trades=trades),
            trades=[BacktestTradeResponse.model_validate(trade) for trade in trades],
            equity_curve=[EquityCurvePointResponse.model_validate(point) for point in equity_points],
            equity_curve_truncated=len(equity_points) >= EQUITY_CURVE_LIMIT,
            risk_free_rate=float(run.risk_free_rate) if run.risk_free_rate is not None else get_settings().risk_free_rate,
        )
