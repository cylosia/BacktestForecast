from __future__ import annotations

import random
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID, uuid4

_ModelT = TypeVar("_ModelT")  # Used by _find_pipeline_run for type-safe model lookups

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session

import structlog
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from apps.worker.app.celery_app import celery_app
from backtestforecast.db.session import create_worker_session
from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import AppError, ExternalServiceError
from backtestforecast.events import _VALID_TARGET_STATUSES, publish_job_status
from backtestforecast.observability.metrics import (
    ANALYSIS_JOBS_TOTAL,
    BACKTEST_RUNS_TOTAL,
    CELERY_TASKS_TOTAL,
    DLQ_DEPTH,
    DLQ_MESSAGES_TOTAL,
    DLQ_WRITE_FAILURES_TOTAL,
    DUPLICATE_TASK_EXECUTION_TOTAL,
    EXPORT_JOBS_TOTAL,
    NIGHTLY_PIPELINE_RUNS_TOTAL,
    REAPER_DURATION_SECONDS,
    SCAN_JOBS_TOTAL,
    SWEEP_JOBS_TOTAL,
)
from backtestforecast.models import (
    BacktestRun,
    ExportJob as ExportJobModel,
    ScannerJob as ScannerJobModel,
    SweepJob as SweepJobModel,
    SymbolAnalysis,
    User,
)
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.exports import ExportService
from backtestforecast.services.scans import ScanService
from backtestforecast.services.sweeps import SweepService

logger = structlog.get_logger("worker.tasks")


from apps.worker.app.task_helpers import commit_then_publish as _commit_then_publish


def _mark_job_failed(
    session: "Session",
    model_cls: type,
    obj_id: UUID,
    *,
    error_code: str,
    error_message: str,
    allowed_from: tuple[str, ...] = ("queued", "running"),
) -> None:
    """Mark a job as failed if it is in one of the allowed source statuses."""
    from datetime import UTC, datetime
    from sqlalchemy import update

    obj = session.get(model_cls, obj_id)
    if obj is not None and getattr(obj, "status", None) in allowed_from:
        values: dict[str, object] = {
            "status": "failed",
            "error_message": error_message,
            "completed_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        if hasattr(model_cls, "error_code"):
            values["error_code"] = error_code
        session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id, model_cls.status.in_(allowed_from))
            .values(**values)
        )
        try:
            session.commit()
        except Exception:
            logger.exception("mark_job_failed.commit_failed", model=model_cls.__name__, obj_id=str(obj_id))
            session.rollback()


_dlq_redis_pool: object | None = None
_dlq_redis_lock = __import__("threading").Lock()


def _get_dlq_redis():
    """Return a reusable Redis connection for DLQ writes.

    Uses a module-level connection pool so repeated DLQ writes during
    cascading failures don't exhaust connections.
    """
    global _dlq_redis_pool
    if _dlq_redis_pool is not None:
        return __import__("redis").Redis(connection_pool=_dlq_redis_pool)
    with _dlq_redis_lock:
        if _dlq_redis_pool is not None:
            return __import__("redis").Redis(connection_pool=_dlq_redis_pool)
        from backtestforecast.config import get_settings
        from redis import ConnectionPool
        _dlq_redis_pool = ConnectionPool.from_url(
            get_settings().redis_url,
            socket_timeout=5,
            max_connections=3,
            decode_responses=False,
        )
        return __import__("redis").Redis(connection_pool=_dlq_redis_pool)


class BaseTaskWithDLQ(celery_app.Task):  # type: ignore[misc]
    """Base class for Celery tasks that persists failure metadata to a Redis
    dead-letter list (``bff:dead_letter_queue``) when all retries are exhausted.

    Usage: set ``base=BaseTaskWithDLQ`` in ``@celery_app.task(...)`` decorators.
    Failed tasks are JSON-serialised and left-pushed so operators can inspect
    or replay them via ``LRANGE bff:dead_letter_queue 0 -1``.
    """

    def before_start(self, task_id, args, kwargs):
        super().before_start(task_id, args, kwargs)
        headers = getattr(self.request, 'headers', None) or {}
        if isinstance(headers, dict):
            ctx: dict[str, str] = {}
            if headers.get('traceparent'):
                ctx['traceparent'] = headers['traceparent']
            if headers.get('request_id'):
                ctx['request_id'] = headers['request_id']
            if ctx:
                structlog.contextvars.bind_contextvars(**ctx)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        is_terminal = (
            (self.max_retries is not None and self.request.retries >= self.max_retries)
            or isinstance(exc, SoftTimeLimitExceeded)
        )
        if is_terminal:
            logger.error(
                "task.dead_letter",
                task_name=self.name,
                task_id=task_id,
                args=args,
                retries=self.request.retries,
                exc=str(exc),
            )
            try:
                import json
                redis_conn = self.app.backend.client if hasattr(self.app, 'backend') and hasattr(self.app.backend, 'client') else None
                if redis_conn is None:
                    redis_conn = _get_dlq_redis()
                _DLQ_REDACT_KEYS = frozenset({
                    "email", "password", "secret", "token", "api_key",
                    "stripe_customer_id", "stripe_subscription_id",
                    "clerk_user_id", "ip_address", "ip_hash",
                })

                def _redact(d: dict) -> dict:
                    return {k: "[REDACTED]" if k in _DLQ_REDACT_KEYS else v for k, v in d.items()}

                def _sanitize_error(err_str: str) -> str:
                    """Truncate and strip sensitive patterns from error messages."""
                    truncated = err_str[:2000]
                    import re
                    truncated = re.sub(
                        r"(password|secret|token|api_key|bearer)\s*[=:]\s*\S+",
                        r"\1=[REDACTED]",
                        truncated,
                        flags=re.IGNORECASE,
                    )
                    truncated = re.sub(
                        r"(sk_live_|sk_test_|whsec_|pk_live_|pk_test_)\w+",
                        "[REDACTED_KEY]",
                        truncated,
                    )
                    return truncated

                def _redact_args(raw_args: tuple | list | None) -> list:
                    """Redact positional args — keep UUIDs/IDs, redact anything else."""
                    if not raw_args:
                        return []
                    result = []
                    for arg in raw_args:
                        if isinstance(arg, str) and len(arg) <= 80:
                            result.append(arg)
                        elif isinstance(arg, (int, float, bool)):
                            result.append(arg)
                        else:
                            result.append("[REDACTED]")
                    return result

                dlq_key = "bff:dead_letter_queue"
                safe_kwargs = _redact(dict(kwargs or {}))
                redis_conn.lpush(dlq_key, json.dumps({
                    "task_name": self.name,
                    "task_id": task_id,
                    "args": _redact_args(args),
                    "kwargs": safe_kwargs,
                    "retries": self.request.retries,
                    "error": _sanitize_error(str(exc)),
                }))
                redis_conn.ltrim(dlq_key, 0, 4999)
                redis_conn.expire(dlq_key, 60 * 60 * 24 * 30)
                DLQ_MESSAGES_TOTAL.labels(task_name=self.name).inc()
                try:
                    DLQ_DEPTH.set(redis_conn.llen(dlq_key))
                except Exception:
                    pass
                try:
                    import sentry_sdk as _sentry
                    _sentry.capture_message(
                        f"Task {self.name} exhausted retries and moved to DLQ",
                        level="error",
                    )
                except Exception:
                    pass
            except Exception:
                DLQ_WRITE_FAILURES_TOTAL.labels(task_name=self.name).inc()
                logger.warning("task.dlq_persist_failed", task_name=self.name, exc_info=True)


@celery_app.task(name="maintenance.ping", ignore_result=True)
def ping() -> dict[str, str]:
    return {
        "status": "ok",
        "task": "maintenance.ping",
        "note": "Worker is reachable.",
    }


def _find_pipeline_run(
    session: Session,
    model_cls: type[_ModelT],
    run: _ModelT | None,
    trade_date: date,
    *,
    run_id: UUID | None = None,
) -> _ModelT | None:
    """Return the pipeline run object for failure marking.

    When *run_id* is provided we look up by exact ID (preferred).
    When *run* was returned by ``run_pipeline`` we use ``run.id``.
    When both are ``None`` (pipeline raised before returning), fall
    back to querying for the most recent running row for *trade_date*
    and log a warning since this heuristic may match the wrong row.
    """
    effective_id = run_id or (run.id if run is not None else None)
    if effective_id is not None:
        return session.get(model_cls, effective_id)
    from sqlalchemy import select, desc

    from sqlalchemy import func

    running_count = session.scalar(
        select(func.count()).select_from(model_cls).where(
            model_cls.trade_date == trade_date, model_cls.status == "running"
        )
    ) or 0
    logger.error(
        "pipeline.find_run_fallback",
        trade_date=str(trade_date),
        running_count=running_count,
        msg=(
            "No run_id available; falling back to heuristic date-based lookup. "
            "Investigate why run_id was not captured."
        ),
    )
    if running_count > 1:
        logger.error(
            "pipeline.find_run_ambiguous",
            trade_date=str(trade_date),
            running_count=running_count,
            msg="Multiple running pipeline runs for this date — refusing to guess.",
        )
        return None
    stmt = (
        select(model_cls)
        .where(model_cls.trade_date == trade_date, model_cls.status == "running")
        .order_by(desc(model_cls.created_at))
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    return session.scalar(stmt)


@celery_app.task(name="pipeline.nightly_scan", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=1800, time_limit=1860)
def nightly_scan_pipeline(
    self,
    symbols: list[str] | None = None,
    max_recommendations: int = 20,
    trade_date_iso: str | None = None,
) -> dict[str, str | int]:
    """Execute the full nightly scan pipeline."""
    if symbols is not None and len(symbols) > 500:
        raise ValueError(f"symbols list too large ({len(symbols)}); maximum is 500")

    from datetime import UTC, date, datetime

    from backtestforecast.config import get_settings
    from backtestforecast.models import NightlyPipelineRun
    from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
    from backtestforecast.integrations.massive_client import MassiveClient
    from backtestforecast.market_data.service import MarketDataService
    from backtestforecast.pipeline.adapters import (
        PipelineBacktestExecutor,
        PipelineForecaster,
        PipelineMarketDataFetcher,
    )
    from backtestforecast.pipeline.service import NightlyPipelineService
    from backtestforecast.services.backtest_execution import BacktestExecutionService

    settings = get_settings()
    client = MassiveClient(api_key=settings.massive_api_key)
    shared_mds = MarketDataService(client)
    shared_exec = BacktestExecutionService(market_data_service=shared_mds)
    executor = PipelineBacktestExecutor(execution_service=shared_exec)
    try:
        market_data = PipelineMarketDataFetcher(client)
        forecaster_engine = HistoricalAnalogForecaster()
        forecaster = PipelineForecaster(forecaster_engine, market_data)

        if symbols is None:
            try:
                from redis import Redis as _SymRedis
                _sym_r = _SymRedis.from_url(settings.redis_url, decode_responses=True, socket_timeout=3)
                _override = _sym_r.get("bff:pipeline:symbols")
                _sym_r.close()
                if _override:
                    parsed = [s.strip() for s in _override.split(",") if s.strip()]
                    if parsed:
                        symbols = parsed[:500]
                        logger.info("pipeline.symbols_from_redis", count=len(symbols))
            except Exception:
                pass
        if symbols is None:
            symbols = settings.pipeline_default_symbols

        from zoneinfo import ZoneInfo

        if trade_date_iso:
            try:
                trade_date = date.fromisoformat(trade_date_iso)
            except ValueError:
                logger.error("pipeline.invalid_trade_date_iso", value=trade_date_iso)
                return {"status": "failed", "reason": "invalid_trade_date_iso"}
        else:
            trade_date = datetime.now(ZoneInfo("America/New_York")).date()

        if trade_date_iso is None and trade_date.weekday() >= 5:
            logger.info("pipeline.skipped_weekend", trade_date=str(trade_date))
            return {"status": "skipped", "reason": "weekend"}

        if trade_date_iso is None:
            from backtestforecast.utils.dates import is_market_holiday
            if is_market_holiday(trade_date):
                logger.info("pipeline.skipped_holiday", trade_date=str(trade_date))
                return {"status": "skipped", "reason": "market_holiday"}

        from redis import Redis
        lock_key = f"bff:pipeline:{trade_date.isoformat()}"
        redis_client = Redis.from_url(settings.redis_url, socket_timeout=5)
        try:
            lock = redis_client.lock(lock_key, timeout=1920, blocking=False)
        except Exception:
            redis_client.close()
            raise
        try:
            acquired = lock.acquire()
        except Exception:
            redis_client.close()
            raise
        if not acquired:
            logger.info("pipeline.already_locked", trade_date=str(trade_date))
            redis_client.close()
            return {"status": "skipped", "reason": "locked"}

        _retrying = False
        try:
            with create_worker_session() as session:
                service = NightlyPipelineService(
                    session,
                    market_data_fetcher=market_data,
                    backtest_executor=executor,
                    forecaster=forecaster,
                )
                run = None  # Assigned by run_pipeline; used by _find_pipeline_run in error handlers
                try:
                    run = service.run_pipeline(
                        trade_date=trade_date,
                        symbols=symbols,
                        max_recommendations=max_recommendations,
                    )
                except SoftTimeLimitExceeded:
                    session.rollback()
                    logger.warning("pipeline.time_limit_exceeded", trade_date=str(trade_date))
                    run_obj = _find_pipeline_run(session, NightlyPipelineRun, run, trade_date)
                    if run_obj is not None and run_obj.status == "running":
                        run_obj.status = "failed"
                        run_obj.error_message = "Pipeline exceeded the time limit."
                        run_obj.completed_at = datetime.now(UTC)
                        try:
                            session.commit()
                        except Exception:
                            session.rollback()
                    CELERY_TASKS_TOTAL.labels(task_name="pipeline.nightly_scan", status="failed").inc()
                    raise
                except Exception as exc:
                    session.rollback()
                    logger.exception("pipeline.task_failed", trade_date=str(trade_date))
                    _retrying = True
                    try:
                        lock.release()
                    except Exception:
                        pass
                    try:
                        raise self.retry(exc=exc, countdown=300, kwargs={
                            "symbols": symbols,
                            "max_recommendations": max_recommendations,
                            "trade_date_iso": trade_date.isoformat(),
                        })
                    except MaxRetriesExceededError:
                        _retrying = False
                        run_obj = _find_pipeline_run(session, NightlyPipelineRun, run, trade_date)
                        if run_obj is not None and run_obj.status == "running":
                            run_obj.status = "failed"
                            run_obj.error_message = "Pipeline failed after maximum retries (max_retries_exceeded)."
                            run_obj.completed_at = datetime.now(UTC)
                            try:
                                session.commit()
                            except Exception:
                                session.rollback()
                        CELERY_TASKS_TOTAL.labels(task_name="pipeline.nightly_scan", status="failed").inc()
                        raise

                effective_status = "succeeded" if run.status == "succeeded" else "failed"
                CELERY_TASKS_TOTAL.labels(task_name="pipeline.nightly_scan", status=effective_status).inc()
                NIGHTLY_PIPELINE_RUNS_TOTAL.labels(status=effective_status).inc()
                return {
                    "status": run.status,
                    "run_id": str(run.id),
                    "recommendations": run.recommendations_produced,
                    "duration_seconds": (float(run.duration_seconds) if run.duration_seconds else 0),
                }
        finally:
            if not _retrying:
                try:
                    lock.release()
                except Exception:
                    pass
            try:
                redis_client.close()
            except Exception:
                pass
    finally:
        try:
            shared_mds.close()
        except Exception:
            logger.exception("shared_mds.close_failed")
        try:
            executor.close()
        except Exception:
            logger.exception("executor.close_failed")
        try:
            client.close()
        except Exception:
            logger.exception("client.close_failed")


_TERMINAL_STATUSES = _VALID_TARGET_STATUSES | frozenset({"expired"})


def _update_heartbeat(session: "Session", model_cls: type, obj_id: UUID) -> None:
    """Best-effort heartbeat update for long-running tasks.

    Uses a savepoint to avoid committing unrelated dirty ORM state from
    the caller's transaction.
    """
    from datetime import UTC, datetime
    from sqlalchemy import update
    try:
        nested = session.begin_nested()
        session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id)
            .values(last_heartbeat_at=datetime.now(UTC))
        )
        nested.commit()
    except Exception:
        try:
            nested.rollback()
        except Exception:
            pass


def _validate_task_ownership(session: "Session", model_cls: type, obj_id: UUID, expected_task_id: str | None) -> bool:
    """Return True if this Celery delivery owns the job, False if it's a duplicate.

    When the DB record has no ``celery_task_id`` yet (API failed to set it, or
    the job was created before that feature), we atomically claim ownership by
    writing our task ID with a ``WHERE celery_task_id IS NULL`` guard.  If
    another worker already claimed it, the UPDATE affects zero rows and we
    treat this delivery as a duplicate.

    Re-delivery after worker crash: if the stored task ID differs but the job
    is still in a non-terminal state, allow the new delivery to claim it.
    """
    from sqlalchemy import or_, update

    if expected_task_id is None:
        return True
    obj = session.get(model_cls, obj_id)
    if obj is None:
        logger.warning("validate_task_ownership.obj_not_found", model=model_cls.__name__, obj_id=str(obj_id))
        return False
    stored = getattr(obj, "celery_task_id", None)
    if stored == expected_task_id:
        return True
    if stored is None:
        result = session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id, model_cls.celery_task_id.is_(None))
            .values(celery_task_id=expected_task_id)
            .returning(model_cls.id)
        )
        claimed = result.fetchone() is not None
        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("validate_task_ownership.commit_failed", model=model_cls.__name__, obj_id=str(obj_id), exc_info=True)
            return False
        if not claimed:
            return False
        session.refresh(obj)
        return True
    current_status = getattr(obj, "status", None)
    if current_status is not None and current_status not in _TERMINAL_STATUSES:
        result = session.execute(
            update(model_cls)
            .where(
                model_cls.id == obj_id,
                model_cls.celery_task_id == stored,
                model_cls.status.notin_(_TERMINAL_STATUSES),
            )
            .values(celery_task_id=expected_task_id)
            .returning(model_cls.id)
        )
        claimed = result.fetchone() is not None
        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("validate_task_ownership.redelivery_commit_failed", model=model_cls.__name__, obj_id=str(obj_id), exc_info=True)
            return False
        if claimed:
            logger.info(
                "validate_task_ownership.redelivery_claimed",
                model=model_cls.__name__,
                obj_id=str(obj_id),
                old_task_id=stored,
                new_task_id=expected_task_id,
            )
            session.refresh(obj)
            return True
    return False


@celery_app.task(name="backtests.run", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=2, soft_time_limit=300, time_limit=330)
def run_backtest(self, run_id: str) -> dict[str, str]:
    with create_worker_session() as session:
        if not _validate_task_ownership(session, BacktestRun, UUID(run_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="backtests.run").inc()
            logger.info("backtests.run.duplicate_delivery", run_id=run_id, task_id=self.request.id)
            return {"status": "skipped", "run_id": run_id, "reason": "duplicate_delivery"}
        run_obj = session.get(BacktestRun, UUID(run_id))
        if run_obj is None:
            CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="failed").inc()
            return {"status": "failed", "run_id": run_id, "error_code": "not_found"}
        user = session.get(User, run_obj.user_id, with_for_update=True)
        if user is None:
            run_obj.status = "failed"
            run_obj.error_code = "entitlement_revoked"
            run_obj.error_message = "User account not found."
            _commit_then_publish(session, "backtest", UUID(run_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "run_id": run_id, "error_code": "entitlement_revoked"}
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        if policy.monthly_backtest_quota is not None:
            if policy.monthly_backtest_quota <= 0:
                run_obj.status = "failed"
                run_obj.error_code = "entitlement_revoked"
                run_obj.error_message = "Your plan no longer supports this operation."
                _commit_then_publish(session, "backtest", UUID(run_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "run_id": run_id, "error_code": "entitlement_revoked"}
            from datetime import UTC, datetime
            from backtestforecast.repositories.backtest_runs import BacktestRunRepository
            repo = BacktestRunRepository(session)
            now = datetime.now(UTC)
            month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
            next_month_start = datetime(now.year + 1, 1, 1, tzinfo=UTC) if now.month == 12 else datetime(now.year, now.month + 1, 1, tzinfo=UTC)
            used = repo.count_for_user_created_between(
                user.id, start_inclusive=month_start, end_exclusive=next_month_start,
                exclude_id=UUID(run_id),
            )
            if used >= policy.monthly_backtest_quota:
                run_obj.status = "failed"
                run_obj.error_code = "quota_exceeded"
                run_obj.error_message = f"Monthly backtest quota ({policy.monthly_backtest_quota}) reached. Used: {used}."
                _commit_then_publish(session, "backtest", UUID(run_id), "failed", metadata={"error_code": "quota_exceeded"})
                return {"status": "failed", "run_id": run_id, "error_code": "quota_exceeded"}
        publish_job_status("backtest", UUID(run_id), "running")
        _update_heartbeat(session, BacktestRun, UUID(run_id))
        service = BacktestService(session)
        try:
            run = service.execute_run_by_id(UUID(run_id))
        except AppError as exc:
            if isinstance(exc, ExternalServiceError):
                session.rollback()
                session.expire_all()
                delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                try:
                    raise self.retry(exc=exc, countdown=delay)
                except self.MaxRetriesExceededError:
                    pass
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime as _dt_app
            run_obj = session.get(BacktestRun, UUID(run_id))
            if run_obj is not None and run_obj.status in ("queued", "running"):
                run_obj.status = "failed"
                run_obj.error_code = exc.code
                run_obj.error_message = str(exc.message)[:500] if exc.message else None
                run_obj.completed_at = _dt_app.now(UTC)
                try:
                    session.commit()
                except Exception:
                    logger.exception("backtest.app_error.commit_failed")
                    session.rollback()
            BACKTEST_RUNS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="failed").inc()
            publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "run_id": run_id,
                "error_code": exc.code,
            }
        except SoftTimeLimitExceeded:
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime

            run_obj = session.get(BacktestRun, UUID(run_id))
            if run_obj is not None and run_obj.status in ("queued", "running"):
                run_obj.status = "failed"
                run_obj.error_code = "time_limit_exceeded"
                run_obj.error_message = "Backtest exceeded the time limit."
                run_obj.completed_at = datetime.now(UTC)
                try:
                    session.commit()
                except Exception:
                    logger.exception("soft_time_limit.commit_failed")
                    session.rollback()
            BACKTEST_RUNS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="failed").inc()
            publish_job_status(
                "backtest", UUID(run_id), "failed",
                metadata={"error_code": "time_limit_exceeded"},
            )
            raise
        except Exception as exc:  # Intentional broad catch: any unexpected failure triggers
            # retry with backoff. After max retries, job is marked failed. Re-raised.
            session.rollback()
            session.expire_all()
            try:
                delay = int(30 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                run_obj = session.get(BacktestRun, UUID(run_id))
                if run_obj is not None and run_obj.status in ("queued", "running"):
                    run_obj.status = "failed"
                    run_obj.error_code = "max_retries_exceeded"
                    run_obj.error_message = "Backtest failed after exhausting retries."
                    run_obj.completed_at = datetime.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        logger.exception("max_retries.commit_failed")
                        session.rollback()
                BACKTEST_RUNS_TOTAL.labels(status="failed").inc()
                CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="failed").inc()
                publish_job_status(
                    "backtest", UUID(run_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                raise
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("service.close_failed")

        _update_heartbeat(session, BacktestRun, UUID(run_id))
        BACKTEST_RUNS_TOTAL.labels(status=run.status).inc()
        CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status=run.status).inc()
        try:
            publish_job_status("backtest", UUID(run_id), run.status)
        except Exception:
            logger.warning("backtest.publish_status_failed", run_id=run_id, exc_info=True)
        return {
            "status": run.status,
            "run_id": run_id,
            "trade_count": run.trade_count,
        }


@celery_app.task(name="exports.generate", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=2, soft_time_limit=120, time_limit=150)
def generate_export(self, export_job_id: str) -> dict[str, str | int]:
    with create_worker_session() as session:
        if not _validate_task_ownership(session, ExportJobModel, UUID(export_job_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="exports.generate").inc()
            logger.info("exports.generate.duplicate_delivery", export_job_id=export_job_id, task_id=self.request.id)
            return {"status": "skipped", "export_job_id": export_job_id, "reason": "duplicate_delivery"}
        ej = session.get(ExportJobModel, UUID(export_job_id))
        if ej is None:
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "not_found"}
        user = session.get(User, ej.user_id)
        if user is None:
            ej.status = "failed"
            ej.error_code = "entitlement_revoked"
            ej.error_message = "User account not found."
            _commit_then_publish(session, "export", UUID(export_job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "entitlement_revoked"}
        from backtestforecast.billing.entitlements import ExportFormat as _EF
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        try:
            requested_format = _EF(ej.export_format)
        except ValueError:
            requested_format = None
        if requested_format is None:
            ej.status = "failed"
            ej.error_code = "unsupported_format"
            ej.error_message = f"Unsupported export format: {ej.export_format}"
            _commit_then_publish(session, "export", UUID(export_job_id), "failed", metadata={"error_code": "unsupported_format"})
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "unsupported_format"}
        if not policy.export_formats or requested_format not in policy.export_formats:
            ej.status = "failed"
            ej.error_code = "entitlement_revoked"
            ej.error_message = f"Your plan no longer supports {ej.export_format} export."
            _commit_then_publish(session, "export", UUID(export_job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "entitlement_revoked"}
        publish_job_status("export", UUID(export_job_id), "running")
        service = ExportService(session)
        try:
            job = service.execute_export_by_id(UUID(export_job_id))
        except AppError as exc:
            if isinstance(exc, ExternalServiceError):
                session.rollback()
                session.expire_all()
                delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                try:
                    raise self.retry(exc=exc, countdown=delay)
                except self.MaxRetriesExceededError:
                    pass
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime as _dt_app
            ej_obj = session.get(ExportJobModel, UUID(export_job_id))
            if ej_obj is not None and ej_obj.status in ("queued", "running"):
                ej_obj.status = "failed"
                ej_obj.error_code = exc.code
                ej_obj.error_message = str(exc.message)
                ej_obj.completed_at = _dt_app.now(UTC)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
            EXPORT_JOBS_TOTAL.labels(status="failed").inc()
            publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "export_job_id": export_job_id,
                "error_code": exc.code,
            }
        except SoftTimeLimitExceeded:
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime

            export_obj = session.get(ExportJobModel, UUID(export_job_id))
            if export_obj is not None and export_obj.status in ("queued", "running"):
                export_obj.status = "failed"
                export_obj.error_code = "time_limit_exceeded"
                export_obj.error_message = "Export exceeded the time limit."
                export_obj.completed_at = datetime.now(UTC)
                try:
                    session.commit()
                except Exception:
                    logger.exception("soft_time_limit.commit_failed")
                    session.rollback()
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
            EXPORT_JOBS_TOTAL.labels(status="failed").inc()
            publish_job_status(
                "export", UUID(export_job_id), "failed",
                metadata={"error_code": "time_limit_exceeded"},
            )
            raise
        except Exception as exc:  # Intentional broad catch: any unexpected export failure
            # triggers retry with backoff. After max retries, job is marked failed.
            session.rollback()
            session.expire_all()
            try:
                delay = int(15 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                export_obj = session.get(ExportJobModel, UUID(export_job_id))
                if export_obj is not None and export_obj.status in ("queued", "running"):
                    export_obj.status = "failed"
                    export_obj.error_code = "max_retries_exceeded"
                    export_obj.error_message = "Export failed after exhausting retries."
                    export_obj.completed_at = datetime.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        logger.exception("max_retries.commit_failed")
                        session.rollback()
                CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
                EXPORT_JOBS_TOTAL.labels(status="failed").inc()
                publish_job_status(
                    "export", UUID(export_job_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                raise
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("service.close_failed")

        if job.status == "succeeded":
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="succeeded").inc()
        else:
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
        EXPORT_JOBS_TOTAL.labels(status=job.status).inc()
        try:
            publish_job_status("export", UUID(export_job_id), job.status)
        except Exception:
            logger.warning("export.publish_status_failed", export_job_id=export_job_id, exc_info=True)
        return {
            "status": job.status,
            "export_job_id": export_job_id,
            "size_bytes": job.size_bytes,
        }


@celery_app.task(name="analysis.deep_symbol", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=600, time_limit=660)
def run_deep_analysis(self, analysis_id: str) -> dict[str, str | int]:
    """Execute a single-symbol deep analysis."""
    from backtestforecast.config import get_settings
    from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
    from backtestforecast.integrations.massive_client import MassiveClient
    from backtestforecast.market_data.service import MarketDataService
    from backtestforecast.pipeline.adapters import (
        PipelineBacktestExecutor,
        PipelineForecaster,
        PipelineMarketDataFetcher,
    )
    from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
    from backtestforecast.services.backtest_execution import BacktestExecutionService as _BES

    settings = get_settings()
    client = MassiveClient(api_key=settings.massive_api_key)
    shared_mds = MarketDataService(client)
    shared_exec = _BES(market_data_service=shared_mds)
    executor = PipelineBacktestExecutor(execution_service=shared_exec)
    try:
        market_data = PipelineMarketDataFetcher(client)
        forecaster = PipelineForecaster(HistoricalAnalogForecaster(), market_data)

        with create_worker_session() as session:
            if not _validate_task_ownership(session, SymbolAnalysis, UUID(analysis_id), self.request.id):
                DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="analysis.deep_symbol").inc()
                logger.info("analysis.deep_symbol.duplicate_delivery", analysis_id=analysis_id, task_id=self.request.id)
                return {"status": "skipped", "analysis_id": analysis_id, "reason": "duplicate_delivery"}
            sa_obj = session.get(SymbolAnalysis, UUID(analysis_id))
            if sa_obj is None:
                CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "not_found"}
            user = session.get(User, sa_obj.user_id)
            if user is None:
                sa_obj.status = "failed"
                sa_obj.error_code = "entitlement_revoked"
                sa_obj.error_message = "User account not found."
                _commit_then_publish(session, "analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
            policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
            if not policy.forecasting_access:
                sa_obj.status = "failed"
                sa_obj.error_code = "entitlement_revoked"
                sa_obj.error_message = "Your plan no longer supports this operation."
                _commit_then_publish(session, "analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
            from sqlalchemy import func, select as sa_select
            concurrent = session.scalar(
                sa_select(func.count()).select_from(SymbolAnalysis).where(
                    SymbolAnalysis.user_id == sa_obj.user_id,
                    SymbolAnalysis.status.in_(["queued", "running"]),
                )
            ) or 0
            max_concurrent = (
                settings.max_concurrent_analyses_premium
                if policy.tier.value == "premium"
                else settings.max_concurrent_analyses_default
            )
            if concurrent > max_concurrent:
                sa_obj.status = "failed"
                sa_obj.error_code = "concurrent_limit"
                sa_obj.error_message = f"Maximum concurrent analyses ({max_concurrent}) exceeded."
                _commit_then_publish(session, "analysis", UUID(analysis_id), "failed", metadata={"error_code": "concurrent_limit"})
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "concurrent_limit"}
            publish_job_status("analysis", UUID(analysis_id), "running")
            _update_heartbeat(session, SymbolAnalysis, UUID(analysis_id))
            service = SymbolDeepAnalysisService(
                session,
                market_data_fetcher=market_data,
                backtest_executor=executor,
                forecaster=forecaster,
            )
            try:
                result = service.execute_analysis(UUID(analysis_id))
            except AppError as exc:
                if isinstance(exc, ExternalServiceError):
                    session.rollback()
                    session.expire_all()
                    delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                    try:
                        raise self.retry(exc=exc, countdown=delay)
                    except self.MaxRetriesExceededError:
                        pass
                session.rollback()
                session.expire_all()
                sa_fail = session.get(SymbolAnalysis, UUID(analysis_id))
                if sa_fail is not None and sa_fail.status in ("queued", "running"):
                    from datetime import UTC, datetime as _dt_analysis_app
                    sa_fail.status = "failed"
                    sa_fail.error_code = exc.code
                    sa_fail.error_message = str(exc.message)
                    sa_fail.completed_at = _dt_analysis_app.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        session.rollback()
                ANALYSIS_JOBS_TOTAL.labels(status="failed").inc()
                CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": exc.code})
                return {
                    "status": "failed",
                    "analysis_id": analysis_id,
                    "error_code": exc.code,
                }
            except SoftTimeLimitExceeded:
                session.rollback()
                session.expire_all()
                analysis = session.get(SymbolAnalysis, UUID(analysis_id))
                if analysis is not None and analysis.status in ("queued", "running"):
                    analysis.status = "failed"
                    analysis.error_code = "time_limit_exceeded"
                    analysis.error_message = "Analysis exceeded the time limit."
                    from datetime import UTC, datetime as _dt_stl
                    analysis.completed_at = _dt_stl.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        logger.exception("soft_time_limit.commit_failed")
                        session.rollback()
                ANALYSIS_JOBS_TOTAL.labels(status="failed").inc()
                CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                publish_job_status(
                    "analysis", UUID(analysis_id), "failed",
                    metadata={"error_code": "time_limit_exceeded"},
                )
                raise
            except Exception as exc:  # Intentional broad catch: any unexpected analysis failure
                # triggers retry. After max retries, analysis is marked failed.
                session.rollback()
                session.expire_all()
                try:
                    delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                    raise self.retry(exc=exc, countdown=delay)
                except self.MaxRetriesExceededError:
                    analysis = session.get(SymbolAnalysis, UUID(analysis_id))
                    if analysis is not None and analysis.status in ("queued", "running"):
                        analysis.status = "failed"
                        analysis.error_code = "max_retries_exceeded"
                        analysis.error_message = "Analysis failed after exhausting retries."
                        from datetime import UTC, datetime as _dt_mr
                        analysis.completed_at = _dt_mr.now(UTC)
                        try:
                            session.commit()
                        except Exception:
                            logger.exception("max_retries.commit_failed")
                            session.rollback()
                    ANALYSIS_JOBS_TOTAL.labels(status="failed").inc()
                    CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                    publish_job_status(
                        "analysis", UUID(analysis_id), "failed",
                        metadata={"error_code": "max_retries_exceeded"},
                    )
                    raise

            _update_heartbeat(session, SymbolAnalysis, UUID(analysis_id))
            effective_status = "succeeded" if result.status == "succeeded" else "failed"
            CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status=effective_status).inc()
            ANALYSIS_JOBS_TOTAL.labels(status=result.status).inc()
            try:
                publish_job_status("analysis", UUID(analysis_id), result.status)
            except Exception:
                logger.warning("analysis.publish_status_failed", analysis_id=analysis_id, exc_info=True)
            return {
                "status": result.status,
                "analysis_id": analysis_id,
                "top_results": result.top_results_count,
            }
    finally:
        try:
            shared_mds.close()
        except Exception:
            logger.exception("shared_mds.close_failed")
        try:
            executor.close()
        except Exception:
            logger.exception("executor.close_failed")
        try:
            client.close()
        except Exception:
            logger.exception("client.close_failed")


@celery_app.task(name="scans.run_job", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=3, soft_time_limit=600, time_limit=660)
def run_scan_job(self, job_id: str) -> dict[str, str | int]:
    with create_worker_session() as session:
        if not _validate_task_ownership(session, ScannerJobModel, UUID(job_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="scans.run_job").inc()
            logger.info("scans.run_job.duplicate_delivery", job_id=job_id, task_id=self.request.id)
            return {"status": "skipped", "job_id": job_id, "reason": "duplicate_delivery"}
        sj = session.get(ScannerJobModel, UUID(job_id))
        if sj is None:
            CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status="failed").inc()
            return {"status": "failed", "job_id": job_id, "error_code": "not_found"}
        user = session.get(User, sj.user_id)
        if user is None:
            sj.status = "failed"
            sj.error_code = "entitlement_revoked"
            sj.error_message = "User account not found."
            session.commit()
            publish_job_status("scan", UUID(job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "job_id": job_id, "error_code": "entitlement_revoked"}
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        mode_requires_advanced = sj.mode == "advanced"
        if not policy.basic_scanner_access or (mode_requires_advanced and not policy.advanced_scanner_access):
            sj.status = "failed"
            sj.error_code = "entitlement_revoked"
            sj.error_message = f"Your plan no longer supports {sj.mode} scanner mode."
            session.commit()
            publish_job_status("scan", UUID(job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "job_id": job_id, "error_code": "entitlement_revoked"}
        publish_job_status("scan", UUID(job_id), "running")
        _update_heartbeat(session, ScannerJobModel, UUID(job_id))
        service = ScanService(session)
        try:
            job = service.run_job(UUID(job_id))
        except AppError as exc:
            if isinstance(exc, ExternalServiceError):
                session.rollback()
                session.expire_all()
                delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                try:
                    raise self.retry(exc=exc, countdown=delay)
                except self.MaxRetriesExceededError:
                    pass
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime as _dt_scan
            sj_obj = session.get(ScannerJobModel, UUID(job_id))
            if sj_obj is not None and sj_obj.status in ("queued", "running"):
                sj_obj.status = "failed"
                sj_obj.error_code = exc.code
                sj_obj.error_message = str(exc.message)[:500] if exc.message else None
                sj_obj.completed_at = _dt_scan.now(UTC)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
            SCAN_JOBS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status="failed").inc()
            publish_job_status("scan", UUID(job_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "job_id": job_id,
                "error_code": exc.code,
            }
        except SoftTimeLimitExceeded:
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime

            scan_obj = session.get(ScannerJobModel, UUID(job_id))
            if scan_obj is not None and scan_obj.status in ("queued", "running"):
                scan_obj.status = "failed"
                scan_obj.error_code = "time_limit_exceeded"
                scan_obj.error_message = "Scan exceeded the time limit."
                scan_obj.completed_at = datetime.now(UTC)
                try:
                    session.commit()
                except Exception:
                    logger.exception("soft_time_limit.commit_failed")
                    session.rollback()
            SCAN_JOBS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status="failed").inc()
            publish_job_status(
                "scan", UUID(job_id), "failed",
                metadata={"error_code": "time_limit_exceeded"},
            )
            raise
        except Exception as exc:  # Intentional broad catch: any unexpected scan failure
            # triggers retry with backoff. After max retries, job is marked failed.
            session.rollback()
            session.expire_all()
            try:
                delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                scan_obj = session.get(ScannerJobModel, UUID(job_id))
                if scan_obj is not None and scan_obj.status in ("queued", "running"):
                    scan_obj.status = "failed"
                    scan_obj.error_code = "max_retries_exceeded"
                    scan_obj.error_message = "Scan failed after exhausting retries."
                    scan_obj.completed_at = datetime.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        logger.exception("max_retries.commit_failed")
                        session.rollback()
                SCAN_JOBS_TOTAL.labels(status="failed").inc()
                CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status="failed").inc()
                publish_job_status(
                    "scan", UUID(job_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                raise
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("service.close_failed")

        _update_heartbeat(session, ScannerJobModel, UUID(job_id))
        effective_status = "succeeded" if job.status == "succeeded" else "failed"
        CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status=effective_status).inc()
        SCAN_JOBS_TOTAL.labels(status=job.status).inc()
        try:
            publish_job_status("scan", UUID(job_id), job.status)
        except Exception:
            logger.warning("scan.publish_status_failed", job_id=job_id, exc_info=True)
        return {
            "status": job.status,
            "job_id": job_id,
            "recommendation_count": job.recommendation_count,
        }


@celery_app.task(name="sweeps.run", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=2, soft_time_limit=3600, time_limit=3660)
def run_sweep(self, job_id: str) -> dict[str, str | int]:
    with create_worker_session() as session:
        if not _validate_task_ownership(session, SweepJobModel, UUID(job_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="sweeps.run").inc()
            logger.info("sweeps.run.duplicate_delivery", job_id=job_id, task_id=self.request.id)
            return {"status": "skipped", "job_id": job_id, "reason": "duplicate_delivery"}
        sj = session.get(SweepJobModel, UUID(job_id))
        if sj is None:
            CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
            return {"status": "failed", "job_id": job_id, "error_code": "not_found"}
        user = session.get(User, sj.user_id)
        if user is None:
            sj.status = "failed"
            sj.error_code = "entitlement_revoked"
            sj.error_message = "User account not found."
            session.commit()
            publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "job_id": job_id, "error_code": "entitlement_revoked"}
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        if not policy.forecasting_access:
            sj.status = "failed"
            sj.error_code = "entitlement_revoked"
            sj.error_message = "Your plan no longer supports this operation."
            session.commit()
            publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "job_id": job_id, "error_code": "entitlement_revoked"}
        if policy.monthly_sweep_quota is not None:
            from datetime import UTC, datetime as _dt_sweep_quota
            from backtestforecast.repositories.sweep_jobs import SweepJobRepository
            sweep_repo = SweepJobRepository(session)
            now = _dt_sweep_quota.now(UTC)
            month_start = _dt_sweep_quota(now.year, now.month, 1, tzinfo=UTC)
            next_month_start = (
                _dt_sweep_quota(now.year + 1, 1, 1, tzinfo=UTC)
                if now.month == 12
                else _dt_sweep_quota(now.year, now.month + 1, 1, tzinfo=UTC)
            )
            sweep_used = sweep_repo.count_for_user_created_between(
                user.id,
                start_inclusive=month_start,
                end_exclusive=next_month_start,
                exclude_id=UUID(job_id),
            )
            if sweep_used >= policy.monthly_sweep_quota:
                sj.status = "failed"
                sj.error_code = "quota_exceeded"
                sj.error_message = f"Monthly sweep quota ({policy.monthly_sweep_quota}) reached. Used: {sweep_used}."
                session.commit()
                publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "quota_exceeded"})
                return {"status": "failed", "job_id": job_id, "error_code": "quota_exceeded"}
        publish_job_status("sweep", UUID(job_id), "running")
        _update_heartbeat(session, SweepJobModel, UUID(job_id))
        service = SweepService(session)
        try:
            job = service.run_job(UUID(job_id))
        except AppError as exc:
            if isinstance(exc, ExternalServiceError):
                session.rollback()
                session.expire_all()
                delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                try:
                    raise self.retry(exc=exc, countdown=delay)
                except self.MaxRetriesExceededError:
                    pass
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime as _dt_sweep_app
            sweep_obj = session.get(SweepJobModel, UUID(job_id))
            if sweep_obj is not None and sweep_obj.status in ("queued", "running"):
                sweep_obj.status = "failed"
                sweep_obj.error_code = exc.code
                sweep_obj.error_message = str(exc.message)
                sweep_obj.completed_at = _dt_sweep_app.now(UTC)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
            SWEEP_JOBS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
            publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": exc.code})
            return {"status": "failed", "job_id": job_id, "error_code": exc.code}
        except SoftTimeLimitExceeded:
            session.rollback()
            session.expire_all()
            from datetime import UTC, datetime

            sweep_obj = session.get(SweepJobModel, UUID(job_id))
            if sweep_obj is not None and sweep_obj.status in ("queued", "running"):
                sweep_obj.status = "failed"
                sweep_obj.error_code = "time_limit_exceeded"
                sweep_obj.error_message = "Sweep exceeded the time limit."
                sweep_obj.completed_at = datetime.now(UTC)
                try:
                    session.commit()
                except Exception:
                    logger.exception("sweep.soft_time_limit.commit_failed")
                    session.rollback()
            SWEEP_JOBS_TOTAL.labels(status="failed").inc()
            CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
            publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "time_limit_exceeded"})
            raise
        except Exception as exc:
            session.rollback()
            session.expire_all()
            try:
                delay = int(120 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                sweep_obj = session.get(SweepJobModel, UUID(job_id))
                if sweep_obj is not None and sweep_obj.status in ("queued", "running"):
                    sweep_obj.status = "failed"
                    sweep_obj.error_code = "max_retries_exceeded"
                    sweep_obj.error_message = "Sweep failed after exhausting retries."
                    sweep_obj.completed_at = datetime.now(UTC)
                    try:
                        session.commit()
                    except Exception:
                        logger.exception("sweep.max_retries.commit_failed")
                        session.rollback()
                SWEEP_JOBS_TOTAL.labels(status="failed").inc()
                CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
                publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "max_retries_exceeded"})
                raise
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("sweep.service.close_failed")

        effective_status = "succeeded" if job.status == "succeeded" else "failed"
        CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status=effective_status).inc()
        SWEEP_JOBS_TOTAL.labels(status=job.status).inc()
        try:
            publish_job_status("sweep", UUID(job_id), job.status)
        except Exception:
            logger.warning("sweep.publish_status_failed", job_id=job_id, exc_info=True)
        return {
            "status": job.status,
            "job_id": job_id,
            "result_count": job.result_count,
        }


@celery_app.task(name="scans.refresh_prioritized", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=300, time_limit=360)
def refresh_prioritized_scans(self) -> dict[str, int]:
    from redis import Redis
    from sqlalchemy import update as sa_update

    from backtestforecast.config import get_settings
    from backtestforecast.models import ScannerJob

    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, socket_timeout=5)
    lock = redis.lock("bff:refresh_scans:lock", timeout=360, blocking_timeout=0)
    if not lock.acquire(blocking=False):
        redis.close()
        return {"scheduled_jobs": 0, "reason": "locked"}

    try:
        dispatched = 0
        with create_worker_session() as session:
            service = ScanService(session)
            try:
                jobs = service.create_scheduled_refresh_jobs(limit=25)
                committed_jobs = list(jobs)

                for job in committed_jobs:
                    try:
                        result = celery_app.send_task("scans.run_job", kwargs={"job_id": str(job.id)})
                        session.execute(
                            sa_update(ScannerJob)
                            .where(ScannerJob.id == job.id)
                            .values(celery_task_id=result.id)
                        )
                        session.commit()
                        dispatched += 1
                    except Exception:
                        logger.exception("refresh.dispatch_failed", job_id=str(job.id))
                        session.rollback()
                        job_obj = session.get(ScannerJob, job.id)
                        if job_obj is not None and job_obj.status == "queued":
                            job_obj.status = "failed"
                            job_obj.error_code = "enqueue_failed"
                            job_obj.error_message = "Unable to dispatch scheduled refresh."
                            try:
                                session.commit()
                                publish_job_status("scan", job.id, "failed", metadata={"error_code": "enqueue_failed"})
                            except Exception:
                                session.rollback()
            finally:
                try:
                    service.close()
                except Exception:
                    logger.exception("service.close_failed")

        return {
            "scheduled_jobs": dispatched,
        }
    finally:
        try:
            lock.release()
        except Exception:
            pass
        redis.close()


_S3_ORPHAN_MAX_DELETIONS = 500


_ORPHAN_IN_CHUNK_SIZE = 100


def _process_orphan_batch(
    session, s3_storage, page_keys: list[str], orphan_count: int,
) -> tuple[int, bool]:
    """Check a batch of S3 keys against the DB and delete orphans."""
    from sqlalchemy import select
    from backtestforecast.models import ExportJob

    existing: set[str] = set()
    for i in range(0, len(page_keys), _ORPHAN_IN_CHUNK_SIZE):
        chunk = page_keys[i : i + _ORPHAN_IN_CHUNK_SIZE]
        existing.update(session.scalars(
            select(ExportJob.storage_key).where(ExportJob.storage_key.in_(chunk))
        ))
    limit_reached = False
    for s3_key in page_keys:
        if orphan_count >= _S3_ORPHAN_MAX_DELETIONS:
            limit_reached = True
            break
        if s3_key not in existing:
            logger.info("s3_orphan_deleting", s3_key=s3_key)
            try:
                s3_storage.delete(s3_key)
            except Exception:
                logger.warning("s3_orphan_delete_failed", s3_key=s3_key, exc_info=True)
                continue
            orphan_count += 1
    return orphan_count, limit_reached


@celery_app.task(name="maintenance.reconcile_s3_orphans", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=0, soft_time_limit=3600, time_limit=3660)
def reconcile_s3_orphans(self) -> None:
    """Remove S3 objects that have no corresponding ExportJob in the database."""
    logger.info("s3_orphan_reconciliation_started")
    try:
        from sqlalchemy import select
        from backtestforecast.config import get_settings
        from backtestforecast.exports.storage import S3Storage
        from backtestforecast.models import ExportJob

        settings = get_settings()
        if not settings.s3_bucket:
            logger.info("s3_orphan_reconciliation_skipped", reason="no_s3_bucket_configured")
            return

        s3_storage = S3Storage(settings)

        with create_worker_session() as session:
            orphan_count = 0
            limit_reached = False
            batch: list[str] = []
            batch_size = 500
            for key in s3_storage.iter_keys():
                batch.append(key)
                if len(batch) < batch_size:
                    continue
                orphan_count, limit_reached = _process_orphan_batch(
                    session, s3_storage, batch, orphan_count,
                )
                batch = []
                if limit_reached:
                    break
            if batch and not limit_reached:
                orphan_count, limit_reached = _process_orphan_batch(
                    session, s3_storage, batch, orphan_count,
                )
            logger.info(
                "s3_orphan_reconciliation_complete",
                orphans_removed=orphan_count,
                limit_reached=limit_reached,
            )
    except SoftTimeLimitExceeded:
        logger.warning("s3_orphan_reconciliation_timeout")
        raise
    except Exception:
        logger.exception("s3_orphan_reconciliation_failed")
        raise


@celery_app.task(name="maintenance.reap_stale_jobs", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=300, time_limit=360)
def reap_stale_jobs(self, stale_minutes: int = 10) -> dict[str, int]:
    """Re-dispatch jobs stuck in 'queued' with no celery_task_id for too long."""
    stale_minutes = max(stale_minutes, 5)

    from redis import Redis

    from backtestforecast.config import get_settings
    from backtestforecast.observability.metrics import CELERY_WORKERS_ONLINE

    settings = get_settings()

    _MAX_HEARTBEAT_SCAN = 500
    try:
        _count_redis = Redis.from_url(settings.redis_url, socket_timeout=5, decode_responses=True)
        try:
            heartbeat_count = 0
            for _ in _count_redis.scan_iter("worker:heartbeat:*", count=100):
                heartbeat_count += 1
                if heartbeat_count >= _MAX_HEARTBEAT_SCAN:
                    break
            CELERY_WORKERS_ONLINE.set(heartbeat_count)
        finally:
            _count_redis.close()
    except Exception:
        pass

    redis = None
    lock = None
    lock_acquired = False
    try:
        redis = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=5.0)
        lock = redis.lock("bff:reaper:lock", timeout=300, blocking_timeout=0)
        lock_acquired = lock.acquire(blocking=False)
        if not lock_acquired:
            logger.info("reaper.skipped_locked")
            return {"skipped": 1}
    except Exception:  # Intentional: if Redis is down we cannot acquire the lock, but the
        # reaper should not crash — it will retry on the next scheduled beat.
        logger.warning("reaper.lock_unavailable", exc_info=True)
        return {"skipped": 1, "reason": "lock_unavailable"}

    import time as _time
    _reaper_start = _time.monotonic()
    try:
        return _reap_stale_jobs_inner(stale_minutes)
    finally:
        REAPER_DURATION_SECONDS.observe(_time.monotonic() - _reaper_start)
        if lock is not None and lock_acquired:
            try:
                lock.release()
            except Exception:
                pass
        if redis is not None:
            try:
                redis.close()
            except Exception:
                pass


def _reap_queued_jobs(
    session,
    model_cls,
    model_name: str,
    task_name: str,
    task_kwarg_key: str,
    cutoff,
    counts: dict[str, int],
    counts_key: str,
) -> None:
    """Re-dispatch queued jobs with no celery_task_id older than *cutoff*."""
    from sqlalchemy import select, update

    from backtestforecast.observability.metrics import JOBS_STUCK_REDISPATCHED_TOTAL

    stale_stmt = (
        select(model_cls.id)
        .where(
            model_cls.status == "queued",
            model_cls.celery_task_id.is_(None),
            model_cls.created_at < cutoff,
        )
        .limit(50)
        .with_for_update(skip_locked=True)
    )
    stale_ids = list(session.scalars(stale_stmt))
    for job_id in stale_ids:
        try:
            task_id = str(uuid4())
            from datetime import UTC, datetime as _dt_reap
            rows = session.execute(
                update(model_cls)
                .where(model_cls.id == job_id, model_cls.celery_task_id.is_(None))
                .values(celery_task_id=task_id, updated_at=_dt_reap.now(UTC))
            )
            if rows.rowcount == 0:
                session.rollback()
                logger.info("reaper.already_dispatched", model=model_name, id=str(job_id))
                continue
            session.commit()
            try:
                celery_app.send_task(task_name, kwargs={task_kwarg_key: str(job_id)}, task_id=task_id)
            except Exception:
                session.execute(
                    update(model_cls)
                    .where(model_cls.id == job_id, model_cls.celery_task_id == task_id)
                    .values(celery_task_id=None)
                )
                session.commit()
                raise
            JOBS_STUCK_REDISPATCHED_TOTAL.labels(model=model_name).inc()
        except Exception:
            session.rollback()
            logger.exception("reaper.redispatch_failed", model=model_name, id=str(job_id))
    counts[counts_key] = len(stale_ids)


def _fail_stale_running_jobs(
    session,
    model_cls,
    model_name: str,
    job_type: str,
    cutoff,
    counts: dict[str, int],
    counts_key: str,
) -> None:
    """Fail jobs stuck in 'running' state longer than *cutoff*."""
    from datetime import UTC, datetime

    from sqlalchemy import or_, select, update

    from backtestforecast.observability.metrics import JOBS_STUCK_RUNNING

    stale_running_stmt = (
        select(model_cls.id)
        .where(
            model_cls.status == "running",
            or_(
                model_cls.last_heartbeat_at.isnot(None) & (model_cls.last_heartbeat_at < cutoff),
                model_cls.last_heartbeat_at.is_(None) & or_(
                    model_cls.started_at.isnot(None) & (model_cls.started_at < cutoff),
                    model_cls.started_at.is_(None) & (model_cls.created_at < cutoff),
                ),
            ),
        )
        .limit(50)
        .with_for_update(skip_locked=True)
    )
    stale_running_ids = list(session.scalars(stale_running_stmt))
    if stale_running_ids:
        now = datetime.now(UTC)
        values = {
            "status": "failed",
            "error_message": "Job was stuck in running state and was automatically failed.",
            "completed_at": now,
            "updated_at": now,
        }
        if hasattr(model_cls, "error_code"):
            values["error_code"] = "stale_running"
        session.execute(
            update(model_cls)
            .where(model_cls.id.in_(stale_running_ids), model_cls.status == "running")
            .values(**values)
        )
        session.commit()
        for rid in stale_running_ids:
            try:
                publish_job_status(job_type, rid, "failed", metadata={"error_code": "stale_running"})
            except Exception:
                pass
    counts[counts_key] = len(stale_running_ids)
    JOBS_STUCK_RUNNING.labels(model=model_name).set(len(stale_running_ids))


def _reap_stale_jobs_inner(stale_minutes: int) -> dict[str, int]:
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import or_, select, update

    from backtestforecast.models import BacktestRun, DailyRecommendation, ExportJob, NightlyPipelineRun, ScannerJob, SweepJob, SymbolAnalysis
    from backtestforecast.observability.metrics import DAILY_RECOMMENDATIONS_COUNT, JOBS_STUCK_RUNNING, OPTION_CACHE_ENTRIES, QUEUE_DEPTH

    try:
        from backtestforecast.market_data.service import get_global_cache_entries
        OPTION_CACHE_ENTRIES.set(get_global_cache_entries())
    except Exception:
        pass

    try:
        from backtestforecast.config import get_settings as _gs
        from redis import Redis as _Redis

        _r = _Redis.from_url(_gs().redis_url, decode_responses=True, socket_timeout=5)
        try:
            for q_name in ("research", "exports", "maintenance", "pipeline"):
                depth = _r.llen(q_name)
                QUEUE_DEPTH.labels(queue=q_name).set(depth)
            dlq_depth = _r.llen("bff:dead_letter_queue")
            DLQ_DEPTH.set(dlq_depth)
        finally:
            _r.close()
    except Exception:
        logger.warning("reaper.queue_depth_unavailable", exc_info=True)

    cutoff = datetime.now(UTC) - timedelta(minutes=stale_minutes)
    pipeline_cutoff = datetime.now(UTC) - timedelta(minutes=max(stale_minutes, 60))
    analysis_cutoff = datetime.now(UTC) - timedelta(minutes=max(stale_minutes, 45))
    sweep_cutoff = datetime.now(UTC) - timedelta(minutes=max(stale_minutes, 65))
    counts: dict[str, int] = {}

    _reap_models = [
        (BacktestRun, "BacktestRun", "backtests.run", "run_id", "backtest", cutoff),
        (ExportJob, "ExportJob", "exports.generate", "export_job_id", "export", cutoff),
        (ScannerJob, "ScannerJob", "scans.run_job", "job_id", "scan", cutoff),
        (SymbolAnalysis, "SymbolAnalysis", "analysis.deep_symbol", "analysis_id", "analysis", analysis_cutoff),
        (SweepJob, "SweepJob", "sweeps.run", "job_id", "sweep", sweep_cutoff),
    ]
    for model_cls, model_name, task_name, kwarg_key, job_type, model_cutoff in _reap_models:
        try:
            with create_worker_session() as session:
                try:
                    _reap_queued_jobs(session, model_cls, model_name, task_name, kwarg_key, model_cutoff, counts, f"{model_name.lower()}_queued")
                except Exception:
                    logger.exception("reaper.model_reap_failed", model=model_name, phase="queued")
                    session.rollback()
                try:
                    _fail_stale_running_jobs(session, model_cls, model_name, job_type, model_cutoff, counts, f"stale_running_{model_name.lower()}")
                except Exception:
                    logger.exception("reaper.model_reap_failed", model=model_name, phase="stale_running")
                    session.rollback()
        except Exception:
            logger.exception("reaper.model_session_failed", model=model_name)

    try:
        with create_worker_session() as session:
            stale_running_pipeline_stmt = (
                select(NightlyPipelineRun.id)
                .where(
                    NightlyPipelineRun.status == "running",
                    or_(
                        NightlyPipelineRun.started_at.isnot(None) & (NightlyPipelineRun.started_at < pipeline_cutoff),
                        NightlyPipelineRun.started_at.is_(None) & (NightlyPipelineRun.created_at < pipeline_cutoff),
                    ),
                )
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            stale_running_pipeline_ids = list(session.scalars(stale_running_pipeline_stmt))
            if stale_running_pipeline_ids:
                now = datetime.now(UTC)
                session.execute(
                    update(NightlyPipelineRun)
                    .where(NightlyPipelineRun.id.in_(stale_running_pipeline_ids), NightlyPipelineRun.status == "running")
                    .values(status="failed", error_message="Pipeline was stuck in running state and was automatically failed.", error_code="stale_running", completed_at=now, updated_at=now)
                )
                session.commit()
            counts["stale_running_pipelines"] = len(stale_running_pipeline_ids)
            JOBS_STUCK_RUNNING.labels(model="NightlyPipelineRun").set(len(stale_running_pipeline_ids))
    except Exception:
        logger.exception("reaper.pipeline_reap_failed")

    orphan_cutoff = datetime.now(UTC) - timedelta(minutes=15)
    _result_expires = celery_app.conf.get("result_expires", 7200)
    if isinstance(_result_expires, timedelta):
        _result_expires = int(_result_expires.total_seconds())
    result_expires_cutoff = datetime.now(UTC) - timedelta(seconds=_result_expires)
    for model_cls, model_name in [
        (BacktestRun, "BacktestRun"),
        (ExportJob, "ExportJob"),
        (ScannerJob, "ScannerJob"),
        (SymbolAnalysis, "SymbolAnalysis"),
        (SweepJob, "SweepJob"),
    ]:
        try:
            with create_worker_session() as session:
                orphan_ids_stmt = (
                    select(model_cls.id, model_cls.celery_task_id, model_cls.created_at)
                    .where(
                        model_cls.status == "queued",
                        model_cls.celery_task_id.isnot(None),
                        model_cls.created_at < orphan_cutoff,
                    )
                    .limit(50)
                    .with_for_update(skip_locked=True)
                )
                orphan_rows = list(session.execute(orphan_ids_stmt))
                recovered = 0
                for row_id, stale_task_id, created_at in orphan_rows:
                    task_alive = False
                    if stale_task_id:
                        try:
                            result_obj = celery_app.AsyncResult(stale_task_id)
                            state = result_obj.state
                            if state in ("STARTED", "RETRY", "RECEIVED"):
                                task_alive = True
                            elif state == "PENDING" and created_at > result_expires_cutoff:
                                task_alive = True
                        except Exception:
                            pass
                    if not task_alive:
                        session.execute(
                            update(model_cls)
                            .where(model_cls.id == row_id, model_cls.celery_task_id == stale_task_id)
                            .values(celery_task_id=None)
                        )
                        recovered += 1
                if recovered > 0:
                    session.commit()
                    logger.warning("reaper.orphan_recovery", model=model_name, count=recovered)
                else:
                    session.rollback()
        except Exception:
            logger.exception("reaper.orphan_recovery_failed", model=model_name)

    try:
        from sqlalchemy import text
        with create_worker_session() as stats_session:
            rec_count = stats_session.scalar(
                text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'daily_recommendations'")
            ) or 0
            DAILY_RECOMMENDATIONS_COUNT.set(rec_count)
    except Exception:
        pass

    total = sum(counts.values())
    if total > 0:
        logger.info("reaper.redispatched", counts=counts, total=total)

    return counts


@celery_app.task(
    name="maintenance.refresh_market_holidays",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=2,
    soft_time_limit=60,
    time_limit=90,
)
def refresh_market_holidays(self) -> dict[str, int | str]:
    """Fetch upcoming NYSE holidays from Massive and cache them in Redis."""
    from backtestforecast.config import get_settings
    from backtestforecast.integrations.massive_client import MassiveClient
    from backtestforecast.utils.dates import invalidate_holiday_cache, store_holidays_in_redis

    settings = get_settings()
    client = MassiveClient(api_key=settings.massive_api_key)
    try:
        holidays = client.get_market_holidays()
        count = store_holidays_in_redis(holidays)
        invalidate_holiday_cache()
        logger.info("market_holidays.refreshed", count=count)
        CELERY_TASKS_TOTAL.labels(task_name="maintenance.refresh_market_holidays", status="succeeded").inc()
        return {"status": "ok", "holidays_cached": count}
    except Exception as exc:
        CELERY_TASKS_TOTAL.labels(task_name="maintenance.refresh_market_holidays", status="failed").inc()
        logger.exception("market_holidays.refresh_failed")
        try:
            raise self.retry(exc=exc, countdown=300)
        except MaxRetriesExceededError:
            raise
    finally:
        try:
            client.close()
        except Exception:
            pass


@celery_app.task(
    name="maintenance.cleanup_audit_events",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=0,
    soft_time_limit=600,
    time_limit=660,
)
def cleanup_audit_events(self) -> dict:  # type: ignore[override]
    """Delete old high-volume audit events in batches to avoid table-locking.

    Before deletion, events are archived to a structured log entry that can be
    forwarded to cold storage (S3, BigQuery, etc.) via the log pipeline.
    Set ``AUDIT_CLEANUP_ENABLED=false`` to disable automatic deletion.
    """
    from backtestforecast.config import get_settings as _gs_audit
    _audit_settings = _gs_audit()
    if not _audit_settings.audit_cleanup_enabled:
        logger.info("audit.cleanup_disabled")
        return {"deleted": 0, "batches_run": 0, "limit_reached": False, "reason": "disabled"}

    from datetime import datetime, timedelta, timezone
    from sqlalchemy import delete, select
    from backtestforecast.models import AuditEvent

    BATCH_SIZE = 5000
    max_batches = 500
    cutoff = datetime.now(timezone.utc) - timedelta(days=_audit_settings.audit_cleanup_retention_days)
    high_volume_types = (
        "export.downloaded",
        "backtest.viewed",
        "scan.viewed",
        "analysis.viewed",
    )
    deleted = 0
    batches_run = 0
    limit_reached = False
    try:
        with create_worker_session() as session:
            for event_type in high_volume_types:
                if limit_reached:
                    break
                while True:
                    if batches_run >= max_batches:
                        limit_reached = True
                        break
                    batch_ids_subq = (
                        select(AuditEvent.id)
                        .where(
                            AuditEvent.event_type == event_type,
                            AuditEvent.created_at < cutoff,
                        )
                        .limit(BATCH_SIZE)
                        .scalar_subquery()
                    )
                    result = session.execute(
                        delete(AuditEvent).where(AuditEvent.id.in_(batch_ids_subq))
                    )
                    if result.rowcount == 0:
                        break
                    logger.info(
                        "audit.archival_batch",
                        event_type=event_type,
                        count=result.rowcount,
                    )
                    deleted += result.rowcount
                    batches_run += 1
                    session.commit()
    except SoftTimeLimitExceeded:
        logger.warning("audit.cleanup_time_limit", deleted=deleted, batches_run=batches_run)
    logger.info("audit.cleanup_complete", deleted=deleted, cutoff=cutoff.isoformat(), batches_run=batches_run, limit_reached=limit_reached)
    return {"deleted": deleted, "batches_run": batches_run, "limit_reached": limit_reached}


@celery_app.task(
    name="maintenance.cleanup_outbox",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=0,
    soft_time_limit=120,
    time_limit=150,
)
def cleanup_outbox(self) -> dict:  # type: ignore[override]
    """Remove outbox messages older than 7 days (batched)."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import delete, select

    from backtestforecast.models import OutboxMessage

    cutoff = datetime.now(UTC) - timedelta(days=7)
    deleted = 0
    batch_size = 500
    with create_worker_session() as session:
        while True:
            batch_ids = list(session.scalars(
                select(OutboxMessage.id)
                .where(OutboxMessage.created_at < cutoff)
                .limit(batch_size)
            ))
            if not batch_ids:
                break
            result = session.execute(
                delete(OutboxMessage).where(OutboxMessage.id.in_(batch_ids))
            )
            deleted += result.rowcount
            session.commit()
        logger.info("outbox.cleanup", deleted=deleted)
    CELERY_TASKS_TOTAL.labels(task_name="maintenance.cleanup_outbox", status="succeeded").inc()
    return {"deleted": deleted}


@celery_app.task(
    name="maintenance.cleanup_daily_recommendations",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=0,
    soft_time_limit=600,
    time_limit=660,
)
def cleanup_daily_recommendations(self, retention_days: int = 90, dry_run: bool = False) -> dict:  # type: ignore[override]
    """Delete old daily recommendations and their parent pipeline runs in batches.

    When *dry_run* is True, counts eligible records but does not delete.
    """
    from datetime import datetime, timedelta, timezone

    from sqlalchemy import delete, select

    from backtestforecast.models import DailyRecommendation, NightlyPipelineRun

    if retention_days < 7:
        logger.error("cleanup.retention_days_too_low", retention_days=retention_days)
        return {"status": "aborted", "reason": "retention_days must be >= 7"}

    BATCH_SIZE = 2000
    max_batches = 200
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted_recs = 0
    deleted_runs = 0
    batches_run = 0
    limit_reached = False

    with create_worker_session() as session:
        while batches_run < max_batches:
            batches_run += 1
            batch_ids = list(session.scalars(
                select(DailyRecommendation.id)
                .where(DailyRecommendation.created_at < cutoff)
                .limit(BATCH_SIZE)
            ))
            if not batch_ids:
                break
            if dry_run:
                deleted_recs += len(batch_ids)
                continue
            result = session.execute(
                delete(DailyRecommendation).where(DailyRecommendation.id.in_(batch_ids))
            )
            deleted_recs += result.rowcount
            session.commit()

        if batches_run >= max_batches:
            limit_reached = True

        orphan_stmt = (
            select(NightlyPipelineRun.id)
            .outerjoin(
                DailyRecommendation,
                NightlyPipelineRun.id == DailyRecommendation.pipeline_run_id,
            )
            .where(
                NightlyPipelineRun.created_at < cutoff,
                DailyRecommendation.id.is_(None),
            )
            .limit(BATCH_SIZE)
        )
        orphan_run_ids = list(session.scalars(orphan_stmt))
        if orphan_run_ids:
            result = session.execute(
                delete(NightlyPipelineRun).where(NightlyPipelineRun.id.in_(orphan_run_ids))
            )
            deleted_runs += result.rowcount
            session.commit()

    logger.info(
        "daily_recommendations.cleanup_complete",
        deleted_recs=deleted_recs,
        deleted_runs=deleted_runs,
        cutoff=cutoff.isoformat(),
        batches_run=batches_run,
        limit_reached=limit_reached,
    )
    return {
        "deleted_recs": deleted_recs,
        "deleted_runs": deleted_runs,
        "batches_run": batches_run,
        "limit_reached": limit_reached,
    }


_OUTBOX_TASK_MODEL_MAP: dict[str, str] = {
    "backtests.run": "BacktestRun",
    "exports.generate": "ExportJob",
    "scans.run_job": "ScannerJob",
    "sweeps.run": "SweepJob",
    "analysis.deep_symbol": "SymbolAnalysis",
}

_OUTBOX_TASK_ID_KWARG: dict[str, str] = {
    "backtests.run": "run_id",
    "exports.generate": "export_job_id",
    "scans.run_job": "job_id",
    "sweeps.run": "job_id",
    "analysis.deep_symbol": "analysis_id",
}


def _fail_outbox_correlated_job(session: "Session", msg: "OutboxMessage") -> None:
    """Mark the job correlated to a permanently-failed outbox message as failed.

    Uses the task name to determine the model class and the kwargs to find
    the job ID, then performs a CAS UPDATE to set the status to "failed"
    only if the job is still in a non-terminal state.
    """
    from datetime import UTC, datetime
    from sqlalchemy import update

    from backtestforecast import models

    model_name = _OUTBOX_TASK_MODEL_MAP.get(msg.task_name)
    if model_name is None:
        return
    model_cls = getattr(models, model_name, None)
    if model_cls is None:
        return
    id_kwarg = _OUTBOX_TASK_ID_KWARG.get(msg.task_name)
    if id_kwarg is None:
        return
    job_id_str = msg.task_kwargs_json.get(id_kwarg)
    if not job_id_str:
        job_id_str = str(msg.correlation_id) if msg.correlation_id else None
    if not job_id_str:
        return
    try:
        job_id = UUID(job_id_str)
    except (ValueError, TypeError):
        return

    now = datetime.now(UTC)
    values: dict[str, object] = {
        "status": "failed",
        "error_message": "Task dispatch failed after exhausting outbox retries.",
        "completed_at": now,
        "updated_at": now,
    }
    if hasattr(model_cls, "error_code"):
        values["error_code"] = "outbox_exhausted"
    try:
        session.execute(
            update(model_cls)
            .where(
                model_cls.id == job_id,
                model_cls.status.in_(("queued", "running")),
            )
            .values(**values)
        )
        logger.warning(
            "outbox.correlated_job_failed",
            task_name=msg.task_name,
            job_id=job_id_str,
            model=model_name,
        )
    except Exception:
        logger.warning(
            "outbox.correlated_job_fail_error",
            task_name=msg.task_name,
            job_id=job_id_str,
            exc_info=True,
        )


@celery_app.task(
    name="maintenance.poll_outbox",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=0,
    soft_time_limit=60,
    time_limit=90,
)
def poll_outbox(self, max_messages: int = 50) -> dict[str, int]:
    """Re-send pending outbox messages that were committed but never sent.

    This is the recovery path for the transactional outbox pattern.  When
    ``dispatch_celery_task`` commits a job + outbox row but fails to send
    the Celery task (broker down, network blip), this task picks up the
    pending message and dispatches it.

    Messages get up to ``_OUTBOX_MAX_RETRIES`` send attempts (across poll
    cycles).  On each failure, ``retry_count`` is incremented and the
    message stays "pending" for the next poll.  After max retries, the
    message is marked "failed" and the correlated job (if any) is also
    marked failed so the user sees an error instead of a stuck job.
    """
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select, update

    from backtestforecast.models import OutboxMessage
    from backtestforecast.observability.metrics import OUTBOX_RECOVERED_TOTAL

    _OUTBOX_MAX_RETRIES = 30  # ~30 poll cycles × 60s = 30min recovery window
    sent = 0
    skipped = 0
    failed = 0
    cutoff = datetime.now(UTC) - timedelta(seconds=60)

    with create_worker_session() as session:
        stmt = (
            select(OutboxMessage)
            .where(
                OutboxMessage.status == "pending",
                OutboxMessage.created_at < cutoff,
            )
            .order_by(OutboxMessage.created_at)
            .limit(max_messages)
            .with_for_update(skip_locked=True)
        )
        messages = list(session.scalars(stmt))
        for msg in messages:
            try:
                celery_app.send_task(
                    msg.task_name,
                    kwargs=msg.task_kwargs_json,
                    queue=msg.queue,
                )
                msg.status = "sent"
                sent += 1
                OUTBOX_RECOVERED_TOTAL.labels(task_name=msg.task_name).inc()
                logger.info(
                    "outbox.recovered",
                    task_name=msg.task_name,
                    correlation_id=str(msg.correlation_id),
                    retry_count=msg.retry_count,
                )
            except Exception:
                msg.retry_count += 1
                if msg.retry_count >= _OUTBOX_MAX_RETRIES:
                    msg.status = "failed"
                    msg.error_message = f"Exhausted {_OUTBOX_MAX_RETRIES} send attempts."
                    failed += 1
                    _fail_outbox_correlated_job(session, msg)
                    logger.error(
                        "outbox.max_retries_exceeded",
                        task_name=msg.task_name,
                        correlation_id=str(msg.correlation_id),
                        retry_count=msg.retry_count,
                    )
                else:
                    skipped += 1
                    logger.warning(
                        "outbox.poll_send_failed",
                        task_name=msg.task_name,
                        correlation_id=str(msg.correlation_id),
                        retry_count=msg.retry_count,
                        max_retries=_OUTBOX_MAX_RETRIES,
                        exc_info=True,
                    )
        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("outbox.poll_commit_failed")

    from backtestforecast.observability.metrics import OUTBOX_RECOVERED_TOTAL as _ort
    if sent or failed or skipped:
        logger.info(
            "outbox.poll_complete",
            sent=sent,
            failed=failed,
            skipped=skipped,
        )
    if failed > 0:
        try:
            import sentry_sdk as _sentry
            _sentry.capture_message(
                f"Outbox poll: {failed} message(s) permanently failed after max retries",
                level="warning",
            )
        except Exception:
            pass
    return {"sent": sent, "failed": failed, "skipped": skipped}


@celery_app.task(name="maintenance.reconcile_subscriptions", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=300, time_limit=360)
def reconcile_subscriptions(self) -> dict[str, int]:
    from backtestforecast.config import get_settings
    with create_worker_session() as session:
        from backtestforecast.services.billing import BillingService
        service = BillingService(session)
        actions = service.reconcile_subscriptions(grace_hours=get_settings().active_renewal_grace_hours)
        CELERY_TASKS_TOTAL.labels(task_name="maintenance.reconcile_subscriptions", status="succeeded").inc()
        return {"reconciled": len(actions)}


@celery_app.task(
    name="maintenance.cleanup_stripe_orphan",
    base=BaseTaskWithDLQ,
    bind=True,
    ignore_result=True,
    max_retries=5,
    soft_time_limit=60,
    time_limit=90,
)
def cleanup_stripe_orphan(
    self,
    subscription_id: str | None = None,
    customer_id: str | None = None,
    user_id_str: str | None = None,
) -> dict[str, str]:
    """Cancel a Stripe subscription and/or delete a Stripe customer after
    the DB user row has already been deleted.

    Dispatched by the account deletion endpoint when synchronous Stripe
    cleanup fails. Retries with exponential backoff (30s, 60s, 120s, 240s,
    480s) to handle transient Stripe outages.
    """
    from backtestforecast.observability.metrics import STRIPE_ORPHAN_CLEANUP_TOTAL

    if not subscription_id and not customer_id:
        STRIPE_ORPHAN_CLEANUP_TOTAL.labels(result="skipped").inc()
        return {"status": "skipped", "reason": "nothing_to_clean"}

    from backtestforecast.config import get_settings

    settings = get_settings()
    if not settings.stripe_secret_key:
        STRIPE_ORPHAN_CLEANUP_TOTAL.labels(result="skipped").inc()
        logger.warning(
            "stripe_orphan.no_stripe_key",
            subscription_id=subscription_id,
            customer_id=customer_id,
        )
        return {"status": "skipped", "reason": "stripe_not_configured"}

    try:
        import stripe
    except ImportError:
        STRIPE_ORPHAN_CLEANUP_TOTAL.labels(result="failed").inc()
        logger.error("stripe_orphan.stripe_sdk_not_installed")
        return {"status": "failed", "reason": "stripe_sdk_missing"}

    client = stripe.StripeClient(settings.stripe_secret_key)
    sub_ok = True
    cust_ok = True

    if subscription_id:
        try:
            client.subscriptions.cancel(subscription_id)
            logger.info(
                "stripe_orphan.subscription_cancelled",
                subscription_id=subscription_id,
                user_id=user_id_str,
            )
        except Exception as exc:
            exc_str = str(exc).lower()
            if "no such subscription" in exc_str or "resource_missing" in exc_str:
                logger.info(
                    "stripe_orphan.subscription_already_gone",
                    subscription_id=subscription_id,
                    user_id=user_id_str,
                )
            else:
                sub_ok = False
                logger.warning(
                    "stripe_orphan.subscription_cancel_failed",
                    subscription_id=subscription_id,
                    user_id=user_id_str,
                    exc_info=True,
                )

    if customer_id:
        try:
            client.customers.delete(customer_id)
            logger.info(
                "stripe_orphan.customer_deleted",
                customer_id=customer_id,
                user_id=user_id_str,
            )
        except Exception as exc:
            exc_str = str(exc).lower()
            if "no such customer" in exc_str or "resource_missing" in exc_str:
                logger.info(
                    "stripe_orphan.customer_already_gone",
                    customer_id=customer_id,
                    user_id=user_id_str,
                )
            else:
                cust_ok = False
                logger.warning(
                    "stripe_orphan.customer_delete_failed",
                    customer_id=customer_id,
                    user_id=user_id_str,
                    exc_info=True,
                )

    if sub_ok and cust_ok:
        STRIPE_ORPHAN_CLEANUP_TOTAL.labels(result="ok").inc()
        CELERY_TASKS_TOTAL.labels(task_name="maintenance.cleanup_stripe_orphan", status="succeeded").inc()
        return {"status": "ok"}

    CELERY_TASKS_TOTAL.labels(task_name="maintenance.cleanup_stripe_orphan", status="failed").inc()
    try:
        delay = 30 * (2 ** self.request.retries)
        raise self.retry(countdown=delay)
    except self.MaxRetriesExceededError:
        STRIPE_ORPHAN_CLEANUP_TOTAL.labels(result="failed").inc()
        logger.error(
            "stripe_orphan.max_retries_exceeded",
            subscription_id=subscription_id,
            customer_id=customer_id,
            user_id=user_id_str,
            sub_ok=sub_ok,
            cust_ok=cust_ok,
        )
        raise
