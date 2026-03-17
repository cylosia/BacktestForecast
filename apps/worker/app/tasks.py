from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar
from uuid import UUID, uuid4

_ModelT = TypeVar("_ModelT")

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session

import structlog
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from apps.worker.app.celery_app import celery_app
from backtestforecast.db.session import SessionLocal, create_worker_session
from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import AppError
from backtestforecast.events import _VALID_TARGET_STATUSES, publish_job_status
from backtestforecast.observability.metrics import (
    ANALYSIS_JOBS_TOTAL,
    BACKTEST_RUNS_TOTAL,
    CELERY_TASKS_TOTAL,
    DLQ_DEPTH,
    DLQ_MESSAGES_TOTAL,
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


class BaseTaskWithDLQ(celery_app.Task):  # type: ignore[misc]
    """Base class for Celery tasks that persists failure metadata to a Redis
    dead-letter list (``bff:dead_letter_queue``) when all retries are exhausted.

    Usage: set ``base=BaseTaskWithDLQ`` in ``@celery_app.task(...)`` decorators.
    Failed tasks are JSON-serialised and left-pushed so operators can inspect
    or replay them via ``LRANGE bff:dead_letter_queue 0 -1``.
    """

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        is_terminal = (
            self.request.retries >= self.max_retries
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
            fallback_conn = None
            try:
                import json
                from backtestforecast.config import get_settings
                redis_conn = self.app.backend.client if hasattr(self.app, 'backend') and hasattr(self.app.backend, 'client') else None
                if redis_conn is None:
                    from redis import Redis
                    fallback_conn = Redis.from_url(get_settings().redis_url, socket_timeout=5)
                    redis_conn = fallback_conn
                dlq_key = "bff:dead_letter_queue"
                redis_conn.lpush(dlq_key, json.dumps({
                    "task_name": self.name,
                    "task_id": task_id,
                    "args": list(args or []),
                    "kwargs": dict(kwargs or {}),
                    "retries": self.request.retries,
                    "error": str(exc),
                }))
                redis_conn.ltrim(dlq_key, 0, 4999)
                redis_conn.expire(dlq_key, 60 * 60 * 24 * 30)
                DLQ_MESSAGES_TOTAL.labels(task_name=self.name).inc()
                try:
                    DLQ_DEPTH.set(redis_conn.llen(dlq_key))
                except Exception:
                    pass
            except Exception:  # Intentional: DLQ persistence is best-effort. Failure to
                # write to the DLQ must not mask the original task failure.
                logger.warning("task.dlq_persist_failed", exc_info=True)
            finally:
                if fallback_conn is not None:
                    try:
                        fallback_conn.close()
                    except Exception:
                        pass


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

    logger.error(
        "pipeline.find_run_fallback",
        trade_date=str(trade_date),
        msg=(
            "No run_id available; falling back to heuristic date-based lookup. "
            "This may mark the WRONG pipeline run as failed if multiple runs "
            "exist for the same trade_date. Investigate why run_id was not "
            "captured — this usually means the pipeline raised before "
            "returning a run object."
        ),
    )
    stmt = (
        select(model_cls)
        .where(model_cls.trade_date == trade_date, model_cls.status == "running")
        .order_by(desc(model_cls.created_at))
        .limit(1)
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
                        symbols = parsed
                        logger.info("pipeline.symbols_from_redis", count=len(parsed))
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

        from redis import Redis
        lock_key = f"bff:pipeline:{trade_date.isoformat()}"
        redis_client = Redis.from_url(settings.redis_url, socket_timeout=5)
        try:
            lock = redis_client.lock(lock_key, timeout=1900, blocking=False)
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

        try:
            with create_worker_session() as session:
                service = NightlyPipelineService(
                    session,
                    market_data_fetcher=market_data,
                    backtest_executor=executor,
                    forecaster=forecaster,
                )
                run = None
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
                    try:
                        raise self.retry(exc=exc, countdown=300, kwargs={
                            "symbols": symbols,
                            "max_recommendations": max_recommendations,
                            "trade_date_iso": trade_date.isoformat(),
                        })
                    except MaxRetriesExceededError:
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
            executor.close()
        except Exception:
            logger.exception("executor.close_failed")
        try:
            client.close()
        except Exception:
            logger.exception("client.close_failed")


_TERMINAL_STATUSES = _VALID_TARGET_STATUSES | frozenset({"expired"})


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
        )
        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("validate_task_ownership.commit_failed", model=model_cls.__name__, obj_id=str(obj_id), exc_info=True)
            return False
        if result.rowcount == 0:
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
        )
        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("validate_task_ownership.redelivery_commit_failed", model=model_cls.__name__, obj_id=str(obj_id), exc_info=True)
            return False
        if result.rowcount > 0:
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
    with SessionLocal() as session:
        if not _validate_task_ownership(session, BacktestRun, UUID(run_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="backtests.run").inc()
            logger.info("backtests.run.duplicate_delivery", run_id=run_id, task_id=self.request.id)
            return {"status": "skipped", "run_id": run_id, "reason": "duplicate_delivery"}
        run_obj = session.get(BacktestRun, UUID(run_id))
        if run_obj is None:
            CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="failed").inc()
            return {"status": "failed", "run_id": run_id, "error_code": "not_found"}
        user = session.get(User, run_obj.user_id)
        if user is None:
            run_obj.status = "failed"
            run_obj.error_code = "entitlement_revoked"
            run_obj.error_message = "User account not found."
            session.commit()
            publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "run_id": run_id, "error_code": "entitlement_revoked"}
        policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        if policy.monthly_backtest_quota is not None:
            if policy.monthly_backtest_quota <= 0:
                run_obj.status = "failed"
                run_obj.error_code = "entitlement_revoked"
                run_obj.error_message = "Your plan no longer supports this operation."
                session.commit()
                publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "run_id": run_id, "error_code": "entitlement_revoked"}
            from datetime import UTC, datetime
            from backtestforecast.repositories.backtest_runs import BacktestRunRepository
            repo = BacktestRunRepository(session)
            now = datetime.now(UTC)
            month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
            next_month_start = datetime(now.year + 1, 1, 1, tzinfo=UTC) if now.month == 12 else datetime(now.year, now.month + 1, 1, tzinfo=UTC)
            used = repo.count_for_user_created_between(user.id, start_inclusive=month_start, end_exclusive=next_month_start)
            used = max(used - 1, 0)
            if used >= policy.monthly_backtest_quota:
                run_obj.status = "failed"
                run_obj.error_code = "quota_exceeded"
                run_obj.error_message = f"Monthly backtest quota ({policy.monthly_backtest_quota}) reached. Used: {used}."
                session.commit()
                publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": "quota_exceeded"})
                return {"status": "failed", "run_id": run_id, "error_code": "quota_exceeded"}
        publish_job_status("backtest", UUID(run_id), "running")
        service = BacktestService(session)
        try:
            run = service.execute_run_by_id(UUID(run_id))
        except AppError as exc:
            session.rollback()
            session.expire_all()
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
                delay = 30 * (self.request.retries + 1)
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

        BACKTEST_RUNS_TOTAL.labels(status=run.status).inc()
        CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status=run.status).inc()
        publish_job_status("backtest", UUID(run_id), run.status)
        return {
            "status": run.status,
            "run_id": run_id,
            "trade_count": run.trade_count,
        }


@celery_app.task(name="exports.generate", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=2, soft_time_limit=120, time_limit=150)
def generate_export(self, export_job_id: str) -> dict[str, str | int]:
    with SessionLocal() as session:
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
            session.commit()
            publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": "entitlement_revoked"})
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
            session.commit()
            publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": "unsupported_format"})
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "unsupported_format"}
        if not policy.export_formats or requested_format not in policy.export_formats:
            ej.status = "failed"
            ej.error_code = "entitlement_revoked"
            ej.error_message = f"Your plan no longer supports {ej.export_format} export."
            session.commit()
            publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "entitlement_revoked"}
        publish_job_status("export", UUID(export_job_id), "running")
        service = ExportService(session)
        try:
            job = service.execute_export_by_id(UUID(export_job_id))
        except AppError as exc:
            session.rollback()
            session.expire_all()
            ej_obj = session.get(ExportJobModel, UUID(export_job_id))
            if ej_obj is not None and ej_obj.status in ("queued", "running"):
                ej_obj.status = "failed"
                ej_obj.error_code = exc.code
                ej_obj.error_message = str(exc.message)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
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
                delay = 15 * (self.request.retries + 1)
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
        publish_job_status("export", UUID(export_job_id), job.status)
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

        with SessionLocal() as session:
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
                session.commit()
                publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
            policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
            if not policy.forecasting_access:
                sa_obj.status = "failed"
                sa_obj.error_code = "entitlement_revoked"
                sa_obj.error_message = "Your plan no longer supports this operation."
                session.commit()
                publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
            publish_job_status("analysis", UUID(analysis_id), "running")
            service = SymbolDeepAnalysisService(
                session,
                market_data_fetcher=market_data,
                backtest_executor=executor,
                forecaster=forecaster,
            )
            try:
                result = service.execute_analysis(UUID(analysis_id))
            except AppError as exc:
                session.rollback()
                session.expire_all()
                sa_fail = session.get(SymbolAnalysis, UUID(analysis_id))
                if sa_fail is not None and sa_fail.status in ("queued", "running"):
                    sa_fail.status = "failed"
                    sa_fail.error_code = exc.code
                    sa_fail.error_message = str(exc.message)
                    try:
                        session.commit()
                    except Exception:
                        session.rollback()
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
                    raise self.retry(exc=exc, countdown=60)
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
                    CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                    publish_job_status(
                        "analysis", UUID(analysis_id), "failed",
                        metadata={"error_code": "max_retries_exceeded"},
                    )
                    raise

            effective_status = "succeeded" if result.status == "succeeded" else "failed"
            CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status=effective_status).inc()
            ANALYSIS_JOBS_TOTAL.labels(status=result.status).inc()
            publish_job_status("analysis", UUID(analysis_id), result.status)
            return {
                "status": result.status,
                "analysis_id": analysis_id,
                "top_results": result.top_results_count,
            }
    finally:
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
    with SessionLocal() as session:
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
        service = ScanService(session)
        try:
            job = service.run_job(UUID(job_id))
        except AppError as exc:
            session.rollback()
            session.expire_all()
            sj_obj = session.get(ScannerJobModel, UUID(job_id))
            if sj_obj is not None and sj_obj.status in ("queued", "running"):
                sj_obj.status = "failed"
                sj_obj.error_code = exc.code
                sj_obj.error_message = str(exc.message)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
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
                delay = 60 * (self.request.retries + 1)
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

        effective_status = "succeeded" if job.status == "succeeded" else "failed"
        CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status=effective_status).inc()
        SCAN_JOBS_TOTAL.labels(status=job.status).inc()
        publish_job_status("scan", UUID(job_id), job.status)
        return {
            "status": job.status,
            "job_id": job_id,
            "recommendation_count": job.recommendation_count,
        }


@celery_app.task(name="sweeps.run", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=2, soft_time_limit=3600, time_limit=3660)
def run_sweep(self, job_id: str) -> dict[str, str | int]:
    with SessionLocal() as session:
        if not _validate_task_ownership(session, SweepJobModel, UUID(job_id), self.request.id):
            DUPLICATE_TASK_EXECUTION_TOTAL.labels(task_name="sweeps.run").inc()
            logger.info("sweeps.run.duplicate_delivery", job_id=job_id, task_id=self.request.id)
            return {"status": "skipped", "job_id": job_id, "reason": "duplicate_delivery"}
        sj = session.get(SweepJobModel, UUID(job_id))
        if sj is None:
            CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
            return {"status": "failed", "job_id": job_id, "error_code": "not_found"}
        publish_job_status("sweep", UUID(job_id), "running")
        service = SweepService(session)
        try:
            job = service.run_job(UUID(job_id))
        except AppError as exc:
            session.rollback()
            session.expire_all()
            sweep_obj = session.get(SweepJobModel, UUID(job_id))
            if sweep_obj is not None and sweep_obj.status in ("queued", "running"):
                sweep_obj.status = "failed"
                sweep_obj.error_code = exc.code
                sweep_obj.error_message = str(exc.message)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
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
            CELERY_TASKS_TOTAL.labels(task_name="sweeps.run", status="failed").inc()
            publish_job_status("sweep", UUID(job_id), "failed", metadata={"error_code": "time_limit_exceeded"})
            raise
        except Exception as exc:
            session.rollback()
            session.expire_all()
            try:
                delay = 120 * (self.request.retries + 1)
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
        publish_job_status("sweep", UUID(job_id), job.status)
        return {
            "status": job.status,
            "job_id": job_id,
            "result_count": job.result_count,
        }


@celery_app.task(name="scans.refresh_prioritized", base=BaseTaskWithDLQ, bind=True, ignore_result=True, max_retries=1, soft_time_limit=300, time_limit=360)
def refresh_prioritized_scans(self) -> dict[str, int]:
    from sqlalchemy import update as sa_update

    from backtestforecast.models import ScannerJob

    dispatched = 0
    with SessionLocal() as session:
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


_S3_ORPHAN_MAX_DELETIONS = 500


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
        prefix = getattr(s3_storage, "_prefix", "")
        s3_client = getattr(s3_storage, "_client", None)
        s3_bucket = getattr(s3_storage, "_bucket", settings.s3_bucket)
        if s3_client is None:
            logger.warning("s3_orphan_reconciliation_skipped", reason="no_s3_client")
            return

        with create_worker_session() as session:
            orphan_count = 0
            limit_reached = False
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                page_keys = [obj["Key"] for obj in page.get("Contents", [])]
                if not page_keys:
                    continue
                existing = set(session.scalars(
                    select(ExportJob.storage_key).where(
                        ExportJob.storage_key.in_(page_keys)
                    )
                ))
                for s3_key in page_keys:
                    if orphan_count >= _S3_ORPHAN_MAX_DELETIONS:
                        limit_reached = True
                        break
                    if s3_key not in existing:
                        logger.info("s3_orphan_deleting", s3_key=s3_key)
                        s3_storage.delete(s3_key)
                        orphan_count += 1
                if limit_reached:
                    break
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
def reap_stale_jobs(self, stale_minutes: int = 30) -> dict[str, int]:
    """Re-dispatch jobs stuck in 'queued' with no celery_task_id for too long."""
    from redis import Redis

    from backtestforecast.config import get_settings
    from backtestforecast.observability.metrics import CELERY_WORKERS_ONLINE

    settings = get_settings()

    try:
        _count_redis = Redis.from_url(settings.redis_url, socket_timeout=5)
        heartbeat_count = 0
        for _ in _count_redis.scan_iter("worker:heartbeat:*", count=100):
            heartbeat_count += 1
        CELERY_WORKERS_ONLINE.set(heartbeat_count)
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
            rows = session.execute(
                update(model_cls)
                .where(model_cls.id == job_id, model_cls.celery_task_id.is_(None))
                .values(celery_task_id=task_id)
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
                model_cls.started_at.isnot(None) & (model_cls.started_at < cutoff),
                model_cls.started_at.is_(None) & (model_cls.created_at < cutoff),
            ),
        )
        .limit(50)
        .with_for_update(skip_locked=True)
    )
    stale_running_ids = list(session.scalars(stale_running_stmt))
    if stale_running_ids:
        values = {
            "status": "failed",
            "error_message": "Job was stuck in running state and was automatically failed.",
            "completed_at": datetime.now(UTC),
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

    from backtestforecast.models import BacktestRun, ExportJob, NightlyPipelineRun, ScannerJob, SymbolAnalysis
    from backtestforecast.observability.metrics import JOBS_STUCK_RUNNING, QUEUE_DEPTH

    try:
        from backtestforecast.config import get_settings as _gs
        from redis import Redis as _Redis

        _r = _Redis.from_url(_gs().redis_url, decode_responses=True, socket_timeout=5)
        try:
            for q_name in ("research", "exports", "maintenance", "pipeline"):
                depth = _r.llen(q_name)
                QUEUE_DEPTH.labels(queue=q_name).set(depth)
        finally:
            _r.close()
    except Exception:
        logger.warning("reaper.queue_depth_unavailable", exc_info=True)

    cutoff = datetime.now(UTC) - timedelta(minutes=stale_minutes)
    pipeline_cutoff = datetime.now(UTC) - timedelta(minutes=max(stale_minutes, 60))
    analysis_cutoff = datetime.now(UTC) - timedelta(minutes=max(stale_minutes, 45))
    counts: dict[str, int] = {}

    with create_worker_session() as session:
        _reap_queued_jobs(session, BacktestRun, "BacktestRun", "backtests.run", "run_id", cutoff, counts, "backtest_runs")
        _fail_stale_running_jobs(session, BacktestRun, "BacktestRun", "backtest", cutoff, counts, "stale_running_backtests")

        _reap_queued_jobs(session, ExportJob, "ExportJob", "exports.generate", "export_job_id", cutoff, counts, "export_jobs")
        _fail_stale_running_jobs(session, ExportJob, "ExportJob", "export", cutoff, counts, "stale_running_exports")

        _reap_queued_jobs(session, ScannerJob, "ScannerJob", "scans.run_job", "job_id", cutoff, counts, "scanner_jobs")
        _fail_stale_running_jobs(session, ScannerJob, "ScannerJob", "scan", cutoff, counts, "stale_running_scans")

        _reap_queued_jobs(session, SymbolAnalysis, "SymbolAnalysis", "analysis.deep_symbol", "analysis_id", analysis_cutoff, counts, "symbol_analyses")
        _fail_stale_running_jobs(session, SymbolAnalysis, "SymbolAnalysis", "analysis", analysis_cutoff, counts, "stale_running_analyses")

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
            session.execute(
                update(NightlyPipelineRun)
                .where(NightlyPipelineRun.id.in_(stale_running_pipeline_ids), NightlyPipelineRun.status == "running")
                .values(status="failed", error_message="Pipeline was stuck in running state and was automatically failed.", completed_at=datetime.now(UTC))
            )
            session.commit()
        counts["stale_running_pipelines"] = len(stale_running_pipeline_ids)
        JOBS_STUCK_RUNNING.labels(model="NightlyPipelineRun").set(len(stale_running_pipeline_ids))

        orphan_cutoff = datetime.now(UTC) - timedelta(minutes=15)
        result_expires_cutoff = datetime.now(UTC) - timedelta(seconds=600)
        for model_cls, model_name in [
            (BacktestRun, "BacktestRun"),
            (ExportJob, "ExportJob"),
            (ScannerJob, "ScannerJob"),
            (SymbolAnalysis, "SymbolAnalysis"),
        ]:
            try:
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
                session.rollback()
                logger.exception("reaper.orphan_recovery_failed", model=model_name)

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
    """Delete old high-volume audit events in batches to avoid table-locking."""
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import delete, select
    from backtestforecast.db.session import SessionLocal
    from backtestforecast.models import AuditEvent

    BATCH_SIZE = 5000
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    high_volume_types = (
        "export.downloaded",
        "backtest.viewed",
        "scan.viewed",
        "analysis.viewed",
    )
    deleted = 0
    with SessionLocal() as session:
        for event_type in high_volume_types:
            while True:
                batch_ids = list(session.scalars(
                    select(AuditEvent.id)
                    .where(
                        AuditEvent.event_type == event_type,
                        AuditEvent.created_at < cutoff,
                    )
                    .limit(BATCH_SIZE)
                ))
                if not batch_ids:
                    break
                result = session.execute(
                    delete(AuditEvent).where(AuditEvent.id.in_(batch_ids))
                )
                deleted += result.rowcount
                session.commit()
    logger.info("audit.cleanup_complete", deleted=deleted, cutoff=cutoff.isoformat())
    return {"deleted": deleted}
