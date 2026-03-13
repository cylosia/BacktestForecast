from __future__ import annotations

from uuid import UUID

import structlog
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from apps.worker.app.celery_app import celery_app
from backtestforecast.db.session import SessionLocal
from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import AppError
from backtestforecast.events import publish_job_status
from backtestforecast.observability.metrics import (
    BACKTEST_RUNS_TOTAL,
    CELERY_TASKS_TOTAL,
)
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.exports import ExportService
from backtestforecast.services.scans import ScanService

logger = structlog.get_logger("worker.tasks")


@celery_app.task(name="maintenance.ping")
def ping() -> dict[str, str]:
    return {
        "status": "ok",
        "task": "maintenance.ping",
        "note": "Worker is reachable.",
    }


def _find_pipeline_run(session, model_cls, run, trade_date):
    """Return the pipeline run object for failure marking.

    When *run* was returned by ``run_pipeline`` we can look it up by ID.
    When *run* is still ``None`` (pipeline raised before returning), fall
    back to querying for the most recent running row for *trade_date*.
    """
    if run is not None:
        return session.get(model_cls, run.id)
    from sqlalchemy import select, desc

    stmt = (
        select(model_cls)
        .where(model_cls.trade_date == trade_date, model_cls.status == "running")
        .order_by(desc(model_cls.created_at))
        .limit(1)
    )
    return session.scalar(stmt)


@celery_app.task(name="pipeline.nightly_scan", bind=True, max_retries=1, soft_time_limit=1800, time_limit=1860)
def nightly_scan_pipeline(
    self,
    symbols: list[str] | None = None,
    max_recommendations: int = 20,
) -> dict[str, str | int]:
    """Execute the full nightly scan pipeline."""
    from datetime import UTC, datetime

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

        # Default universe: configurable symbol list
        if symbols is None:
            symbols = settings.pipeline_default_symbols

        from zoneinfo import ZoneInfo

        trade_date = datetime.now(ZoneInfo("America/New_York")).date()

        with SessionLocal() as session:
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
                    raise self.retry(exc=exc, countdown=300)
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
            return {
                "status": run.status,
                "run_id": str(run.id),
                "recommendations": run.recommendations_produced,
                "duration_seconds": (float(run.duration_seconds) if run.duration_seconds else 0),
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


def _validate_task_ownership(session, model_cls, obj_id: UUID, expected_task_id: str | None) -> bool:
    """Return True if this Celery delivery owns the job, False if it's a duplicate."""
    if expected_task_id is None:
        return True
    obj = session.get(model_cls, obj_id)
    if obj is None:
        return True
    stored = getattr(obj, "celery_task_id", None)
    if stored is None:
        return True
    return stored == expected_task_id


@celery_app.task(name="backtests.run", bind=True, max_retries=2, soft_time_limit=300, time_limit=330)
def run_backtest(self, run_id: str) -> dict[str, str]:
    try:
        publish_job_status("backtest", UUID(run_id), "running")
    except Exception:
        logger.warning("publish_job_status.failed", job_id=run_id, exc_info=True)
    with SessionLocal() as session:
        from backtestforecast.models import BacktestRun, User
        if not _validate_task_ownership(session, BacktestRun, UUID(run_id), self.request.id):
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
        if policy.monthly_backtest_quota is not None and policy.monthly_backtest_quota <= 0:
            run_obj.status = "failed"
            run_obj.error_code = "entitlement_revoked"
            run_obj.error_message = "Your plan no longer supports this operation."
            session.commit()
            publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": "entitlement_revoked"})
            return {"status": "failed", "run_id": run_id, "error_code": "entitlement_revoked"}
        service = BacktestService(session)
        try:
            run = service.execute_run_by_id(UUID(run_id))
        except AppError as exc:
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
            from datetime import UTC, datetime

            from backtestforecast.models import BacktestRun

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
            return {"status": "failed", "run_id": run_id, "error_code": "time_limit_exceeded"}
        except Exception as exc:  # pragma: no cover
            session.rollback()
            try:
                delay = 30 * (self.request.retries + 1)
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                from backtestforecast.models import BacktestRun

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
                return {"status": "failed", "run_id": run_id, "error_code": "max_retries_exceeded"}
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


@celery_app.task(name="exports.generate", bind=True, max_retries=2, soft_time_limit=120, time_limit=150)
def generate_export(self, export_job_id: str) -> dict[str, str | int]:
    try:
        publish_job_status("export", UUID(export_job_id), "running")
    except Exception:
        logger.warning("publish_job_status.failed", job_id=export_job_id, exc_info=True)
    with SessionLocal() as session:
        from backtestforecast.models import ExportJob as ExportJobModel, User
        if not _validate_task_ownership(session, ExportJobModel, UUID(export_job_id), self.request.id):
            logger.info("exports.generate.duplicate_delivery", export_job_id=export_job_id, task_id=self.request.id)
            return {"status": "skipped", "export_job_id": export_job_id, "reason": "duplicate_delivery"}
        ej = session.get(ExportJobModel, UUID(export_job_id))
        if ej is not None:
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
            if not policy.export_formats or (requested_format is not None and requested_format not in policy.export_formats):
                ej.status = "failed"
                ej.error_code = "entitlement_revoked"
                ej.error_message = f"Your plan no longer supports {ej.export_format} export."
                session.commit()
                publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": "entitlement_revoked"})
                return {"status": "failed", "export_job_id": export_job_id, "error_code": "entitlement_revoked"}
        service = ExportService(session)
        try:
            job = service.execute_export_by_id(UUID(export_job_id))
        except AppError as exc:
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
            from datetime import UTC, datetime

            from backtestforecast.models import ExportJob

            export_obj = session.get(ExportJob, UUID(export_job_id))
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
            return {"status": "failed", "export_job_id": export_job_id, "error_code": "time_limit_exceeded"}
        except Exception as exc:  # pragma: no cover
            session.rollback()
            try:
                delay = 15 * (self.request.retries + 1)
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                from backtestforecast.models import ExportJob

                export_obj = session.get(ExportJob, UUID(export_job_id))
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
                return {"status": "failed", "export_job_id": export_job_id, "error_code": "max_retries_exceeded"}
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("service.close_failed")

        if job.status == "succeeded":
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="succeeded").inc()
        else:
            CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="failed").inc()
        publish_job_status("export", UUID(export_job_id), job.status)
        return {
            "status": job.status,
            "export_job_id": export_job_id,
            "size_bytes": job.size_bytes,
        }


@celery_app.task(name="analysis.deep_symbol", bind=True, max_retries=1, soft_time_limit=600, time_limit=660)
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

    try:
        publish_job_status("analysis", UUID(analysis_id), "running")
    except Exception:
        logger.warning("publish_job_status.failed", job_id=analysis_id, exc_info=True)
    settings = get_settings()
    client = MassiveClient(api_key=settings.massive_api_key)
    shared_mds = MarketDataService(client)
    shared_exec = _BES(market_data_service=shared_mds)
    executor = PipelineBacktestExecutor(execution_service=shared_exec)
    try:
        market_data = PipelineMarketDataFetcher(client)
        forecaster = PipelineForecaster(HistoricalAnalogForecaster(), market_data)

        with SessionLocal() as session:
            from backtestforecast.models import SymbolAnalysis, User
            if not _validate_task_ownership(session, SymbolAnalysis, UUID(analysis_id), self.request.id):
                logger.info("analysis.deep_symbol.duplicate_delivery", analysis_id=analysis_id, task_id=self.request.id)
                return {"status": "skipped", "analysis_id": analysis_id, "reason": "duplicate_delivery"}
            sa_obj = session.get(SymbolAnalysis, UUID(analysis_id))
            if sa_obj is not None:
                user = session.get(User, sa_obj.user_id)
                if user is None:
                    sa_obj.status = "failed"
                    sa_obj.error_message = "User account not found."
                    session.commit()
                    publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                    return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
                policy = resolve_feature_policy(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
                if not policy.forecasting_access:
                    sa_obj.status = "failed"
                    sa_obj.error_message = "Your plan no longer supports this operation."
                    session.commit()
                    publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": "entitlement_revoked"})
                    return {"status": "failed", "analysis_id": analysis_id, "error_code": "entitlement_revoked"}
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
                CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="failed").inc()
                publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": exc.code})
                return {
                    "status": "failed",
                    "analysis_id": analysis_id,
                    "error_code": exc.code,
                }
            except SoftTimeLimitExceeded:
                session.rollback()
                from backtestforecast.models import SymbolAnalysis as _SA

                analysis = session.get(_SA, UUID(analysis_id))
                if analysis is not None and analysis.status in ("queued", "running"):
                    analysis.status = "failed"
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
                return {
                    "status": "failed",
                    "analysis_id": analysis_id,
                    "error_code": "time_limit_exceeded",
                }
            except Exception as exc:
                session.rollback()
                try:
                    raise self.retry(exc=exc, countdown=60)
                except self.MaxRetriesExceededError:
                    from backtestforecast.models import SymbolAnalysis

                    analysis = session.get(SymbolAnalysis, UUID(analysis_id))
                    if analysis is not None and analysis.status in ("queued", "running"):
                        analysis.status = "failed"
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
                    return {
                        "status": "failed",
                        "analysis_id": analysis_id,
                        "error_code": "max_retries_exceeded",
                    }

            effective_status = "succeeded" if result.status == "succeeded" else "failed"
            CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status=effective_status).inc()
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


@celery_app.task(name="scans.run_job", bind=True, max_retries=3, soft_time_limit=600, time_limit=660)
def run_scan_job(self, job_id: str) -> dict[str, str | int]:
    try:
        publish_job_status("scan", UUID(job_id), "running")
    except Exception:
        logger.warning("publish_job_status.failed", job_id=job_id, exc_info=True)
    with SessionLocal() as session:
        from backtestforecast.models import ScannerJob as ScannerJobModel, User
        if not _validate_task_ownership(session, ScannerJobModel, UUID(job_id), self.request.id):
            logger.info("scans.run_job.duplicate_delivery", job_id=job_id, task_id=self.request.id)
            return {"status": "skipped", "job_id": job_id, "reason": "duplicate_delivery"}
        sj = session.get(ScannerJobModel, UUID(job_id))
        if sj is not None:
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
        service = ScanService(session)
        try:
            job = service.run_job(UUID(job_id))
        except AppError as exc:
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
            from datetime import UTC, datetime

            from backtestforecast.models import ScannerJob as ScannerJobModel

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
            return {"status": "failed", "job_id": job_id, "error_code": "time_limit_exceeded"}
        except Exception as exc:  # pragma: no cover
            session.rollback()
            try:
                delay = 60 * (self.request.retries + 1)
                raise self.retry(exc=exc, countdown=delay)
            except self.MaxRetriesExceededError:
                from datetime import UTC, datetime

                from backtestforecast.models import ScannerJob as ScannerJobModel

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
                return {"status": "failed", "job_id": job_id, "error_code": "max_retries_exceeded"}
        finally:
            try:
                service.close()
            except Exception:
                logger.exception("service.close_failed")

        effective_status = "succeeded" if job.status == "succeeded" else "failed"
        CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status=effective_status).inc()
        publish_job_status("scan", UUID(job_id), job.status)
        return {
            "status": job.status,
            "job_id": job_id,
            "recommendation_count": job.recommendation_count,
        }


@celery_app.task(name="scans.refresh_prioritized")
def refresh_prioritized_scans() -> dict[str, int]:
    dispatched = 0
    with SessionLocal() as session:
        service = ScanService(session)
        try:
            jobs = service.create_scheduled_refresh_jobs(limit=25)

            for job in jobs:
                try:
                    result = celery_app.send_task("scans.run_job", kwargs={"job_id": str(job.id)})
                    job.celery_task_id = result.id
                    session.commit()
                    dispatched += 1
                except Exception:
                    logger.exception("refresh.dispatch_failed", job_id=str(job.id))
                    session.rollback()
                    from backtestforecast.models import ScannerJob
                    job_obj = session.get(ScannerJob, job.id)
                    if job_obj is not None and job_obj.status == "queued":
                        job_obj.status = "failed"
                        job_obj.error_code = "enqueue_failed"
                        job_obj.error_message = "Unable to dispatch scheduled refresh."
                        try:
                            session.commit()
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


@celery_app.task(name="maintenance.reap_stale_jobs")
def reap_stale_jobs(stale_minutes: int = 30) -> dict[str, int]:
    """Re-dispatch jobs stuck in 'queued' with no celery_task_id for too long."""
    from datetime import UTC, datetime, timedelta

    from redis import Redis
    from sqlalchemy import select, update

    from backtestforecast.config import get_settings
    from backtestforecast.models import BacktestRun, ExportJob, NightlyPipelineRun, ScannerJob, SymbolAnalysis
    from backtestforecast.observability.metrics import JOBS_STUCK_REDISPATCHED_TOTAL

    settings = get_settings()
    try:
        redis = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=5.0)
        lock = redis.lock("bff:reaper:lock", timeout=300, blocking_timeout=0)
        if not lock.acquire(blocking=False):
            logger.info("reaper.skipped_locked")
            return {"skipped": 1}
    except Exception:
        logger.warning("reaper.lock_unavailable", exc_info=True)
        lock = None

    try:
        return _reap_stale_jobs_inner(stale_minutes)
    finally:
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass


def _reap_stale_jobs_inner(stale_minutes: int) -> dict[str, int]:
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select, update

    from backtestforecast.models import BacktestRun, ExportJob, NightlyPipelineRun, ScannerJob, SymbolAnalysis
    from backtestforecast.observability.metrics import JOBS_STUCK_REDISPATCHED_TOTAL

    cutoff = datetime.now(UTC) - timedelta(minutes=stale_minutes)
    counts: dict[str, int] = {}

    with SessionLocal() as session:
        stale_runs_stmt = (
            select(BacktestRun.id)
            .where(
                BacktestRun.status == "queued",
                BacktestRun.celery_task_id.is_(None),
                BacktestRun.created_at < cutoff,
            )
            .limit(50)
        )
        stale_run_ids = list(session.scalars(stale_runs_stmt))
        for run_id in stale_run_ids:
            try:
                result = celery_app.send_task("backtests.run", kwargs={"run_id": str(run_id)})
                session.execute(
                    update(BacktestRun).where(BacktestRun.id == run_id).values(celery_task_id=result.id)
                )
                session.commit()
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="BacktestRun").inc()
            except Exception:
                session.rollback()
                logger.exception("reaper.redispatch_failed", model="BacktestRun", id=str(run_id))
        counts["backtest_runs"] = len(stale_run_ids)

        stale_running_stmt = (
            select(BacktestRun.id)
            .where(
                BacktestRun.status == "running",
                BacktestRun.started_at.isnot(None),
                BacktestRun.started_at < cutoff,
            )
            .limit(50)
        )
        stale_running_ids = list(session.scalars(stale_running_stmt))
        if stale_running_ids:
            session.execute(
                update(BacktestRun)
                .where(BacktestRun.id.in_(stale_running_ids), BacktestRun.status == "running")
                .values(
                    status="failed",
                    error_code="stale_running",
                    error_message="Job was stuck in running state and was automatically failed.",
                    completed_at=datetime.now(UTC),
                )
            )
            session.commit()
        counts["stale_running_backtests"] = len(stale_running_ids)

        stale_exports_stmt = (
            select(ExportJob.id)
            .where(
                ExportJob.status == "queued",
                ExportJob.celery_task_id.is_(None),
                ExportJob.created_at < cutoff,
            )
            .limit(50)
        )
        stale_export_ids = list(session.scalars(stale_exports_stmt))
        for eid in stale_export_ids:
            try:
                result = celery_app.send_task("exports.generate", kwargs={"export_job_id": str(eid)})
                session.execute(
                    update(ExportJob).where(ExportJob.id == eid).values(celery_task_id=result.id)
                )
                session.commit()
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="ExportJob").inc()
            except Exception:
                session.rollback()
                logger.exception("reaper.redispatch_failed", model="ExportJob", id=str(eid))
        counts["export_jobs"] = len(stale_export_ids)

        stale_running_exports_stmt = (
            select(ExportJob.id)
            .where(
                ExportJob.status == "running",
                ExportJob.started_at.isnot(None),
                ExportJob.started_at < cutoff,
            )
            .limit(50)
        )
        stale_running_export_ids = list(session.scalars(stale_running_exports_stmt))
        if stale_running_export_ids:
            session.execute(
                update(ExportJob)
                .where(ExportJob.id.in_(stale_running_export_ids), ExportJob.status == "running")
                .values(
                    status="failed",
                    error_code="stale_running",
                    error_message="Job was stuck in running state and was automatically failed.",
                    completed_at=datetime.now(UTC),
                )
            )
            session.commit()
        counts["stale_running_exports"] = len(stale_running_export_ids)

        stale_scans_stmt = (
            select(ScannerJob.id)
            .where(
                ScannerJob.status == "queued",
                ScannerJob.celery_task_id.is_(None),
                ScannerJob.created_at < cutoff,
            )
            .limit(50)
        )
        stale_scan_ids = list(session.scalars(stale_scans_stmt))
        for sid in stale_scan_ids:
            try:
                result = celery_app.send_task("scans.run_job", kwargs={"job_id": str(sid)})
                session.execute(
                    update(ScannerJob).where(ScannerJob.id == sid).values(celery_task_id=result.id)
                )
                session.commit()
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="ScannerJob").inc()
            except Exception:
                session.rollback()
                logger.exception("reaper.redispatch_failed", model="ScannerJob", id=str(sid))
        counts["scanner_jobs"] = len(stale_scan_ids)

        stale_running_scans_stmt = (
            select(ScannerJob.id)
            .where(
                ScannerJob.status == "running",
                ScannerJob.started_at.isnot(None),
                ScannerJob.started_at < cutoff,
            )
            .limit(50)
        )
        stale_running_scan_ids = list(session.scalars(stale_running_scans_stmt))
        if stale_running_scan_ids:
            session.execute(
                update(ScannerJob)
                .where(ScannerJob.id.in_(stale_running_scan_ids), ScannerJob.status == "running")
                .values(
                    status="failed",
                    error_code="stale_running",
                    error_message="Job was stuck in running state and was automatically failed.",
                    completed_at=datetime.now(UTC),
                )
            )
            session.commit()
        counts["stale_running_scans"] = len(stale_running_scan_ids)

        stale_analyses_stmt = (
            select(SymbolAnalysis.id)
            .where(
                SymbolAnalysis.status == "queued",
                SymbolAnalysis.celery_task_id.is_(None),
                SymbolAnalysis.created_at < cutoff,
            )
            .limit(50)
        )
        stale_analysis_ids = list(session.scalars(stale_analyses_stmt))
        for aid in stale_analysis_ids:
            try:
                result = celery_app.send_task("analysis.deep_symbol", kwargs={"analysis_id": str(aid)})
                session.execute(
                    update(SymbolAnalysis).where(SymbolAnalysis.id == aid).values(celery_task_id=result.id)
                )
                session.commit()
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="SymbolAnalysis").inc()
            except Exception:
                session.rollback()
                logger.exception("reaper.redispatch_failed", model="SymbolAnalysis", id=str(aid))
        counts["symbol_analyses"] = len(stale_analysis_ids)

        stale_running_analyses_stmt = (
            select(SymbolAnalysis.id)
            .where(
                SymbolAnalysis.status == "running",
                SymbolAnalysis.created_at < cutoff,
            )
            .limit(50)
        )
        stale_running_analysis_ids = list(session.scalars(stale_running_analyses_stmt))
        if stale_running_analysis_ids:
            session.execute(
                update(SymbolAnalysis)
                .where(SymbolAnalysis.id.in_(stale_running_analysis_ids), SymbolAnalysis.status == "running")
                .values(
                    status="failed",
                    error_code="stale_running",
                    error_message="Job was stuck in running state and was automatically failed (stale_running).",
                    completed_at=datetime.now(UTC),
                )
            )
            session.commit()
        counts["stale_running_analyses"] = len(stale_running_analysis_ids)

        stale_running_pipeline_stmt = (
            select(NightlyPipelineRun.id)
            .where(
                NightlyPipelineRun.status == "running",
                NightlyPipelineRun.created_at < cutoff,
            )
            .limit(50)
        )
        stale_running_pipeline_ids = list(session.scalars(stale_running_pipeline_stmt))
        if stale_running_pipeline_ids:
            session.execute(
                update(NightlyPipelineRun)
                .where(NightlyPipelineRun.id.in_(stale_running_pipeline_ids), NightlyPipelineRun.status == "running")
                .values(
                    status="failed",
                    error_message="Pipeline was stuck in running state and was automatically failed (stale_running).",
                    completed_at=datetime.now(UTC),
                )
            )
            session.commit()
        counts["stale_running_pipelines"] = len(stale_running_pipeline_ids)

    total = sum(counts.values())
    if total > 0:
        logger.info("reaper.redispatched", counts=counts, total=total)

    return counts
