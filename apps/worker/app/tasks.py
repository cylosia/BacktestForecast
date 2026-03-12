from __future__ import annotations

from uuid import UUID

import structlog
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from apps.worker.app.celery_app import celery_app
from backtestforecast.db.session import SessionLocal
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


@celery_app.task(name="pipeline.nightly_scan", bind=True, max_retries=1)
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
            except Exception as exc:
                session.rollback()
                logger.exception("pipeline.task_failed", trade_date=str(trade_date))
                try:
                    raise self.retry(exc=exc, countdown=300)
                except MaxRetriesExceededError:
                    run_obj = session.get(NightlyPipelineRun, run.id) if run else None
                    if run_obj is not None and run_obj.status == "running":
                        run_obj.status = "failed"
                        run_obj.error_message = "Pipeline failed after maximum retries (max_retries_exceeded)."
                        run_obj.completed_at = datetime.now(UTC)
                        try:
                            session.commit()
                        except Exception:
                            session.rollback()
                    raise

            return {
                "status": run.status,
                "run_id": str(run.id),
                "recommendations": run.recommendations_produced,
                "duration_seconds": (float(run.duration_seconds) if run.duration_seconds else 0),
            }
    finally:
        executor.close()
        client.close()


@celery_app.task(name="backtests.run", bind=True, max_retries=2)
def run_backtest(self, run_id: str) -> dict[str, str]:
    publish_job_status("backtest", UUID(run_id), "running")
    with SessionLocal() as session:
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
        CELERY_TASKS_TOTAL.labels(task_name="backtests.run", status="succeeded").inc()
        publish_job_status("backtest", UUID(run_id), run.status)
        return {
            "status": run.status,
            "run_id": run_id,
            "trade_count": run.trade_count,
        }


@celery_app.task(name="exports.generate", bind=True, max_retries=2)
def generate_export(self, export_job_id: str) -> dict[str, str | int]:
    publish_job_status("export", UUID(export_job_id), "running")
    with SessionLocal() as session:
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

        CELERY_TASKS_TOTAL.labels(task_name="exports.generate", status="succeeded").inc()
        publish_job_status("export", UUID(export_job_id), job.status)
        return {
            "status": job.status,
            "export_job_id": export_job_id,
            "size_bytes": job.size_bytes,
        }


@celery_app.task(name="analysis.deep_symbol", bind=True, max_retries=1)
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

    publish_job_status("analysis", UUID(analysis_id), "running")
    settings = get_settings()
    client = MassiveClient(api_key=settings.massive_api_key)
    shared_mds = MarketDataService(client)
    shared_exec = _BES(market_data_service=shared_mds)
    executor = PipelineBacktestExecutor(execution_service=shared_exec)
    try:
        market_data = PipelineMarketDataFetcher(client)
        forecaster = PipelineForecaster(HistoricalAnalogForecaster(), market_data)

        with SessionLocal() as session:
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

            CELERY_TASKS_TOTAL.labels(task_name="analysis.deep_symbol", status="succeeded").inc()
            publish_job_status("analysis", UUID(analysis_id), result.status)
            return {
                "status": result.status,
                "analysis_id": analysis_id,
                "top_results": result.top_results_count,
            }
    finally:
        executor.close()
        client.close()


@celery_app.task(name="scans.run_job", bind=True, max_retries=3)
def run_scan_job(self, job_id: str) -> dict[str, str | int]:
    publish_job_status("scan", UUID(job_id), "running")
    with SessionLocal() as session:
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

        CELERY_TASKS_TOTAL.labels(task_name="scans.run_job", status="succeeded").inc()
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

    from sqlalchemy import select, update

    from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SymbolAnalysis
    from backtestforecast.observability.metrics import JOBS_STUCK_REDISPATCHED_TOTAL

    cutoff = datetime.now(UTC) - timedelta(minutes=stale_minutes)
    counts: dict[str, int] = {}

    with SessionLocal() as session:
        dirty = False
        redispatched = 0

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
                dirty = True
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="BacktestRun").inc()
            except Exception:
                logger.exception("reaper.redispatch_failed", model="BacktestRun", id=str(run_id))
        counts["backtest_runs"] = len(stale_run_ids)

        stale_running_stmt = (
            select(BacktestRun.id)
            .where(
                BacktestRun.status == "running",
                BacktestRun.created_at < cutoff,
            )
            .limit(50)
        )
        stale_running_ids = list(session.scalars(stale_running_stmt))
        for run_id_val in stale_running_ids:
            run_obj = session.get(BacktestRun, run_id_val)
            if run_obj is not None:
                run_obj.status = "failed"
                run_obj.error_code = "stale_running"
                run_obj.error_message = "Job was stuck in running state and was automatically failed."
                run_obj.completed_at = datetime.now(UTC)
                redispatched += 1
                dirty = True

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
                dirty = True
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="ExportJob").inc()
            except Exception:
                logger.exception("reaper.redispatch_failed", model="ExportJob", id=str(eid))
        counts["export_jobs"] = len(stale_export_ids)

        stale_running_exports_stmt = (
            select(ExportJob.id)
            .where(
                ExportJob.status == "running",
                ExportJob.created_at < cutoff,
            )
            .limit(50)
        )
        stale_running_export_ids = list(session.scalars(stale_running_exports_stmt))
        for eid_val in stale_running_export_ids:
            export_obj = session.get(ExportJob, eid_val)
            if export_obj is not None:
                export_obj.status = "failed"
                export_obj.error_code = "stale_running"
                export_obj.error_message = "Job was stuck in running state and was automatically failed."
                export_obj.completed_at = datetime.now(UTC)
                redispatched += 1
                dirty = True

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
                dirty = True
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="ScannerJob").inc()
            except Exception:
                logger.exception("reaper.redispatch_failed", model="ScannerJob", id=str(sid))
        counts["scanner_jobs"] = len(stale_scan_ids)

        stale_running_scans_stmt = (
            select(ScannerJob.id)
            .where(
                ScannerJob.status == "running",
                ScannerJob.created_at < cutoff,
            )
            .limit(50)
        )
        stale_running_scan_ids = list(session.scalars(stale_running_scans_stmt))
        for sid_val in stale_running_scan_ids:
            scan_obj = session.get(ScannerJob, sid_val)
            if scan_obj is not None:
                scan_obj.status = "failed"
                scan_obj.error_code = "stale_running"
                scan_obj.error_message = "Job was stuck in running state and was automatically failed."
                scan_obj.completed_at = datetime.now(UTC)
                redispatched += 1
                dirty = True

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
                dirty = True
                JOBS_STUCK_REDISPATCHED_TOTAL.labels(model="SymbolAnalysis").inc()
            except Exception:
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
        for aid_val in stale_running_analysis_ids:
            analysis_obj = session.get(SymbolAnalysis, aid_val)
            if analysis_obj is not None:
                analysis_obj.status = "failed"
                analysis_obj.error_message = "Job was stuck in running state and was automatically failed (stale_running)."
                analysis_obj.completed_at = datetime.now(UTC)
                redispatched += 1
                dirty = True

        if dirty:
            session.commit()

    total = sum(counts.values())
    if total > 0:
        logger.info("reaper.redispatched", counts=counts, total=total)

    return counts
