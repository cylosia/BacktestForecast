from __future__ import annotations

from uuid import UUID

import structlog

from apps.worker.app.celery_app import celery_app
from backtestforecast.db.session import SessionLocal
from backtestforecast.errors import AppError
from backtestforecast.events import publish_job_status
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
    from datetime import date

    from backtestforecast.config import get_settings
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

        trade_date = date.today()

        with SessionLocal() as session:
            service = NightlyPipelineService(
                session,
                market_data_fetcher=market_data,
                backtest_executor=executor,
                forecaster=forecaster,
            )
            try:
                run = service.run_pipeline(
                    trade_date=trade_date,
                    symbols=symbols,
                    max_recommendations=max_recommendations,
                )
            except Exception as exc:
                session.rollback()
                logger.exception("pipeline.task_failed", trade_date=str(trade_date))
                raise self.retry(exc=exc, countdown=300)

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
            publish_job_status("backtest", UUID(run_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "run_id": run_id,
                "error_code": exc.code,
            }
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
                    session.commit()
                publish_job_status(
                    "backtest", UUID(run_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                return {"status": "failed", "run_id": run_id, "error_code": "max_retries_exceeded"}
        finally:
            service.close()

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
            publish_job_status("export", UUID(export_job_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "export_job_id": export_job_id,
                "error_code": exc.code,
            }
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
                    session.commit()
                publish_job_status(
                    "export", UUID(export_job_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                return {"status": "failed", "export_job_id": export_job_id, "error_code": "max_retries_exceeded"}
        finally:
            service.close()

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
                publish_job_status("analysis", UUID(analysis_id), "failed", metadata={"error_code": exc.code})
                return {
                    "status": "failed",
                    "analysis_id": analysis_id,
                    "error_code": exc.code,
                }
            except Exception as exc:
                session.rollback()
                try:
                    raise self.retry(exc=exc, countdown=60)
                except self.MaxRetriesExceededError:
                    from backtestforecast.models import SymbolAnalysis

                    analysis = session.get(SymbolAnalysis, UUID(analysis_id))
                    if analysis is not None:
                        analysis.status = "failed"
                        analysis.error_message = "Analysis failed after exhausting retries."
                        session.commit()
                    publish_job_status(
                        "analysis", UUID(analysis_id), "failed",
                        metadata={"error_code": "max_retries_exceeded"},
                    )
                    return {
                        "status": "failed",
                        "analysis_id": analysis_id,
                        "error_code": "max_retries_exceeded",
                    }

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
            publish_job_status("scan", UUID(job_id), "failed", metadata={"error_code": exc.code})
            return {
                "status": "failed",
                "job_id": job_id,
                "error_code": exc.code,
            }
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
                    session.commit()
                publish_job_status(
                    "scan", UUID(job_id), "failed",
                    metadata={"error_code": "max_retries_exceeded"},
                )
                return {"status": "failed", "job_id": job_id, "error_code": "max_retries_exceeded"}
        finally:
            service.close()

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
                    dispatched += 1
                except Exception:
                    logger.exception("refresh.dispatch_failed", job_id=str(job.id))

            if dispatched:
                session.commit()
        finally:
            service.close()

    return {
        "scheduled_jobs": dispatched,
    }


@celery_app.task(name="maintenance.reap_stale_jobs")
def reap_stale_jobs(stale_minutes: int = 30) -> dict[str, int]:
    """Re-dispatch jobs stuck in 'queued' with no celery_task_id for too long."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select, update

    from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SymbolAnalysis

    cutoff = datetime.now(UTC) - timedelta(minutes=stale_minutes)
    counts: dict[str, int] = {}

    with SessionLocal() as session:
        dirty = False

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
            except Exception:
                logger.exception("reaper.redispatch_failed", model="BacktestRun", id=str(run_id))
        counts["backtest_runs"] = len(stale_run_ids)

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
            except Exception:
                logger.exception("reaper.redispatch_failed", model="ExportJob", id=str(eid))
        counts["export_jobs"] = len(stale_export_ids)

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
            except Exception:
                logger.exception("reaper.redispatch_failed", model="ScannerJob", id=str(sid))
        counts["scanner_jobs"] = len(stale_scan_ids)

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
            except Exception:
                logger.exception("reaper.redispatch_failed", model="SymbolAnalysis", id=str(aid))
        counts["symbol_analyses"] = len(stale_analysis_ids)

        if dirty:
            session.commit()

    total = sum(counts.values())
    if total > 0:
        logger.info("reaper.redispatched", counts=counts, total=total)

    return counts
