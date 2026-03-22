from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import Select, exists, func, select, update
from sqlalchemy.orm import Session

from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.models import BacktestRun, ExportJob, OutboxMessage, ScannerJob, SweepJob, SymbolAnalysis
from backtestforecast.observability.metrics import (
    IDEMPOTENT_DUPLICATE_RETURNS_TOTAL,
    JOB_CREATE_TO_RUNNING_LATENCY_SECONDS,
    ORPHAN_DETECTIONS_TOTAL,
    STALE_QUEUED_DUPLICATE_RETURNS_TOTAL,
    QUEUED_JOBS_PAST_DISPATCH_SLA,
    QUEUED_JOBS_WITHOUT_OUTBOX,
)
from backtestforecast.services.audit import AuditService

UTC = timezone.utc
_STALE_QUEUED_REUSE_AFTER = timedelta(minutes=15)
DISPATCH_SLA = timedelta(minutes=5)


@dataclass(frozen=True)
class DispatchTarget:
    model: type
    model_name: str
    task_name: str
    task_kwarg_key: str
    queue: str
    log_event: str


DISPATCH_TARGETS: tuple[DispatchTarget, ...] = (
    DispatchTarget(BacktestRun, "BacktestRun", "backtests.run", "run_id", "research", "backtest"),
    DispatchTarget(ScannerJob, "ScannerJob", "scans.run_job", "job_id", "research", "scan"),
    DispatchTarget(SweepJob, "SweepJob", "sweeps.run", "job_id", "research", "sweep"),
    DispatchTarget(ExportJob, "ExportJob", "exports.generate", "export_job_id", "exports", "export"),
    DispatchTarget(SymbolAnalysis, "SymbolAnalysis", "analysis.deep_symbol", "analysis_id", "research", "analysis"),
)


def get_dispatch_target(model_name: str) -> DispatchTarget:
    for target in DISPATCH_TARGETS:
        if target.model_name == model_name:
            return target
    raise KeyError(f"Unknown dispatch target: {model_name}")


def get_dispatch_diagnostic(job: Any, *, now: datetime | None = None) -> tuple[str, str] | None:
    """Return an API-visible diagnostic code/message for obviously stuck queued jobs."""
    if getattr(job, "status", None) != "queued":
        return None
    created_at = getattr(job, "created_at", None)
    if created_at is None:
        return None
    if getattr(created_at, "tzinfo", None) is None:
        created_at = created_at.replace(tzinfo=UTC)
    now = now or datetime.now(UTC)
    if created_at > now - DISPATCH_SLA:
        return None
    if getattr(job, "celery_task_id", None) is None:
        return (
            "dispatch_stuck",
            "This job has remained queued past the dispatch SLA and is awaiting automatic recovery.",
        )
    return (
        "dispatch_delayed",
        "This job has remained queued longer than expected and may still be waiting on worker capacity.",
    )


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def observe_job_create_to_running_latency(job: Any, *, model_name: str | None = None, now: datetime | None = None) -> None:
    created_at = _normalize_ts(getattr(job, "created_at", None))
    started_at = _normalize_ts(getattr(job, "started_at", None)) or now
    if created_at is None or started_at is None or started_at < created_at:
        return
    JOB_CREATE_TO_RUNNING_LATENCY_SECONDS.labels(model=model_name or type(job).__name__).observe(
        (started_at - created_at).total_seconds()
    )


def get_queue_diagnostics(
    session: Session,
    *,
    now: datetime | None = None,
    older_than: timedelta = DISPATCH_SLA,
    targets: Iterable[DispatchTarget] = DISPATCH_TARGETS,
) -> dict[str, object]:
    now = now or datetime.now(UTC)
    cutoff = now - older_than
    models: dict[str, dict[str, object]] = {}
    total_stale_queued = 0
    total_without_outbox = 0

    for target in targets:
        model = target.model
        correlated_outbox = (
            select(OutboxMessage.id)
            .where(OutboxMessage.correlation_id == model.id)
            .correlate(model)
        )
        stale_queued = session.scalar(
            select(func.count(model.id)).where(model.status == "queued", model.created_at < cutoff)
        ) or 0
        stale_without_outbox = session.scalar(
            select(func.count(model.id)).where(
                model.status == "queued",
                model.created_at < cutoff,
                model.celery_task_id.is_(None),
                ~exists(correlated_outbox),
            )
        ) or 0
        oldest_created = session.scalar(select(func.min(model.created_at)).where(model.status == "queued"))
        oldest_stale = _normalize_ts(oldest_created)

        model_payload: dict[str, object] = {
            "stale_queued": int(stale_queued),
            "stale_without_outbox": int(stale_without_outbox),
        }
        if oldest_stale is not None:
            model_payload["oldest_queued_age_seconds"] = round((now - oldest_stale).total_seconds(), 1)
        models[target.model_name] = model_payload
        total_stale_queued += int(stale_queued)
        total_without_outbox += int(stale_without_outbox)

    status = "ok"
    if total_without_outbox > 0:
        status = "stale_without_outbox"
    elif total_stale_queued > 0:
        status = "delayed"

    return {
        "status": status,
        "dispatch_sla_seconds": int(older_than.total_seconds()),
        "stale_queued_total": total_stale_queued,
        "stale_without_outbox_total": total_without_outbox,
        "models": models,
    }


def update_queue_diagnostic_gauges(session: Session) -> dict[str, object]:
    diagnostics = get_queue_diagnostics(session)
    for model_name, payload in diagnostics["models"].items():
        model_payload = payload if isinstance(payload, dict) else {}
        QUEUED_JOBS_PAST_DISPATCH_SLA.labels(model=model_name).set(int(model_payload.get("stale_queued", 0)))
        QUEUED_JOBS_WITHOUT_OUTBOX.labels(model=model_name).set(int(model_payload.get("stale_without_outbox", 0)))
    return diagnostics


def _stranded_job_stmt(target: DispatchTarget, *, cutoff: datetime) -> Select[tuple[Any]]:
    model = target.model
    correlated_outbox = (
        select(OutboxMessage.id)
        .where(OutboxMessage.correlation_id == model.id)
        .correlate(model)
    )
    return (
        select(model)
        .where(
            model.status == "queued",
            model.celery_task_id.is_(None),
            model.created_at < cutoff,
            ~exists(correlated_outbox),
        )
        .order_by(model.created_at)
    )


def find_stranded_jobs(
    session: Session,
    *,
    cutoff: datetime,
    targets: Iterable[DispatchTarget] = DISPATCH_TARGETS,
    limit_per_model: int = 50,
) -> list[tuple[DispatchTarget, Any]]:
    stranded: list[tuple[DispatchTarget, Any]] = []
    for target in targets:
        stmt = _stranded_job_stmt(target, cutoff=cutoff).limit(limit_per_model).with_for_update(skip_locked=True)
        stranded.extend((target, job) for job in session.scalars(stmt))
    return stranded


def repair_stranded_jobs(
    session: Session,
    *,
    logger: Any,
    action: Literal["requeue", "fail", "list"] = "requeue",
    older_than: timedelta = DISPATCH_SLA,
    request_id: str | None = None,
    traceparent: str | None = None,
) -> dict[str, int]:
    """Find queued jobs with no task claim or outbox row and repair them safely."""
    cutoff = datetime.now(UTC) - older_than
    counts = {"found": 0, "requeued": 0, "failed": 0}
    audit = AuditService(session)
    for target, job in find_stranded_jobs(session, cutoff=cutoff):
        counts["found"] += 1
        ORPHAN_DETECTIONS_TOTAL.labels(kind="queued_job", source="stranded_reconcile", model=target.model_name).inc()
        logger.warning(
            "dispatch.stranded_job_detected",
            model=target.model_name,
            job_id=str(getattr(job, "id", None)),
            action=action,
            created_at=getattr(job, "created_at", None),
        )
        if action == "list":
            continue
        if action == "fail":
            now = datetime.now(UTC)
            job.status = "failed"
            job.error_code = "dispatch_stuck"
            job.error_message = "Job was stranded in the queue before dispatch and was safely failed."
            job.completed_at = now
            job.updated_at = now
            audit.record_always(
                event_type="dispatch.repair_failed",
                subject_type=target.model_name,
                subject_id=getattr(job, "id", None),
                user_id=getattr(job, "user_id", None),
                request_id=request_id,
                metadata={"action": action, "traceparent": traceparent},
            )
            session.commit()
            counts["failed"] += 1
            continue

        dispatch_celery_task(
            db=session,
            job=job,
            task_name=target.task_name,
            task_kwargs={target.task_kwarg_key: str(getattr(job, "id"))},
            queue=target.queue,
            log_event=target.log_event,
            logger=logger,
            request_id=request_id,
            traceparent=traceparent,
        )
        audit.record_always(
            event_type="dispatch.repaired",
            subject_type=target.model_name,
            subject_id=getattr(job, "id", None),
            user_id=getattr(job, "user_id", None),
            request_id=request_id,
            metadata={"action": action, "traceparent": traceparent},
        )
        session.commit()
        session.refresh(job)
        counts["requeued"] += 1
    return counts


def redispatch_if_stale_queued(
    session: Session,
    job: Any,
    *,
    model_name: str,
    task_name: str,
    task_kwargs: dict[str, str],
    queue: str,
    log_event: str,
    logger: Any,
    request_id: str | None = None,
    traceparent: str | None = None,
) -> Any:
    """Re-dispatch a stale queued job reused through idempotency/dup detection.

    If the job is still queued after the stale threshold, clear any stale task
    claim, fail superseded pending outbox rows, and issue a fresh dispatch so a
    user retry can recover a stranded job without creating a duplicate record.
    """
    created_at = getattr(job, "created_at", None)
    if getattr(job, "status", None) != "queued" or created_at is None:
        IDEMPOTENT_DUPLICATE_RETURNS_TOTAL.labels(model=model_name, status=str(getattr(job, "status", "unknown"))).inc()
        return job
    if getattr(created_at, "tzinfo", None) is None:
        created_at = created_at.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    if created_at >= now - _STALE_QUEUED_REUSE_AFTER:
        IDEMPOTENT_DUPLICATE_RETURNS_TOTAL.labels(model=model_name, status="queued").inc()
        return job

    IDEMPOTENT_DUPLICATE_RETURNS_TOTAL.labels(model=model_name, status="queued").inc()
    STALE_QUEUED_DUPLICATE_RETURNS_TOTAL.labels(model=model_name).inc()
    ORPHAN_DETECTIONS_TOTAL.labels(kind="queued_job", source="idempotency_reuse", model=model_name).inc()
    logger.warning(
        "dispatch.stale_queued_job_reused",
        model=model_name,
        job_id=str(getattr(job, "id", None)),
        created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
        stale_after_seconds=int(_STALE_QUEUED_REUSE_AFTER.total_seconds()),
    )
    AuditService(session).record_always(
        event_type="dispatch.idempotency_requeued",
        subject_type=model_name,
        subject_id=getattr(job, "id", None),
        user_id=getattr(job, "user_id", None),
        request_id=request_id,
        metadata={"traceparent": traceparent, "queue": queue, "task_name": task_name},
    )

    model_cls = type(job)
    session.execute(
        update(OutboxMessage)
        .where(
            OutboxMessage.correlation_id == job.id,
            OutboxMessage.status == "pending",
        )
        .values(
            status="failed",
            error_message="Superseded by stale idempotency retry redispatch.",
            completed_at=now,
            updated_at=now,
        )
    )
    session.execute(
        update(model_cls)
        .where(model_cls.id == job.id, model_cls.status == "queued")
        .values(
            celery_task_id=None,
            error_code=None,
            error_message=None,
            updated_at=now,
        )
    )
    session.flush()
    session.refresh(job)

    if getattr(job, "status", None) != "queued":
        return job

    dispatch_celery_task(
        db=session,
        job=job,
        task_name=task_name,
        task_kwargs=task_kwargs,
        queue=queue,
        log_event=log_event,
        logger=logger,
        request_id=request_id,
        traceparent=traceparent,
    )
    session.refresh(job)
    return job
