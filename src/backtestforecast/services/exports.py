from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID

import structlog
from sqlalchemy import update as sa_update_top
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import ExportFormat, ensure_export_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import AppError, NotFoundError, ValidationError
from backtestforecast.exports.storage import DatabaseStorage, ExportStorage, get_storage
from backtestforecast.models import ExportJob, User
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.repositories.export_jobs import ExportJobRepository
from backtestforecast.schemas.backtests import BacktestRunDetailResponse
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobResponse
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtests import BacktestService

logger = structlog.get_logger("services.exports")

_LOOKS_NUMERIC = re.compile(r"^-?(\d[\d,]*\.?\d*|\.\d+)([eE][+-]?\d+)?$")

_MAX_CSV_TRADES = 10_000
_MAX_CSV_EQUITY_POINTS = 50_000
_MAX_PDF_TRADES = 100
_MAX_EXPORT_BYTES = 10 * 1024 * 1024  # 10 MB


class ExportService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
        storage: ExportStorage | None = None,
    ) -> None:
        self.session = session
        self.exports = ExportJobRepository(session)
        self.backtests = BacktestRunRepository(session)
        self.audit = AuditService(session)
        self.backtest_service = BacktestService(session)
        self._storage = storage or get_storage(settings or get_settings())

    def close(self) -> None:
        self.backtest_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def enqueue_export(
        self,
        user: User,
        payload: CreateExportRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJobResponse:
        """Create a queued export job. Caller dispatches to Celery."""
        ensure_export_access(
            user.plan_tier, user.subscription_status, payload.export_format,
            user.subscription_current_period_end,
        )
        if payload.idempotency_key:
            existing = self.exports.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing is not None:
                return self._to_response(existing)

        run = self.backtests.get_lightweight_for_user(payload.run_id, user.id)
        if run is None:
            raise NotFoundError("Backtest run not found.")
        if run.status != "succeeded":
            raise ValidationError(
                f"Cannot export a backtest run with status \"{run.status}\". "
                "Only succeeded runs can be exported."
            )

        export_job = ExportJob(
            user_id=user.id,
            backtest_run_id=run.id,
            export_format=payload.export_format.value,
            status="queued",
            file_name=self._build_file_name(run.symbol, run.strategy_type, payload.export_format),
            mime_type=self._mime_type(payload.export_format),
            idempotency_key=payload.idempotency_key,
            expires_at=datetime.now(UTC) + timedelta(days=30),
        )
        self.exports.add(export_job)
        self.audit.record(
            event_type="export.enqueued",
            subject_type="export_job",
            subject_id=export_job.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "run_id": str(run.id),
                "format": payload.export_format.value,
            },
        )
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            if payload.idempotency_key:
                existing = self.exports.get_by_idempotency_key(user.id, payload.idempotency_key)
                if existing is not None:
                    return self._to_response(existing)
            raise
        self.session.refresh(export_job)
        return self._to_response(export_job)

    def execute_export_by_id(self, export_job_id: UUID) -> ExportJob:
        """Generate the export content. Called by the Celery worker."""
        from sqlalchemy import update as sa_update

        export_job = self.exports.get(export_job_id, for_update=True)
        if export_job is None:
            raise NotFoundError("Export job not found.")

        if export_job.status != "queued":
            logger.info("export.execute_skipped", export_job_id=str(export_job_id), status=export_job.status)
            return export_job

        rows = self.session.execute(
            sa_update(ExportJob)
            .where(ExportJob.id == export_job_id, ExportJob.status == "queued")
            .values(status="running", started_at=datetime.now(UTC))
        )
        self.session.commit()
        if rows.rowcount == 0:
            self.session.refresh(export_job)
            return export_job
        self.session.refresh(export_job)

        try:
            detail = self.backtest_service.get_run_for_owner(
                user_id=export_job.user_id, run_id=export_job.backtest_run_id
            )
            fmt = ExportFormat(export_job.export_format)
            if fmt == ExportFormat.CSV:
                content = self._build_csv(detail)
            else:
                content = self._build_pdf(detail)
            if len(content) > _MAX_EXPORT_BYTES:
                raise ValueError(
                    f"Generated export exceeds the {_MAX_EXPORT_BYTES // (1024 * 1024)} MB size limit."
                )
            # ORPHAN RISK: The storage write below happens outside the DB
            # transaction.  If the subsequent commit fails, the uploaded object
            # will remain in storage with no matching DB record pointing to it.
            # A periodic cleanup job should reconcile storage keys against the
            # export_jobs table and remove orphans.
            storage_key = self._storage.put(export_job.id, content, export_job.file_name)
            if isinstance(self._storage, DatabaseStorage):
                export_job.content_bytes = content
            export_job.storage_key = storage_key
            export_job.size_bytes = len(content)
            export_job.sha256_hex = hashlib.sha256(content).hexdigest()
            export_job.status = "succeeded"
            export_job.completed_at = datetime.now(UTC)
            try:
                self.session.commit()
            except Exception:
                logger.warning(
                    "export.commit_failed_after_storage_write",
                    export_job_id=str(export_job.id),
                    storage_key=storage_key,
                    exc_info=True,
                )
                if not isinstance(self._storage, DatabaseStorage):
                    try:
                        self._storage.delete(storage_key)
                        logger.info("export.orphan_cleaned", storage_key=storage_key)
                    except Exception:
                        logger.warning(
                            "export.orphan_cleanup_failed",
                            storage_key=storage_key,
                            exc_info=True,
                        )
                raise
        except AppError:
            self.session.rollback()
            logger.exception("export.execution_failed", export_job_id=str(export_job.id))
            self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id == export_job.id, ExportJob.status != "succeeded")
                .values(
                    status="failed",
                    error_code="export_generation_failed",
                    error_message="Export generation failed. Please try again.",
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except (ValueError, RuntimeError) as exc:
            self.session.rollback()
            logger.exception("export.terminal_failure", export_job_id=str(export_job.id), error=str(exc))
            self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id == export_job.id, ExportJob.status != "succeeded")
                .values(
                    status="failed",
                    error_code="export_generation_failed",
                    error_message="Export generation failed due to a data or configuration error.",
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise
        except Exception:
            self.session.rollback()
            logger.exception("export.execution_failed", export_job_id=str(export_job.id))
            self.session.execute(
                sa_update_top(ExportJob)
                .where(ExportJob.id == export_job.id, ExportJob.status != "succeeded")
                .values(
                    status="failed",
                    error_code="export_generation_failed",
                    error_message="Export generation failed due to an unexpected error.",
                    completed_at=datetime.now(UTC),
                )
            )
            self.session.commit()
            raise

        self.session.refresh(export_job)
        return export_job

    _CLEANUP_COMMIT_INTERVAL = 10

    def cleanup_expired_exports(self, *, batch_size: int = 100, max_batches: int = 100) -> int:
        """Delete storage content for expired exports. Returns count cleaned."""
        cleaned = 0
        now = datetime.now(UTC)
        batch_count = 0
        pending_commit = 0

        while batch_count < max_batches:
            batch_count += 1
            jobs = self.exports.list_expired_for_cleanup(now, batch_size)
            if not jobs:
                break

            for job in jobs:
                old_storage_key = job.storage_key

                storage_deleted = True
                if old_storage_key:
                    try:
                        self._storage.delete(old_storage_key)
                    except (OSError, ConnectionError, TimeoutError, RuntimeError, ValueError) as exc:
                        logger.warning(
                            "cleanup.storage_delete_failed",
                            export_job_id=str(job.id),
                            storage_key=old_storage_key,
                            error=str(exc),
                        )
                        storage_deleted = False

                job.content_bytes = None
                if storage_deleted:
                    job.storage_key = None
                job.status = "expired"
                job.size_bytes = 0
                job.sha256_hex = None
                cleaned += 1
                pending_commit += 1
                logger.info(
                    "cleanup.expired",
                    export_job_id=str(job.id),
                    storage_deleted=storage_deleted,
                    expires_at=str(job.expires_at) if job.expires_at else None,
                )

                if pending_commit >= self._CLEANUP_COMMIT_INTERVAL:
                    try:
                        self.session.commit()
                    except Exception:
                        self.session.rollback()
                        cleaned -= pending_commit
                        logger.warning("cleanup.batch_commit_failed", pending=pending_commit, exc_info=True)
                    pending_commit = 0

            if len(jobs) < batch_size:
                break

        if pending_commit > 0:
            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                cleaned -= pending_commit
                logger.warning("cleanup.final_commit_failed", pending=pending_commit, exc_info=True)

        return cleaned

    def get_export_status(self, user: User, export_job_id: UUID) -> ExportJobResponse:
        """Return current status of an export job (for polling)."""
        export_job = self.exports.get_for_user(export_job_id, user.id)
        if export_job is None:
            raise NotFoundError("Export not found.")
        return self._to_response(export_job)

    def create_export(
        self,
        user: User,
        payload: CreateExportRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJobResponse:
        """Synchronous create-and-execute. Preserved for tests/fallback."""
        enqueued = self.enqueue_export(user, payload, request_id=request_id, ip_address=ip_address)
        export_job = self.execute_export_by_id(enqueued.id)
        if export_job.status == "succeeded":
            self.audit.record(
                event_type="export.created",
                subject_type="export_job",
                subject_id=export_job.id,
                user_id=user.id,
                request_id=request_id,
                ip_address=ip_address,
                metadata={
                    "run_id": str(export_job.backtest_run_id),
                    "format": export_job.export_format,
                    "size_bytes": export_job.size_bytes,
                },
            )
            self.session.commit()
        return self._to_response(export_job)

    def get_export_for_download(
        self,
        user: User,
        export_job_id: UUID,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> ExportJob:
        use_db_content = isinstance(self._storage, DatabaseStorage)
        export_job = self.exports.get_for_user(
            export_job_id, user.id, include_content=use_db_content,
        )
        if export_job is None:
            raise NotFoundError("Export not found.")
        if export_job.status != "succeeded":
            raise NotFoundError("Export content is not available.")
        if use_db_content and not export_job.content_bytes:
            raise NotFoundError("Export content is not available.")
        if not use_db_content and not export_job.storage_key:
            raise NotFoundError("Export content is not available.")
        self.audit.record_always(
            event_type="export.downloaded",
            subject_type="export_job",
            subject_id=export_job.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "run_id": str(export_job.backtest_run_id),
                "format": export_job.export_format,
            },
        )
        self.session.commit()
        return export_job

    @staticmethod
    def _build_file_name(symbol: str, strategy_type: str, export_format: ExportFormat) -> str:
        import re

        safe_symbol = re.sub(r'[<>:"/\\|?*\s]', "-", symbol).strip("-").lower()
        safe_strategy = re.sub(r'[<>:"/\\|?*\s]', "-", strategy_type).strip("-").lower()
        extension = "csv" if export_format == ExportFormat.CSV else "pdf"
        return f"{safe_symbol}-{safe_strategy}-backtest.{extension}"

    @staticmethod
    def _mime_type(export_format: ExportFormat) -> str:
        if export_format == ExportFormat.CSV:
            return "text/csv; charset=utf-8"
        return "application/pdf"

    def _build_csv(self, detail: BacktestRunDetailResponse) -> bytes:
        output = io.StringIO()
        writer = csv.writer(output)

        def safe_row(values: list[object]) -> list[object]:
            return [self._sanitize_csv_cell(value) for value in values]

        writer.writerow(safe_row(["section", "field", "value"]))
        writer.writerow(safe_row(["run", "symbol", detail.symbol]))
        writer.writerow(safe_row(["run", "strategy_type", detail.strategy_type]))
        writer.writerow(safe_row(["run", "status", detail.status]))
        writer.writerow(safe_row(["run", "date_from", detail.date_from.isoformat()]))
        writer.writerow(safe_row(["run", "date_to", detail.date_to.isoformat()]))
        writer.writerow(safe_row(["summary", "trade_count", detail.summary.trade_count]))
        writer.writerow(safe_row(["summary", "win_rate", detail.summary.win_rate]))
        writer.writerow(safe_row(["summary", "total_roi_pct", detail.summary.total_roi_pct]))
        writer.writerow(safe_row(["summary", "total_net_pnl", detail.summary.total_net_pnl]))
        writer.writerow(safe_row(["summary", "max_drawdown_pct", detail.summary.max_drawdown_pct]))
        writer.writerow(safe_row(["summary", "profit_factor", detail.summary.profit_factor]))
        writer.writerow(safe_row(["summary", "sharpe_ratio", detail.summary.sharpe_ratio]))
        writer.writerow(safe_row(["summary", "sortino_ratio", detail.summary.sortino_ratio]))
        writer.writerow(safe_row(["summary", "expectancy", detail.summary.expectancy]))
        writer.writerow(safe_row(["summary", "cagr_pct", detail.summary.cagr_pct]))
        writer.writerow([])
        writer.writerow(
            safe_row(
                [
                    "trades",
                    "option_ticker",
                    "entry_date",
                    "exit_date",
                    "quantity",
                    "entry_mid",
                    "exit_mid",
                    "gross_pnl",
                    "net_pnl",
                    "holding_period_days",
                    "entry_reason",
                    "exit_reason",
                ]
            )
        )
        exported_trades = detail.trades[:_MAX_CSV_TRADES]
        for trade in exported_trades:
            writer.writerow(
                safe_row(
                    [
                        "trade",
                        trade.option_ticker,
                        trade.entry_date.isoformat(),
                        trade.exit_date.isoformat(),
                        trade.quantity,
                        trade.entry_mid,
                        trade.exit_mid,
                        trade.gross_pnl,
                        trade.net_pnl,
                        trade.holding_period_days,
                        trade.entry_reason,
                        trade.exit_reason,
                    ]
                )
            )
        if len(detail.trades) > _MAX_CSV_TRADES:
            writer.writerow(
                safe_row(["trade", f"... {len(detail.trades) - _MAX_CSV_TRADES} additional trades omitted ..."])
            )

        writer.writerow([])
        writer.writerow(safe_row(["equity_curve", "trade_date", "equity", "cash", "position_value", "drawdown_pct"]))
        exported_points = detail.equity_curve[:_MAX_CSV_EQUITY_POINTS]
        for point in exported_points:
            writer.writerow(
                safe_row(
                    [
                        "equity_point",
                        point.trade_date.isoformat(),
                        point.equity,
                        point.cash,
                        point.position_value,
                        point.drawdown_pct,
                    ]
                )
            )
        if len(detail.equity_curve) > _MAX_CSV_EQUITY_POINTS:
            writer.writerow(
                safe_row(
                    ["equity_point", f"... {len(detail.equity_curve) - _MAX_CSV_EQUITY_POINTS} additional points omitted ..."]
                )
            )

        return output.getvalue().encode("utf-8")

    def _build_pdf(self, detail: BacktestRunDetailResponse) -> bytes:
        try:
            from reportlab.lib.pagesizes import letter  # type: ignore
            from reportlab.lib.units import inch  # type: ignore
            from reportlab.pdfgen import canvas  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("The reportlab dependency is required for PDF export.") from exc

        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        y = height - 0.75 * inch

        _BASE_FONT = "Helvetica"
        _BOLD_FONT = "Helvetica-Bold"
        try:
            pdf.setFont(_BASE_FONT, 10)
        except KeyError:
            from reportlab.pdfbase import pdfmetrics  # type: ignore
            available = pdfmetrics.getRegisteredFontNames()
            _BASE_FONT = available[0] if available else "Courier"
            _BOLD_FONT = _BASE_FONT

        def line(text: str, *, bold: bool = False, step: float = 16.0) -> None:
            nonlocal y
            pdf.setFont(_BOLD_FONT if bold else _BASE_FONT, 10 if not bold else 12)
            pdf.drawString(0.75 * inch, y, text)
            y -= step
            if y < 0.75 * inch:
                pdf.showPage()
                y = height - 0.75 * inch

        line("BacktestForecast.com Export", bold=True, step=22.0)
        line(f"Symbol: {detail.symbol}")
        line(f"Strategy: {detail.strategy_type}")
        line(f"Status: {detail.status}")
        line(f"Date range: {detail.date_from.isoformat()} to {detail.date_to.isoformat()}")
        line(f"Created: {detail.created_at.isoformat()}")
        line("")
        line("Summary", bold=True, step=20.0)
        line(f"Trades: {detail.summary.trade_count}")
        line(f"Win rate: {detail.summary.win_rate}%")
        line(f"ROI: {detail.summary.total_roi_pct}%")
        line(f"Net P&L: {detail.summary.total_net_pnl}")
        line(f"Max drawdown: {detail.summary.max_drawdown_pct}%")
        s = detail.summary
        if s.profit_factor is not None:
            line(f"Profit factor: {s.profit_factor}")
        if s.sharpe_ratio is not None:
            line(f"Sharpe ratio: {s.sharpe_ratio}")
        if s.sortino_ratio is not None:
            line(f"Sortino ratio: {s.sortino_ratio}")
        line(f"Expectancy: {s.expectancy}")
        if s.cagr_pct is not None:
            line(f"CAGR: {s.cagr_pct}%")
        line("")
        line("Trades", bold=True, step=20.0)
        for trade in detail.trades[:_MAX_PDF_TRADES]:
            line(
                f"{trade.entry_date.isoformat()} -> {trade.exit_date.isoformat()} | "
                f"{trade.option_ticker} | qty {trade.quantity} | net {trade.net_pnl}",
                step=14.0,
            )
        if len(detail.trades) > _MAX_PDF_TRADES:
            line(f"... {len(detail.trades) - _MAX_PDF_TRADES} additional trades omitted from PDF view ...")

        if detail.equity_curve:
            line("")
            line("Equity Curve Summary", bold=True, step=20.0)
            equities = [p.equity for p in detail.equity_curve]
            peak = max(equities)
            trough = min(equities)
            line(f"Starting equity: {detail.equity_curve[0].equity}")
            line(f"Ending equity: {detail.equity_curve[-1].equity}")
            line(f"Peak equity: {peak}")
            line(f"Trough equity: {trough}")
            line(f"Max drawdown: {detail.summary.max_drawdown_pct}%")
            line(f"Data points: {len(detail.equity_curve)}")

        pdf.showPage()
        pdf.save()
        return buffer.getvalue()

    @staticmethod
    def _sanitize_csv_cell(value: object) -> object:
        if not isinstance(value, str):
            return value
        original_first = value[:1]
        if original_first in {"\t", "\r"}:
            return "'" + value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        sanitized = value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        stripped = sanitized.strip()
        first = stripped[:1]
        if first in {"=", "+", "@", "|"}:
            return "'" + sanitized
        if first == "-" and not _LOOKS_NUMERIC.match(stripped):
            return "'" + sanitized
        return sanitized

    @staticmethod
    def _to_response(job: ExportJob) -> ExportJobResponse:
        return ExportJobResponse(
            id=job.id,
            run_id=job.backtest_run_id,
            export_format=job.export_format,
            status=job.status,
            file_name=job.file_name,
            mime_type=job.mime_type,
            size_bytes=job.size_bytes,
            error_code=job.error_code,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            expires_at=job.expires_at,
        )
