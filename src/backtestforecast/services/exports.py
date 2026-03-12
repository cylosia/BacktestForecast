from __future__ import annotations

import csv
import hashlib
import io
from datetime import UTC, datetime
from typing import Self
from uuid import UUID

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import ExportFormat, ensure_export_access
from backtestforecast.errors import AppError, NotFoundError
from backtestforecast.models import ExportJob, User
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.repositories.export_jobs import ExportJobRepository
from backtestforecast.schemas.backtests import BacktestRunDetailResponse
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobResponse
from backtestforecast.services.audit import AuditService
from backtestforecast.services.backtests import BacktestService

logger = structlog.get_logger("services.exports")

_MAX_CSV_TRADES = 10_000
_MAX_CSV_EQUITY_POINTS = 50_000
_MAX_PDF_TRADES = 100
_MAX_EXPORT_BYTES = 10 * 1024 * 1024  # 10 MB


class ExportService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.exports = ExportJobRepository(session)
        self.backtests = BacktestRunRepository(session)
        self.audit = AuditService(session)
        self.backtest_service = BacktestService(session)

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

        run = self.backtests.get_for_user(payload.run_id, user.id)
        if run is None:
            raise NotFoundError("Backtest run not found.")

        export_job = ExportJob(
            user_id=user.id,
            backtest_run_id=run.id,
            export_format=payload.export_format.value,
            status="queued",
            file_name=self._build_file_name(run.symbol, run.strategy_type, payload.export_format),
            mime_type=self._mime_type(payload.export_format),
            idempotency_key=payload.idempotency_key,
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
        export_job = self.exports.get(export_job_id, for_update=True)
        if export_job is None:
            raise NotFoundError("Export job not found.")

        if export_job.status not in ("queued", "running"):
            return export_job

        export_job.status = "running"
        export_job.started_at = datetime.now(UTC)
        self.session.flush()

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
            export_job.content_bytes = content
            export_job.size_bytes = len(content)
            export_job.sha256_hex = hashlib.sha256(content).hexdigest()
            export_job.status = "succeeded"
            export_job.completed_at = datetime.now(UTC)
            self.session.commit()
        except AppError:
            logger.exception("export.execution_failed", export_job_id=str(export_job.id))
            export_job.status = "failed"
            export_job.error_code = "export_generation_failed"
            export_job.error_message = "Export generation failed. Please try again."
            export_job.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        except (ValueError, RuntimeError) as exc:
            logger.exception("export.terminal_failure", export_job_id=str(export_job.id))
            export_job.status = "failed"
            export_job.error_code = "export_generation_failed"
            export_job.error_message = str(exc)
            export_job.completed_at = datetime.now(UTC)
            self.session.commit()
            raise
        except Exception:
            logger.exception("export.execution_failed", export_job_id=str(export_job.id))
            export_job.status = "queued"
            export_job.started_at = None
            self.session.commit()
            raise

        self.session.refresh(export_job)
        return export_job

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
        export_job = self.exports.get_for_user(export_job_id, user.id)
        if export_job is None:
            raise NotFoundError("Export not found.")
        if export_job.status != "succeeded" or not export_job.content_bytes:
            raise NotFoundError("Export content is not available.")
        self.audit.record(
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

        def line(text: str, *, bold: bool = False, step: float = 16.0) -> None:
            nonlocal y
            pdf.setFont("Helvetica-Bold" if bold else "Helvetica", 10 if not bold else 12)
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
        if value[:1] in {"=", "+", "-", "@"}:
            return "'" + value
        return value

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
            completed_at=job.completed_at,
        )
