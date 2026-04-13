from __future__ import annotations

import argparse
import json
import os
import time
from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.config import get_settings
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.integrations.massive_flatfiles import MassiveFlatFilesClient, option_day_dataset
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.historical_store import parse_option_ticker_metadata
from backtestforecast.models import (
    HistoricalEarningsEvent,
    HistoricalOptionDayBar,
    HistoricalUnderlyingDayBar,
)
from backtestforecast.utils.dates import market_date_today


@dataclass(frozen=True, slots=True)
class EarningsBackfillChunk:
    symbol: str
    start_date: date
    end_date: date

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.start_date.isoformat()}|{self.end_date.isoformat()}"


@dataclass(frozen=True, slots=True)
class ChunkSyncResult:
    chunk: EarningsBackfillChunk
    earnings_count: int
    stored_count: int
    error: str | None = None


class ChunkResultPayload(TypedDict):
    symbol: str
    chunk_start: str
    chunk_end: str
    earnings_count: int
    stored_count: int
    error: str | None


class ChunkCheckpointPayload(TypedDict):
    symbol: str
    chunk_start: str
    chunk_end: str
    earnings_count: int
    stored_count: int
    error: str | None


class EarningsBackfillStatusSnapshot(TypedDict, total=False):
    started_at: str
    command: str
    launcher_pid: int
    python_pid: int
    status_path: str
    status: str
    updated_at: str
    universe_source: str
    optionable_as_of_date: str | None
    chunk_mode: str
    status_write_every: int
    window_start: str
    window_end: str
    requested_symbols: list[str]
    resolved_symbols: list[str]
    resolved_symbol_count: int
    total_chunks: int
    completed_chunks: int
    completed_pct: float
    processed_chunks: int
    processed_pct: float
    completed_earnings_rows: int
    completed_stored_rows: int
    completed_chunks_list: list[str]
    processed_chunks_list: list[str]
    remaining_chunks: int
    remaining_chunks_sample: list[str]
    next_pending_chunk: str | None
    last_completed_chunk: str | None
    last_result: ChunkResultPayload | None
    chunk_checkpoints: dict[str, ChunkCheckpointPayload]
    run_signature: dict[str, object]
    resume_requested: bool
    resume_applied: bool
    resumed_chunks: int
    completed_at: str | None
    failed_at: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class ResumeCheckpoint:
    chunk_checkpoints: dict[str, ChunkCheckpointPayload]
    last_result: ChunkResultPayload | None
    applied: bool


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _parse_symbols_csv(raw: str) -> set[str]:
    return {item.strip().upper() for item in raw.split(",") if item.strip()}


def _load_symbols_file(path: str) -> set[str]:
    raw_text = Path(path).read_text(encoding="utf-8")
    symbols: set[str] = set()
    for line in raw_text.splitlines():
        for item in line.split(","):
            normalized = item.strip().upper()
            if normalized:
                symbols.add(normalized)
    return symbols


def _discover_symbols(*, start_date: date, end_date: date) -> list[str]:
    symbols: set[str] = set()
    with create_readonly_session() as session:
        symbols.update(
            session.scalars(
                select(HistoricalUnderlyingDayBar.symbol)
                .where(
                    HistoricalUnderlyingDayBar.trade_date >= start_date,
                    HistoricalUnderlyingDayBar.trade_date <= end_date,
                )
                .distinct()
            )
        )
        symbols.update(
            session.scalars(
                select(HistoricalOptionDayBar.underlying_symbol)
                .where(
                    HistoricalOptionDayBar.trade_date >= start_date,
                    HistoricalOptionDayBar.trade_date <= end_date,
                )
                .distinct()
            )
        )
    return sorted(symbol.strip().upper() for symbol in symbols if symbol and symbol.strip())


def _discover_optionable_symbols_from_flatfile(
    flatfiles: MassiveFlatFilesClient,
    *,
    as_of_date: date,
) -> list[str]:
    symbols: set[str] = set()
    for row in flatfiles.iter_csv_rows(option_day_dataset(), as_of_date):
        raw_ticker = row.get("ticker") or row.get("sym") or row.get("option_ticker") or ""
        if not isinstance(raw_ticker, str):
            continue
        metadata = parse_option_ticker_metadata(raw_ticker.strip().upper())
        if metadata is None:
            continue
        underlying_symbol, _, _, _ = metadata
        if underlying_symbol:
            symbols.add(underlying_symbol)
    return sorted(symbols)


def _resolve_symbols(
    *,
    start_date: date,
    end_date: date,
    explicit_symbols: set[str],
    universe_source: str,
    client: MassiveClient,
    store: HistoricalMarketDataStore | None = None,
    flatfiles: MassiveFlatFilesClient | None = None,
    optionable_as_of_date: date | None = None,
) -> list[str]:
    if explicit_symbols:
        return sorted(explicit_symbols)
    if universe_source == "massive-optionable":
        return client.list_optionable_underlyings(as_of_date=end_date, include_expired=True)
    if universe_source == "flatfile-optionable":
        if flatfiles is None:
            raise ValueError("flatfile-optionable universe resolution requires a flatfiles client.")
        as_of_date = optionable_as_of_date or end_date
        return _discover_optionable_symbols_from_flatfile(flatfiles, as_of_date=as_of_date)
    settings = get_settings()
    configured = sorted(set(settings.historical_data_sync_symbols))
    if configured:
        return configured
    if store is not None:
        try:
            imported = sorted(store.list_imported_symbols_for_window(start_date, end_date))
        except SQLAlchemyError:
            imported = []
        if imported:
            return imported
    return _discover_symbols(start_date=start_date, end_date=end_date)


def _quarter_end_for_day(day: date) -> date:
    quarter_end_month = ((day.month - 1) // 3 + 1) * 3
    return date(day.year, quarter_end_month, monthrange(day.year, quarter_end_month)[1])


def _iter_symbol_chunks(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    chunk_mode: str,
) -> Iterator[EarningsBackfillChunk]:
    cursor = start_date
    while cursor <= end_date:
        if chunk_mode == "year":
            chunk_end = date(cursor.year, 12, 31)
        elif chunk_mode == "quarter":
            chunk_end = _quarter_end_for_day(cursor)
        else:  # pragma: no cover
            raise ValueError(f"Unsupported chunk mode: {chunk_mode}")
        if chunk_end > end_date:
            chunk_end = end_date
        yield EarningsBackfillChunk(symbol=symbol, start_date=cursor, end_date=chunk_end)
        cursor = chunk_end + timedelta(days=1)


def _build_chunks(
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    chunk_mode: str,
) -> list[EarningsBackfillChunk]:
    chunks: list[EarningsBackfillChunk] = []
    for symbol in symbols:
        chunks.extend(
            _iter_symbol_chunks(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                chunk_mode=chunk_mode,
            )
        )
    return chunks


def _sync_chunk(
    store: HistoricalMarketDataStore | None,
    client: MassiveClient,
    chunk: EarningsBackfillChunk,
    *,
    dry_run: bool,
) -> ChunkSyncResult:
    try:
        events = client.list_earnings_event_records(chunk.symbol, chunk.start_date, chunk.end_date)
    except ExternalServiceError as exc:
        return ChunkSyncResult(
            chunk=chunk,
            earnings_count=0,
            stored_count=0,
            error=exc.message,
        )

    rows = [
        HistoricalEarningsEvent(
            symbol=chunk.symbol,
            event_date=item.event_date,
            event_type=item.event_type,
            provider_event_id=item.provider_event_id,
            source_file_date=item.event_date,
        )
        for item in sorted(events, key=lambda record: (record.event_date, record.event_type, record.provider_event_id or ""))
    ]
    stored_count = len(rows) if dry_run or store is None else store.upsert_earnings_events(rows)
    return ChunkSyncResult(
        chunk=chunk,
        earnings_count=len(rows),
        stored_count=stored_count,
        error=None,
    )


def _result_status_payload(result: ChunkSyncResult) -> ChunkResultPayload:
    return {
        "symbol": result.chunk.symbol,
        "chunk_start": result.chunk.start_date.isoformat(),
        "chunk_end": result.chunk.end_date.isoformat(),
        "earnings_count": result.earnings_count,
        "stored_count": result.stored_count,
        "error": result.error,
    }


def _checkpoint_payload(result: ChunkSyncResult) -> ChunkCheckpointPayload:
    return _result_status_payload(result)


def _read_status_snapshot(snapshot_path: str | None) -> EarningsBackfillStatusSnapshot:
    if not snapshot_path:
        return {}
    path = Path(snapshot_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Unable to parse earnings backfill status file '{snapshot_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Earnings backfill status file '{snapshot_path}' does not contain a JSON object.")
    return payload


def _run_signature(
    *,
    start_date: date,
    end_date: date,
    symbols: list[str],
    universe_source: str,
    optionable_as_of_date: date | None,
    chunk_mode: str,
    dry_run: bool,
) -> dict[str, object]:
    return {
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "symbols": symbols,
        "universe_source": universe_source,
        "optionable_as_of_date": optionable_as_of_date.isoformat() if optionable_as_of_date is not None else None,
        "chunk_mode": chunk_mode,
        "dry_run": dry_run,
    }


def _parse_chunk_checkpoints(raw: object) -> dict[str, ChunkCheckpointPayload]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("chunk_checkpoints must be a JSON object keyed by symbol|start|end.")
    checkpoints: dict[str, ChunkCheckpointPayload] = {}
    for raw_key, payload in raw.items():
        if not isinstance(raw_key, str):
            raise ValueError("chunk_checkpoints keys must be strings.")
        if not isinstance(payload, dict):
            raise ValueError(f"chunk_checkpoints['{raw_key}'] must be an object.")
        symbol = str(payload.get("symbol", "")).strip().upper()
        chunk_start = str(payload.get("chunk_start", "")).strip()
        chunk_end = str(payload.get("chunk_end", "")).strip()
        if not symbol or not chunk_start or not chunk_end:
            raise ValueError(f"chunk_checkpoints['{raw_key}'] is missing symbol/chunk_start/chunk_end.")
        expected_key = f"{symbol}|{chunk_start}|{chunk_end}"
        if expected_key != raw_key:
            raise ValueError(
                f"chunk_checkpoints['{raw_key}'] key does not match payload contents ('{expected_key}')."
            )
        try:
            date.fromisoformat(chunk_start)
            date.fromisoformat(chunk_end)
        except ValueError as exc:
            raise ValueError(f"chunk_checkpoints['{raw_key}'] contains an invalid ISO date.") from exc
        checkpoints[raw_key] = {
            "symbol": symbol,
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "earnings_count": int(payload.get("earnings_count", 0)),
            "stored_count": int(payload.get("stored_count", 0)),
            "error": None if payload.get("error") is None else str(payload.get("error")),
        }
    return checkpoints


def _completed_chunk_keys(checkpoints: dict[str, ChunkCheckpointPayload]) -> set[str]:
    return {
        chunk_key
        for chunk_key, payload in checkpoints.items()
        if payload["error"] is None
    }


def _checkpoint_row_totals(checkpoints: dict[str, ChunkCheckpointPayload]) -> tuple[int, int]:
    earnings_total = sum(payload["earnings_count"] for payload in checkpoints.values())
    stored_total = sum(payload["stored_count"] for payload in checkpoints.values())
    return earnings_total, stored_total


def _progress_snapshot_fields(
    chunks: list[EarningsBackfillChunk],
    checkpoints: dict[str, ChunkCheckpointPayload],
) -> dict[str, object]:
    ordered_chunk_keys = [chunk.key for chunk in chunks]
    processed_keys = set(checkpoints)
    completed_keys = _completed_chunk_keys(checkpoints)
    remaining_keys = [chunk_key for chunk_key in ordered_chunk_keys if chunk_key not in completed_keys]
    total_chunks = len(ordered_chunk_keys)
    completed_count = len(completed_keys)
    processed_count = len(processed_keys)
    return {
        "total_chunks": total_chunks,
        "completed_chunks": completed_count,
        "completed_pct": round((completed_count / total_chunks) * 100, 2) if total_chunks else 0.0,
        "processed_chunks": processed_count,
        "processed_pct": round((processed_count / total_chunks) * 100, 2) if total_chunks else 0.0,
        "completed_chunks_list": sorted(completed_keys),
        "processed_chunks_list": sorted(processed_keys),
        "remaining_chunks": len(remaining_keys),
        "remaining_chunks_sample": remaining_keys[:10],
        "next_pending_chunk": remaining_keys[0] if remaining_keys else None,
        "last_completed_chunk": max(completed_keys) if completed_keys else None,
    }


def _load_resume_checkpoint(
    *,
    status_path: str | None,
    run_signature: dict[str, object],
    chunks: list[EarningsBackfillChunk],
    resume_requested: bool,
) -> ResumeCheckpoint:
    if not resume_requested:
        return ResumeCheckpoint(chunk_checkpoints={}, last_result=None, applied=False)
    if not status_path:
        raise ValueError("--resume requires --status-path or BACKTESTFORECAST_EARNINGS_BACKFILL_STATUS_PATH.")

    snapshot = _read_status_snapshot(status_path)
    if not snapshot:
        return ResumeCheckpoint(chunk_checkpoints={}, last_result=None, applied=False)

    existing_signature = snapshot.get("run_signature")
    if existing_signature is not None and existing_signature != run_signature:
        raise ValueError("Existing status checkpoint belongs to a different earnings backfill window or symbol set; refusing to resume.")

    checkpoints = _parse_chunk_checkpoints(snapshot.get("chunk_checkpoints"))
    if not checkpoints:
        legacy_completed = int(snapshot.get("completed_chunks", 0) or 0)
        legacy_processed = int(snapshot.get("processed_chunks", 0) or 0)
        if legacy_completed > 0 or legacy_processed > 0:
            raise ValueError("Existing status file does not contain per-chunk checkpoints; rerun without --resume.")

    valid_chunk_keys = {chunk.key for chunk in chunks}
    unknown_keys = set(checkpoints) - valid_chunk_keys
    if unknown_keys:
        sample = ", ".join(sorted(unknown_keys)[:5])
        raise ValueError(f"Existing status checkpoint references chunks outside the requested window: {sample}")

    last_result = snapshot.get("last_result")
    if last_result is not None and not isinstance(last_result, dict):
        last_result = None
    return ResumeCheckpoint(
        chunk_checkpoints=dict(checkpoints),
        last_result=last_result if isinstance(last_result, dict) else None,
        applied=bool(checkpoints),
    )


def _write_status_snapshot(
    snapshot_path: str | None,
    *,
    allow_failure: bool = False,
    max_attempts: int = 8,
    retry_delay_seconds: float = 0.25,
    **fields: object,
) -> bool:
    if not snapshot_path:
        return True
    path = Path(snapshot_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    last_error: PermissionError | None = None
    for attempt in range(1, max_attempts + 1):
        payload: EarningsBackfillStatusSnapshot = {}
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
        payload.update(fields)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        try:
            temp_path.replace(path)
            return True
        except PermissionError as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(retry_delay_seconds)
    if allow_failure:
        print(
            f"warning: unable to update status snapshot '{snapshot_path}' after {max_attempts} attempts ({last_error})",
            file=os.sys.stderr,
            flush=True,
        )
        return False
    if last_error is not None:
        raise last_error
    return True


def _should_write_progress_snapshot(*, processed_count: int, status_write_every: int, result: ChunkSyncResult) -> bool:
    if status_write_every <= 1:
        return True
    if result.error is not None:
        return True
    return processed_count % status_write_every == 0


def _print_chunk_result(result: ChunkSyncResult, *, processed: int, total: int, dry_run: bool) -> None:
    prefix = f"[{processed}/{total}] " if total else ""
    action = "matched" if dry_run else "stored"
    if result.error is None:
        print(
            f"{prefix}{result.chunk.symbol} {result.chunk.start_date.isoformat()}..{result.chunk.end_date.isoformat()}: "
            f"earnings={result.earnings_count} {action}={result.stored_count}",
            flush=True,
        )
        return
    print(
        f"{prefix}{result.chunk.symbol} {result.chunk.start_date.isoformat()}..{result.chunk.end_date.isoformat()}: "
        f"skipped ({result.error})",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill historical earnings events from Massive into Postgres by symbol/date-range chunks."
    )
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols to backfill.")
    parser.add_argument("--symbols-file", default="", help="Optional file containing newline- or comma-separated symbols.")
    parser.add_argument(
        "--universe-source",
        choices=("auto", "massive-optionable", "flatfile-optionable"),
        default="auto",
        help="How to resolve symbols when --symbols is omitted.",
    )
    parser.add_argument(
        "--optionable-as-of-date",
        default="",
        help="Optional ISO date to use when resolving an optionable universe from a snapshot source.",
    )
    parser.add_argument(
        "--chunk-mode",
        choices=("year", "quarter"),
        default="year",
        help="Chunk size for each symbol/date-range request.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and count earnings without writing to Postgres.")
    parser.add_argument("--resume", action="store_true", help="Resume from the existing status checkpoint.")
    parser.add_argument("--status-path", default="", help="Optional JSON status file to update incrementally.")
    parser.add_argument(
        "--status-write-every",
        type=int,
        default=100,
        help="How many processed chunks to accumulate before rewriting the status snapshot. Errors and final completion always flush immediately.",
    )
    args = parser.parse_args()

    settings = get_settings()
    default_end = market_date_today() - timedelta(days=1)
    start_date = date.fromisoformat(args.start_date) if args.start_date else date(settings.historical_data_start_year, 1, 1)
    end_date = date.fromisoformat(args.end_date) if args.end_date else default_end
    optionable_as_of_date = date.fromisoformat(args.optionable_as_of_date) if args.optionable_as_of_date else None
    if start_date > end_date:
        raise ValueError("--start-date must be <= --end-date")
    if optionable_as_of_date is not None and optionable_as_of_date > market_date_today():
        raise ValueError("--optionable-as-of-date must not be in the future")
    if args.status_write_every < 1:
        raise ValueError("--status-write-every must be >= 1")

    status_path = (
        args.status_path
        or os.getenv("BACKTESTFORECAST_EARNINGS_BACKFILL_STATUS_PATH")
        or os.getenv("BACKTESTFORECAST_IMPORT_STATUS_PATH")
        or os.getenv("HISTORICAL_IMPORT_STATUS_PATH")
    )
    client = MassiveClient()
    flatfiles = MassiveFlatFilesClient.from_settings() if args.universe_source == "flatfile-optionable" else None
    store = None if args.dry_run else HistoricalMarketDataStore(create_session, create_readonly_session)
    requested_symbols = sorted(_parse_symbols_csv(args.symbols) | (_load_symbols_file(args.symbols_file) if args.symbols_file else set()))
    resolved_symbols: list[str] = []
    chunks: list[EarningsBackfillChunk] = []
    chunk_checkpoints: dict[str, ChunkCheckpointPayload] = {}
    progress_fields = _progress_snapshot_fields(chunks, chunk_checkpoints)
    try:
        resolved_symbols = _resolve_symbols(
            start_date=start_date,
            end_date=end_date,
            explicit_symbols=set(requested_symbols),
            universe_source=args.universe_source,
            client=client,
            store=store,
            flatfiles=flatfiles,
            optionable_as_of_date=optionable_as_of_date,
        )
        if not resolved_symbols:
            raise ValueError("No symbols were resolved for the requested earnings backfill window.")

        chunks = _build_chunks(
            symbols=resolved_symbols,
            start_date=start_date,
            end_date=end_date,
            chunk_mode=args.chunk_mode,
        )
        if not chunks:
            raise ValueError("No chunks were generated for the requested earnings backfill window.")

        if args.resume and not status_path:
            raise ValueError("--resume requires --status-path or BACKTESTFORECAST_EARNINGS_BACKFILL_STATUS_PATH.")

        run_signature = _run_signature(
            start_date=start_date,
            end_date=end_date,
            symbols=resolved_symbols,
            universe_source=args.universe_source,
            optionable_as_of_date=optionable_as_of_date,
            chunk_mode=args.chunk_mode,
            dry_run=args.dry_run,
        )
        resume_checkpoint = _load_resume_checkpoint(
            status_path=status_path,
            run_signature=run_signature,
            chunks=chunks,
            resume_requested=args.resume,
        )
        chunk_checkpoints = dict(resume_checkpoint.chunk_checkpoints)
        progress_fields = _progress_snapshot_fields(chunks, chunk_checkpoints)
        completed_earnings_rows, completed_stored_rows = _checkpoint_row_totals(chunk_checkpoints)
        pending_chunks = [chunk for chunk in chunks if chunk.key not in _completed_chunk_keys(chunk_checkpoints)]
        started_at = _iso_now()

        _write_status_snapshot(
            status_path,
            started_at=started_at,
            command=" ".join(os.sys.argv),
            launcher_pid=os.getppid(),
            python_pid=os.getpid(),
            status_path=status_path,
            status="running",
            updated_at=started_at,
            universe_source=args.universe_source,
            optionable_as_of_date=optionable_as_of_date.isoformat() if optionable_as_of_date is not None else None,
            chunk_mode=args.chunk_mode,
            status_write_every=args.status_write_every,
            window_start=start_date.isoformat(),
            window_end=end_date.isoformat(),
            requested_symbols=requested_symbols,
            resolved_symbols=resolved_symbols,
            resolved_symbol_count=len(resolved_symbols),
            run_signature=run_signature,
            resume_requested=args.resume,
            resume_applied=resume_checkpoint.applied,
            resumed_chunks=progress_fields["completed_chunks"],
            completed_earnings_rows=completed_earnings_rows,
            completed_stored_rows=completed_stored_rows,
            chunk_checkpoints={chunk_key: payload for chunk_key, payload in sorted(chunk_checkpoints.items())},
            last_result=resume_checkpoint.last_result,
            completed_at=None,
            failed_at=None,
            error=None,
            **progress_fields,
        )

        processed_count = int(progress_fields["processed_chunks"])
        total_chunks = len(chunks)
        for chunk in pending_chunks:
            result = _sync_chunk(store, client, chunk, dry_run=args.dry_run)
            processed_count += 1
            _print_chunk_result(result, processed=processed_count, total=total_chunks, dry_run=args.dry_run)
            chunk_checkpoints[chunk.key] = _checkpoint_payload(result)
            completed_earnings_rows, completed_stored_rows = _checkpoint_row_totals(chunk_checkpoints)
            progress_fields = _progress_snapshot_fields(chunks, chunk_checkpoints)
            if _should_write_progress_snapshot(
                processed_count=processed_count,
                status_write_every=args.status_write_every,
                result=result,
            ):
                progress_timestamp = _iso_now()
                _write_status_snapshot(
                    status_path,
                    allow_failure=True,
                    status="running",
                    updated_at=progress_timestamp,
                    completed_earnings_rows=completed_earnings_rows,
                    completed_stored_rows=completed_stored_rows,
                    chunk_checkpoints={chunk_key: payload for chunk_key, payload in sorted(chunk_checkpoints.items())},
                    last_result=_result_status_payload(result),
                    **progress_fields,
                )
        completion_timestamp = _iso_now()
        progress_fields = _progress_snapshot_fields(chunks, chunk_checkpoints)
        completion_status = "completed" if int(progress_fields["remaining_chunks"]) == 0 else "completed_with_errors"
        completion_error = None
        if completion_status != "completed":
            completion_error = (
                f"{progress_fields['remaining_chunks']} chunk(s) remain incomplete. "
                "Resolve the per-chunk errors and rerun with --resume."
            )
        _write_status_snapshot(
            status_path,
            allow_failure=True,
            status=completion_status,
            updated_at=completion_timestamp,
            completed_earnings_rows=completed_earnings_rows,
            completed_stored_rows=completed_stored_rows,
            completed_at=completion_timestamp,
            chunk_checkpoints={chunk_key: payload for chunk_key, payload in sorted(chunk_checkpoints.items())},
            error=completion_error,
            **progress_fields,
        )
    except Exception as exc:
        failure_timestamp = _iso_now()
        completed_earnings_rows, completed_stored_rows = _checkpoint_row_totals(chunk_checkpoints)
        progress_fields = _progress_snapshot_fields(chunks, chunk_checkpoints)
        _write_status_snapshot(
            status_path,
            allow_failure=True,
            status="failed",
            updated_at=failure_timestamp,
            failed_at=failure_timestamp,
            completed_earnings_rows=completed_earnings_rows,
            completed_stored_rows=completed_stored_rows,
            chunk_checkpoints={chunk_key: payload for chunk_key, payload in sorted(chunk_checkpoints.items())},
            error=str(exc),
            **progress_fields,
        )
        raise
    finally:
        if flatfiles is not None:
            flatfiles.close()
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
