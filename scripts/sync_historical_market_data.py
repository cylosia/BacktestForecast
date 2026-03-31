from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TypedDict

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.config import get_settings
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.integrations.massive_flatfiles import (
    MassiveFlatFilesClient,
    chunked,
    iter_option_day_bar_records,
    iter_stock_day_bar_payloads,
    option_day_dataset,
    stock_day_dataset,
)
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.models import HistoricalEarningsEvent, HistoricalExDividendDate, HistoricalTreasuryYield
from backtestforecast.utils.dates import is_trading_day, market_date_today


@dataclass(slots=True)
class TradeDateSyncResult:
    trade_date: date
    stock_count: int
    option_count: int
    stock_error: str | None = None
    option_error: str | None = None
    stock_symbols: tuple[str, ...] = ()
    option_symbols: tuple[str, ...] = ()


class TradeDateResultPayload(TypedDict):
    trade_date: str
    stock_count: int
    option_count: int
    stock_error: str | None
    option_error: str | None
    stock_symbols: list[str]
    option_symbols: list[str]


class TradeDateCheckpointPayload(TypedDict):
    stock_count: int
    option_count: int
    stock_error: str | None
    option_error: str | None


class ImportStatusSnapshot(TypedDict, total=False):
    started_at: str
    command: str
    launcher_pid: int
    python_pid: int
    stdout_log_path: str
    stderr_log_path: str
    status_path: str
    log_path: str
    status: str
    updated_at: str
    window_start: str
    window_end: str
    total_trade_dates: int
    completed_trade_dates: int
    completed_pct: float
    processed_trade_dates: int
    processed_pct: float
    completed_stock_rows: int
    completed_option_rows: int
    last_completed_trade_date: str | None
    last_result: TradeDateResultPayload | None
    trade_date_checkpoints: dict[str, TradeDateCheckpointPayload]
    completed_trade_dates_list: list[str]
    processed_trade_dates_list: list[str]
    requested_symbols: list[str]
    run_signature: dict[str, object]
    resume_requested: bool
    resume_applied: bool
    resumed_trade_dates: int
    remaining_trade_dates: int
    remaining_trade_dates_sample: list[str]
    next_pending_trade_date: str | None
    window_target_trade_date: str | None
    window_coverage_status: str
    window_freshness_trade_date_lag: int
    completed_at: str | None
    failed_at: str | None
    error: str | None


@dataclass(slots=True)
class ResumeCheckpoint:
    trade_date_checkpoints: dict[date, TradeDateCheckpointPayload]
    last_result: TradeDateResultPayload | None
    applied: bool


def _iter_dates(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _iter_trading_dates(start_date: date, end_date: date) -> Iterator[date]:
    for trade_date in _iter_dates(start_date, end_date):
        if is_trading_day(trade_date):
            yield trade_date


def _sync_stock_day(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> int:
    inserted, _symbols = _sync_stock_day_with_symbols(
        store,
        flatfiles,
        trade_date,
        symbols=symbols,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    return inserted


def _sync_stock_day_with_symbols(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> tuple[int, tuple[str, ...]]:
    inserted = 0
    seen_symbols: set[str] = set()

    def _payloads() -> Iterator[dict[str, object]]:
        for payload in iter_stock_day_bar_payloads(
            flatfiles.iter_csv_rows(stock_day_dataset(), trade_date),
            trade_date,
            symbols=symbols,
        ):
            seen_symbols.add(str(payload["symbol"]))
            yield payload

    payloads = _payloads()
    for batch in chunked(payloads, batch_size):
        inserted += len(batch) if dry_run else store.upsert_underlying_day_bar_payloads(batch)
    return inserted, tuple(sorted(seen_symbols))


def _sync_option_day(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> int:
    inserted, _symbols = _sync_option_day_with_symbols(
        store,
        flatfiles,
        trade_date,
        symbols=symbols,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    return inserted


def _sync_option_day_with_symbols(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> tuple[int, tuple[str, ...]]:
    inserted = 0
    seen_symbols: set[str] = set()

    def _records() -> Iterator[tuple[object, ...]]:
        for record in iter_option_day_bar_records(
            flatfiles.iter_csv_rows(option_day_dataset(), trade_date),
            trade_date,
            symbols=symbols,
        ):
            seen_symbols.add(str(record[2]))
            yield record

    records = _records()
    for batch in chunked(records, batch_size):
        inserted += len(batch) if dry_run else store.upsert_option_day_bar_records(batch)
    return inserted, tuple(sorted(seen_symbols))


def _sync_trade_date(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> TradeDateSyncResult:
    stock_error: str | None = None
    option_error: str | None = None
    stock_symbols: tuple[str, ...] = ()
    option_symbols: tuple[str, ...] = ()
    try:
        stock_count, stock_symbols = _sync_stock_day_with_symbols(
            store,
            flatfiles,
            trade_date,
            symbols=symbols,
            batch_size=batch_size,
            dry_run=dry_run,
        )
    except ExternalServiceError as exc:
        stock_count = 0
        stock_error = exc.message
    try:
        option_count, option_symbols = _sync_option_day_with_symbols(
            store,
            flatfiles,
            trade_date,
            symbols=symbols,
            batch_size=batch_size,
            dry_run=dry_run,
        )
    except ExternalServiceError as exc:
        option_count = 0
        option_error = exc.message
    return TradeDateSyncResult(
        trade_date=trade_date,
        stock_count=stock_count,
        option_count=option_count,
        stock_error=stock_error,
        option_error=option_error,
        stock_symbols=stock_symbols,
        option_symbols=option_symbols,
    )


def _sync_trade_dates(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_dates: list[date],
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
    workers: int = 1,
) -> list[TradeDateSyncResult]:
    results_by_date = {
        result.trade_date: result
        for result in _iter_sync_trade_dates(
            store,
            flatfiles,
            trade_dates,
            symbols=symbols,
            batch_size=batch_size,
            dry_run=dry_run,
            workers=workers,
        )
    }
    return [results_by_date[trade_date] for trade_date in trade_dates if trade_date in results_by_date]


def _iter_sync_trade_dates(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_dates: list[date],
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
    workers: int = 1,
) -> Iterator[TradeDateSyncResult]:
    if not trade_dates:
        return
    if workers <= 1 or len(trade_dates) == 1:
        for trade_date in trade_dates:
            yield _sync_trade_date(
                store,
                flatfiles,
                trade_date,
                symbols=symbols,
                batch_size=batch_size,
                dry_run=dry_run,
            )
        return

    with ThreadPoolExecutor(max_workers=min(workers, len(trade_dates))) as executor:
        futures = [
            executor.submit(
                _sync_trade_date,
                store,
                flatfiles,
                trade_date,
                symbols=symbols,
                batch_size=batch_size,
                dry_run=dry_run,
            )
            for trade_date in trade_dates
        ]
        for future in as_completed(futures):
            yield future.result()


def _print_trade_date_result(
    result: TradeDateSyncResult,
    *,
    dry_run: bool,
    completed: int | None = None,
    total: int | None = None,
) -> None:
    progress_prefix = f"[{completed}/{total}] " if completed is not None and total is not None else ""
    print(
        f"{progress_prefix}{result.trade_date.isoformat()}: {'parsed' if dry_run else 'synced'} "
        f"stock_rows={result.stock_count} option_rows={result.option_count}",
        flush=True,
    )
    if result.stock_error is not None:
        print(f"{progress_prefix}{result.trade_date.isoformat()}: skipped stock sync ({result.stock_error})", flush=True)
    if result.option_error is not None:
        print(f"{progress_prefix}{result.trade_date.isoformat()}: skipped option sync ({result.option_error})", flush=True)


def _maybe_enrich_trade_date(
    store: HistoricalMarketDataStore | None,
    rest_client: MassiveClient | None,
    result: TradeDateSyncResult,
    *,
    symbols: set[str],
    skip_rest_enrichment: bool,
) -> None:
    if store is None or rest_client is None or skip_rest_enrichment:
        return
    if result.stock_error is not None and result.option_error is not None:
        return
    trade_date = result.trade_date
    try:
        treasury = rest_client.get_average_treasury_yield(trade_date, trade_date)
        if treasury is not None:
            store.upsert_treasury_yields(
                [
                    HistoricalTreasuryYield(
                        trade_date=trade_date,
                        yield_3_month=treasury,
                        source_file_date=trade_date,
                    )
                ]
            )
    except ExternalServiceError:
        pass


def _backfill_dividends_for_window(
    store: HistoricalMarketDataStore | None,
    rest_client: MassiveClient | None,
    *,
    start_date: date,
    end_date: date,
    symbols: set[str],
    skip_rest_enrichment: bool,
) -> int:
    if store is None or rest_client is None or skip_rest_enrichment:
        return 0

    effective_symbols = set(symbols) if symbols else store.list_imported_symbols_for_window(start_date, end_date)
    stored_rows = 0
    for symbol in sorted(effective_symbols):
        try:
            dividends = rest_client.list_ex_dividend_records(symbol, start_date, end_date)
        except ExternalServiceError:
            continue
        if not dividends:
            continue
        stored_rows += store.upsert_ex_dividend_dates(
            [
                HistoricalExDividendDate(
                    symbol=symbol.upper(),
                    ex_dividend_date=item.ex_dividend_date,
                    provider_dividend_id=item.provider_dividend_id,
                    cash_amount=item.cash_amount,
                    currency=item.currency,
                    declaration_date=item.declaration_date,
                    record_date=item.record_date,
                    pay_date=item.pay_date,
                    frequency=item.frequency,
                    distribution_type=item.distribution_type,
                    historical_adjustment_factor=item.historical_adjustment_factor,
                    split_adjusted_cash_amount=item.split_adjusted_cash_amount,
                    source_file_date=item.ex_dividend_date,
                )
                for item in sorted(dividends, key=lambda record: (record.ex_dividend_date, record.provider_dividend_id or ""))
            ]
        )
    return stored_rows


def _backfill_earnings_for_window(
    store: HistoricalMarketDataStore | None,
    rest_client: MassiveClient | None,
    *,
    start_date: date,
    end_date: date,
    symbols: set[str],
    skip_rest_enrichment: bool,
) -> int:
    if store is None or rest_client is None or skip_rest_enrichment:
        return 0

    effective_symbols = set(symbols) if symbols else store.list_imported_symbols_for_window(start_date, end_date)
    stored_rows = 0
    for symbol in sorted(effective_symbols):
        try:
            events = rest_client.list_earnings_event_records(symbol, start_date, end_date)
        except ExternalServiceError:
            continue
        if not events:
            continue
        stored_rows += store.upsert_earnings_events(
            [
                HistoricalEarningsEvent(
                    symbol=symbol.upper(),
                    event_date=item.event_date,
                    event_type=item.event_type,
                    provider_event_id=item.provider_event_id,
                    source_file_date=item.event_date,
                )
                for item in sorted(events, key=lambda record: (record.event_date, record.event_type, record.provider_event_id or ""))
            ]
        )
    return stored_rows


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _result_status_payload(result: TradeDateSyncResult) -> TradeDateResultPayload:
    return {
        "trade_date": result.trade_date.isoformat(),
        "stock_count": result.stock_count,
        "option_count": result.option_count,
        "stock_error": result.stock_error,
        "option_error": result.option_error,
        "stock_symbols": list(result.stock_symbols),
        "option_symbols": list(result.option_symbols),
    }


def _checkpoint_payload(result: TradeDateSyncResult) -> TradeDateCheckpointPayload:
    return {
        "stock_count": result.stock_count,
        "option_count": result.option_count,
        "stock_error": result.stock_error,
        "option_error": result.option_error,
    }


def _read_status_snapshot(status_path: str | None) -> ImportStatusSnapshot:
    if not status_path:
        return {}
    path = Path(status_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - exercised through main()
        raise ValueError(f"Unable to parse import status file '{status_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Import status file '{status_path}' does not contain a JSON object.")
    return payload


def _run_signature(
    *,
    start_date: date,
    end_date: date,
    symbols: set[str],
    dry_run: bool,
    skip_rest_enrichment: bool,
) -> dict[str, object]:
    return {
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "symbols": sorted(symbols),
        "dry_run": dry_run,
        "skip_rest_enrichment": skip_rest_enrichment,
    }


def _parse_trade_date_checkpoints(raw: object) -> dict[date, TradeDateCheckpointPayload]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("trade_date_checkpoints must be a JSON object keyed by ISO trade date.")
    checkpoints: dict[date, TradeDateCheckpointPayload] = {}
    for raw_trade_date, payload in raw.items():
        if not isinstance(raw_trade_date, str):
            raise ValueError("trade_date_checkpoints keys must be ISO date strings.")
        if not isinstance(payload, dict):
            raise ValueError(f"trade_date_checkpoints['{raw_trade_date}'] must be an object.")
        try:
            trade_date = date.fromisoformat(raw_trade_date)
        except ValueError as exc:
            raise ValueError(f"trade_date_checkpoints contains an invalid ISO date: {raw_trade_date}") from exc
        checkpoints[trade_date] = {
            "stock_count": int(payload.get("stock_count", 0)),
            "option_count": int(payload.get("option_count", 0)),
            "stock_error": None if payload.get("stock_error") is None else str(payload.get("stock_error")),
            "option_error": None if payload.get("option_error") is None else str(payload.get("option_error")),
        }
    return checkpoints


def _completed_trade_dates(checkpoints: dict[date, TradeDateCheckpointPayload]) -> set[date]:
    return {
        trade_date
        for trade_date, payload in checkpoints.items()
        if payload["stock_error"] is None and payload["option_error"] is None
    }


def _checkpoint_row_totals(checkpoints: dict[date, TradeDateCheckpointPayload]) -> tuple[int, int]:
    stock_total = sum(payload["stock_count"] for payload in checkpoints.values())
    option_total = sum(payload["option_count"] for payload in checkpoints.values())
    return stock_total, option_total


def _progress_snapshot_fields(
    trade_dates: list[date],
    checkpoints: dict[date, TradeDateCheckpointPayload],
) -> dict[str, object]:
    processed_dates = set(checkpoints)
    completed_dates = _completed_trade_dates(checkpoints)
    remaining_dates = [trade_date for trade_date in trade_dates if trade_date not in completed_dates]
    total_trade_dates = len(trade_dates)
    completed_count = len(completed_dates)
    processed_count = len(processed_dates)
    return {
        "total_trade_dates": total_trade_dates,
        "completed_trade_dates": completed_count,
        "completed_pct": round((completed_count / total_trade_dates) * 100, 2) if total_trade_dates else 0.0,
        "processed_trade_dates": processed_count,
        "processed_pct": round((processed_count / total_trade_dates) * 100, 2) if total_trade_dates else 0.0,
        "completed_trade_dates_list": [item.isoformat() for item in sorted(completed_dates)],
        "processed_trade_dates_list": [item.isoformat() for item in sorted(processed_dates)],
        "remaining_trade_dates": len(remaining_dates),
        "remaining_trade_dates_sample": [item.isoformat() for item in remaining_dates[:10]],
        "next_pending_trade_date": remaining_dates[0].isoformat() if remaining_dates else None,
        "last_completed_trade_date": max(completed_dates).isoformat() if completed_dates else None,
        "window_target_trade_date": trade_dates[-1].isoformat() if trade_dates else None,
        "window_coverage_status": "complete" if not remaining_dates else "incomplete",
        "window_freshness_trade_date_lag": len(remaining_dates),
    }


def _load_resume_checkpoint(
    *,
    status_path: str | None,
    run_signature: dict[str, object],
    trade_dates: list[date],
    resume_requested: bool,
) -> ResumeCheckpoint:
    if not resume_requested:
        return ResumeCheckpoint(trade_date_checkpoints={}, last_result=None, applied=False)
    if not status_path:
        raise ValueError("--resume requires --status-path or BACKTESTFORECAST_IMPORT_STATUS_PATH.")

    snapshot = _read_status_snapshot(status_path)
    if not snapshot:
        return ResumeCheckpoint(trade_date_checkpoints={}, last_result=None, applied=False)

    existing_signature = snapshot.get("run_signature")
    if existing_signature is not None and existing_signature != run_signature:
        raise ValueError("Existing status checkpoint belongs to a different import window or symbol set; refusing to resume.")

    checkpoints = _parse_trade_date_checkpoints(snapshot.get("trade_date_checkpoints"))
    if not checkpoints:
        legacy_completed = int(snapshot.get("completed_trade_dates", 0) or 0)
        legacy_processed = int(snapshot.get("processed_trade_dates", 0) or 0)
        if legacy_completed > 0 or legacy_processed > 0:
            raise ValueError("Existing status file does not contain per-trade-date checkpoints; rerun without --resume.")

    valid_trade_dates = set(trade_dates)
    unknown_dates = set(checkpoints) - valid_trade_dates
    if unknown_dates:
        sample = ", ".join(item.isoformat() for item in sorted(unknown_dates)[:5])
        raise ValueError(f"Existing status checkpoint references trade dates outside the requested window: {sample}")

    last_result = snapshot.get("last_result")
    if last_result is not None and not isinstance(last_result, dict):
        last_result = None
    return ResumeCheckpoint(
        trade_date_checkpoints=dict(checkpoints),
        last_result=last_result if isinstance(last_result, dict) else None,
        applied=bool(checkpoints),
    )


def _write_status_snapshot(status_path: str | None, **fields: object) -> None:
    if not status_path:
        return
    path = Path(status_path)
    payload: ImportStatusSnapshot = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
    payload.update(fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync historical market data from Massive flat files into Postgres.")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying symbols to keep.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Bulk upsert batch size for streaming import.")
    parser.add_argument("--workers", type=int, default=1, help="Parallel trade-date workers for flat-file sync.")
    parser.add_argument("--skip-rest-enrichment", action="store_true", help="Skip treasury/dividend REST enrichment during bulk backfills.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and count rows without writing to Postgres.")
    parser.add_argument("--resume", action="store_true", help="Resume from the existing status checkpoint instead of replaying already completed trade dates.")
    parser.add_argument("--status-path", default="", help="Optional JSON status file to update incrementally during the import.")
    args = parser.parse_args()

    settings = get_settings()
    default_end = market_date_today() - timedelta(days=1)
    start_date = date.fromisoformat(args.start_date) if args.start_date else date(settings.historical_data_start_year, 1, 1)
    end_date = date.fromisoformat(args.end_date) if args.end_date else default_end
    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()}
    if not symbols:
        symbols = set(settings.historical_data_sync_symbols)
    if args.batch_size < 1:
        raise ValueError("--batch-size must be >= 1")
    if args.workers < 1:
        raise ValueError("--workers must be >= 1")
    status_path = args.status_path or os.getenv("BACKTESTFORECAST_IMPORT_STATUS_PATH") or os.getenv("HISTORICAL_IMPORT_STATUS_PATH")
    if args.resume and not status_path:
        raise ValueError("--resume requires --status-path or BACKTESTFORECAST_IMPORT_STATUS_PATH.")

    store = None if args.dry_run else HistoricalMarketDataStore(create_session, create_readonly_session)
    flatfiles = MassiveFlatFilesClient.from_settings()
    rest_client = None if args.dry_run or args.skip_rest_enrichment else MassiveClient()
    all_trade_dates = list(_iter_trading_dates(start_date, end_date))
    total_trade_dates = len(all_trade_dates)
    run_signature = _run_signature(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        dry_run=args.dry_run,
        skip_rest_enrichment=args.skip_rest_enrichment,
    )
    resume_checkpoint = _load_resume_checkpoint(
        status_path=status_path,
        run_signature=run_signature,
        trade_dates=all_trade_dates,
        resume_requested=args.resume,
    )
    trade_date_checkpoints = dict(resume_checkpoint.trade_date_checkpoints)
    progress_fields = _progress_snapshot_fields(all_trade_dates, trade_date_checkpoints)
    completed_stock_rows, completed_option_rows = _checkpoint_row_totals(trade_date_checkpoints)
    pending_trade_dates = [trade_date for trade_date in all_trade_dates if trade_date not in _completed_trade_dates(trade_date_checkpoints)]
    initial_timestamp = _iso_now()

    _write_status_snapshot(
        status_path,
        status="running",
        updated_at=initial_timestamp,
        window_start=start_date.isoformat(),
        window_end=end_date.isoformat(),
        requested_symbols=sorted(symbols),
        run_signature=run_signature,
        resume_requested=args.resume,
        resume_applied=resume_checkpoint.applied,
        resumed_trade_dates=progress_fields["completed_trade_dates"],
        completed_stock_rows=completed_stock_rows,
        completed_option_rows=completed_option_rows,
        trade_date_checkpoints={trade_date.isoformat(): payload for trade_date, payload in sorted(trade_date_checkpoints.items())},
        last_result=resume_checkpoint.last_result,
        completed_at=None,
        failed_at=None,
        error=None,
        **progress_fields,
    )

    try:
        with flatfiles:
            for index, result in enumerate(_iter_sync_trade_dates(
                store,
                flatfiles,
                pending_trade_dates,
                symbols=symbols or None,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                workers=args.workers,
            ), start=1 + int(progress_fields["processed_trade_dates"])):
                _print_trade_date_result(
                    result,
                    dry_run=args.dry_run,
                    completed=index,
                    total=total_trade_dates,
                )
                _maybe_enrich_trade_date(
                    store,
                    rest_client,
                    result,
                    symbols=symbols,
                    skip_rest_enrichment=args.skip_rest_enrichment,
                )
                trade_date_checkpoints[result.trade_date] = _checkpoint_payload(result)
                completed_stock_rows, completed_option_rows = _checkpoint_row_totals(trade_date_checkpoints)
                progress_fields = _progress_snapshot_fields(all_trade_dates, trade_date_checkpoints)
                progress_timestamp = _iso_now()
                _write_status_snapshot(
                    status_path,
                    status="running",
                    updated_at=progress_timestamp,
                    completed_stock_rows=completed_stock_rows,
                    completed_option_rows=completed_option_rows,
                    trade_date_checkpoints={trade_date.isoformat(): payload for trade_date, payload in sorted(trade_date_checkpoints.items())},
                    last_result=_result_status_payload(result),
                    **progress_fields,
                )
        _backfill_dividends_for_window(
            store,
            rest_client,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            skip_rest_enrichment=args.skip_rest_enrichment,
        )
        _backfill_earnings_for_window(
            store,
            rest_client,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            skip_rest_enrichment=args.skip_rest_enrichment,
        )
        completion_timestamp = _iso_now()
        progress_fields = _progress_snapshot_fields(all_trade_dates, trade_date_checkpoints)
        completion_status = "completed" if int(progress_fields["remaining_trade_dates"]) == 0 else "completed_with_errors"
        completion_error = None
        if completion_status != "completed":
            completion_error = (
                f"{progress_fields['remaining_trade_dates']} trade date(s) remain incomplete. "
                "Resolve the per-date sync errors and rerun with --resume."
            )
        _write_status_snapshot(
            status_path,
            status=completion_status,
            updated_at=completion_timestamp,
            completed_stock_rows=completed_stock_rows,
            completed_option_rows=completed_option_rows,
            completed_at=completion_timestamp,
            trade_date_checkpoints={trade_date.isoformat(): payload for trade_date, payload in sorted(trade_date_checkpoints.items())},
            error=completion_error,
            **progress_fields,
        )
    except Exception as exc:
        failure_timestamp = _iso_now()
        completed_stock_rows, completed_option_rows = _checkpoint_row_totals(trade_date_checkpoints)
        progress_fields = _progress_snapshot_fields(all_trade_dates, trade_date_checkpoints)
        _write_status_snapshot(
            status_path,
            status="failed",
            updated_at=failure_timestamp,
            failed_at=failure_timestamp,
            completed_stock_rows=completed_stock_rows,
            completed_option_rows=completed_option_rows,
            trade_date_checkpoints={trade_date.isoformat(): payload for trade_date, payload in sorted(trade_date_checkpoints.items())},
            error=str(exc),
            **progress_fields,
        )
        raise
    finally:
        if rest_client is not None:
            rest_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
