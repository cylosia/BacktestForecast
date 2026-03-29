from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.config import get_settings
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.integrations.massive_flatfiles import (
    MassiveFlatFilesClient,
    chunked,
    iter_option_day_bar_payloads,
    iter_stock_day_bar_payloads,
    option_day_dataset,
    stock_day_dataset,
)
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.models import HistoricalExDividendDate, HistoricalTreasuryYield
from backtestforecast.utils.dates import is_trading_day, market_date_today


@dataclass(slots=True)
class TradeDateSyncResult:
    trade_date: date
    stock_count: int
    option_count: int
    stock_error: str | None = None
    option_error: str | None = None


def _iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _iter_trading_dates(start_date: date, end_date: date):
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
    inserted = 0
    payloads = iter_stock_day_bar_payloads(
        flatfiles.iter_csv_rows(stock_day_dataset(), trade_date),
        trade_date,
        symbols=symbols,
    )
    for batch in chunked(payloads, batch_size):
        inserted += len(batch) if dry_run else store.upsert_underlying_day_bar_payloads(batch)
    return inserted


def _sync_option_day(
    store: HistoricalMarketDataStore | None,
    flatfiles: MassiveFlatFilesClient,
    trade_date: date,
    *,
    symbols: set[str] | None,
    batch_size: int,
    dry_run: bool = False,
) -> int:
    inserted = 0
    payloads = iter_option_day_bar_payloads(
        flatfiles.iter_csv_rows(option_day_dataset(), trade_date),
        trade_date,
        symbols=symbols,
    )
    for batch in chunked(payloads, batch_size):
        inserted += len(batch) if dry_run else store.upsert_option_day_bar_payloads(batch)
    return inserted


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
    try:
        stock_count = _sync_stock_day(
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
        option_count = _sync_option_day(
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
    if not trade_dates:
        return []
    if workers <= 1 or len(trade_dates) == 1:
        return [
            _sync_trade_date(
                store,
                flatfiles,
                trade_date,
                symbols=symbols,
                batch_size=batch_size,
                dry_run=dry_run,
            )
            for trade_date in trade_dates
        ]

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
        return [future.result() for future in futures]


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
    for symbol in sorted(symbols):
        try:
            dividends = rest_client.list_ex_dividend_dates(symbol, trade_date, trade_date)
        except ExternalServiceError:
            continue
        if not dividends:
            continue
        store.upsert_ex_dividend_dates(
            [
                HistoricalExDividendDate(
                    symbol=symbol,
                    ex_dividend_date=item,
                    source_file_date=trade_date,
                )
                for item in dividends
            ]
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync historical market data from Massive flat files into Postgres.")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying symbols to keep.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Bulk upsert batch size for streaming import.")
    parser.add_argument("--workers", type=int, default=1, help="Parallel trade-date workers for flat-file sync.")
    parser.add_argument("--skip-rest-enrichment", action="store_true", help="Skip treasury/dividend REST enrichment during bulk backfills.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and count rows without writing to Postgres.")
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

    store = None if args.dry_run else HistoricalMarketDataStore(create_session, create_readonly_session)
    flatfiles = MassiveFlatFilesClient.from_settings()
    rest_client = None if args.dry_run or args.skip_rest_enrichment else MassiveClient()
    trade_dates = list(_iter_trading_dates(start_date, end_date))

    try:
        with flatfiles:
            total_trade_dates = len(trade_dates)
            for index, result in enumerate(_sync_trade_dates(
                store,
                flatfiles,
                trade_dates,
                symbols=symbols or None,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                workers=args.workers,
            ), start=1):
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
    finally:
        if rest_client is not None:
            rest_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
