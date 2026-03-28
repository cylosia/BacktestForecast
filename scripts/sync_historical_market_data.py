from __future__ import annotations

import argparse
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


def _iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync historical market data from Massive flat files into Postgres.")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying symbols to keep.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Bulk upsert batch size for streaming import.")
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

    store = None if args.dry_run else HistoricalMarketDataStore(create_session, create_readonly_session)
    flatfiles = MassiveFlatFilesClient.from_settings()
    rest_client = MassiveClient()

    try:
        with flatfiles:
            for trade_date in _iter_dates(start_date, end_date):
                if not is_trading_day(trade_date):
                    continue
                stock_count = _sync_stock_day(
                    store,
                    flatfiles,
                    trade_date,
                    symbols=symbols or None,
                    batch_size=args.batch_size,
                    dry_run=args.dry_run,
                )
                option_count = _sync_option_day(
                    store,
                    flatfiles,
                    trade_date,
                    symbols=symbols or None,
                    batch_size=args.batch_size,
                    dry_run=args.dry_run,
                )
                print(
                    f"{trade_date.isoformat()}: {'parsed' if args.dry_run else 'synced'} stock_rows={stock_count} option_rows={option_count}"
                )
                if args.dry_run:
                    continue
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
    finally:
        rest_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
