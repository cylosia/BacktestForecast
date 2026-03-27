from __future__ import annotations

import argparse
from datetime import date, timedelta

from backtestforecast.config import get_settings
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.integrations.massive_flatfiles import (
    MassiveFlatFilesClient,
    option_day_dataset,
    parse_option_day_rows,
    parse_stock_day_rows,
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync historical market data from Massive flat files into Postgres.")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying symbols to keep.")
    args = parser.parse_args()

    settings = get_settings()
    default_end = market_date_today() - timedelta(days=1)
    start_date = date.fromisoformat(args.start_date) if args.start_date else date(settings.historical_data_start_year, 1, 1)
    end_date = date.fromisoformat(args.end_date) if args.end_date else default_end
    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()}
    if not symbols:
        symbols = set(settings.historical_data_sync_symbols)

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    flatfiles = MassiveFlatFilesClient.from_settings()
    rest_client = MassiveClient()

    try:
        for trade_date in _iter_dates(start_date, end_date):
            if not is_trading_day(trade_date):
                continue
            stock_rows = flatfiles.download_csv_rows(stock_day_dataset(), trade_date)
            option_rows = flatfiles.download_csv_rows(option_day_dataset(), trade_date)
            store.upsert_underlying_day_bars(parse_stock_day_rows(stock_rows, trade_date, symbols=symbols or None))
            store.upsert_option_day_bars(parse_option_day_rows(option_rows, trade_date, symbols=symbols or None))
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
