from __future__ import annotations

import argparse
from datetime import date, timedelta
from decimal import Decimal

from _bootstrap import bootstrap_repo
from sqlalchemy import func

bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.integrations.massive_client import MassiveClient  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar, HistoricalUnderlyingRawDayBar  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    if not symbols:
        raise ValueError("--symbols must contain at least one symbol.")
    return symbols


def _daterange_chunks(start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current_start = start_date
    while current_start <= end_date:
        current_end = min(end_date, current_start + timedelta(days=chunk_days - 1))
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return chunks


def _payload_from_bar(symbol: str, bar, *, source_file_date: date, raw_prices: bool) -> dict[str, object]:
    return {
        "symbol": symbol,
        "trade_date": bar.trade_date,
        "open_price": Decimal(str(bar.open_price)),
        "high_price": Decimal(str(bar.high_price)),
        "low_price": Decimal(str(bar.low_price)),
        "close_price": Decimal(str(bar.close_price)),
        "volume": Decimal(str(bar.volume)),
        "source_dataset": "massive_rest_day_aggs_raw" if raw_prices else "massive_rest_day_aggs",
        "source_file_date": source_file_date,
    }


def _target_model(*, raw_prices: bool):
    return HistoricalUnderlyingRawDayBar if raw_prices else HistoricalUnderlyingDayBar


def _current_coverage(symbol: str, *, raw_prices: bool) -> tuple[date | None, date | None, int]:
    model = _target_model(raw_prices=raw_prices)
    with create_readonly_session() as session:
        row = (
            session.query(
                func.min(model.trade_date),
                func.max(model.trade_date),
                func.count(),
            )
            .filter(model.symbol == symbol)
            .one()
        )
    return row[0], row[1], int(row[2] or 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill underlying daily bars from Massive REST into the local database.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbol list, e.g. FAS,FAZ")
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--chunk-days", type=int, default=3650, help="Date chunk size per request. Default: 3650 days.")
    parser.add_argument("--price-mode", choices=("adjusted", "raw"), default="adjusted")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and count without writing to Postgres.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.chunk_days < 1:
        raise SystemExit("--chunk-days must be >= 1")

    symbols = _parse_symbols(args.symbols)
    chunks = _daterange_chunks(args.start_date, args.end_date, args.chunk_days)
    store = None if args.dry_run else HistoricalMarketDataStore(create_session, create_readonly_session)
    total_written = 0
    source_file_date = date.today()
    raw_prices = args.price_mode == "raw"

    with MassiveClient() as client:
        for symbol in symbols:
            before_min, before_max, before_count = _current_coverage(symbol, raw_prices=raw_prices)
            print(f"{symbol} before: start={before_min} end={before_max} rows={before_count}")
            symbol_written = 0
            symbol_fetched = 0
            for chunk_start, chunk_end in chunks:
                bars = client.get_stock_daily_bars(symbol, chunk_start, chunk_end, adjusted=not raw_prices)
                payloads = [
                    _payload_from_bar(symbol, bar, source_file_date=source_file_date, raw_prices=raw_prices)
                    for bar in bars
                ]
                fetched = len(payloads)
                symbol_fetched += fetched
                if args.dry_run:
                    written = fetched
                else:
                    if raw_prices:
                        written = store.upsert_underlying_raw_day_bar_payloads(payloads)
                    else:
                        written = store.upsert_underlying_day_bar_payloads(payloads)
                symbol_written += written
                total_written += written
                print(
                    f"{symbol} chunk {chunk_start} -> {chunk_end}: fetched={fetched} "
                    f"{'would_write' if args.dry_run else 'upserted'}={written}"
                )
            after_min, after_max, after_count = (
                _current_coverage(symbol, raw_prices=raw_prices)
                if not args.dry_run
                else (before_min, before_max, before_count)
            )
            print(
                f"{symbol} after: start={after_min} end={after_max} rows={after_count} "
                f"fetched={symbol_fetched} {'would_write' if args.dry_run else 'upserted'}={symbol_written}"
            )

    print(f"total {'would_write' if args.dry_run else 'upserted'}={total_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
