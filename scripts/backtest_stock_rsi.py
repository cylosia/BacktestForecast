from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.services.serialization import serialize_equity_point, serialize_summary, serialize_trade  # noqa: E402
from backtestforecast.stock_rsi import StockRsiConfig, run_stock_rsi_backtest  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest a single-stock RSI crossover strategy.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0)
    parser.add_argument("--symbol", default="FAS")
    parser.add_argument("--start-date", type=_parse_date, default=date(2010, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=date(2010, 12, 31))
    parser.add_argument("--rsi-period", type=int, default=14)
    parser.add_argument("--entry-level", type=float, default=30.0)
    parser.add_argument("--exit-level", type=float, default=70.0)
    parser.add_argument("--entry-direction", choices=("crosses_above", "crosses_below"), default="crosses_above")
    parser.add_argument("--exit-direction", choices=("crosses_above", "crosses_below"), default="crosses_below")
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--trade-ledger-csv", type=Path, default=None)
    parser.add_argument("--equity-curve-csv", type=Path, default=None)
    return parser


def _default_output_paths(symbol: str, start_date: date, end_date: date) -> tuple[Path, Path, Path]:
    suffix = f"{symbol.lower()}_rsi_{start_date.isoformat()}_{end_date.isoformat()}"
    return (
        ROOT / "logs" / f"{suffix}.json",
        ROOT / "logs" / f"{suffix}_trade_ledger.csv",
        ROOT / "logs" / f"{suffix}_equity_curve.csv",
    )


def _load_bars(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    rsi_period: int,
) -> list[DailyBar]:
    warmup_start = start_date - timedelta(days=max(60, rsi_period * 10))
    stmt = (
        select(
            HistoricalUnderlyingDayBar.trade_date,
            HistoricalUnderlyingDayBar.open_price,
            HistoricalUnderlyingDayBar.high_price,
            HistoricalUnderlyingDayBar.low_price,
            HistoricalUnderlyingDayBar.close_price,
            HistoricalUnderlyingDayBar.volume,
        )
        .where(
            HistoricalUnderlyingDayBar.symbol == symbol,
            HistoricalUnderlyingDayBar.trade_date >= warmup_start,
            HistoricalUnderlyingDayBar.trade_date <= end_date,
        )
        .order_by(HistoricalUnderlyingDayBar.trade_date)
    )
    rows = session.execute(stmt).all()
    return [
        DailyBar(
            trade_date=row.trade_date,
            open_price=float(row.open_price),
            high_price=float(row.high_price),
            low_price=float(row.low_price),
            close_price=float(row.close_price),
            volume=float(row.volume),
        )
        for row in rows
    ]


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    output_json, trade_ledger_csv, equity_curve_csv = _default_output_paths(
        args.symbol.upper(),
        args.start_date,
        args.end_date,
    )
    if args.output_json is not None:
        output_json = args.output_json
    if args.trade_ledger_csv is not None:
        trade_ledger_csv = args.trade_ledger_csv
    if args.equity_curve_csv is not None:
        equity_curve_csv = args.equity_curve_csv

    config = StockRsiConfig(
        symbol=args.symbol,
        rsi_period=args.rsi_period,
        entry_level=args.entry_level,
        exit_level=args.exit_level,
        entry_direction=args.entry_direction,
        exit_direction=args.exit_direction,
    )

    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            bars = _load_bars(
                session,
                symbol=config.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                rsi_period=config.rsi_period,
            )
    finally:
        engine.dispose()

    result = run_stock_rsi_backtest(
        bars,
        config=config,
        start_date=args.start_date,
        end_date=args.end_date,
        starting_equity=args.starting_equity,
        risk_free_rate=args.risk_free_rate,
    )
    payload = {
        "symbol": config.symbol,
        "period": {"start": args.start_date.isoformat(), "end": args.end_date.isoformat()},
        "config": {
            "rsi_period": config.rsi_period,
            "entry_level": config.entry_level,
            "exit_level": config.exit_level,
            "entry_direction": config.entry_direction,
            "exit_direction": config.exit_direction,
        },
        "summary": serialize_summary(result.summary),
        "warnings": list(result.warnings),
        "trade_count": len(result.trades),
        "trades": [serialize_trade(item) for item in result.trades],
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(trade_ledger_csv, [serialize_trade(item) for item in result.trades])
    _write_csv(equity_curve_csv, [serialize_equity_point(item) for item in result.equity_curve])

    print(f"Summary: {json.dumps(serialize_summary(result.summary), sort_keys=True)}")
    print(f"Trade count: {len(result.trades)}")
    print(f"Wrote {output_json}")
    print(f"Wrote {trade_ledger_csv}")
    print(f"Wrote {equity_curve_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
