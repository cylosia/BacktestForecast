from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.indicators.calculations import ema, sma  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.stock_trend import run_stock_condition_backtest  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest SMA and EMA stock trend filters.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0)
    parser.add_argument("--symbol", default="FAS")
    parser.add_argument("--start-date", type=_parse_date, default=date(2010, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=date(2010, 12, 31))
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def _load_bars(session: Session, *, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
    warmup_start = start_date - timedelta(days=450)
    rows = session.execute(
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
    ).all()
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


def _default_output_path(symbol: str, start_date: date, end_date: date) -> Path:
    suffix = f"{symbol.lower()}_trend_filters_{start_date.isoformat()}_{end_date.isoformat()}"
    return ROOT / "logs" / f"{suffix}.json"


def _valid_pair(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None


def _build_filter_conditions(closes: list[float]) -> dict[str, list[bool]]:
    sma20 = sma(closes, 20)
    sma50 = sma(closes, 50)
    sma200 = sma(closes, 200)
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200)
    conditions: dict[str, list[bool]] = {
        "close_above_sma20": [ma is not None and close > ma for close, ma in zip(closes, sma20, strict=False)],
        "close_above_sma50": [ma is not None and close > ma for close, ma in zip(closes, sma50, strict=False)],
        "sma20_above_sma50": [
            _valid_pair(fast, slow) and float(fast) > float(slow)
            for fast, slow in zip(sma20, sma50, strict=False)
        ],
        "close_above_sma20_and_sma20_above_sma50": [
            sma20_value is not None and sma50_value is not None and close > sma20_value and sma20_value > sma50_value
            for close, sma20_value, sma50_value in zip(closes, sma20, sma50, strict=False)
        ],
        "close_above_sma50_and_sma50_above_sma200": [
            sma50_value is not None and sma200_value is not None and close > sma50_value and sma50_value > sma200_value
            for close, sma50_value, sma200_value in zip(closes, sma50, sma200, strict=False)
        ],
        "close_above_ema20": [ma is not None and close > ma for close, ma in zip(closes, ema20, strict=False)],
        "close_above_ema50": [ma is not None and close > ma for close, ma in zip(closes, ema50, strict=False)],
        "ema20_above_ema50": [
            _valid_pair(fast, slow) and float(fast) > float(slow)
            for fast, slow in zip(ema20, ema50, strict=False)
        ],
        "close_above_ema20_and_ema20_above_ema50": [
            ema20_value is not None and ema50_value is not None and close > ema20_value and ema20_value > ema50_value
            for close, ema20_value, ema50_value in zip(closes, ema20, ema50, strict=False)
        ],
        "close_above_ema50_and_ema50_above_ema200": [
            ema50_value is not None and ema200_value is not None and close > ema50_value and ema50_value > ema200_value
            for close, ema50_value, ema200_value in zip(closes, ema50, ema200, strict=False)
        ],
    }
    return conditions


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    output_json = args.output_json or _default_output_path(args.symbol.upper(), args.start_date, args.end_date)
    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            bars = _load_bars(
                session,
                symbol=args.symbol.upper(),
                start_date=args.start_date,
                end_date=args.end_date,
            )
    finally:
        engine.dispose()

    closes = [bar.close_price for bar in bars]
    results = []
    for strategy_name, condition_series in _build_filter_conditions(closes).items():
        result = run_stock_condition_backtest(
            bars,
            symbol=args.symbol.upper(),
            strategy_name=strategy_name,
            condition_series=condition_series,
            start_date=args.start_date,
            end_date=args.end_date,
            starting_equity=args.starting_equity,
            risk_free_rate=args.risk_free_rate,
        )
        results.append(
            {
                "strategy_name": strategy_name,
                "summary": serialize_summary(result.summary),
                "trade_count": len(result.trades),
                "warnings": list(result.warnings),
            }
        )

    results.sort(
        key=lambda item: (
            item["summary"]["total_roi_pct"],
            item["summary"]["sharpe_ratio"] if item["summary"]["sharpe_ratio"] is not None else float("-inf"),
            -item["summary"]["max_drawdown_pct"],
        ),
        reverse=True,
    )
    payload = {
        "symbol": args.symbol.upper(),
        "period": {"start": args.start_date.isoformat(), "end": args.end_date.isoformat()},
        "strategy_count": len(results),
        "top_results": results[:10],
        "all_results": results,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload["top_results"], indent=2, sort_keys=True))
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
