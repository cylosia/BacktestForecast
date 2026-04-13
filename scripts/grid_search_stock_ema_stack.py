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
from backtestforecast.indicators.calculations import adx, ema  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.stock_trend import run_stock_condition_backtest  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_int_list(value: str) -> tuple[int, ...]:
    values = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not values:
        raise ValueError("Expected at least one integer value.")
    return values


def _parse_percent_list(value: str) -> tuple[float, ...]:
    values = tuple(sorted({float(item.strip()) / 100.0 for item in value.split(",") if item.strip()}))
    if not values:
        raise ValueError("Expected at least one percentage value.")
    return values


def _parse_str_list(value: str) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))
    if not values:
        raise ValueError("Expected at least one string value.")
    return values


def _build_entry_gate_series(
    *,
    gate_name: str,
    condition_series: list[bool],
    fast_series: list[float | None],
    adx14_series: list[float | None],
) -> list[bool] | None:
    if gate_name == "baseline":
        return None
    if gate_name == "fast_slope_positive":
        return [
            fast_value is not None and index >= 5 and fast_series[index - 5] is not None and fast_value > fast_series[index - 5]
            for index, fast_value in enumerate(fast_series)
        ]
    if gate_name == "condition_streak_2":
        return [condition_series[index] and index >= 1 and condition_series[index - 1] for index in range(len(condition_series))]
    if gate_name == "condition_streak_3":
        return [
            condition_series[index] and index >= 2 and condition_series[index - 1] and condition_series[index - 2]
            for index in range(len(condition_series))
        ]
    if gate_name == "adx20":
        return [value is not None and value >= 20.0 for value in adx14_series]
    if gate_name == "adx25":
        return [value is not None and value >= 25.0 for value in adx14_series]
    if gate_name == "fast_slope_positive_and_adx20":
        fast_slope_positive = _build_entry_gate_series(
            gate_name="fast_slope_positive",
            condition_series=condition_series,
            fast_series=fast_series,
            adx14_series=adx14_series,
        )
        adx20 = _build_entry_gate_series(
            gate_name="adx20",
            condition_series=condition_series,
            fast_series=fast_series,
            adx14_series=adx14_series,
        )
        return [fast_slope_positive[index] and adx20[index] for index in range(len(condition_series))]
    if gate_name == "fast_slope_positive_and_adx25":
        fast_slope_positive = _build_entry_gate_series(
            gate_name="fast_slope_positive",
            condition_series=condition_series,
            fast_series=fast_series,
            adx14_series=adx14_series,
        )
        adx25 = _build_entry_gate_series(
            gate_name="adx25",
            condition_series=condition_series,
            fast_series=fast_series,
            adx14_series=adx14_series,
        )
        return [fast_slope_positive[index] and adx25[index] for index in range(len(condition_series))]
    raise ValueError(f"Unsupported entry gate: {gate_name}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Grid-search close > EMA_fast and EMA_fast > EMA_slow stock trend filters.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0)
    parser.add_argument("--symbol", default="FAS")
    parser.add_argument("--start-date", type=_parse_date, default=date(2010, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=date(2010, 12, 31))
    parser.add_argument("--fast-emas", default="5,7,10,14,20,21,30,40,50,63")
    parser.add_argument("--slow-emas", default="50,63,100,150,200,250")
    parser.add_argument("--trailing-stop-pcts", default="0,5,8,10,12,15")
    parser.add_argument(
        "--entry-gates",
        default="baseline,fast_slope_positive,condition_streak_2,fast_slope_positive_and_adx25",
    )
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--top-print-count", type=int, default=10)
    return parser


def _load_bars(session: Session, *, symbol: str, start_date: date, end_date: date, max_slow_ema: int) -> list[DailyBar]:
    warmup_start = start_date - timedelta(days=max(500, max_slow_ema * 4))
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


def _default_output_path(symbol: str, start_date: date, end_date: date, *, include_entry_gates: bool) -> Path:
    suffix = f"{symbol.lower()}_ema_stack_grid_{start_date.isoformat()}_{end_date.isoformat()}"
    if include_entry_gates:
        suffix = f"{suffix}_entry_gates"
    return ROOT / "logs" / f"{suffix}.json"


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    fast_emas = _parse_int_list(args.fast_emas)
    slow_emas = _parse_int_list(args.slow_emas)
    trailing_stop_pcts = _parse_percent_list(args.trailing_stop_pcts)
    entry_gates = _parse_str_list(args.entry_gates)
    valid_pairs = tuple((fast, slow) for fast in fast_emas for slow in slow_emas if fast < slow)
    if not valid_pairs:
        raise SystemExit("No valid EMA pairs. Ensure at least one fast EMA is shorter than one slow EMA.")

    output_json = args.output_json or _default_output_path(
        args.symbol.upper(),
        args.start_date,
        args.end_date,
        include_entry_gates=entry_gates != ("baseline",),
    )
    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            bars = _load_bars(
                session,
                symbol=args.symbol.upper(),
                start_date=args.start_date,
                end_date=args.end_date,
                max_slow_ema=max(slow_emas),
            )
    finally:
        engine.dispose()

    closes = [bar.close_price for bar in bars]
    highs = [bar.high_price for bar in bars]
    lows = [bar.low_price for bar in bars]
    ema_cache = {period: ema(closes, period) for period in set(fast_emas).union(slow_emas)}
    adx14_series = adx(highs, lows, closes, 14)

    rows: list[dict[str, object]] = []
    for fast_period, slow_period in valid_pairs:
        fast_series = ema_cache[fast_period]
        slow_series = ema_cache[slow_period]
        condition_series = [
            fast_value is not None and slow_value is not None and close > fast_value and fast_value > slow_value
            for close, fast_value, slow_value in zip(closes, fast_series, slow_series, strict=False)
        ]
        entry_gate_map = {
            gate_name: _build_entry_gate_series(
                gate_name=gate_name,
                condition_series=condition_series,
                fast_series=fast_series,
                adx14_series=adx14_series,
            )
            for gate_name in entry_gates
        }
        for gate_name, entry_gate_series in entry_gate_map.items():
            for trailing_stop_pct in trailing_stop_pcts:
                stop_pct_label = int(round(trailing_stop_pct * 100.0))
                strategy_name = (
                    f"close_above_ema{fast_period}_and_ema{fast_period}_above_ema{slow_period}"
                    f"_entry_gate_{gate_name}_stop_{stop_pct_label}"
                )
                result = run_stock_condition_backtest(
                    bars,
                    symbol=args.symbol.upper(),
                    strategy_name=strategy_name,
                    condition_series=condition_series,
                    entry_gate_series=entry_gate_series,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    starting_equity=args.starting_equity,
                    risk_free_rate=args.risk_free_rate,
                    trailing_stop_pct=trailing_stop_pct,
                )
                total_trade_calendar_days = sum(item.holding_period_days for item in result.trades)
                total_trade_trading_days = sum(item.holding_period_trading_days or 0 for item in result.trades)
                roi_pct_per_calendar_day = (
                    result.summary.total_roi_pct / total_trade_calendar_days if total_trade_calendar_days > 0 else None
                )
                roi_pct_per_trading_day = (
                    result.summary.total_roi_pct / total_trade_trading_days if total_trade_trading_days > 0 else None
                )
                rows.append(
                    {
                        "fast_ema": fast_period,
                        "slow_ema": slow_period,
                        "entry_gate": gate_name,
                        "trailing_stop_pct": round(trailing_stop_pct * 100.0, 4),
                        "strategy_name": strategy_name,
                        "summary": serialize_summary(result.summary),
                        "trade_count": len(result.trades),
                        "total_trade_calendar_days": total_trade_calendar_days,
                        "total_trade_trading_days": total_trade_trading_days,
                        "roi_pct_per_calendar_day": round(roi_pct_per_calendar_day, 6)
                        if roi_pct_per_calendar_day is not None
                        else None,
                        "roi_pct_per_trading_day": round(roi_pct_per_trading_day, 6)
                        if roi_pct_per_trading_day is not None
                        else None,
                        "warnings": list(result.warnings),
                    }
                )

    rows.sort(
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
        "fast_emas": list(fast_emas),
        "slow_emas": list(slow_emas),
        "entry_gates": list(entry_gates),
        "trailing_stop_pcts": [round(item * 100.0, 4) for item in trailing_stop_pcts],
        "candidate_count": len(rows),
        "best_result": rows[0],
        "top_results": rows[: max(1, args.top_print_count)],
        "all_results": rows,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload["top_results"], indent=2, sort_keys=True))
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
