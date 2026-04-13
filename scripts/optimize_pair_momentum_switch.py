from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.pair_momentum_switch import (  # noqa: E402
    PairMomentumSwitchConfig,
    PairMomentumSwitchOptimizationResult,
    optimize_pair_momentum_switch,
)
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.underlying_rotation import UnderlyingUniverseFilter, load_rotation_dataset  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str) -> tuple[str, str]:
    symbols = tuple(item.strip().upper() for item in value.split(",") if item.strip())
    if len(symbols) != 2:
        raise ValueError("--symbols must contain exactly 2 comma-separated symbols.")
    if len(set(symbols)) != 2:
        raise ValueError("--symbols entries must be unique.")
    return symbols


def _parse_lookback_days(value: str) -> tuple[int, ...]:
    raw = value.strip()
    if not raw:
        raise ValueError("--lookback-days must not be empty.")
    if "-" in raw and "," not in raw:
        start_text, end_text = raw.split("-", maxsplit=1)
        start = int(start_text.strip())
        end = int(end_text.strip())
        if start > end:
            raise ValueError("--lookback-days range must be ascending.")
        return tuple(range(start, end + 1))
    values = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not values:
        raise ValueError("--lookback-days must contain at least one integer.")
    return values


def _parse_int_list(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _parse_percent_list(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) / 100.0 for item in value.split(",") if item.strip())


def _parse_bool_list(value: str) -> tuple[bool, ...]:
    normalized: list[bool] = []
    for item in value.split(","):
        raw = item.strip().lower()
        if not raw:
            continue
        if raw in {"1", "true", "t", "yes", "y"}:
            normalized.append(True)
            continue
        if raw in {"0", "false", "f", "no", "n"}:
            normalized.append(False)
            continue
        raise ValueError(f"Unsupported boolean value: {item}")
    if not normalized:
        raise ValueError("Boolean list must contain at least one value.")
    return tuple(normalized)


def _config_payload(config: PairMomentumSwitchConfig) -> dict[str, object]:
    return {
        "symbols": list(config.symbols),
        "lookback_days": config.lookback_days,
        "rebalance_frequency_days": config.rebalance_frequency_days,
        "trailing_stop_pct": round(config.trailing_stop_pct * 100.0, 4),
        "require_positive_momentum": config.require_positive_momentum,
        "position_direction": config.position_direction,
        "invert_ranking": config.invert_ranking,
        "use_raw_execution_prices": config.use_raw_execution_prices,
    }


def _result_payload(result: PairMomentumSwitchOptimizationResult) -> dict[str, object]:
    top_rows = []
    for row in result.top_rows:
        top_rows.append(
            {
                "config": _config_payload(row.config),
                "train_summary": serialize_summary(row.train_result.summary),
                "validation_summary": serialize_summary(row.validation_result.summary) if row.validation_result is not None else None,
            }
        )
    return {
        "candidate_count": result.candidate_count,
        "best_config": _config_payload(result.best_config),
        "best_train_summary": serialize_summary(result.best_train_result.summary),
        "best_validation_summary": serialize_summary(result.best_validation_result.summary) if result.best_validation_result is not None else None,
        "top_rows": top_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Optimize a momentum-switch strategy over a fixed pair of underlying symbols.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""), help="SQLAlchemy database URL. Defaults to DATABASE_URL.")
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0, help="Postgres statement_timeout in milliseconds. Use 0 to disable for this script.")
    parser.add_argument("--symbols", default="FAS,FAZ")
    parser.add_argument("--position-direction", choices=("long", "short"), default="long")
    parser.add_argument("--invert-ranking", action="store_true")
    parser.add_argument("--use-raw-execution-prices", action="store_true")
    parser.add_argument("--train-start", type=_parse_date, default=date(2015, 1, 1))
    parser.add_argument("--train-end", type=_parse_date, default=date(2015, 12, 31))
    parser.add_argument("--validation-start", type=_parse_date, default=date(2016, 1, 1))
    parser.add_argument("--validation-end", type=_parse_date, default=date(2016, 12, 31))
    parser.add_argument("--lookback-days", default="1-252", help="Either a comma-separated list like 5,10,21 or an inclusive range like 1-252.")
    parser.add_argument("--rebalance-frequencies", default="1", help="Comma-separated trading-day rebalance frequencies, e.g. 1,5,10,21.")
    parser.add_argument("--trailing-stop-pcts", default="0", help="Comma-separated trailing stop percentages, e.g. 0,10,15.")
    parser.add_argument("--cash-filter-values", default="false,true", help="Comma-separated booleans for whether to hold cash when neither leg has positive momentum.")
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--objective", choices=("roi", "sharpe"), default="roi")
    parser.add_argument("--top-validation-count", type=int, default=20)
    parser.add_argument("--top-print-count", type=int, default=10)
    parser.add_argument("--output-json", type=Path, default=None, help="Optional path to write the result payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    symbols = _parse_symbols(args.symbols)
    lookback_days = _parse_lookback_days(args.lookback_days)
    rebalance_frequencies = _parse_int_list(args.rebalance_frequencies)
    trailing_stop_pcts = _parse_percent_list(args.trailing_stop_pcts)
    cash_filter_values = _parse_bool_list(args.cash_filter_values)
    max_lookback = max(lookback_days)
    loader_warmup_days = max(31, max_lookback * 2)

    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            dataset = load_rotation_dataset(
                session,
                train_start=args.train_start,
                train_end=args.train_end,
                end_date=args.validation_end,
                max_lookback_days=loader_warmup_days,
                universe_filter=UnderlyingUniverseFilter(
                    min_training_bars=1,
                    min_training_avg_dollar_volume=0.0,
                    min_training_close_price=0.0,
                ),
                symbols=symbols,
                include_raw_histories=args.use_raw_execution_prices,
            )
            result = optimize_pair_momentum_switch(
                dataset,
                symbols=symbols,
                lookback_days=lookback_days,
                rebalance_frequency_days=rebalance_frequencies,
                position_direction=args.position_direction,
                invert_ranking=args.invert_ranking,
                use_raw_execution_prices=args.use_raw_execution_prices,
                train_start=args.train_start,
                train_end=args.train_end,
                validation_start=args.validation_start,
                validation_end=args.validation_end,
                starting_equity=args.starting_equity,
                risk_free_rate=args.risk_free_rate,
                top_validation_count=args.top_validation_count,
                objective=args.objective,
                trailing_stop_pcts=trailing_stop_pcts,
                require_positive_momentum_values=cash_filter_values,
            )
    finally:
        engine.dispose()

    payload = {
        "symbols": list(symbols),
        "train_period": {"start": args.train_start.isoformat(), "end": args.train_end.isoformat()},
        "validation_period": {"start": args.validation_start.isoformat(), "end": args.validation_end.isoformat()},
        "lookback_days": list(lookback_days),
        "rebalance_frequencies": list(rebalance_frequencies),
        "trailing_stop_pcts": [round(item * 100.0, 4) for item in trailing_stop_pcts],
        "cash_filter_values": list(cash_filter_values),
        "objective": args.objective,
        **_result_payload(result),
    }

    print(f"Candidate count: {result.candidate_count}")
    print(f"Best config: {json.dumps(_config_payload(result.best_config), sort_keys=True)}")
    print(f"Best train summary: {json.dumps(serialize_summary(result.best_train_result.summary), sort_keys=True)}")
    if result.best_validation_result is not None:
        print(
            "Best validation summary: "
            f"{json.dumps(serialize_summary(result.best_validation_result.summary), sort_keys=True)}"
        )
    print("Top rows:")
    for index, row in enumerate(result.top_rows[: max(1, args.top_print_count)], start=1):
        line_payload = {
            "rank": index,
            "config": _config_payload(row.config),
            "train_sharpe": row.train_result.summary.sharpe_ratio,
            "train_roi_pct": row.train_result.summary.total_roi_pct,
            "train_max_drawdown_pct": row.train_result.summary.max_drawdown_pct,
            "validation_sharpe": row.validation_result.summary.sharpe_ratio if row.validation_result is not None else None,
            "validation_roi_pct": row.validation_result.summary.total_roi_pct if row.validation_result is not None else None,
            "validation_max_drawdown_pct": row.validation_result.summary.max_drawdown_pct if row.validation_result is not None else None,
        }
        print(json.dumps(line_payload, sort_keys=True))

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Wrote {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
