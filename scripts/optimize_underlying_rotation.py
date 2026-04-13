from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.underlying_rotation import (  # noqa: E402
    UnderlyingRotationConfig,
    UnderlyingRotationOptimizationResult,
    UnderlyingRotationSearchSpace,
    UnderlyingUniverseFilter,
    load_rotation_dataset,
    optimize_underlying_rotation,
)


DEFAULT_LOOKBACK_TRIPLETS = "30:180:365,21:63:252,30:90:180,63:126:252"
DEFAULT_WEIGHT_TRIPLETS = "0.50:0.30:0.20,0.40:0.30:0.30,0.34:0.33:0.33,0.20:0.30:0.50,0.20:0.20:0.60"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_int_list(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _parse_float_list(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _parse_triplets(value: str, *, item_parser) -> tuple[tuple[Any, Any, Any], ...]:
    triplets = []
    for raw_triplet in value.split(","):
        raw_triplet = raw_triplet.strip()
        if not raw_triplet:
            continue
        parts = tuple(item_parser(part.strip()) for part in raw_triplet.split(":") if part.strip())
        if len(parts) != 3:
            raise ValueError(f"Triplet '{raw_triplet}' must contain exactly 3 values separated by ':'.")
        triplets.append(parts)
    return tuple(triplets)


def _parse_percent_list(value: str) -> tuple[float, ...]:
    return tuple(item / 100.0 for item in _parse_float_list(value))


def _parse_symbols(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    symbols = tuple(sorted({item.strip().upper() for item in value.split(",") if item.strip()}))
    return symbols or None


def _config_payload(config: UnderlyingRotationConfig) -> dict[str, object]:
    return {
        "portfolio_size": config.portfolio_size,
        "lookback_days": list(config.lookback_days),
        "lookback_weights": list(config.lookback_weights),
        "trailing_stop_pct": round(config.trailing_stop_pct * 100.0, 4),
        "rebalance_frequency_days": config.rebalance_frequency_days,
    }


def _result_payload(result: UnderlyingRotationOptimizationResult) -> dict[str, object]:
    top_rows = []
    for row in result.top_rows:
        top_rows.append(
            {
                "config": _config_payload(row.config),
                "train_summary": serialize_summary(row.train_result.summary),
                "train_warning_count": len(row.train_result.warnings),
                "validation_summary": serialize_summary(row.validation_result.summary) if row.validation_result is not None else None,
                "validation_warning_count": len(row.validation_result.warnings) if row.validation_result is not None else 0,
            }
        )
    return {
        "universe_size": result.universe_size,
        "candidate_count": result.candidate_count,
        "best_config": _config_payload(result.best_config),
        "best_train_summary": serialize_summary(result.best_train_result.summary),
        "best_validation_summary": serialize_summary(result.best_validation_result.summary) if result.best_validation_result is not None else None,
        "top_rows": top_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Optimize a stock-only underlying rotation strategy on historical DB data.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""), help="SQLAlchemy database URL. Defaults to DATABASE_URL.")
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0, help="Postgres statement_timeout in milliseconds. Use 0 to disable for this script.")
    parser.add_argument("--train-start", type=_parse_date, default=date(2015, 1, 1))
    parser.add_argument("--train-end", type=_parse_date, default=date(2015, 12, 31))
    parser.add_argument("--validation-start", type=_parse_date, default=date(2016, 1, 1))
    parser.add_argument("--validation-end", type=_parse_date, default=date(2016, 12, 31))
    parser.add_argument("--portfolio-sizes", default="20,30")
    parser.add_argument("--lookback-triplets", default=DEFAULT_LOOKBACK_TRIPLETS)
    parser.add_argument("--weight-triplets", default=DEFAULT_WEIGHT_TRIPLETS)
    parser.add_argument("--trailing-stop-pcts", default="0,10,15,20")
    parser.add_argument("--rebalance-frequencies", default="5,7,10,14,21")
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--objective", choices=("sharpe", "roi"), default="sharpe")
    parser.add_argument("--max-drawdown-pct-cap", type=float, default=None)
    parser.add_argument("--min-training-bars", type=int, default=126)
    parser.add_argument("--min-training-avg-dollar-volume", type=float, default=1_000_000.0)
    parser.add_argument("--min-training-close-price", type=float, default=5.0)
    parser.add_argument("--top-validation-count", type=int, default=20)
    parser.add_argument("--top-print-count", type=int, default=10)
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol allowlist.")
    parser.add_argument("--output-json", type=Path, default=None, help="Optional path to write the result payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    lookback_triplets = _parse_triplets(args.lookback_triplets, item_parser=int)
    weight_triplets = _parse_triplets(args.weight_triplets, item_parser=float)
    search_space = UnderlyingRotationSearchSpace(
        portfolio_sizes=_parse_int_list(args.portfolio_sizes),
        lookback_triplets=lookback_triplets,
        weight_triplets=weight_triplets,
        trailing_stop_pcts=_parse_percent_list(args.trailing_stop_pcts),
        rebalance_frequencies=_parse_int_list(args.rebalance_frequencies),
    )
    universe_filter = UnderlyingUniverseFilter(
        min_training_bars=args.min_training_bars,
        min_training_avg_dollar_volume=args.min_training_avg_dollar_volume,
        min_training_close_price=args.min_training_close_price,
    )
    allowed_symbols = _parse_symbols(args.symbols)
    max_lookback_days = max(lookback for triplet in lookback_triplets for lookback in triplet)

    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            dataset = load_rotation_dataset(
                session,
                train_start=args.train_start,
                train_end=args.train_end,
                end_date=args.validation_end,
                max_lookback_days=max_lookback_days,
                universe_filter=universe_filter,
                symbols=allowed_symbols,
            )
            result = optimize_underlying_rotation(
                dataset,
                search_space=search_space,
                train_start=args.train_start,
                train_end=args.train_end,
                validation_start=args.validation_start,
                validation_end=args.validation_end,
                starting_equity=args.starting_equity,
                risk_free_rate=args.risk_free_rate,
                top_validation_count=args.top_validation_count,
                objective=args.objective,
                max_drawdown_pct_cap=args.max_drawdown_pct_cap,
            )
    finally:
        engine.dispose()

    payload = {
        "train_period": {"start": args.train_start.isoformat(), "end": args.train_end.isoformat()},
        "validation_period": {"start": args.validation_start.isoformat(), "end": args.validation_end.isoformat()},
        "search_space": {
            "portfolio_sizes": list(search_space.portfolio_sizes),
            "lookback_triplets": [list(item) for item in search_space.lookback_triplets],
            "weight_triplets": [list(item) for item in search_space.weight_triplets],
            "trailing_stop_pcts": [round(item * 100.0, 4) for item in search_space.trailing_stop_pcts],
            "rebalance_frequencies": list(search_space.rebalance_frequencies),
        },
        "objective": args.objective,
        "max_drawdown_pct_cap": args.max_drawdown_pct_cap,
        "universe_filter": {
            "min_training_bars": universe_filter.min_training_bars,
            "min_training_avg_dollar_volume": universe_filter.min_training_avg_dollar_volume,
            "min_training_close_price": universe_filter.min_training_close_price,
            "allowed_symbols": list(allowed_symbols) if allowed_symbols is not None else None,
        },
        **_result_payload(result),
    }

    print(f"Universe size: {result.universe_size}")
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
