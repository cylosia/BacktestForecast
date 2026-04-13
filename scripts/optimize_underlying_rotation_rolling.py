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
from backtestforecast.underlying_rotation import (  # noqa: E402
    UnderlyingRotationConfig,
    UnderlyingRotationRollingConfigStats,
    UnderlyingRotationSearchSpace,
    UnderlyingUniverseFilter,
    aggregate_rolling_walk_forward_results,
    build_rolling_split_result,
    build_trailing_annual_walk_forward_splits,
    load_rotation_dataset,
    optimize_underlying_rotation,
    recommend_rolling_challenger,
)


DEFAULT_LOOKBACK_TRIPLETS = "25:75:180,30:90:150,30:90:180,30:90:210,30:120:180"
DEFAULT_WEIGHT_TRIPLETS = "0.35:0.35:0.30,0.40:0.30:0.30,0.45:0.30:0.25,0.50:0.30:0.20,0.45:0.25:0.30,0.40:0.35:0.25"


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


def _rolling_stats_payload(stats: UnderlyingRotationRollingConfigStats) -> dict[str, object]:
    return {
        "config": _config_payload(stats.config),
        "median_validation_roi_pct": stats.median_validation_roi_pct,
        "average_validation_roi_pct": stats.average_validation_roi_pct,
        "median_validation_max_drawdown_pct": stats.median_validation_max_drawdown_pct,
        "average_validation_sharpe": stats.average_validation_sharpe,
        "positive_validation_split_count": stats.positive_validation_split_count,
        "within_drawdown_cap_split_count": stats.within_drawdown_cap_split_count,
        "split_count": stats.split_count,
        "split_metrics": list(stats.split_metrics),
    }


def _load_incumbent_config(path: Path | None) -> UnderlyingRotationConfig | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    config_payload = payload.get("best_config", payload)
    return UnderlyingRotationConfig(
        portfolio_size=int(config_payload["portfolio_size"]),
        lookback_days=tuple(int(item) for item in config_payload["lookback_days"]),
        lookback_weights=tuple(float(item) for item in config_payload["lookback_weights"]),
        trailing_stop_pct=float(config_payload["trailing_stop_pct"]) / 100.0,
        rebalance_frequency_days=int(config_payload["rebalance_frequency_days"]),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run annual rolling walk-forward optimization for the underlying rotation strategy.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""), help="SQLAlchemy database URL. Defaults to DATABASE_URL.")
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0, help="Postgres statement_timeout in milliseconds. Use 0 to disable for this script.")
    parser.add_argument("--validation-start-year", type=int, default=2016)
    parser.add_argument("--validation-end-year", type=int, default=2025)
    parser.add_argument("--train-years", type=int, default=5)
    parser.add_argument("--validation-years", type=int, default=1)
    parser.add_argument("--step-years", type=int, default=1)
    parser.add_argument("--portfolio-sizes", default="8,10,12,15,20,25")
    parser.add_argument("--lookback-triplets", default=DEFAULT_LOOKBACK_TRIPLETS)
    parser.add_argument("--weight-triplets", default=DEFAULT_WEIGHT_TRIPLETS)
    parser.add_argument("--trailing-stop-pcts", default="8,9,10,11,12")
    parser.add_argument("--rebalance-frequencies", default="17,21,25")
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--objective", choices=("sharpe", "roi"), default="roi")
    parser.add_argument("--max-drawdown-pct-cap", type=float, default=15.0)
    parser.add_argument("--min-training-bars", type=int, default=126)
    parser.add_argument("--min-training-avg-dollar-volume", type=float, default=1_000_000.0)
    parser.add_argument("--min-training-close-price", type=float, default=5.0)
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol allowlist.")
    parser.add_argument("--incumbent-config-path", type=Path, default=None, help="Optional JSON file containing either a direct config payload or a prior result payload with best_config.")
    parser.add_argument("--min-median-validation-roi-improvement-pct", type=float, default=1.0)
    parser.add_argument("--top-print-count", type=int, default=10)
    parser.add_argument("--output-json", type=Path, default=None, help="Optional path to write the rolling optimization payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")

    search_space = UnderlyingRotationSearchSpace(
        portfolio_sizes=_parse_int_list(args.portfolio_sizes),
        lookback_triplets=_parse_triplets(args.lookback_triplets, item_parser=int),
        weight_triplets=_parse_triplets(args.weight_triplets, item_parser=float),
        trailing_stop_pcts=_parse_percent_list(args.trailing_stop_pcts),
        rebalance_frequencies=_parse_int_list(args.rebalance_frequencies),
    )
    universe_filter = UnderlyingUniverseFilter(
        min_training_bars=args.min_training_bars,
        min_training_avg_dollar_volume=args.min_training_avg_dollar_volume,
        min_training_close_price=args.min_training_close_price,
    )
    allowed_symbols = _parse_symbols(args.symbols)
    max_lookback_days = max(lookback for triplet in search_space.lookback_triplets for lookback in triplet)
    candidate_count = len(search_space.iter_configs())
    splits = build_trailing_annual_walk_forward_splits(
        validation_start_year=args.validation_start_year,
        validation_end_year=args.validation_end_year,
        train_years=args.train_years,
        validation_years=args.validation_years,
        step_years=args.step_years,
    )
    incumbent_config = _load_incumbent_config(args.incumbent_config_path)

    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    split_results = []
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        for split in splits:
            with factory() as session:
                dataset = load_rotation_dataset(
                    session,
                    train_start=split.train_start,
                    train_end=split.train_end,
                    end_date=split.validation_end,
                    max_lookback_days=max_lookback_days,
                    universe_filter=universe_filter,
                    symbols=allowed_symbols,
                )
                result = optimize_underlying_rotation(
                    dataset,
                    search_space=search_space,
                    train_start=split.train_start,
                    train_end=split.train_end,
                    validation_start=split.validation_start,
                    validation_end=split.validation_end,
                    starting_equity=args.starting_equity,
                    risk_free_rate=args.risk_free_rate,
                    top_validation_count=candidate_count,
                    objective=args.objective,
                    max_drawdown_pct_cap=args.max_drawdown_pct_cap,
                )
            split_result = build_rolling_split_result(split, result)
            split_results.append(split_result)
            best_validation = split_result.best_row.validation_result.summary if split_result.best_row.validation_result is not None else None
            print(
                f"Completed {split.train_start.year}-{split.train_end.year} -> "
                f"{split.validation_start.year}-{split.validation_end.year}; "
                f"universe={split_result.universe_size}; "
                f"winner_validation_roi={best_validation.total_roi_pct if best_validation is not None else None}"
            )
    finally:
        engine.dispose()

    aggregated = aggregate_rolling_walk_forward_results(tuple(split_results), max_drawdown_pct_cap=args.max_drawdown_pct_cap)
    decision = recommend_rolling_challenger(
        aggregated,
        incumbent_config=incumbent_config,
        min_median_validation_roi_improvement_pct=args.min_median_validation_roi_improvement_pct,
    )

    payload = {
        "search_space": {
            "portfolio_sizes": list(search_space.portfolio_sizes),
            "lookback_triplets": [list(item) for item in search_space.lookback_triplets],
            "weight_triplets": [list(item) for item in search_space.weight_triplets],
            "trailing_stop_pcts": [round(item * 100.0, 4) for item in search_space.trailing_stop_pcts],
            "rebalance_frequencies": list(search_space.rebalance_frequencies),
        },
        "rolling_schedule": {
            "validation_start_year": args.validation_start_year,
            "validation_end_year": args.validation_end_year,
            "train_years": args.train_years,
            "validation_years": args.validation_years,
            "step_years": args.step_years,
        },
        "objective": args.objective,
        "max_drawdown_pct_cap": args.max_drawdown_pct_cap,
        "min_median_validation_roi_improvement_pct": args.min_median_validation_roi_improvement_pct,
        "universe_filter": {
            "min_training_bars": universe_filter.min_training_bars,
            "min_training_avg_dollar_volume": universe_filter.min_training_avg_dollar_volume,
            "min_training_close_price": universe_filter.min_training_close_price,
            "allowed_symbols": list(allowed_symbols) if allowed_symbols is not None else None,
        },
        "split_winners": [
            {
                "split": {
                    "train_start": item.split.train_start.isoformat(),
                    "train_end": item.split.train_end.isoformat(),
                    "validation_start": item.split.validation_start.isoformat(),
                    "validation_end": item.split.validation_end.isoformat(),
                },
                "universe_size": item.universe_size,
                "best_config": _config_payload(item.best_row.config),
                "best_validation_roi_pct": item.best_row.validation_result.summary.total_roi_pct if item.best_row.validation_result is not None else None,
                "best_validation_max_drawdown_pct": item.best_row.validation_result.summary.max_drawdown_pct if item.best_row.validation_result is not None else None,
                "best_validation_sharpe": item.best_row.validation_result.summary.sharpe_ratio if item.best_row.validation_result is not None else None,
            }
            for item in split_results
        ],
        "top_aggregated": [_rolling_stats_payload(item) for item in aggregated[: max(1, args.top_print_count)]],
        "decision": {
            "action": decision.action,
            "reason": decision.reason,
            "median_validation_roi_improvement_pct": decision.median_validation_roi_improvement_pct,
            "challenger": _rolling_stats_payload(decision.challenger),
            "incumbent": _rolling_stats_payload(decision.incumbent) if decision.incumbent is not None else None,
        },
    }

    print(f"Split count: {len(split_results)}")
    print(f"Top challenger: {json.dumps(_config_payload(decision.challenger.config), sort_keys=True)}")
    print(
        "Top challenger rolling stats: "
        f"{json.dumps(_rolling_stats_payload(decision.challenger), sort_keys=True)}"
    )
    print(
        "Decision: "
        f"{json.dumps({'action': decision.action, 'reason': decision.reason, 'median_validation_roi_improvement_pct': decision.median_validation_roi_improvement_pct}, sort_keys=True)}"
    )

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Wrote {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
