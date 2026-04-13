from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.pair_momentum_switch import (  # noqa: E402
    PairMomentumSwitchConfig,
    PairMomentumSwitchBacktestResult,
    optimize_pair_momentum_switch,
    run_pair_momentum_switch_backtest,
)
from backtestforecast.pair_signal_plans import (  # noqa: E402
    build_pair_return_spread_signal_plan,
    build_xlf_regime_signal_plan,
)
from backtestforecast.services.serialization import serialize_summary  # noqa: E402
from backtestforecast.underlying_rotation import UnderlyingUniverseFilter, load_rotation_dataset  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_range_or_list(value: str) -> tuple[int, ...]:
    raw = value.strip()
    if not raw:
        raise ValueError("value must not be empty")
    if "-" in raw and "," not in raw:
        start_text, end_text = raw.split("-", maxsplit=1)
        start = int(start_text.strip())
        end = int(end_text.strip())
        if start > end:
            raise ValueError("range must be ascending")
        return tuple(range(start, end + 1))
    return tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))


def _parse_int_list(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _parse_percent_list(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) / 100.0 for item in value.split(",") if item.strip())


def _parse_bool_list(value: str) -> tuple[bool, ...]:
    result: list[bool] = []
    for item in value.split(","):
        raw = item.strip().lower()
        if not raw:
            continue
        if raw in {"1", "true", "t", "yes", "y"}:
            result.append(True)
        elif raw in {"0", "false", "f", "no", "n"}:
            result.append(False)
        else:
            raise ValueError(f"Unsupported boolean value: {item}")
    return tuple(result)


def _summary_sort_key(summary, *, objective: str) -> tuple[float, float, float]:
    sharpe = summary.sharpe_ratio if summary.sharpe_ratio is not None else float("-inf")
    if objective == "sharpe":
        return (sharpe, summary.total_roi_pct, -summary.max_drawdown_pct)
    if objective == "roi":
        return (summary.total_roi_pct, sharpe, -summary.max_drawdown_pct)
    raise ValueError(f"Unsupported objective: {objective}")


@dataclass(frozen=True, slots=True)
class _SelectorRow:
    selector: str
    params: dict[str, object]
    result: PairMomentumSwitchBacktestResult


def _run_custom_selector_grid(
    dataset,
    *,
    selector_name: str,
    lookback_days: tuple[int, ...],
    rebalance_frequencies: tuple[int, ...],
    trailing_stop_pcts: tuple[float, ...],
    threshold_pcts: tuple[float, ...],
    start_date: date,
    end_date: date,
    starting_equity: float,
    risk_free_rate: float,
    objective: str,
    signal_symbol: str,
    use_raw_execution_prices: bool,
) -> tuple[_SelectorRow, tuple[_SelectorRow, ...]]:
    rows: list[_SelectorRow] = []
    for lookback_day in lookback_days:
        for rebalance_frequency in rebalance_frequencies:
            for trailing_stop_pct in trailing_stop_pcts:
                for threshold_pct in threshold_pcts:
                    if selector_name == "xlf_regime":
                        signal_plan = build_xlf_regime_signal_plan(
                            dataset,
                            pair_symbols=("FAS", "FAZ"),
                            signal_symbol=signal_symbol,
                            start_date=start_date,
                            end_date=end_date,
                            lookback_days=lookback_day,
                            rebalance_frequency_days=rebalance_frequency,
                            neutral_threshold_pct=threshold_pct,
                            positive_signal_short_symbol="FAZ",
                            negative_signal_short_symbol="FAS",
                        )
                    elif selector_name == "spread_threshold":
                        signal_plan = build_pair_return_spread_signal_plan(
                            dataset,
                            pair_symbols=("FAS", "FAZ"),
                            start_date=start_date,
                            end_date=end_date,
                            lookback_days=lookback_day,
                            rebalance_frequency_days=rebalance_frequency,
                            neutral_threshold_pct=threshold_pct,
                            positive_spread_short_symbol="FAZ",
                            negative_spread_short_symbol="FAS",
                        )
                    else:
                        raise ValueError(f"Unsupported selector: {selector_name}")
                    config = PairMomentumSwitchConfig(
                        symbols=("FAS", "FAZ"),
                        lookback_days=lookback_day,
                        rebalance_frequency_days=rebalance_frequency,
                        trailing_stop_pct=trailing_stop_pct,
                        require_positive_momentum=False,
                        position_direction="short",
                        invert_ranking=True,
                        use_raw_execution_prices=use_raw_execution_prices,
                    )
                    result = run_pair_momentum_switch_backtest(
                        dataset,
                        config=config,
                        start_date=start_date,
                        end_date=end_date,
                        starting_equity=starting_equity,
                        risk_free_rate=risk_free_rate,
                        signal_plan=signal_plan,
                    )
                    rows.append(
                        _SelectorRow(
                            selector=selector_name,
                            params={
                                "lookback_days": lookback_day,
                                "rebalance_frequency_days": rebalance_frequency,
                                "trailing_stop_pct": round(trailing_stop_pct * 100.0, 4),
                                "threshold_pct": round(threshold_pct * 100.0, 4),
                            },
                            result=result,
                        )
                    )
    ranked = tuple(sorted(rows, key=lambda row: _summary_sort_key(row.result.summary, objective=objective), reverse=True))
    return ranked[0], ranked[:10]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare pair short signal metrics over FAS/FAZ.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=0)
    parser.add_argument("--train-start", type=_parse_date, default=date(2010, 1, 1))
    parser.add_argument("--train-end", type=_parse_date, default=date(2025, 12, 31))
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--risk-free-rate", type=float, default=0.0)
    parser.add_argument("--objective", choices=("roi", "sharpe"), default="roi")
    parser.add_argument("--signal-symbol", default="XLF")
    parser.add_argument("--use-raw-execution-prices", action="store_true")

    parser.add_argument("--baseline-lookbacks", default="1-100")
    parser.add_argument("--baseline-rebalances", default="1,5,10,21")
    parser.add_argument("--baseline-stops", default="0,10,15,20")
    parser.add_argument("--baseline-cash-filters", default="false,true")

    parser.add_argument("--xlf-lookbacks", default="1-100")
    parser.add_argument("--xlf-rebalances", default="1,10")
    parser.add_argument("--xlf-stops", default="0,10")
    parser.add_argument("--xlf-thresholds", default="0,2,5,10")

    parser.add_argument("--spread-lookbacks", default="1-100")
    parser.add_argument("--spread-rebalances", default="1,10")
    parser.add_argument("--spread-stops", default="0,10")
    parser.add_argument("--spread-thresholds", default="0,5,10,15,20")

    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required.")

    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            max_lookback = max(
                max(_parse_range_or_list(args.baseline_lookbacks)),
                max(_parse_range_or_list(args.xlf_lookbacks)),
                max(_parse_range_or_list(args.spread_lookbacks)),
            )
            dataset = load_rotation_dataset(
                session,
                train_start=args.train_start,
                train_end=args.train_end,
                end_date=args.train_end,
                max_lookback_days=max(31, max_lookback * 2),
                universe_filter=UnderlyingUniverseFilter(
                    min_training_bars=1,
                    min_training_avg_dollar_volume=0.0,
                    min_training_close_price=0.0,
                ),
                symbols=("FAS", "FAZ", args.signal_symbol.upper()),
                include_raw_histories=args.use_raw_execution_prices,
            )
    finally:
        engine.dispose()

    baseline_result = optimize_pair_momentum_switch(
        dataset,
        symbols=("FAS", "FAZ"),
        lookback_days=_parse_range_or_list(args.baseline_lookbacks),
        rebalance_frequency_days=_parse_int_list(args.baseline_rebalances),
        trailing_stop_pcts=_parse_percent_list(args.baseline_stops),
        require_positive_momentum_values=_parse_bool_list(args.baseline_cash_filters),
        position_direction="short",
        invert_ranking=True,
        use_raw_execution_prices=args.use_raw_execution_prices,
        train_start=args.train_start,
        train_end=args.train_end,
        validation_start=None,
        validation_end=None,
        starting_equity=args.starting_equity,
        risk_free_rate=args.risk_free_rate,
        top_validation_count=20,
        objective=args.objective,
    )

    xlf_best, xlf_top = _run_custom_selector_grid(
        dataset,
        selector_name="xlf_regime",
        lookback_days=_parse_range_or_list(args.xlf_lookbacks),
        rebalance_frequencies=_parse_int_list(args.xlf_rebalances),
        trailing_stop_pcts=_parse_percent_list(args.xlf_stops),
        threshold_pcts=_parse_percent_list(args.xlf_thresholds),
        start_date=args.train_start,
        end_date=args.train_end,
        starting_equity=args.starting_equity,
        risk_free_rate=args.risk_free_rate,
        objective=args.objective,
        signal_symbol=args.signal_symbol.upper(),
        use_raw_execution_prices=args.use_raw_execution_prices,
    )

    spread_best, spread_top = _run_custom_selector_grid(
        dataset,
        selector_name="spread_threshold",
        lookback_days=_parse_range_or_list(args.spread_lookbacks),
        rebalance_frequencies=_parse_int_list(args.spread_rebalances),
        trailing_stop_pcts=_parse_percent_list(args.spread_stops),
        threshold_pcts=_parse_percent_list(args.spread_thresholds),
        start_date=args.train_start,
        end_date=args.train_end,
        starting_equity=args.starting_equity,
        risk_free_rate=args.risk_free_rate,
        objective=args.objective,
        signal_symbol=args.signal_symbol.upper(),
        use_raw_execution_prices=args.use_raw_execution_prices,
    )

    payload = {
        "train_period": {"start": args.train_start.isoformat(), "end": args.train_end.isoformat()},
        "objective": args.objective,
        "signal_symbol": args.signal_symbol.upper(),
        "use_raw_execution_prices": args.use_raw_execution_prices,
        "baseline_pair_return_ranking": {
            "candidate_count": baseline_result.candidate_count,
            "best_config": {
                "lookback_days": baseline_result.best_config.lookback_days,
                "rebalance_frequency_days": baseline_result.best_config.rebalance_frequency_days,
                "trailing_stop_pct": round(baseline_result.best_config.trailing_stop_pct * 100.0, 4),
                "require_positive_momentum": baseline_result.best_config.require_positive_momentum,
            },
            "best_summary": serialize_summary(baseline_result.best_train_result.summary),
        },
        "xlf_regime": {
            "candidate_count": len(_parse_range_or_list(args.xlf_lookbacks))
            * len(_parse_int_list(args.xlf_rebalances))
            * len(_parse_percent_list(args.xlf_stops))
            * len(_parse_percent_list(args.xlf_thresholds)),
            "best_params": xlf_best.params,
            "best_summary": serialize_summary(xlf_best.result.summary),
            "top_rows": [
                {"params": row.params, "summary": serialize_summary(row.result.summary)}
                for row in xlf_top
            ],
        },
        "spread_threshold": {
            "candidate_count": len(_parse_range_or_list(args.spread_lookbacks))
            * len(_parse_int_list(args.spread_rebalances))
            * len(_parse_percent_list(args.spread_stops))
            * len(_parse_percent_list(args.spread_thresholds)),
            "best_params": spread_best.params,
            "best_summary": serialize_summary(spread_best.result.summary),
            "top_rows": [
                {"params": row.params, "summary": serialize_summary(row.result.summary)}
                for row in spread_top
            ],
        },
    }

    print(f"Baseline best: {json.dumps(payload['baseline_pair_return_ranking']['best_config'], sort_keys=True)}")
    print(
        "Baseline summary: "
        f"{json.dumps(payload['baseline_pair_return_ranking']['best_summary'], sort_keys=True)}"
    )
    print(f"XLF regime best: {json.dumps(payload['xlf_regime']['best_params'], sort_keys=True)}")
    print(f"XLF regime summary: {json.dumps(payload['xlf_regime']['best_summary'], sort_keys=True)}")
    print(f"Spread threshold best: {json.dumps(payload['spread_threshold']['best_params'], sort_keys=True)}")
    print(f"Spread threshold summary: {json.dumps(payload['spread_threshold']['best_summary'], sort_keys=True)}")

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Wrote {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
