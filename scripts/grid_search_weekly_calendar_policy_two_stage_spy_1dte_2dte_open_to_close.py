from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import grid_search_weekly_calendar_policy_two_stage as base  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_open_to_close_common import (  # noqa: E402
    CACHE_ROOT,
    DEFAULT_SYMBOL,
    FilterConfig,
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _build_strategy_sets,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
    build_daily_entry_dates,
    precompute_open_to_close_trade_maps,
    shift_indicator_cache_to_entry_dates,
)


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _value_for_flag(argv: list[str], flag: str) -> str | None:
    for index, arg in enumerate(argv):
        if arg == flag:
            return argv[index + 1] if index + 1 < len(argv) else None
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return None


def _patched_argv(argv: list[str]) -> list[str]:
    patched = list(argv)
    if not _has_flag(patched, "--symbol"):
        patched.extend(["--symbol", DEFAULT_SYMBOL])
    if not _has_flag(patched, "--output"):
        start_date = _value_for_flag(patched, "--start-date")
        requested_end_date = _value_for_flag(patched, "--requested-end-date") or REQUESTED_END_DATE.isoformat()
        if start_date is not None:
            output_path = ROOT / "logs" / (
                f"{DEFAULT_SYMBOL.lower()}_weekly_calendar_policy_two_stage_1dte_2dte_open_to_close_day1_"
                f"{start_date}_{requested_end_date}.json"
            )
            patched.extend(["--output", str(output_path)])
    return patched


def _apply_overrides() -> None:
    base.REQUESTED_END_DATE = REQUESTED_END_DATE
    base.STARTING_EQUITY = STARTING_EQUITY
    base.STARTING_EQUITY_PCT_MULTIPLIER = 100.0 / STARTING_EQUITY
    base.FilterConfig = FilterConfig
    base.StrategyConfig = StrategyConfig
    base.CACHE_ROOT = CACHE_ROOT
    base._build_bundle = _build_bundle
    base._build_calendar_config = _build_calendar_config
    base._load_risk_free_curve = _load_risk_free_curve
    base._trade_roi_on_margin_pct = _trade_roi_on_margin_pct


def main() -> int:
    _apply_overrides()
    sys.argv = [sys.argv[0], *_patched_argv(sys.argv[1:])]
    args = base._parse_args()
    symbol = args.symbol.upper()
    output_json = args.output or ROOT / "logs" / (
        f"{symbol.lower()}_weekly_calendar_policy_two_stage_1dte_2dte_open_to_close_day1_"
        f"{args.start_date.isoformat()}_{args.requested_end_date.isoformat()}.json"
    )
    use_cache = not args.disable_cache

    base.engine_module.logger = SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )

    bullish_strategies, bearish_strategies, neutral_strategies = _build_strategy_sets(symbol)
    all_strategies = bullish_strategies + bearish_strategies + neutral_strategies
    bull_filters = base._build_default_bull_filters()
    bear_filters = base._build_default_bear_filters()
    bull_filter_lookup, bear_filter_lookup, strategy_lookup = base._label_maps(
        bull_filters=bull_filters,
        bear_filters=bear_filters,
        strategies=all_strategies,
    )

    store = base.HistoricalMarketDataStore(base.create_session, base.create_readonly_session)
    bundle = base._build_bundle(store, symbol=symbol, start_date=args.start_date, end_date=args.requested_end_date)
    latest_available_date = base._resolve_latest_available_date_from_bundle(bundle, args.requested_end_date)
    curve = base._load_risk_free_curve(store, start_date=args.start_date, end_date=latest_available_date)
    entry_dates = build_daily_entry_dates(
        bars=bundle.bars,
        start_date=args.start_date,
        end_date=latest_available_date,
    )

    precomputed = precompute_open_to_close_trade_maps(
        strategies=all_strategies,
        bundle=bundle,
        trading_fridays=entry_dates,
        latest_available_date=latest_available_date,
        curve=curve,
        start_date=args.start_date,
        use_cache=use_cache,
        worker_count=args.precompute_workers,
        cache_root=CACHE_ROOT,
    )
    strategy_series = base._build_strategy_trade_series(
        strategies=all_strategies,
        precomputed=precomputed,
        trading_fridays=entry_dates,
    )

    broad_period_configs = tuple(
        base.IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in base.BROAD_ROC_PERIODS
        for adx_period in base.BROAD_ADX_PERIODS
        for rsi_period in base.BROAD_RSI_PERIODS
    )
    raw_indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=broad_period_configs,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
    )
    indicator_cache = shift_indicator_cache_to_entry_dates(
        indicator_cache=raw_indicator_cache,
        entry_dates=entry_dates,
    )

    broad_strategy_triplets = tuple(
        (bull_strategy, bear_strategy, neutral_strategy)
        for bull_strategy in bullish_strategies
        for bear_strategy in bearish_strategies
        for neutral_strategy in neutral_strategies
    )
    broad_stage = base._evaluate_stage(
        stage_name="broad",
        objective=args.objective,
        search_config=base.StageSearchConfig(
            period_configs=broad_period_configs,
            bull_filters=bull_filters,
            bear_filters=bear_filters,
            strategy_triplets=broad_strategy_triplets,
        ),
        trading_fridays=entry_dates,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
    )

    broad_ranked = list(broad_stage["top_100_ranked_results"])
    broad_seeds_source = broad_ranked[: args.refine_top_rows]
    if broad_stage["best_result_by_total_roi_pct"] is not None:
        broad_seeds_source.append(broad_stage["best_result_by_total_roi_pct"])

    refine_period_seeds = base._unique_period_seeds(broad_seeds_source, args.refine_top_period_seeds)
    refine_period_configs = base._build_refine_period_configs(refine_period_seeds)
    raw_indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=refine_period_configs,
        cache=raw_indicator_cache,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
    )
    indicator_cache = shift_indicator_cache_to_entry_dates(
        indicator_cache=raw_indicator_cache,
        entry_dates=entry_dates,
    )

    refine_bull_filters = base._unique_bull_filters(
        broad_seeds_source,
        bull_filter_lookup,
        args.refine_top_bull_filters,
    )
    refine_bear_filters = base._unique_bear_filters(
        broad_seeds_source,
        bear_filter_lookup,
        args.refine_top_bear_filters,
    )
    refine_strategy_triplets = base._unique_strategy_triplets(
        broad_seeds_source,
        strategy_lookup,
        args.refine_top_strategy_triplets,
    )
    refine_stage = base._evaluate_stage(
        stage_name="refine",
        objective=args.objective,
        search_config=base.StageSearchConfig(
            period_configs=refine_period_configs,
            bull_filters=refine_bull_filters,
            bear_filters=refine_bear_filters,
            strategy_triplets=refine_strategy_triplets,
        ),
        trading_fridays=entry_dates,
        strategy_series=strategy_series,
        indicators_by_period=indicator_cache,
    )

    best_primary = broad_stage["best_result"]
    best_primary_stage = "broad"
    if refine_stage["best_result"] is not None and (
        best_primary is None
        or base._ranking_key(refine_stage["best_result"], objective=args.objective)
        > base._ranking_key(best_primary, objective=args.objective)
    ):
        best_primary = refine_stage["best_result"]
        best_primary_stage = "refine"

    best_total = broad_stage["best_result_by_total_roi_pct"]
    best_total_stage = "broad"
    if refine_stage["best_result_by_total_roi_pct"] is not None and (
        best_total is None
        or float(refine_stage["best_result_by_total_roi_pct"]["total_roi_pct"]) > float(best_total["total_roi_pct"])
    ):
        best_total = refine_stage["best_result_by_total_roi_pct"]
        best_total_stage = "refine"

    payload = {
        "symbol": symbol,
        "selection_objective": args.objective,
        "period": {
            "start": args.start_date.isoformat(),
            "requested_end": args.requested_end_date.isoformat(),
            "latest_available_date": latest_available_date.isoformat(),
        },
        "starting_equity": STARTING_EQUITY,
        "cache_enabled": use_cache,
        "cache_root": str(
            base._symbol_cache_dir(
                symbol=symbol,
                start_date=args.start_date,
                latest_available_date=latest_available_date,
            )
        ),
        "entry_cadence": "daily",
        "entry_price_source": "open",
        "exit_policy": "same_day_close",
        "signal_shift": "t_minus_1_close_to_open",
        "stage_1_broad": broad_stage,
        "stage_2_refine": {
            **refine_stage,
            "seeded_from_top_rows": args.refine_top_rows,
            "seeded_period_configs": [item.label for item in refine_period_seeds],
            "seeded_strategy_triplets": [
                {
                    "bull_strategy": bull_strategy.label,
                    "bear_strategy": bear_strategy.label,
                    "neutral_strategy": neutral_strategy.label,
                }
                for bull_strategy, bear_strategy, neutral_strategy in refine_strategy_triplets
            ],
        },
        "combined_best_result": base._with_stage(best_primary, best_primary_stage),
        "combined_best_result_by_total_roi_pct": base._with_stage(best_total, best_total_stage),
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "combined_best_result": payload["combined_best_result"],
                "combined_best_result_by_total_roi_pct": payload["combined_best_result_by_total_roi_pct"],
                "entry_cadence": "daily",
                "entry_price_source": "open",
                "exit_policy": "same_day_close",
                "signal_shift": "t_minus_1_close_to_open",
                "output": str(output_json),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
