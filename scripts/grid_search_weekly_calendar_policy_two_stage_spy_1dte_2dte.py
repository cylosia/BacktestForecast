from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import grid_search_weekly_calendar_policy_two_stage as base  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_common import (  # noqa: E402
    CACHE_ROOT,
    DEFAULT_SYMBOL,
    FilterConfig,
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
    build_daily_entry_dates,
)


def _consume_flag(argv: list[str], flag: str) -> tuple[list[str], str | None]:
    consumed: str | None = None
    kept: list[str] = []
    skip_next = False
    for index, arg in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        if arg == flag:
            if index + 1 < len(argv):
                consumed = argv[index + 1]
                skip_next = True
            else:
                consumed = None
            continue
        if arg.startswith(f"{flag}="):
            consumed = arg.split("=", 1)[1]
            continue
        kept.append(arg)
    return kept, consumed


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _value_for_flag(argv: list[str], flag: str) -> str | None:
    for index, arg in enumerate(argv):
        if arg == flag:
            return argv[index + 1] if index + 1 < len(argv) else None
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return None


def _parse_profit_targets(raw_value: str | None) -> tuple[int, ...] | None:
    if raw_value is None:
        return None
    values: list[int] = []
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(int(item))
    if not values:
        raise SystemExit("--profit-targets must contain at least one integer value.")
    return tuple(values)


def _profit_target_suffix(profit_targets: tuple[int, ...] | None) -> str:
    if not profit_targets:
        return ""
    return "_pt" + "_".join(str(value) for value in profit_targets)


def _patched_argv(argv: list[str], *, profit_targets: tuple[int, ...] | None) -> list[str]:
    patched = list(argv)
    if not _has_flag(patched, "--symbol"):
        patched.extend(["--symbol", DEFAULT_SYMBOL])
    if not _has_flag(patched, "--output"):
        start_date = _value_for_flag(patched, "--start-date")
        requested_end_date = _value_for_flag(patched, "--requested-end-date") or REQUESTED_END_DATE.isoformat()
        if start_date is not None:
            suffix = _profit_target_suffix(profit_targets)
            output_path = ROOT / "logs" / (
                f"{DEFAULT_SYMBOL.lower()}_weekly_calendar_policy_two_stage_1dte_2dte_daily_"
                f"{start_date}_{requested_end_date}{suffix}.json"
            )
            patched.extend(["--output", str(output_path)])
    return patched


def _build_strategy_sets(symbol: str, profit_targets: tuple[int, ...]) -> tuple[
    tuple[StrategyConfig, ...],
    tuple[StrategyConfig, ...],
    tuple[StrategyConfig, ...],
]:
    lower = symbol.lower()
    bullish = tuple(
        StrategyConfig(f"{lower}_call_d{delta}_pt{profit_target}", symbol, base.StrategyType.CALENDAR_SPREAD, delta, profit_target)
        for delta in (40, 50)
        for profit_target in profit_targets
    )
    bearish = tuple(
        StrategyConfig(f"bear_{lower}_{side}_d{delta}_pt{profit_target}", symbol, strategy_type, delta, profit_target)
        for side, strategy_type in (
            ("call", base.StrategyType.CALENDAR_SPREAD),
            ("put", base.StrategyType.PUT_CALENDAR_SPREAD),
        )
        for delta in (30, 40, 50)
        for profit_target in profit_targets
    )
    neutral = tuple(
        StrategyConfig(
            f"neutral_{lower}_call_d{delta}_pt{profit_target}",
            symbol,
            base.StrategyType.CALENDAR_SPREAD,
            delta,
            profit_target,
        )
        for delta in (40, 50)
        for profit_target in profit_targets
    )
    return bullish, bearish, neutral


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
    stripped_argv, raw_profit_targets = _consume_flag(sys.argv[1:], "--profit-targets")
    profit_targets = _parse_profit_targets(raw_profit_targets)
    sys.argv = [sys.argv[0], *_patched_argv(stripped_argv, profit_targets=profit_targets)]
    args = base._parse_args()
    symbol = args.symbol.upper()
    output_json = args.output or ROOT / "logs" / (
        f"{symbol.lower()}_weekly_calendar_policy_two_stage_1dte_2dte_daily_"
        f"{args.start_date.isoformat()}_{args.requested_end_date.isoformat()}.json"
    )
    use_cache = not args.disable_cache

    base.engine_module.logger = SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    base._install_quote_series_expiration_cap()
    base._install_single_contract_position_sizing()

    if profit_targets is None:
        bullish_strategies, bearish_strategies, neutral_strategies = base._build_strategy_sets(symbol)
    else:
        bullish_strategies, bearish_strategies, neutral_strategies = _build_strategy_sets(symbol, profit_targets)
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

    precomputed = base._precompute_trade_maps(
        strategies=all_strategies,
        bundle=bundle,
        trading_fridays=entry_dates,
        latest_available_date=latest_available_date,
        curve=curve,
        start_date=args.start_date,
        use_cache=use_cache,
        worker_count=args.precompute_workers,
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
    indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=broad_period_configs,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
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
    indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=args.start_date,
        end_date=latest_available_date,
        period_configs=refine_period_configs,
        cache=indicator_cache,
        use_cache=use_cache,
        worker_count=args.indicator_workers,
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
        "profit_targets": list(profit_targets) if profit_targets is not None else None,
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
    output_json.write_text(json.dumps(payload, indent=2))
    print(
        json.dumps(
            {
                "combined_best_result": payload["combined_best_result"],
                "combined_best_result_by_total_roi_pct": payload["combined_best_result_by_total_roi_pct"],
                "entry_cadence": "daily",
                "output": str(output_json),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
