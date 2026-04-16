from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import run_weekly_calendar_policy_walk_forward as base  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_common import (  # noqa: E402
    CACHE_ROOT,
    DEFAULT_BATCH_SUMMARY_CSV,
    FilterConfig,
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
    build_daily_entry_dates,
    build_daily_entry_dates_from_indicator_index,
)


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _default_output_prefix(*, top_k: int) -> Path:
    return ROOT / "logs" / f"weekly_calendar_policy_walk_forward_spy_1dte_2dte_daily_top{top_k}_train2y_20251231_q1_2026"


def _patched_argv(argv: list[str]) -> list[str]:
    patched = list(argv)
    if not _has_flag(patched, "--summary-csv"):
        patched.extend(["--summary-csv", str(DEFAULT_BATCH_SUMMARY_CSV)])
    if not _has_flag(patched, "--top-k"):
        patched.extend(["--top-k", "1"])
    if not _has_flag(patched, "--max-workers"):
        patched.extend(["--max-workers", "1"])
    if not _has_flag(patched, "--max-symbol-weight-pct"):
        patched.extend(["--max-symbol-weight-pct", "100"])
    if not _has_flag(patched, "--output-prefix"):
        patched.extend(["--output-prefix", str(_default_output_prefix(top_k=1))])
    return patched


def _apply_overrides() -> None:
    base.DEFAULT_SUMMARY_CSV = DEFAULT_BATCH_SUMMARY_CSV
    base.DEFAULT_TOP_K = 1
    base.DEFAULT_MAX_WORKERS = 1
    base.DEFAULT_MAX_SYMBOL_WEIGHT_PCT = 100.0
    base._default_output_prefix = _default_output_prefix
    base._build_bundle = _build_bundle

    base.two_stage.REQUESTED_END_DATE = REQUESTED_END_DATE
    base.two_stage.STARTING_EQUITY = STARTING_EQUITY
    base.two_stage.STARTING_EQUITY_PCT_MULTIPLIER = 100.0 / STARTING_EQUITY
    base.two_stage.FilterConfig = FilterConfig
    base.two_stage.StrategyConfig = StrategyConfig
    base.two_stage.CACHE_ROOT = CACHE_ROOT
    base.two_stage._build_bundle = _build_bundle
    base.two_stage._build_calendar_config = _build_calendar_config
    base.two_stage._load_risk_free_curve = _load_risk_free_curve
    base.two_stage._trade_roi_on_margin_pct = _trade_roi_on_margin_pct


def _load_candidate_training_assignment_metrics_daily(candidate: dict[str, object]) -> dict[str, object]:
    best = dict(candidate["best"])
    embedded_keys = (
        "assignment_count",
        "assignment_rate_pct",
        "put_assignment_count",
        "put_assignment_rate_pct",
    )
    if all(key in best for key in embedded_keys):
        return {
            "training_assignment_count": int(best["assignment_count"]),
            "training_assignment_rate_pct": round(float(best["assignment_rate_pct"]), 4),
            "training_put_assignment_count": int(best["put_assignment_count"]),
            "training_put_assignment_rate_pct": round(float(best["put_assignment_rate_pct"]), 4),
        }

    components = base._resolve_candidate_components(candidate)
    symbol = str(components["symbol"])
    train_start_date = components["train_start_date"]
    latest_available_date = components["latest_available_date"]
    bull_strategy = components["bull_strategy"]
    bear_strategy = components["bear_strategy"]
    neutral_strategy = components["neutral_strategy"]
    period_config = components["period_config"]
    bull_filter = components["bull_filter"]
    bear_filter = components["bear_filter"]

    indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = indicator_cache[period_config.label]
    entry_dates = build_daily_entry_dates_from_indicator_index(
        indicator_dates=sorted(indicators),
        start_date=train_start_date,
        end_date=latest_available_date,
    )

    trade_maps: dict[str, dict[date, dict[str, object]]] = {}
    strategies = (bull_strategy, bear_strategy, neutral_strategy)
    missing_cache = False
    for strategy in strategies:
        cache_path = base._trade_map_cache_path(
            symbol=symbol,
            start_date=train_start_date,
            latest_available_date=latest_available_date,
            strategy_label=strategy.label,
        )
        if not cache_path.exists():
            missing_cache = True
            break
        trade_maps[strategy.label] = base._load_cached_trade_map_rows(cache_path)

    if missing_cache:
        store = base.HistoricalMarketDataStore(base.create_session, base.create_readonly_session)
        bundle = base._build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=latest_available_date)
        curve = base.two_stage._load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
        trade_maps = base.two_stage._precompute_trade_maps(
            strategies=strategies,
            bundle=bundle,
            trading_fridays=entry_dates,
            latest_available_date=latest_available_date,
            curve=curve,
            start_date=train_start_date,
            use_cache=True,
            worker_count=1,
        )

    trade_count = 0
    assignment_count = 0
    put_assignment_count = 0
    for entry_date in entry_dates:
        indicator_row = indicators.get(entry_date)
        bull = bull_filter.matches(indicator_row)
        bear = bear_filter.matches(indicator_row)
        if bull and not bear:
            strategy = bull_strategy
        elif bear and not bull:
            strategy = bear_strategy
        else:
            strategy = neutral_strategy
        trade_row = trade_maps.get(strategy.label, {}).get(entry_date)
        if trade_row is None:
            continue
        trade_count += 1
        exit_reason = trade_row.get("exit_reason")
        if base._is_assignment_exit_reason(exit_reason):
            assignment_count += 1
        if base._is_put_assignment_exit_reason(exit_reason):
            put_assignment_count += 1

    return {
        "training_assignment_count": assignment_count,
        "training_assignment_rate_pct": round((assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
        "training_put_assignment_count": put_assignment_count,
        "training_put_assignment_rate_pct": round((put_assignment_count / trade_count * 100.0) if trade_count else 0.0, 4),
    }


def _replay_symbol_daily(
    *,
    candidate: dict[str, object],
    entry_start_date: date,
    entry_end_date: date,
    replay_data_end: date,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    components = base._resolve_candidate_components(candidate)
    payload = dict(components["payload"])
    best = dict(components["best"])
    symbol = str(components["symbol"])
    train_start_date = components["train_start_date"]
    period_config = components["period_config"]
    bull_filter = components["bull_filter"]
    bear_filter = components["bear_filter"]
    bull_strategy = components["bull_strategy"]
    bear_strategy = components["bear_strategy"]
    neutral_strategy = components["neutral_strategy"]
    training_assignment_metrics = dict(candidate.get("training_assignment_metrics") or {})

    store = base.HistoricalMarketDataStore(base.create_session, base.create_readonly_session)
    bundle = base._build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=replay_data_end)
    latest_available_date = base._resolve_latest_available_date_from_bundle(bundle, replay_data_end)
    curve = base.two_stage._load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
    indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = indicator_cache[period_config.label]
    entry_dates = build_daily_entry_dates(
        bars=bundle.bars,
        start_date=entry_start_date,
        end_date=entry_end_date,
    )

    strategies_to_run = (bull_strategy, bear_strategy, neutral_strategy)
    engine = base.OptionsBacktestEngine()
    trade_map: dict[str, dict[date, object]] = {}
    for strategy in strategies_to_run:
        local_entry_rule_cache = bundle.entry_rule_cache.__class__()
        per_date: dict[date, object] = {}
        for entry_date in entry_dates:
            config = base.two_stage._build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                latest_available_date=latest_available_date,
                risk_free_curve=curve,
            )
            result = engine.run(
                config=config,
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                ex_dividend_dates=bundle.ex_dividend_dates,
                option_gateway=bundle.option_gateway,
                shared_entry_rule_cache=local_entry_rule_cache,
            )
            trade = next((item for item in result.trades if item.entry_date == entry_date), None)
            if trade is not None:
                per_date[entry_date] = trade
        trade_map[strategy.label] = per_date

    ledger_rows: list[dict[str, object]] = []
    for entry_date in entry_dates:
        indicator_row = indicators.get(entry_date)
        bull = bull_filter.matches(indicator_row)
        bear = bear_filter.matches(indicator_row)
        if bull and not bear:
            regime = "bullish"
            strategy = bull_strategy
        elif bear and not bull:
            regime = "bearish"
            strategy = bear_strategy
        else:
            regime = "neutral"
            strategy = neutral_strategy
        trade = trade_map[strategy.label].get(entry_date)
        if trade is None:
            continue

        quantity = base._safe_float(getattr(trade, "quantity", 1.0)) or 1.0
        detail_json = getattr(trade, "detail_json", {}) or {}
        entry_debit = base._safe_float(detail_json.get("entry_package_market_value"))
        capital_required = base._safe_float(detail_json.get("capital_required_per_unit"))
        total_capital_required = None if capital_required is None else capital_required * quantity
        net_pnl = float(trade.net_pnl)
        roi_capital = base.two_stage._trade_roi_on_margin_pct(trade)
        roi_debit = None
        if entry_debit is not None and entry_debit > 0:
            roi_debit = net_pnl / entry_debit * 100.0

        ledger_rows.append(
            {
                "symbol": symbol,
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "regime": regime,
                "strategy": strategy.label,
                "option_ticker": getattr(trade, "option_ticker", ""),
                "quantity": round(quantity, 4),
                "entry_debit": None if entry_debit is None else round(entry_debit, 4),
                "capital_required": None if total_capital_required is None else round(total_capital_required, 4),
                "net_pnl": round(net_pnl, 4),
                "roi_on_debit_pct": None if roi_debit is None else round(roi_debit, 4),
                "roi_on_capital_required_pct": None if roi_capital is None else round(roi_capital, 4),
                "exit_reason": getattr(trade, "exit_reason", ""),
                "entry_underlying_close": round(float(getattr(trade, "entry_underlying_close", 0.0)), 4),
                "exit_underlying_close": round(float(getattr(trade, "exit_underlying_close", 0.0)), 4),
                "training_trade_count": int(best["trade_count"]),
                "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
                "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
                "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
                "training_assignment_count": int(training_assignment_metrics.get("training_assignment_count") or 0),
                "training_assignment_rate_pct": round(float(training_assignment_metrics.get("training_assignment_rate_pct") or 0.0), 4),
                "training_put_assignment_count": int(training_assignment_metrics.get("training_put_assignment_count") or 0),
                "training_put_assignment_rate_pct": round(float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0), 4),
            }
        )

    capital_values = [float(item["capital_required"]) for item in ledger_rows if item["capital_required"] is not None]
    roi_values = [float(item["roi_on_capital_required_pct"]) for item in ledger_rows if item["roi_on_capital_required_pct"] is not None]
    total_capital = sum(capital_values)
    total_pnl = sum(float(item["net_pnl"]) for item in ledger_rows)
    result_row = {
        "symbol": symbol,
        "train_start_date": train_start_date.isoformat(),
        "train_end_date": payload["period"]["requested_end"],
        "entry_window_start": entry_start_date.isoformat(),
        "entry_window_end": entry_end_date.isoformat(),
        "replay_data_end": latest_available_date.isoformat(),
        "training_stage": best.get("stage", ""),
        "training_trade_count": int(best["trade_count"]),
        "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
        "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
        "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
        "training_assignment_count": int(training_assignment_metrics.get("training_assignment_count") or 0),
        "training_assignment_rate_pct": round(float(training_assignment_metrics.get("training_assignment_rate_pct") or 0.0), 4),
        "training_put_assignment_count": int(training_assignment_metrics.get("training_put_assignment_count") or 0),
        "training_put_assignment_rate_pct": round(float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0), 4),
        "trade_count": len(ledger_rows),
        "total_capital_required": round(total_capital, 4),
        "total_net_pnl": round(total_pnl, 4),
        "roi_on_capital_required_pct": round(total_pnl / total_capital * 100.0, 4) if total_capital else 0.0,
        "average_roi_on_capital_required_pct": round(sum(roi_values) / len(roi_values), 4) if roi_values else 0.0,
        "median_roi_on_capital_required_pct": round(base.median(roi_values), 4) if roi_values else 0.0,
    }
    return result_row, ledger_rows


def main() -> int:
    _apply_overrides()
    base._load_candidate_training_assignment_metrics = _load_candidate_training_assignment_metrics_daily
    base._replay_symbol = _replay_symbol_daily
    sys.argv = [sys.argv[0], *_patched_argv(sys.argv[1:])]
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
