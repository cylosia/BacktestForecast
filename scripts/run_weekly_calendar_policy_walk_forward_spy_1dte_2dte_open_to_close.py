from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.intraday_option_quotes import IntradayOptionQuoteCache  # noqa: E402

import run_weekly_calendar_policy_walk_forward as base  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_open_to_close_common import (  # noqa: E402
    CACHE_ROOT,
    DEFAULT_BATCH_SUMMARY_CSV,
    FilterConfig,
    HistoricalOptionPriceSourceView,
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _build_strategy_sets,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
    build_daily_entry_dates,
    shift_indicator_rows_to_entry_dates,
    simulate_intraday_open_to_close_trade,
    simulate_open_to_close_trade,
)


_USE_MASSIVE_INTRADAY_QUOTES = False
_INTRADAY_STOP_LOSS_PCT: float | None = None
_INTRADAY_PROFIT_TARGET_PCT: float | None = None
_INTRADAY_CACHE_DIR: Path | None = None


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _extract_intraday_args(argv: list[str]) -> tuple[list[str], dict[str, object]]:
    cleaned: list[str] = []
    config: dict[str, object] = {
        "use_massive_intraday_quotes": False,
        "intraday_stop_loss_pct": None,
        "intraday_profit_target_pct": None,
        "intraday_cache_dir": None,
    }
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--use-massive-intraday-quotes":
            config["use_massive_intraday_quotes"] = True
            index += 1
            continue
        if arg.startswith("--intraday-stop-loss-pct="):
            config["intraday_stop_loss_pct"] = float(arg.split("=", 1)[1])
            index += 1
            continue
        if arg == "--intraday-stop-loss-pct":
            if index + 1 >= len(argv):
                raise ValueError("--intraday-stop-loss-pct requires a value")
            config["intraday_stop_loss_pct"] = float(argv[index + 1])
            index += 2
            continue
        if arg.startswith("--intraday-profit-target-pct="):
            config["intraday_profit_target_pct"] = float(arg.split("=", 1)[1])
            index += 1
            continue
        if arg == "--intraday-profit-target-pct":
            if index + 1 >= len(argv):
                raise ValueError("--intraday-profit-target-pct requires a value")
            config["intraday_profit_target_pct"] = float(argv[index + 1])
            index += 2
            continue
        if arg.startswith("--intraday-cache-dir="):
            config["intraday_cache_dir"] = Path(arg.split("=", 1)[1])
            index += 1
            continue
        if arg == "--intraday-cache-dir":
            if index + 1 >= len(argv):
                raise ValueError("--intraday-cache-dir requires a value")
            config["intraday_cache_dir"] = Path(argv[index + 1])
            index += 2
            continue
        cleaned.append(arg)
        index += 1
    return cleaned, config


def _resolved_intraday_cache_dir() -> Path:
    if _INTRADAY_CACHE_DIR is not None:
        return _INTRADAY_CACHE_DIR
    return ROOT / "logs" / "massive_intraday_option_quotes" / "spy_1dte_2dte_open_to_close_day1"


def _default_output_prefix(*, top_k: int) -> Path:
    suffix = ""
    if _USE_MASSIVE_INTRADAY_QUOTES:
        suffix = "_massive_intraday"
        if _INTRADAY_STOP_LOSS_PCT is not None:
            stop_loss_pct = int(_INTRADAY_STOP_LOSS_PCT) if float(_INTRADAY_STOP_LOSS_PCT).is_integer() else _INTRADAY_STOP_LOSS_PCT
            suffix += f"_sl{stop_loss_pct}"
        if _INTRADAY_PROFIT_TARGET_PCT is not None:
            profit_target_pct = (
                int(_INTRADAY_PROFIT_TARGET_PCT)
                if float(_INTRADAY_PROFIT_TARGET_PCT).is_integer()
                else _INTRADAY_PROFIT_TARGET_PCT
            )
            suffix += f"_tp{profit_target_pct}"
    return ROOT / "logs" / (
        f"weekly_calendar_policy_walk_forward_spy_1dte_2dte_open_to_close_day1_top{top_k}_train2y_20251231_q1_2026{suffix}"
    )


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


def _resolve_candidate_components_open_close(candidate: dict[str, object]) -> dict[str, object]:
    payload = dict(candidate["payload"])
    best = dict(candidate["best"])
    symbol = str(candidate["symbol"])
    train_start_date = date.fromisoformat(payload["period"]["start"])
    latest_available_date = date.fromisoformat(
        payload["period"].get("latest_available_date") or payload["period"]["requested_end"]
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

    period_config = base.IndicatorPeriodConfig(
        roc_period=int(best["roc_period"]),
        adx_period=int(best["adx_period"]),
        rsi_period=int(best["rsi_period"]),
    )
    return {
        "payload": payload,
        "best": best,
        "symbol": symbol,
        "train_start_date": train_start_date,
        "latest_available_date": latest_available_date,
        "period_config": period_config,
        "bull_filter": bull_filter_lookup[str(best["bull_filter"])],
        "bear_filter": bear_filter_lookup[str(best["bear_filter"])],
        "bull_strategy": strategy_lookup[str(best["bull_strategy"])],
        "bear_strategy": strategy_lookup[str(best["bear_strategy"])],
        "neutral_strategy": strategy_lookup[str(best["neutral_strategy"])],
    }


def _load_candidate_training_assignment_metrics_open_close(candidate: dict[str, object]) -> dict[str, object]:
    best = dict(candidate["best"])
    if all(
        key in best
        for key in ("assignment_count", "assignment_rate_pct", "put_assignment_count", "put_assignment_rate_pct")
    ):
        return {
            "training_assignment_count": int(best["assignment_count"]),
            "training_assignment_rate_pct": round(float(best["assignment_rate_pct"]), 4),
            "training_put_assignment_count": int(best["put_assignment_count"]),
            "training_put_assignment_rate_pct": round(float(best["put_assignment_rate_pct"]), 4),
        }
    return {
        "training_assignment_count": 0,
        "training_assignment_rate_pct": 0.0,
        "training_put_assignment_count": 0,
        "training_put_assignment_rate_pct": 0.0,
    }


def _build_ledger_row(
    *,
    symbol: str,
    regime: str,
    strategy_label: str,
    trade,
    best: dict[str, object],
    training_assignment_metrics: dict[str, object],
) -> dict[str, object]:
    quantity = base._safe_float(getattr(trade, "quantity", 1.0)) or 1.0
    detail_json = getattr(trade, "detail_json", {}) or {}
    entry_debit = base._safe_float(detail_json.get("entry_package_market_value"))
    capital_required = base._safe_float(detail_json.get("capital_required_per_unit"))
    total_capital_required = None if capital_required is None else capital_required * quantity
    net_pnl = float(trade.net_pnl)
    roi_capital = _trade_roi_on_margin_pct(trade)
    roi_debit = None
    if entry_debit is not None and entry_debit > 0:
        roi_debit = net_pnl / entry_debit * 100.0

    return {
        "symbol": symbol,
        "entry_date": trade.entry_date.isoformat(),
        "exit_date": trade.exit_date.isoformat(),
        "regime": regime,
        "strategy": strategy_label,
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
        "execution_policy": str(detail_json.get("execution_policy") or ""),
        "intraday_stop_loss_pct": detail_json.get("intraday_stop_loss_pct"),
        "intraday_profit_target_pct": detail_json.get("intraday_profit_target_pct"),
        "intraday_exit_mode": detail_json.get("intraday_exit_mode"),
        "training_trade_count": int(best["trade_count"]),
        "training_total_net_pnl": round(float(best["total_net_pnl"]), 4),
        "training_average_roi_on_margin_pct": round(float(best["average_roi_on_margin_pct"]), 4),
        "training_median_roi_on_margin_pct": round(float(best["median_roi_on_margin_pct"]), 4),
        "training_assignment_count": int(training_assignment_metrics.get("training_assignment_count") or 0),
        "training_assignment_rate_pct": round(
            float(training_assignment_metrics.get("training_assignment_rate_pct") or 0.0),
            4,
        ),
        "training_put_assignment_count": int(training_assignment_metrics.get("training_put_assignment_count") or 0),
        "training_put_assignment_rate_pct": round(
            float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0),
            4,
        ),
    }


def _replay_symbol_open_close(
    *,
    candidate: dict[str, object],
    entry_start_date: date,
    entry_end_date: date,
    replay_data_end: date,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    components = _resolve_candidate_components_open_close(candidate)
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
    bundle = _build_bundle(store, symbol=symbol, start_date=train_start_date, end_date=replay_data_end)
    latest_available_date = base._resolve_latest_available_date_from_bundle(bundle, replay_data_end)
    curve = base.two_stage._load_risk_free_curve(store, start_date=train_start_date, end_date=latest_available_date)
    entry_dates = build_daily_entry_dates(
        bars=bundle.bars,
        start_date=entry_start_date,
        end_date=entry_end_date,
    )
    raw_indicator_cache = base._build_period_cache(
        symbol=symbol,
        start_date=train_start_date,
        end_date=latest_available_date,
        period_configs=(period_config,),
        use_cache=True,
        worker_count=1,
    )
    indicators = shift_indicator_rows_to_entry_dates(
        indicators_by_date=raw_indicator_cache[period_config.label],
        entry_dates=entry_dates,
    )

    contract_gateway = HistoricalOptionGateway(
        HistoricalOptionPriceSourceView(store, price_source="open"),
        symbol,
    )
    close_gateway = HistoricalOptionGateway(
        HistoricalOptionPriceSourceView(store, price_source="close"),
        symbol,
    )
    intraday_quote_cache = (
        IntradayOptionQuoteCache(_resolved_intraday_cache_dir() / symbol.lower())
        if _USE_MASSIVE_INTRADAY_QUOTES
        else None
    )
    engine = OptionsBacktestEngine()

    strategies_to_run = (bull_strategy, bear_strategy, neutral_strategy)
    trade_map: dict[str, dict[date, object]] = {strategy.label: {} for strategy in strategies_to_run}
    bars_by_date = {bar.trade_date: bar for bar in bundle.bars}
    for strategy in strategies_to_run:
        for entry_date in entry_dates:
            bar = bars_by_date.get(entry_date)
            if bar is None:
                continue
            config = base.two_stage._build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                latest_available_date=latest_available_date,
                risk_free_curve=curve,
            )
            if intraday_quote_cache is not None:
                trade = simulate_intraday_open_to_close_trade(
                    strategy=strategy,
                    config=config,
                    bar=bar,
                    contract_gateway=contract_gateway,
                    intraday_quote_cache=intraday_quote_cache,
                    engine=engine,
                    stop_loss_pct=_INTRADAY_STOP_LOSS_PCT,
                    profit_target_pct=_INTRADAY_PROFIT_TARGET_PCT,
                )
            else:
                trade = simulate_open_to_close_trade(
                    strategy=strategy,
                    config=config,
                    bar=bar,
                    open_gateway=contract_gateway,
                    close_gateway=close_gateway,
                    engine=engine,
                )
            if trade is not None:
                trade_map[strategy.label][entry_date] = trade

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

        ledger_rows.append(
            _build_ledger_row(
                symbol=symbol,
                regime=regime,
                strategy_label=strategy.label,
                trade=trade,
                best=best,
                training_assignment_metrics=training_assignment_metrics,
            )
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
        "training_put_assignment_rate_pct": round(
            float(training_assignment_metrics.get("training_put_assignment_rate_pct") or 0.0),
            4,
        ),
        "execution_policy": (
            "massive_intraday_open_quote_to_close_quote_day1"
            if _USE_MASSIVE_INTRADAY_QUOTES
            else "open_to_close_day1"
        ),
        "intraday_stop_loss_pct": _INTRADAY_STOP_LOSS_PCT,
        "intraday_profit_target_pct": _INTRADAY_PROFIT_TARGET_PCT,
        "trade_count": len(ledger_rows),
        "total_capital_required": round(total_capital, 4),
        "total_net_pnl": round(total_pnl, 4),
        "roi_on_capital_required_pct": round(total_pnl / total_capital * 100.0, 4) if total_capital else 0.0,
        "average_roi_on_capital_required_pct": round(sum(roi_values) / len(roi_values), 4) if roi_values else 0.0,
        "median_roi_on_capital_required_pct": round(base.median(roi_values), 4) if roi_values else 0.0,
    }
    return result_row, ledger_rows


def main() -> int:
    global _USE_MASSIVE_INTRADAY_QUOTES, _INTRADAY_STOP_LOSS_PCT, _INTRADAY_PROFIT_TARGET_PCT, _INTRADAY_CACHE_DIR

    raw_argv, intraday_config = _extract_intraday_args(sys.argv[1:])
    _USE_MASSIVE_INTRADAY_QUOTES = bool(intraday_config["use_massive_intraday_quotes"])
    _INTRADAY_STOP_LOSS_PCT = (
        float(intraday_config["intraday_stop_loss_pct"])
        if intraday_config["intraday_stop_loss_pct"] is not None
        else None
    )
    _INTRADAY_PROFIT_TARGET_PCT = (
        float(intraday_config["intraday_profit_target_pct"])
        if intraday_config["intraday_profit_target_pct"] is not None
        else None
    )
    _INTRADAY_CACHE_DIR = (
        Path(intraday_config["intraday_cache_dir"])
        if intraday_config["intraday_cache_dir"] is not None
        else None
    )
    if _INTRADAY_STOP_LOSS_PCT is not None or _INTRADAY_PROFIT_TARGET_PCT is not None or _INTRADAY_CACHE_DIR is not None:
        _USE_MASSIVE_INTRADAY_QUOTES = True

    _apply_overrides()
    base._resolve_candidate_components = _resolve_candidate_components_open_close
    base._load_candidate_training_assignment_metrics = _load_candidate_training_assignment_metrics_open_close
    base._replay_symbol = _replay_symbol_open_close
    sys.argv = [sys.argv[0], *_patched_argv(raw_argv)]
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
