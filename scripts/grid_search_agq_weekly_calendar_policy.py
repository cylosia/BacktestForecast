from __future__ import annotations

import heapq
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import fmean, median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import adx, roc, rsi  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.schemas.backtests import StrategyType  # noqa: E402
from grid_search_fas_faz_weekly_calendar_policy import (  # noqa: E402
    REQUESTED_END_DATE,
    START_DATE,
    FilterConfig,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

SYMBOL = "AGQ"
OUTPUT_JSON = ROOT / "logs" / "agq_weekly_calendar_policy_grid_indicator_periods_2015_2026.json"
STARTING_EQUITY = 100_000.0
ROC_PERIODS = (21, 42, 63, 126)
ADX_PERIODS = (7, 14, 21)
RSI_PERIODS = (7, 14, 21)
TOP_RESULT_LIMIT = 100


@dataclass(frozen=True, slots=True)
class IndicatorPeriodConfig:
    roc_period: int
    adx_period: int
    rsi_period: int

    @property
    def label(self) -> str:
        return f"roc{self.roc_period}_adx{self.adx_period}_rsi{self.rsi_period}"


@dataclass(frozen=True, slots=True)
class NegativeFilterConfig:
    roc_threshold: float
    adx_threshold: float
    rsi_threshold: float | None

    @property
    def label(self) -> str:
        rsi_part = "none" if self.rsi_threshold is None else str(int(self.rsi_threshold))
        return f"roc{int(self.roc_threshold)}_adx{int(self.adx_threshold)}_rsi{rsi_part}"

    def matches(self, indicators: dict[str, float | None] | None) -> bool:
        if indicators is None:
            return False
        roc63 = indicators.get("roc63")
        adx14 = indicators.get("adx14")
        rsi14 = indicators.get("rsi14")
        if not isinstance(roc63, float) or roc63 >= self.roc_threshold:
            return False
        adx_ok = isinstance(adx14, float) and adx14 > self.adx_threshold
        if self.rsi_threshold is None:
            return adx_ok
        rsi_ok = isinstance(rsi14, float) and rsi14 < self.rsi_threshold
        return adx_ok or rsi_ok


BULLISH_STRATEGIES = [
    StrategyConfig("agq_call_d40_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("agq_call_d40_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("agq_call_d50_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("agq_call_d50_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 75),
]

BEARISH_STRATEGIES = [
    StrategyConfig("bear_agq_call_d30_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 30, 50),
    StrategyConfig("bear_agq_call_d30_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 30, 75),
    StrategyConfig("bear_agq_call_d40_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("bear_agq_call_d40_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("bear_agq_call_d50_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("bear_agq_call_d50_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 75),
    StrategyConfig("bear_agq_put_d30_pt50", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 30, 50),
    StrategyConfig("bear_agq_put_d30_pt75", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 30, 75),
    StrategyConfig("bear_agq_put_d40_pt50", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 40, 50),
    StrategyConfig("bear_agq_put_d40_pt75", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 40, 75),
    StrategyConfig("bear_agq_put_d50_pt50", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 50, 50),
    StrategyConfig("bear_agq_put_d50_pt75", SYMBOL, StrategyType.PUT_CALENDAR_SPREAD, 50, 75),
]

NEUTRAL_STRATEGIES = [
    StrategyConfig("neutral_agq_call_d40_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("neutral_agq_call_d40_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("neutral_agq_call_d50_pt50", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("neutral_agq_call_d50_pt75", SYMBOL, StrategyType.CALENDAR_SPREAD, 50, 75),
]


def _resolve_latest_available_date(requested_end: date) -> date:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    bars = store.get_underlying_day_bars(SYMBOL, requested_end - timedelta(days=30), requested_end)
    if not bars:
        raise SystemExit(f"Missing underlying bars for {SYMBOL}.")
    return min(max(bar.trade_date for bar in bars), requested_end)


def _load_adjusted_indicators_for_periods(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    period_config: IndicatorPeriodConfig,
) -> dict[date, dict[str, float | None]]:
    warmup_days = max(450, period_config.roc_period * 4)
    warmup_start = start_date - timedelta(days=warmup_days)
    with create_readonly_session() as session:
        rows = session.query(
            HistoricalUnderlyingDayBar.trade_date,
            HistoricalUnderlyingDayBar.high_price,
            HistoricalUnderlyingDayBar.low_price,
            HistoricalUnderlyingDayBar.close_price,
        ).filter(
            HistoricalUnderlyingDayBar.symbol == symbol,
            HistoricalUnderlyingDayBar.trade_date >= warmup_start,
            HistoricalUnderlyingDayBar.trade_date <= end_date,
        ).order_by(HistoricalUnderlyingDayBar.trade_date).all()
    if not rows:
        raise SystemExit(f"Missing adjusted bars for {symbol}.")

    dates = [row.trade_date for row in rows]
    highs = [float(row.high_price) for row in rows]
    lows = [float(row.low_price) for row in rows]
    closes = [float(row.close_price) for row in rows]

    roc_values = roc(closes, period_config.roc_period)
    adx_values = adx(highs, lows, closes, period_config.adx_period)
    rsi_values = rsi(closes, period_config.rsi_period)

    return {
        trade_date: {
            "roc63": roc_values[index],
            "adx14": adx_values[index],
            "rsi14": rsi_values[index],
        }
        for index, trade_date in enumerate(dates)
    }


def _summarize(
    *,
    selected_records: list[dict[str, object]],
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
    indicator_periods: IndicatorPeriodConfig,
    bull_filter: FilterConfig,
    bear_filter: NegativeFilterConfig,
    bull_strategy: StrategyConfig,
    bear_strategy: StrategyConfig,
    neutral_strategy: StrategyConfig,
) -> dict[str, object]:
    net_pnls = [float(item["net_pnl"]) for item in selected_records]
    rois = [float(item["roi_on_margin_pct"]) for item in selected_records if item["roi_on_margin_pct"] is not None]
    wins = [value for value in net_pnls if value > 0]
    losses = [value for value in net_pnls if value < 0]
    total_net_pnl = sum(net_pnls)
    return {
        "indicator_periods": indicator_periods.label,
        "roc_period": indicator_periods.roc_period,
        "adx_period": indicator_periods.adx_period,
        "rsi_period": indicator_periods.rsi_period,
        "bull_filter": bull_filter.label,
        "bear_filter": bear_filter.label,
        "bull_strategy": bull_strategy.label,
        "bear_strategy": bear_strategy.label,
        "neutral_strategy": neutral_strategy.label,
        "trade_count": len(selected_records),
        "selection_counts": selection_counts,
        "entered_counts": entered_counts,
        "overlap_signal_count": overlap_signal_count,
        "total_net_pnl": round(total_net_pnl, 4),
        "total_roi_pct": round(total_net_pnl / STARTING_EQUITY * 100.0, 4),
        "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
        "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
        "win_rate_pct": round(len(wins) / len(selected_records) * 100.0, 4) if selected_records else 0.0,
        "average_win": round(fmean(wins), 4) if wins else 0.0,
        "average_loss": round(fmean(losses), 4) if losses else 0.0,
    }


def _ranking_key(item: dict[str, object]) -> tuple[float, float, float, float, int]:
    return (
        float(item["average_roi_on_margin_pct"]),
        float(item["median_roi_on_margin_pct"]),
        float(item["total_roi_pct"]),
        float(item["win_rate_pct"]),
        int(item["trade_count"]),
    )


def _push_top_result(
    *,
    heap: list[tuple[tuple[float, float, float, float, int], int, dict[str, object]]],
    row: dict[str, object],
    counter: int,
    limit: int,
) -> None:
    entry = (_ranking_key(row), counter, row)
    if len(heap) < limit:
        heapq.heappush(heap, entry)
        return
    if entry[0] > heap[0][0]:
        heapq.heapreplace(heap, entry)


def main() -> int:
    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    latest_available_date = _resolve_latest_available_date(REQUESTED_END_DATE)
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    curve = _load_risk_free_curve(store, start_date=START_DATE, end_date=latest_available_date)
    bundle = _build_bundle(store, symbol=SYMBOL, start_date=START_DATE, end_date=latest_available_date)
    trading_fridays = [
        bar.trade_date
        for bar in bundle.bars
        if START_DATE <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]

    engine = OptionsBacktestEngine()
    candidate_strategies = BULLISH_STRATEGIES + BEARISH_STRATEGIES + NEUTRAL_STRATEGIES
    precomputed: dict[str, dict[object, dict[str, object]]] = {}

    for index, strategy in enumerate(candidate_strategies, start=1):
        trade_map: dict[object, dict[str, object]] = {}
        for entry_date in trading_fridays:
            config = _build_calendar_config(
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
                shared_entry_rule_cache=bundle.entry_rule_cache,
            )
            trade = next((item for item in result.trades if item.entry_date == entry_date), None)
            if trade is None:
                continue
            trade_map[entry_date] = {
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "option_ticker": trade.option_ticker,
                "net_pnl": round(float(trade.net_pnl), 4),
                "roi_on_margin_pct": None if _trade_roi_on_margin_pct(trade) is None else round(_trade_roi_on_margin_pct(trade), 4),
                "exit_reason": trade.exit_reason,
            }
        precomputed[strategy.label] = trade_map
        print(f"[precompute {index}/{len(candidate_strategies)}] {strategy.label}: {len(trade_map)} tradable Fridays")

    bull_filters = [
        FilterConfig(0.0, 10.0, None),
        FilterConfig(0.0, 14.0, None),
        FilterConfig(0.0, 18.0, None),
        FilterConfig(0.0, 14.0, 55.0),
        FilterConfig(0.0, 14.0, 60.0),
        FilterConfig(5.0, 14.0, None),
        FilterConfig(5.0, 18.0, None),
        FilterConfig(5.0, 14.0, 55.0),
        FilterConfig(5.0, 18.0, 60.0),
        FilterConfig(10.0, 14.0, None),
        FilterConfig(10.0, 18.0, None),
        FilterConfig(10.0, 18.0, 60.0),
    ]
    bear_filters = [
        NegativeFilterConfig(0.0, 14.0, None),
        NegativeFilterConfig(0.0, 18.0, None),
        NegativeFilterConfig(0.0, 22.0, None),
        NegativeFilterConfig(0.0, 18.0, 45.0),
        NegativeFilterConfig(0.0, 18.0, 40.0),
        NegativeFilterConfig(-5.0, 18.0, None),
        NegativeFilterConfig(-5.0, 22.0, None),
        NegativeFilterConfig(-5.0, 18.0, 45.0),
        NegativeFilterConfig(-5.0, 22.0, 40.0),
        NegativeFilterConfig(-10.0, 18.0, None),
        NegativeFilterConfig(-10.0, 22.0, None),
        NegativeFilterConfig(-10.0, 18.0, 40.0),
    ]

    indicator_period_configs = [
        IndicatorPeriodConfig(roc_period=roc_period, adx_period=adx_period, rsi_period=rsi_period)
        for roc_period in ROC_PERIODS
        for adx_period in ADX_PERIODS
        for rsi_period in RSI_PERIODS
    ]
    indicators_by_period = {}
    for period_config in indicator_period_configs:
        indicators_by_period[period_config.label] = _load_adjusted_indicators_for_periods(
            symbol=SYMBOL,
            start_date=START_DATE,
            end_date=latest_available_date,
            period_config=period_config,
        )
        print(f"[indicators] {period_config.label}")

    top_ranked_heap: list[tuple[tuple[float, float, float, float, int], int, dict[str, object]]] = []
    best_result: dict[str, object] | None = None
    best_total_roi_result: dict[str, object] | None = None
    combo_count = 0
    total_combos = (
        len(indicator_period_configs)
        * len(bull_filters)
        * len(bear_filters)
        * len(BULLISH_STRATEGIES)
        * len(BEARISH_STRATEGIES)
        * len(NEUTRAL_STRATEGIES)
    )
    row_counter = 0

    for period_config in indicator_period_configs:
        indicators = indicators_by_period[period_config.label]
        for bull_filter in bull_filters:
            bull_signal_by_date = {trade_date: bull_filter.matches(indicators.get(trade_date)) for trade_date in trading_fridays}
            for bear_filter in bear_filters:
                bear_signal_by_date = {trade_date: bear_filter.matches(indicators.get(trade_date)) for trade_date in trading_fridays}
                for bull_strategy in BULLISH_STRATEGIES:
                    bull_map = precomputed[bull_strategy.label]
                    for bear_strategy in BEARISH_STRATEGIES:
                        bear_map = precomputed[bear_strategy.label]
                        for neutral_strategy in NEUTRAL_STRATEGIES:
                            combo_count += 1
                            neutral_map = precomputed[neutral_strategy.label]
                            selection_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
                            entered_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
                            overlap_signal_count = 0
                            selected_records: list[dict[str, object]] = []

                            for trade_date in trading_fridays:
                                bull_signal = bull_signal_by_date[trade_date]
                                bear_signal = bear_signal_by_date[trade_date]
                                if bull_signal and bear_signal:
                                    overlap_signal_count += 1
                                if bull_signal and not bear_signal:
                                    branch = "bullish"
                                    trade_record = bull_map.get(trade_date)
                                elif bear_signal and not bull_signal:
                                    branch = "bearish"
                                    trade_record = bear_map.get(trade_date)
                                else:
                                    branch = "neutral"
                                    trade_record = neutral_map.get(trade_date)
                                selection_counts[branch] += 1
                                if trade_record is None:
                                    continue
                                entered_counts[branch] += 1
                                selected_records.append({**trade_record, "branch": branch})

                            row = _summarize(
                                selected_records=selected_records,
                                selection_counts=selection_counts,
                                entered_counts=entered_counts,
                                overlap_signal_count=overlap_signal_count,
                                indicator_periods=period_config,
                                bull_filter=bull_filter,
                                bear_filter=bear_filter,
                                bull_strategy=bull_strategy,
                                bear_strategy=bear_strategy,
                                neutral_strategy=neutral_strategy,
                            )
                            row_counter += 1
                            if best_result is None or _ranking_key(row) > _ranking_key(best_result):
                                best_result = row
                            if best_total_roi_result is None or float(row["total_roi_pct"]) > float(best_total_roi_result["total_roi_pct"]):
                                best_total_roi_result = row
                            _push_top_result(heap=top_ranked_heap, row=row, counter=row_counter, limit=TOP_RESULT_LIMIT)
                            if combo_count % 1000 == 0 or combo_count == total_combos:
                                best_avg = 0.0 if best_result is None else float(best_result["average_roi_on_margin_pct"])
                                print(f"[grid {combo_count}/{total_combos}] best-so-far avg_roi_margin={best_avg:.4f}")

    ranked = [item[2] for item in sorted(top_ranked_heap, key=lambda entry: entry[0], reverse=True)]

    payload = {
        "symbol": SYMBOL,
        "period": {
            "start": START_DATE.isoformat(),
            "requested_end": REQUESTED_END_DATE.isoformat(),
            "latest_available_date": latest_available_date.isoformat(),
        },
        "indicator_period_search": {
            "roc_periods": list(ROC_PERIODS),
            "adx_periods": list(ADX_PERIODS),
            "rsi_periods": list(RSI_PERIODS),
        },
        "search_space": {
            "bull_filters": [item.label for item in bull_filters],
            "bear_filters": [item.label for item in bear_filters],
            "bullish_strategies": [item.label for item in BULLISH_STRATEGIES],
            "bearish_strategies": [item.label for item in BEARISH_STRATEGIES],
            "neutral_strategies": [item.label for item in NEUTRAL_STRATEGIES],
            "selection_rule": "Bullish if only bullish filter passes, bearish if only bearish filter passes, otherwise neutral.",
        },
        "evaluated_combo_count": combo_count,
        "best_result": best_result,
        "best_result_by_total_roi_pct": best_total_roi_result,
        "top_100_ranked_by_average_roi_on_margin_pct": ranked,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    if best_result is not None:
        print(json.dumps({"best_result": best_result}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
