from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean, median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.schemas.backtests import StrategyType  # noqa: E402
from grid_search_fas_faz_weekly_calendar_policy import (  # noqa: E402
    FAS_BULLISH_STRATEGIES,
    NEUTRAL_STRATEGIES,
    REQUESTED_END_DATE,
    START_DATE,
    FilterConfig,
    StrategyConfig,
    _build_bundle,
    _build_calendar_config,
    _load_adjusted_indicators,
    _load_risk_free_curve,
    _resolve_latest_available_date,
    _trade_roi_on_margin_pct,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

OUTPUT_JSON = ROOT / "logs" / "fas_weekly_calendar_policy_grid_2015_2026.json"
STARTING_EQUITY = 100_000.0


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


BEARISH_STRATEGIES = [
    StrategyConfig("bear_fas_call_d30_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 30, 50),
    StrategyConfig("bear_fas_call_d30_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 30, 75),
    StrategyConfig("bear_fas_call_d40_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("bear_fas_call_d40_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("bear_fas_call_d50_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("bear_fas_call_d50_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 50, 75),
    StrategyConfig("bear_fas_put_d30_pt50", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 30, 50),
    StrategyConfig("bear_fas_put_d30_pt75", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 30, 75),
    StrategyConfig("bear_fas_put_d40_pt50", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 40, 50),
    StrategyConfig("bear_fas_put_d40_pt75", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 40, 75),
    StrategyConfig("bear_fas_put_d50_pt50", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 50, 50),
    StrategyConfig("bear_fas_put_d50_pt75", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 50, 75),
]


def _summarize(
    *,
    selected_records: list[dict[str, object]],
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
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


def main() -> int:
    engine_module.logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    latest_available_date = _resolve_latest_available_date(REQUESTED_END_DATE)
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    curve = _load_risk_free_curve(store, start_date=START_DATE, end_date=latest_available_date)
    bundle = _build_bundle(store, symbol="FAS", start_date=START_DATE, end_date=latest_available_date)
    indicators = _load_adjusted_indicators(symbol="FAS", start_date=START_DATE, end_date=latest_available_date)
    trading_fridays = [
        bar.trade_date
        for bar in bundle.bars
        if START_DATE <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]

    engine = OptionsBacktestEngine()
    candidate_strategies = FAS_BULLISH_STRATEGIES + BEARISH_STRATEGIES + NEUTRAL_STRATEGIES
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

    ranked_rows: list[dict[str, object]] = []
    combo_count = 0
    total_combos = len(bull_filters) * len(bear_filters) * len(FAS_BULLISH_STRATEGIES) * len(BEARISH_STRATEGIES) * len(NEUTRAL_STRATEGIES)

    for bull_filter in bull_filters:
        bull_signal_by_date = {trade_date: bull_filter.matches(indicators.get(trade_date)) for trade_date in trading_fridays}
        for bear_filter in bear_filters:
            bear_signal_by_date = {trade_date: bear_filter.matches(indicators.get(trade_date)) for trade_date in trading_fridays}
            for bull_strategy in FAS_BULLISH_STRATEGIES:
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

                        ranked_rows.append(
                            _summarize(
                                selected_records=selected_records,
                                selection_counts=selection_counts,
                                entered_counts=entered_counts,
                                overlap_signal_count=overlap_signal_count,
                                bull_filter=bull_filter,
                                bear_filter=bear_filter,
                                bull_strategy=bull_strategy,
                                bear_strategy=bear_strategy,
                                neutral_strategy=neutral_strategy,
                            )
                        )
                        if combo_count % 1000 == 0 or combo_count == total_combos:
                            print(f"[grid {combo_count}/{total_combos}] best-so-far avg_roi_margin={max(item['average_roi_on_margin_pct'] for item in ranked_rows):.4f}")

    ranked = sorted(
        ranked_rows,
        key=lambda item: (
            float(item["average_roi_on_margin_pct"]),
            float(item["median_roi_on_margin_pct"]),
            float(item["total_roi_pct"]),
            float(item["win_rate_pct"]),
            int(item["trade_count"]),
        ),
        reverse=True,
    )

    payload = {
        "period": {
            "start": START_DATE.isoformat(),
            "requested_end": REQUESTED_END_DATE.isoformat(),
            "latest_available_date": latest_available_date.isoformat(),
        },
        "search_space": {
            "bull_filters": [item.label for item in bull_filters],
            "bear_filters": [item.label for item in bear_filters],
            "bullish_strategies": [item.label for item in FAS_BULLISH_STRATEGIES],
            "bearish_strategies": [item.label for item in BEARISH_STRATEGIES],
            "neutral_strategies": [item.label for item in NEUTRAL_STRATEGIES],
            "selection_rule": "Bullish if only bullish filter passes, bearish if only bearish filter passes, otherwise neutral.",
        },
        "best_result": ranked[0] if ranked else None,
        "top_50_ranked_by_average_roi_on_margin_pct": ranked[:50],
        "full_ranked_results": ranked,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    if ranked:
        print(json.dumps({"best_result": ranked[0]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
