from __future__ import annotations

import json
from dataclasses import dataclass
from statistics import fmean, median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.schemas.backtests import StrategyType  # noqa: E402
from grid_search_fas_faz_weekly_calendar_policy import (  # noqa: E402
    REQUESTED_END_DATE,
    ROOT as POLICY_ROOT,
    START_DATE,
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
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402

STARTING_EQUITY = 100_000.0
OUTPUT_JSON = POLICY_ROOT / "logs" / "fas_weekly_calendar_policy_2015_2026.json"


@dataclass(frozen=True, slots=True)
class NegativeFilterConfig:
    roc_threshold: float
    adx_threshold: float
    rsi_threshold: float | None

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


class PositiveFilterConfig(NegativeFilterConfig):
    def matches(self, indicators: dict[str, float | None] | None) -> bool:
        if indicators is None:
            return False
        roc63 = indicators.get("roc63")
        adx14 = indicators.get("adx14")
        rsi14 = indicators.get("rsi14")
        if not isinstance(roc63, float) or roc63 <= self.roc_threshold:
            return False
        adx_ok = isinstance(adx14, float) and adx14 > self.adx_threshold
        if self.rsi_threshold is None:
            return adx_ok
        rsi_ok = isinstance(rsi14, float) and rsi14 > self.rsi_threshold
        return adx_ok or rsi_ok


BULLISH_FILTER = PositiveFilterConfig(5.0, 18.0, 60.0)
BEARISH_FILTER = NegativeFilterConfig(-5.0, 22.0, 40.0)

BULLISH_STRATEGY = StrategyConfig("fas_call_d50_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 50, 50)
BEARISH_STRATEGY = StrategyConfig("bear_fas_put_d50_pt50", "FAS", StrategyType.PUT_CALENDAR_SPREAD, 50, 50)
NEUTRAL_STRATEGY = StrategyConfig("neutral_fas_call_d40_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 40, 50)


def _summarize(records: list[dict[str, object]]) -> dict[str, object]:
    pnls = [float(item["net_pnl"]) for item in records]
    rois = [float(item["roi_on_margin_pct"]) for item in records if item["roi_on_margin_pct"] is not None]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    total_net_pnl = sum(pnls)
    return {
        "starting_equity": STARTING_EQUITY,
        "ending_equity": round(STARTING_EQUITY + total_net_pnl, 4),
        "total_net_pnl": round(total_net_pnl, 4),
        "total_roi_pct": round(total_net_pnl / STARTING_EQUITY * 100.0, 4),
        "trade_count": len(records),
        "win_rate_pct": round(len(wins) / len(records) * 100.0, 4) if records else 0.0,
        "average_win": round(fmean(wins), 4) if wins else 0.0,
        "average_loss": round(fmean(losses), 4) if losses else 0.0,
        "max_win": round(max(wins), 4) if wins else 0.0,
        "max_loss": round(min(losses), 4) if losses else 0.0,
        "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
        "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
    }


def _yearly(records: list[dict[str, object]]) -> list[dict[str, object]]:
    buckets: dict[str, list[dict[str, object]]] = {}
    for record in records:
        buckets.setdefault(str(record["entry_date"])[:4], []).append(record)
    rows = []
    for year in sorted(buckets):
        bucket = buckets[year]
        rois = [float(item["roi_on_margin_pct"]) for item in bucket if item["roi_on_margin_pct"] is not None]
        pnl = sum(float(item["net_pnl"]) for item in bucket)
        rows.append(
            {
                "year": year,
                "trade_count": len(bucket),
                "net_pnl": round(pnl, 4),
                "roi_pct": round(pnl / STARTING_EQUITY * 100.0, 4),
                "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
                "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
            }
        )
    return rows


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
    records: list[dict[str, object]] = []
    selection_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    entered_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    overlap_signal_dates: list[str] = []
    skipped_selected_dates: list[dict[str, str]] = []

    for entry_date in trading_fridays:
        signal = indicators.get(entry_date)
        bull = BULLISH_FILTER.matches(signal)
        bear = BEARISH_FILTER.matches(signal)
        if bull and bear:
            overlap_signal_dates.append(entry_date.isoformat())
        if bull and not bear:
            branch = "bullish"
            strategy = BULLISH_STRATEGY
        elif bear and not bull:
            branch = "bearish"
            strategy = BEARISH_STRATEGY
        else:
            branch = "neutral"
            strategy = NEUTRAL_STRATEGY
        selection_counts[branch] += 1

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
            skipped_selected_dates.append(
                {
                    "entry_date": entry_date.isoformat(),
                    "branch": branch,
                    "symbol": "FAS",
                    "strategy_type": strategy.strategy_type.value,
                }
            )
            continue

        entered_counts[branch] += 1
        records.append(
            {
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "branch": branch,
                "symbol": "FAS",
                "strategy_type": strategy.strategy_type.value,
                "delta_target": strategy.delta_target,
                "far_leg_target_dte": 14,
                "profit_target_pct": strategy.profit_target_pct,
                "option_ticker": trade.option_ticker,
                "net_pnl": round(float(trade.net_pnl), 4),
                "roi_on_margin_pct": None if _trade_roi_on_margin_pct(trade) is None else round(_trade_roi_on_margin_pct(trade), 4),
                "entry_reason": trade.entry_reason,
                "exit_reason": trade.exit_reason,
            }
        )

    payload = {
        "period": {
            "start": START_DATE.isoformat(),
            "requested_end": REQUESTED_END_DATE.isoformat(),
            "latest_available_date": latest_available_date.isoformat(),
        },
        "policy": {
            "bullish_filter": {"roc63_gt": 5.0, "adx14_gt": 18.0, "rsi14_gt": 60.0},
            "bearish_filter": {"roc63_lt": -5.0, "adx14_gt": 22.0, "rsi14_lt": 40.0},
            "bullish_strategy": {"symbol": "FAS", "strategy_type": BULLISH_STRATEGY.strategy_type.value, "delta_target": 50, "far_leg_target_dte": 14, "profit_target_pct": 50},
            "bearish_strategy": {"symbol": "FAS", "strategy_type": BEARISH_STRATEGY.strategy_type.value, "delta_target": 50, "far_leg_target_dte": 14, "profit_target_pct": 50},
            "neutral_strategy": {"symbol": "FAS", "strategy_type": NEUTRAL_STRATEGY.strategy_type.value, "delta_target": 40, "far_leg_target_dte": 14, "profit_target_pct": 50},
        },
        "selection_counts": selection_counts,
        "entered_counts": entered_counts,
        "overlap_signal_dates": overlap_signal_dates,
        "summary": _summarize(records),
        "yearly_breakdown": _yearly(records),
        "trades": records,
        "skipped_selected_dates": skipped_selected_dates,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload["summary"], indent=2))
    print(json.dumps({"selection_counts": selection_counts, "entered_counts": entered_counts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
