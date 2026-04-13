from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import fmean, median
from types import SimpleNamespace

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, TradeResult, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import adx, roc, rsi  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.service import HistoricalDataBundle  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402
from backtestforecast.schemas.backtests import StrategyOverrides, StrategyType, StrikeSelection, StrikeSelectionMode  # noqa: E402
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

START_DATE = date(2015, 1, 1)
REQUESTED_END_DATE = date(2026, 4, 2)
STARTING_EQUITY = 100_000.0
FAR_LEG_TARGET_DTE = 14

OUTPUT_JSON = ROOT / "logs" / "fas_faz_weekly_calendar_policy_grid_2015_2026.json"


@dataclass(frozen=True, slots=True)
class FilterConfig:
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
        if not isinstance(roc63, float) or roc63 <= self.roc_threshold:
            return False
        adx_ok = isinstance(adx14, float) and adx14 > self.adx_threshold
        if self.rsi_threshold is None:
            return adx_ok
        rsi_ok = isinstance(rsi14, float) and rsi14 > self.rsi_threshold
        return adx_ok or rsi_ok


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    label: str
    symbol: str
    strategy_type: StrategyType
    delta_target: int
    profit_target_pct: int


FAS_BULLISH_STRATEGIES = [
    StrategyConfig("fas_call_d40_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("fas_call_d40_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("fas_call_d50_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("fas_call_d50_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 50, 75),
]

FAZ_BULLISH_STRATEGIES = [
    StrategyConfig("faz_put_d30_pt50", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 30, 50),
    StrategyConfig("faz_put_d30_pt75", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 30, 75),
    StrategyConfig("faz_put_d40_pt50", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 40, 50),
    StrategyConfig("faz_put_d40_pt75", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 40, 75),
    StrategyConfig("faz_put_d50_pt50", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 50, 50),
    StrategyConfig("faz_put_d50_pt75", "FAZ", StrategyType.PUT_CALENDAR_SPREAD, 50, 75),
]

NEUTRAL_STRATEGIES = [
    StrategyConfig("neutral_fas_call_d40_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 40, 50),
    StrategyConfig("neutral_fas_call_d40_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 40, 75),
    StrategyConfig("neutral_fas_call_d50_pt50", "FAS", StrategyType.CALENDAR_SPREAD, 50, 50),
    StrategyConfig("neutral_fas_call_d50_pt75", "FAS", StrategyType.CALENDAR_SPREAD, 50, 75),
]


def _resolve_latest_available_date(requested_end: date) -> date:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    latest_dates: list[date] = []
    for symbol in ("FAS", "FAZ"):
        bars = store.get_underlying_day_bars(symbol, requested_end - timedelta(days=30), requested_end)
        if bars:
            latest_dates.append(max(bar.trade_date for bar in bars))
    if not latest_dates:
        raise SystemExit("Missing underlying bars for FAS/FAZ.")
    return min(min(latest_dates), requested_end)


def _load_risk_free_curve(
    store: HistoricalMarketDataStore,
    *,
    start_date: date,
    end_date: date,
) -> RiskFreeRateCurve:
    local_series = store.get_treasury_yield_series(start_date, end_date)
    default_rate = store.get_average_treasury_yield(start_date, start_date)
    if default_rate is None:
        default_rate = estimate_risk_free_rate(start_date, end_date)
    if local_series:
        ordered_dates = tuple(sorted(local_series))
        ordered_rates = tuple(float(local_series[trade_date]) for trade_date in ordered_dates)
        return RiskFreeRateCurve(
            default_rate=float(default_rate),
            dates=ordered_dates,
            rates=ordered_rates,
        )
    return RiskFreeRateCurve(default_rate=float(default_rate))


def _build_bundle(
    store: HistoricalMarketDataStore,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> HistoricalDataBundle:
    warmup_start = start_date - timedelta(days=210 * 3)
    return HistoricalDataBundle(
        bars=store.get_underlying_day_bars(symbol, warmup_start, end_date),
        earnings_dates=store.list_earnings_event_dates(symbol, warmup_start, end_date),
        ex_dividend_dates=store.list_ex_dividend_dates(symbol, warmup_start, end_date),
        option_gateway=HistoricalOptionGateway(store, symbol),
        data_source="local",
        entry_rule_cache=EntryRuleComputationCache(),
    )


def _load_adjusted_indicators(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> dict[date, dict[str, float | None]]:
    warmup_start = start_date - timedelta(days=450)
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

    roc63 = roc(closes, 63)
    adx14 = adx(highs, lows, closes, 14)
    rsi14 = rsi(closes, 14)

    return {
        trade_date: {
            "roc63": roc63[index],
            "adx14": adx14[index],
            "rsi14": rsi14[index],
        }
        for index, trade_date in enumerate(dates)
    }


def _build_calendar_config(
    *,
    strategy: StrategyConfig,
    entry_date: date,
    latest_available_date: date,
    risk_free_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    if strategy.strategy_type == StrategyType.CALENDAR_SPREAD:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=FAR_LEG_TARGET_DTE,
            short_call_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    else:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=FAR_LEG_TARGET_DTE,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    return BacktestConfig(
        symbol=strategy.symbol,
        strategy_type=strategy.strategy_type.value,
        start_date=entry_date,
        end_date=min(latest_available_date, entry_date + timedelta(days=35)),
        target_dte=7,
        dte_tolerance_days=3,
        max_holding_days=10,
        account_size=Decimal(str(STARTING_EQUITY)),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        risk_free_rate=risk_free_curve.default_rate,
        risk_free_rate_curve=risk_free_curve,
        dividend_yield=0.0,
        slippage_pct=0.0,
        strategy_overrides=overrides,
        profit_target_pct=float(strategy.profit_target_pct),
        stop_loss_pct=None,
    )


def _trade_roi_on_margin_pct(trade: TradeResult) -> float | None:
    capital_required = trade.detail_json.get("capital_required_per_unit")
    if capital_required is None:
        return None
    total_capital = float(capital_required) * float(trade.quantity or 1)
    if total_capital <= 0:
        return None
    return float(trade.net_pnl) / total_capital * 100.0


def _summarize_policy(
    *,
    selected_records: list[dict[str, object]],
    selection_counts: dict[str, int],
    entered_counts: dict[str, int],
    overlap_signal_count: int,
    fas_filter: FilterConfig,
    faz_filter: FilterConfig,
    fas_strategy: StrategyConfig,
    faz_strategy: StrategyConfig,
    neutral_strategy: StrategyConfig,
) -> dict[str, object]:
    net_pnls = [float(item["net_pnl"]) for item in selected_records]
    rois = [float(item["roi_on_margin_pct"]) for item in selected_records if item["roi_on_margin_pct"] is not None]
    wins = [value for value in net_pnls if value > 0]
    losses = [value for value in net_pnls if value < 0]
    total_net_pnl = sum(net_pnls)
    return {
        "fas_filter": fas_filter.label,
        "faz_filter": faz_filter.label,
        "fas_strategy": fas_strategy.label,
        "faz_strategy": faz_strategy.label,
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
    engine_module.logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )
    _install_quote_series_expiration_cap()
    _install_single_contract_position_sizing()

    latest_available_date = _resolve_latest_available_date(REQUESTED_END_DATE)
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    risk_free_curve = _load_risk_free_curve(store, start_date=START_DATE, end_date=latest_available_date)

    bundles = {
        "FAS": _build_bundle(store, symbol="FAS", start_date=START_DATE, end_date=latest_available_date),
        "FAZ": _build_bundle(store, symbol="FAZ", start_date=START_DATE, end_date=latest_available_date),
    }
    indicators_by_symbol = {
        "FAS": _load_adjusted_indicators(symbol="FAS", start_date=START_DATE, end_date=latest_available_date),
        "FAZ": _load_adjusted_indicators(symbol="FAZ", start_date=START_DATE, end_date=latest_available_date),
    }

    trading_fridays = [
        bar.trade_date
        for bar in bundles["FAS"].bars
        if START_DATE <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]

    engine = OptionsBacktestEngine()
    candidate_strategies = FAS_BULLISH_STRATEGIES + FAZ_BULLISH_STRATEGIES + NEUTRAL_STRATEGIES
    precomputed: dict[str, dict[date, dict[str, object]]] = {}

    for index, strategy in enumerate(candidate_strategies, start=1):
        trade_map: dict[date, dict[str, object]] = {}
        bundle = bundles[strategy.symbol]
        for entry_date in trading_fridays:
            config = _build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                latest_available_date=latest_available_date,
                risk_free_curve=risk_free_curve,
            )
            result = engine.run(
                config=config,
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                ex_dividend_dates=bundle.ex_dividend_dates,
                option_gateway=bundle.option_gateway,
                shared_entry_rule_cache=bundle.entry_rule_cache,
            )
            matching = [trade for trade in result.trades if trade.entry_date == entry_date]
            if matching:
                trade = matching[0]
                trade_map[entry_date] = {
                    "entry_date": trade.entry_date.isoformat(),
                    "exit_date": trade.exit_date.isoformat(),
                    "option_ticker": trade.option_ticker,
                    "net_pnl": round(float(trade.net_pnl), 4),
                    "roi_on_margin_pct": None
                    if _trade_roi_on_margin_pct(trade) is None
                    else round(_trade_roi_on_margin_pct(trade), 4),
                    "exit_reason": trade.exit_reason,
                }
        precomputed[strategy.label] = trade_map
        print(f"[precompute {index}/{len(candidate_strategies)}] {strategy.label}: {len(trade_map)} tradable Fridays")

    fas_filters = [
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
    faz_filters = [
        FilterConfig(0.0, 14.0, None),
        FilterConfig(0.0, 18.0, None),
        FilterConfig(0.0, 22.0, None),
        FilterConfig(0.0, 18.0, 60.0),
        FilterConfig(0.0, 18.0, 65.0),
        FilterConfig(5.0, 18.0, None),
        FilterConfig(5.0, 22.0, None),
        FilterConfig(5.0, 18.0, 60.0),
        FilterConfig(5.0, 22.0, 65.0),
        FilterConfig(10.0, 18.0, None),
        FilterConfig(10.0, 22.0, None),
        FilterConfig(10.0, 18.0, 60.0),
    ]

    ranked_rows: list[dict[str, object]] = []
    combo_count = 0
    total_combos = len(fas_filters) * len(faz_filters) * len(FAS_BULLISH_STRATEGIES) * len(FAZ_BULLISH_STRATEGIES) * len(NEUTRAL_STRATEGIES)

    for fas_filter in fas_filters:
        for faz_filter in faz_filters:
            fas_signal_by_date = {
                trade_date: fas_filter.matches(indicators_by_symbol["FAS"].get(trade_date))
                for trade_date in trading_fridays
            }
            faz_signal_by_date = {
                trade_date: faz_filter.matches(indicators_by_symbol["FAZ"].get(trade_date))
                for trade_date in trading_fridays
            }
            for fas_strategy in FAS_BULLISH_STRATEGIES:
                fas_map = precomputed[fas_strategy.label]
                for faz_strategy in FAZ_BULLISH_STRATEGIES:
                    faz_map = precomputed[faz_strategy.label]
                    for neutral_strategy in NEUTRAL_STRATEGIES:
                        combo_count += 1
                        neutral_map = precomputed[neutral_strategy.label]
                        selection_counts = {"fas_bullish": 0, "faz_bullish": 0, "neutral": 0}
                        entered_counts = {"fas_bullish": 0, "faz_bullish": 0, "neutral": 0}
                        overlap_signal_count = 0
                        selected_records: list[dict[str, object]] = []

                        for trade_date in trading_fridays:
                            fas_signal = fas_signal_by_date[trade_date]
                            faz_signal = faz_signal_by_date[trade_date]
                            if fas_signal and faz_signal:
                                overlap_signal_count += 1
                            if fas_signal and not faz_signal:
                                branch = "fas_bullish"
                                trade_record = fas_map.get(trade_date)
                            elif faz_signal and not fas_signal:
                                branch = "faz_bullish"
                                trade_record = faz_map.get(trade_date)
                            else:
                                branch = "neutral"
                                trade_record = neutral_map.get(trade_date)
                            selection_counts[branch] += 1
                            if trade_record is None:
                                continue
                            entered_counts[branch] += 1
                            selected_records.append(
                                {
                                    **trade_record,
                                    "branch": branch,
                                }
                            )

                        summary = _summarize_policy(
                            selected_records=selected_records,
                            selection_counts=selection_counts,
                            entered_counts=entered_counts,
                            overlap_signal_count=overlap_signal_count,
                            fas_filter=fas_filter,
                            faz_filter=faz_filter,
                            fas_strategy=fas_strategy,
                            faz_strategy=faz_strategy,
                            neutral_strategy=neutral_strategy,
                        )
                        ranked_rows.append(summary)
                        if combo_count % 1000 == 0 or combo_count == total_combos:
                            print(
                                f"[grid {combo_count}/{total_combos}] "
                                f"best-so-far avg_roi_margin={max(item['average_roi_on_margin_pct'] for item in ranked_rows):.4f}"
                            )

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
            "fas_filters": [item.label for item in fas_filters],
            "faz_filters": [item.label for item in faz_filters],
            "fas_bullish_strategies": [item.label for item in FAS_BULLISH_STRATEGIES],
            "faz_bullish_strategies": [item.label for item in FAZ_BULLISH_STRATEGIES],
            "neutral_strategies": [item.label for item in NEUTRAL_STRATEGIES],
            "selection_rule": "FAS if only FAS filter passes, FAZ if only FAZ filter passes, otherwise neutral.",
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
