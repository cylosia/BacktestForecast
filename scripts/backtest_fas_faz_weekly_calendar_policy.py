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

from backtest_fas_weekly_bull_put_spreads import FILTER_SPECS, run_backtest  # noqa: E402
from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, TradeResult, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.service import HistoricalDataBundle  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    SpreadWidthConfig,
    SpreadWidthMode,
    StrategyOverrides,
    StrategyType,
    StrikeSelection,
    StrikeSelectionMode,
)
from run_uvxy_post_2018_rule_book_replay import (  # noqa: E402
    _install_quote_series_expiration_cap,
    _install_single_contract_position_sizing,
)

START_DATE = date(2015, 1, 1)
REQUESTED_END_DATE = date(2026, 4, 2)
STARTING_EQUITY = 100_000.0

OUTPUT_JSON = ROOT / "logs" / "fas_faz_weekly_calendar_policy_2015_2026.json"


@dataclass(frozen=True, slots=True)
class BranchConfig:
    label: str
    symbol: str
    strategy_type: StrategyType
    delta_target: int
    far_leg_target_dte: int
    profit_target_pct: int


FAS_BRANCH = BranchConfig(
    label="fas_bullish",
    symbol="FAS",
    strategy_type=StrategyType.CALENDAR_SPREAD,
    delta_target=40,
    far_leg_target_dte=14,
    profit_target_pct=50,
)

FAZ_BRANCH = BranchConfig(
    label="faz_bullish",
    symbol="FAZ",
    strategy_type=StrategyType.PUT_CALENDAR_SPREAD,
    delta_target=30,
    far_leg_target_dte=14,
    profit_target_pct=75,
)

NEUTRAL_BRANCH = BranchConfig(
    label="neutral",
    symbol="FAS",
    strategy_type=StrategyType.CALENDAR_SPREAD,
    delta_target=50,
    far_leg_target_dte=14,
    profit_target_pct=50,
)


def _resolve_latest_available_date(requested_end: date) -> date:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    latest_dates = []
    for symbol in ("FAS", "FAZ"):
        raw_dates = store.get_underlying_day_bars(symbol, requested_end - timedelta(days=30), requested_end)
        if raw_dates:
            latest_dates.append(max(bar.trade_date for bar in raw_dates))
    if not latest_dates:
        raise SystemExit("Missing underlying data for FAS/FAZ.")
    return min(min(latest_dates), requested_end)


def _load_risk_free_curve(store: HistoricalMarketDataStore, start_date: date, end_date: date) -> RiskFreeRateCurve:
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


def _calendar_config(
    *,
    branch: BranchConfig,
    entry_date: date,
    latest_available_date: date,
    risk_free_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    if branch.strategy_type == StrategyType.CALENDAR_SPREAD:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=branch.far_leg_target_dte,
            short_call_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(branch.delta_target)),
            ),
        )
    else:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=branch.far_leg_target_dte,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(branch.delta_target)),
            ),
        )
    return BacktestConfig(
        symbol=branch.symbol,
        strategy_type=branch.strategy_type.value,
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
        profit_target_pct=float(branch.profit_target_pct),
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


def _summary_from_records(records: list[dict[str, object]]) -> dict[str, object]:
    net_pnls = [float(item["net_pnl"]) for item in records]
    rois = [float(item["roi_on_margin_pct"]) for item in records if item["roi_on_margin_pct"] is not None]
    wins = [value for value in net_pnls if value > 0]
    losses = [value for value in net_pnls if value < 0]
    total_net_pnl = sum(net_pnls)
    ending_equity = STARTING_EQUITY + total_net_pnl
    return {
        "starting_equity": round(STARTING_EQUITY, 4),
        "ending_equity": round(ending_equity, 4),
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


def _yearly_breakdown(records: list[dict[str, object]]) -> list[dict[str, object]]:
    by_year: dict[str, list[dict[str, object]]] = {}
    for record in records:
        year = str(record["entry_date"])[:4]
        by_year.setdefault(year, []).append(record)
    rows = []
    for year in sorted(by_year):
        bucket = by_year[year]
        rois = [float(item["roi_on_margin_pct"]) for item in bucket if item["roi_on_margin_pct"] is not None]
        net_pnl = sum(float(item["net_pnl"]) for item in bucket)
        rows.append(
            {
                "year": year,
                "trade_count": len(bucket),
                "net_pnl": round(net_pnl, 4),
                "roi_pct": round(net_pnl / STARTING_EQUITY * 100.0, 4),
                "average_roi_on_margin_pct": round(fmean(rois), 4) if rois else 0.0,
                "median_roi_on_margin_pct": round(median(rois), 4) if rois else 0.0,
            }
        )
    return rows


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
    risk_free_curve = _load_risk_free_curve(store, START_DATE, latest_available_date)
    bundles = {
        "FAS": _build_bundle(store, symbol="FAS", start_date=START_DATE, end_date=latest_available_date),
        "FAZ": _build_bundle(store, symbol="FAZ", start_date=START_DATE, end_date=latest_available_date),
    }
    engine = OptionsBacktestEngine()

    fas_bps = run_backtest(
        symbol="FAS",
        start_date=START_DATE,
        end_date=latest_available_date,
        delta_target=30,
        width_config=SpreadWidthConfig(mode=SpreadWidthMode.STRIKE_STEPS, value=Decimal("1")),
        starting_equity=STARTING_EQUITY,
        filter_spec=FILTER_SPECS["roc63_pos_and_adx14"],
        profit_take_capture_fraction=0.75,
        stop_loss_multiple=0.0,
    )
    faz_bps = run_backtest(
        symbol="FAZ",
        start_date=START_DATE,
        end_date=latest_available_date,
        delta_target=30,
        width_config=SpreadWidthConfig(mode=SpreadWidthMode.STRIKE_STEPS, value=Decimal("1")),
        starting_equity=STARTING_EQUITY,
        filter_spec=FILTER_SPECS["roc63_pos_and_adx18_or_rsi60"],
        profit_take_capture_fraction=0.75,
        stop_loss_multiple=0.0,
    )

    fas_dates = {date.fromisoformat(trade["entry_date"]) for trade in fas_bps["trades"]}
    faz_dates = {date.fromisoformat(trade["entry_date"]) for trade in faz_bps["trades"]}
    overlap_dates = sorted(fas_dates & faz_dates)

    trading_fridays = [
        bar.trade_date
        for bar in bundles["FAS"].bars
        if START_DATE <= bar.trade_date <= latest_available_date and bar.trade_date.weekday() == 4
    ]

    records: list[dict[str, object]] = []
    branch_selection_counts = {
        FAS_BRANCH.label: 0,
        FAZ_BRANCH.label: 0,
        NEUTRAL_BRANCH.label: 0,
    }
    branch_entered_counts = {
        FAS_BRANCH.label: 0,
        FAZ_BRANCH.label: 0,
        NEUTRAL_BRANCH.label: 0,
    }
    skipped_records: list[dict[str, str]] = []

    for entry_date in trading_fridays:
        if entry_date in fas_dates:
            branch = FAS_BRANCH
        elif entry_date in faz_dates:
            branch = FAZ_BRANCH
        else:
            branch = NEUTRAL_BRANCH
        branch_selection_counts[branch.label] += 1

        config = _calendar_config(
            branch=branch,
            entry_date=entry_date,
            latest_available_date=latest_available_date,
            risk_free_curve=risk_free_curve,
        )
        bundle = bundles[branch.symbol]
        result = engine.run(
            config=config,
            bars=bundle.bars,
            earnings_dates=bundle.earnings_dates,
            ex_dividend_dates=bundle.ex_dividend_dates,
            option_gateway=bundle.option_gateway,
            shared_entry_rule_cache=bundle.entry_rule_cache,
        )
        matching = [trade for trade in result.trades if trade.entry_date == entry_date]
        if not matching:
            skipped_records.append(
                {
                    "entry_date": entry_date.isoformat(),
                    "branch": branch.label,
                    "symbol": branch.symbol,
                    "strategy_type": branch.strategy_type.value,
                }
            )
            continue

        trade = matching[0]
        branch_entered_counts[branch.label] += 1
        records.append(
            {
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "branch": branch.label,
                "symbol": branch.symbol,
                "strategy_type": branch.strategy_type.value,
                "delta_target": branch.delta_target,
                "far_leg_target_dte": branch.far_leg_target_dte,
                "profit_target_pct": branch.profit_target_pct,
                "option_ticker": trade.option_ticker,
                "net_pnl": round(float(trade.net_pnl), 4),
                "roi_on_margin_pct": None
                if _trade_roi_on_margin_pct(trade) is None
                else round(_trade_roi_on_margin_pct(trade), 4),
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
            "fas_bullish_filter_source": "FAS BPS entry dates from roc63_pos_and_adx14 / 30 delta / 1-step / 75% PT",
            "faz_bullish_filter_source": "FAZ BPS entry dates from roc63_pos_and_adx18_or_rsi60 / 30 delta / 1-step / 75% PT",
            "fas_bullish_strategy": {
                "symbol": FAS_BRANCH.symbol,
                "strategy_type": FAS_BRANCH.strategy_type.value,
                "delta_target": FAS_BRANCH.delta_target,
                "far_leg_target_dte": FAS_BRANCH.far_leg_target_dte,
                "profit_target_pct": FAS_BRANCH.profit_target_pct,
            },
            "faz_bullish_strategy": {
                "symbol": FAZ_BRANCH.symbol,
                "strategy_type": FAZ_BRANCH.strategy_type.value,
                "delta_target": FAZ_BRANCH.delta_target,
                "far_leg_target_dte": FAZ_BRANCH.far_leg_target_dte,
                "profit_target_pct": FAZ_BRANCH.profit_target_pct,
            },
            "neutral_strategy": {
                "symbol": NEUTRAL_BRANCH.symbol,
                "strategy_type": NEUTRAL_BRANCH.strategy_type.value,
                "delta_target": NEUTRAL_BRANCH.delta_target,
                "far_leg_target_dte": NEUTRAL_BRANCH.far_leg_target_dte,
                "profit_target_pct": NEUTRAL_BRANCH.profit_target_pct,
            },
        },
        "selection_counts": branch_selection_counts,
        "entered_counts": branch_entered_counts,
        "selected_fas_faz_overlap_dates": [item.isoformat() for item in overlap_dates],
        "summary": _summary_from_records(records),
        "yearly_breakdown": _yearly_breakdown(records),
        "trades": records,
        "skipped_selected_dates": skipped_records,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload["summary"], indent=2))
    print(json.dumps({"selection_counts": branch_selection_counts, "entered_counts": branch_entered_counts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
