from __future__ import annotations

import argparse
import json
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median

from sqlalchemy import func, select

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.margin import credit_spread_margin  # noqa: E402
from backtestforecast.backtests.strategies.common import (  # noqa: E402
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    resolve_wing_strike,
    sorted_unique_strikes,
    valid_entry_mids,
)
from backtestforecast.backtests.types import estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import adx, roc, rsi  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.models import (  # noqa: E402
    HistoricalOptionDayBar,
    HistoricalUnderlyingDayBar,
    HistoricalUnderlyingRawDayBar,
)
from backtestforecast.schemas.backtests import (  # noqa: E402
    SpreadWidthConfig,
    SpreadWidthMode,
    StrikeSelection,
    StrikeSelectionMode,
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest weekly FAS bull put credit spreads.")
    parser.add_argument("--symbol", default="FAS")
    parser.add_argument("--start-date", type=_parse_date, default=date(2015, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=date(2015, 12, 31))
    parser.add_argument("--delta-targets", default="20,25,30")
    parser.add_argument(
        "--filter-modes",
        default="none,adx18_or_hot_breakout",
        help="Comma-separated entry filter modes.",
    )
    parser.add_argument(
        "--width-mode",
        choices=[mode.value for mode in SpreadWidthMode],
        default=SpreadWidthMode.STRIKE_STEPS.value,
    )
    parser.add_argument(
        "--width-values",
        default="1,2,3",
        help="Comma-separated spread width values interpreted using --width-mode.",
    )
    parser.add_argument(
        "--profit-take-capture-fractions",
        default="0",
        help=(
            "Comma-separated fractions of entry credit to capture before exiting early. "
            "Example: 0,0.5,0.75 means no profit-take, 50%% capture, and 75%% capture."
        ),
    )
    parser.add_argument(
        "--stop-loss-multiples",
        default="0",
        help=(
            "Comma-separated stop-loss multiples of entry credit. "
            "Example: 0,1.5,2.0 means no stop, 1.5x credit, and 2.0x credit."
        ),
    )
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


@dataclass(frozen=True)
class EntryFilterSpec:
    name: str
    description: str

    def allows(self, indicators: dict[str, float | None] | None) -> bool:
        if self.name == "none":
            return True
        if indicators is None:
            return False

        adx14 = indicators.get("adx14")
        rsi14 = indicators.get("rsi14")
        roc63 = indicators.get("roc63")
        hot_breakout = (
            isinstance(rsi14, float)
            and isinstance(roc63, float)
            and rsi14 > 60.0
            and roc63 > 10.0
        )
        trend_strength = isinstance(adx14, float) and adx14 > 18.0

        if self.name == "adx18":
            return trend_strength
        if self.name == "adx18_or_hot_breakout":
            return trend_strength or hot_breakout
        if self.name == "roc63_pos":
            return isinstance(roc63, float) and roc63 > 0.0
        if self.name == "roc63_pos_and_adx14":
            return (
                isinstance(roc63, float)
                and roc63 > 0.0
                and isinstance(adx14, float)
                and adx14 > 14.0
            )
        if self.name == "roc63_pos_and_adx18_or_rsi60":
            return (
                isinstance(roc63, float)
                and roc63 > 0.0
                and (
                    (isinstance(adx14, float) and adx14 > 18.0)
                    or (isinstance(rsi14, float) and rsi14 > 60.0)
                )
            )
        raise ValueError(f"Unknown filter: {self.name}")


FILTER_SPECS = {
    "none": EntryFilterSpec(
        name="none",
        description="No entry filter. Sell every eligible Friday.",
    ),
    "adx18": EntryFilterSpec(
        name="adx18",
        description="Sell only when adjusted FAS ADX14 > 18.",
    ),
    "adx18_or_hot_breakout": EntryFilterSpec(
        name="adx18_or_hot_breakout",
        description="Sell when ADX14 > 18, or when RSI14 > 60 and ROC63 > 10.",
    ),
    "roc63_pos": EntryFilterSpec(
        name="roc63_pos",
        description="Sell only when adjusted FAS ROC63 is positive.",
    ),
    "roc63_pos_and_adx14": EntryFilterSpec(
        name="roc63_pos_and_adx14",
        description="Sell only when adjusted FAS ROC63 > 0 and ADX14 > 14.",
    ),
    "roc63_pos_and_adx18_or_rsi60": EntryFilterSpec(
        name="roc63_pos_and_adx18_or_rsi60",
        description="Sell only when adjusted FAS ROC63 > 0 and either ADX14 > 18 or RSI14 > 60.",
    ),
}


def _resolve_latest_available_date(symbol: str, requested_end: date) -> date:
    with create_readonly_session() as session:
        latest_raw = session.execute(
            select(func.max(HistoricalUnderlyingRawDayBar.trade_date)).where(HistoricalUnderlyingRawDayBar.symbol == symbol)
        ).scalar_one()
        latest_opt = session.execute(
            select(func.max(HistoricalOptionDayBar.trade_date)).where(HistoricalOptionDayBar.underlying_symbol == symbol)
        ).scalar_one()
    if latest_raw is None or latest_opt is None:
        raise SystemExit(f"Missing raw or option data for {symbol}.")
    return min(latest_raw, latest_opt, requested_end)


def _load_raw_closes(symbol: str, start_date: date, end_date: date) -> OrderedDict[date, float]:
    raw_end = end_date + timedelta(days=14)
    with create_readonly_session() as session:
        rows = session.execute(
            select(
                HistoricalUnderlyingRawDayBar.trade_date,
                HistoricalUnderlyingRawDayBar.close_price,
            )
            .where(
                HistoricalUnderlyingRawDayBar.symbol == symbol,
                HistoricalUnderlyingRawDayBar.trade_date >= start_date,
                HistoricalUnderlyingRawDayBar.trade_date <= raw_end,
            )
            .order_by(HistoricalUnderlyingRawDayBar.trade_date)
        ).all()
    return OrderedDict((row.trade_date, float(row.close_price)) for row in rows)


def _load_adjusted_indicators(symbol: str, start_date: date, end_date: date) -> dict[date, dict[str, float | None]]:
    warmup_start = start_date - timedelta(days=450)
    with create_readonly_session() as session:
        rows = session.execute(
            select(
                HistoricalUnderlyingDayBar.trade_date,
                HistoricalUnderlyingDayBar.high_price,
                HistoricalUnderlyingDayBar.low_price,
                HistoricalUnderlyingDayBar.close_price,
            )
            .where(
                HistoricalUnderlyingDayBar.symbol == symbol,
                HistoricalUnderlyingDayBar.trade_date >= warmup_start,
                HistoricalUnderlyingDayBar.trade_date <= end_date,
            )
            .order_by(HistoricalUnderlyingDayBar.trade_date)
        ).all()
    if not rows:
        raise SystemExit(f"Missing adjusted underlying data for {symbol}.")

    dates = [row.trade_date for row in rows]
    highs = [float(row.high_price) for row in rows]
    lows = [float(row.low_price) for row in rows]
    closes = [float(row.close_price) for row in rows]

    rsi14 = rsi(closes, 14)
    roc63 = roc(closes, 63)
    adx14 = adx(highs, lows, closes, 14)

    return {
        trade_date: {
            "rsi14": rsi14[index],
            "roc63": roc63[index],
            "adx14": adx14[index],
        }
        for index, trade_date in enumerate(dates)
    }


def _holiday_fridays(start_date: date, end_date: date, trading_fridays: set[date]) -> list[str]:
    result: list[str] = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() == 4 and cursor not in trading_fridays:
            result.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return result


def _find_expiration(symbol: str, entry_date: date) -> date | None:
    min_expiration = entry_date + timedelta(days=5)
    with create_readonly_session() as session:
        row = session.execute(
            select(HistoricalOptionDayBar.expiration_date)
            .where(
                HistoricalOptionDayBar.underlying_symbol == symbol,
                HistoricalOptionDayBar.trade_date == entry_date,
                HistoricalOptionDayBar.contract_type == "put",
                HistoricalOptionDayBar.expiration_date >= min_expiration,
            )
            .distinct()
            .order_by(HistoricalOptionDayBar.expiration_date)
            .limit(1)
        ).first()
    return None if row is None else row[0]


def _resolve_exit_quotes(
    *,
    gateway: HistoricalOptionGateway,
    short_ticker: str,
    long_ticker: str,
    entry_date: date,
    expiration_date: date,
    trade_dates: list[date],
    width_per_share: float,
    entry_credit: float,
    profit_take_capture_fraction: float,
    stop_loss_multiple: float,
):
    short_entry_quote = gateway.get_quote(short_ticker, entry_date)
    long_entry_quote = gateway.get_quote(long_ticker, entry_date)
    if short_entry_quote is None or long_entry_quote is None:
        return None
    if short_entry_quote.mid_price is None or long_entry_quote.mid_price is None:
        return None

    last_quote_date = entry_date
    last_short_quote = short_entry_quote
    last_long_quote = long_entry_quote
    profit_take_exit_value = None
    if profit_take_capture_fraction > 0:
        remaining_value = entry_credit * (1.0 - profit_take_capture_fraction)
        profit_take_exit_value = max(remaining_value, 0.0)
    stop_loss_exit_value = None
    if stop_loss_multiple > 0:
        stop_loss_exit_value = min(entry_credit * stop_loss_multiple, width_per_share)

    for quote_date in trade_dates:
        if quote_date <= entry_date or quote_date > expiration_date:
            continue
        short_quote = gateway.get_quote(short_ticker, quote_date)
        long_quote = gateway.get_quote(long_ticker, quote_date)
        if short_quote is None or long_quote is None:
            continue
        if short_quote.mid_price is None or long_quote.mid_price is None:
            continue
        last_quote_date = quote_date
        last_short_quote = short_quote
        last_long_quote = long_quote
        spread_value = _clamp_spread_value(float(short_quote.mid_price) - float(long_quote.mid_price), width_per_share)
        if profit_take_exit_value is not None and spread_value <= profit_take_exit_value:
            return last_quote_date, last_short_quote, last_long_quote, "profit_take"
        if stop_loss_exit_value is not None and spread_value >= stop_loss_exit_value:
            return last_quote_date, last_short_quote, last_long_quote, "stop_loss"

    return last_quote_date, last_short_quote, last_long_quote, "expiration"


def _clamp_spread_value(value_per_share: float, width_per_share: float) -> float:
    return min(max(value_per_share, 0.0), width_per_share)


def _summary_from_trades(
    *,
    starting_equity: float,
    ending_equity: float,
    trades: list[dict[str, object]],
    trading_friday_count: int,
    holiday_fridays_skipped: list[str],
    latest_available_date: date,
) -> dict[str, object]:
    wins = [float(trade["profit_loss"]) for trade in trades if float(trade["profit_loss"]) > 0]
    losses = [float(trade["profit_loss"]) for trade in trades if float(trade["profit_loss"]) < 0]
    roi_on_margin_values = [float(trade["roi_on_margin_pct"]) for trade in trades]
    margins = [float(trade["margin_per_spread"]) for trade in trades]
    peak_equity = starting_equity
    max_drawdown_pct = 0.0
    for trade in trades:
        equity = float(trade["ending_equity_after_trade"])
        peak_equity = max(peak_equity, equity)
        drawdown_pct = 0.0 if peak_equity <= 0 else ((peak_equity - equity) / peak_equity) * 100.0
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)

    total_net_pnl = ending_equity - starting_equity
    peak_margin = max(margins) if margins else 0.0
    average_margin = sum(margins) / len(margins) if margins else 0.0

    return {
        "starting_equity": round(starting_equity, 4),
        "ending_equity": round(ending_equity, 4),
        "total_net_pnl": round(total_net_pnl, 4),
        "total_roi_pct": round((total_net_pnl / starting_equity) * 100.0, 4) if starting_equity else 0.0,
        "trade_count": len(trades),
        "win_rate_pct": round((len(wins) / len(trades) * 100.0), 4) if trades else 0.0,
        "average_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
        "average_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
        "max_win": round(max(wins), 4) if wins else 0.0,
        "max_loss": round(min(losses), 4) if losses else 0.0,
        "average_roi_on_margin_pct": round(sum(roi_on_margin_values) / len(roi_on_margin_values), 4)
        if roi_on_margin_values
        else 0.0,
        "median_roi_on_margin_pct": round(median(roi_on_margin_values), 4) if roi_on_margin_values else 0.0,
        "peak_margin_per_spread": round(peak_margin, 4),
        "average_margin_per_spread": round(average_margin, 4),
        "roi_on_peak_margin_pct": round((total_net_pnl / peak_margin) * 100.0, 4) if peak_margin else 0.0,
        "roi_on_average_margin_pct": round((total_net_pnl / average_margin) * 100.0, 4) if average_margin else 0.0,
        "entered_trading_fridays": len(trades),
        "total_trading_fridays": trading_friday_count,
        "holiday_fridays_skipped": holiday_fridays_skipped,
        "latest_available_date": latest_available_date.isoformat(),
        "max_drawdown_pct": round(max_drawdown_pct, 4),
    }


def _yearly_breakdown(trades: list[dict[str, object]], starting_equity: float) -> list[dict[str, object]]:
    rows: dict[str, dict[str, float | int | list[float]]] = {}
    for trade in trades:
        year = str(trade["entry_date"])[:4]
        bucket = rows.setdefault(
            year,
            {
                "trade_count": 0,
                "net_pnl": 0.0,
                "roi_on_margin": [],
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"]) + 1
        bucket["net_pnl"] = float(bucket["net_pnl"]) + float(trade["profit_loss"])
        cast_list = bucket["roi_on_margin"]
        assert isinstance(cast_list, list)
        cast_list.append(float(trade["roi_on_margin_pct"]))

    result = []
    for year in sorted(rows):
        bucket = rows[year]
        roi_values = bucket["roi_on_margin"]
        assert isinstance(roi_values, list)
        net_pnl = float(bucket["net_pnl"])
        result.append(
            {
                "year": year,
                "trade_count": int(bucket["trade_count"]),
                "net_pnl": round(net_pnl, 4),
                "roi_pct": round((net_pnl / starting_equity) * 100.0, 4) if starting_equity else 0.0,
                "average_roi_on_margin_pct": round(sum(roi_values) / len(roi_values), 4) if roi_values else 0.0,
                "median_roi_on_margin_pct": round(median(roi_values), 4) if roi_values else 0.0,
            }
        )
    return result


def run_backtest(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    delta_target: int,
    width_config: SpreadWidthConfig,
    starting_equity: float,
    filter_spec: EntryFilterSpec,
    profit_take_capture_fraction: float,
    stop_loss_multiple: float,
) -> dict[str, object]:
    latest_available_date = _resolve_latest_available_date(symbol, end_date)
    raw_close_by_date = _load_raw_closes(symbol, start_date, latest_available_date)
    indicator_by_date = _load_adjusted_indicators(symbol, start_date, latest_available_date)
    trade_dates = list(raw_close_by_date.keys())
    trading_fridays = [trade_date for trade_date in trade_dates if start_date <= trade_date <= latest_available_date and trade_date.weekday() == 4]
    holiday_fridays_skipped = _holiday_fridays(start_date, latest_available_date, set(trading_fridays))

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    gateway = HistoricalOptionGateway(store, symbol)
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal(str(delta_target)))

    equity = starting_equity
    trades: list[dict[str, object]] = []

    for entry_date in trading_fridays:
        indicators = indicator_by_date.get(entry_date)
        if not filter_spec.allows(indicators):
            continue

        expiration_date = _find_expiration(symbol, entry_date)
        if expiration_date is None:
            continue

        contracts = gateway.list_contracts_for_expiration(
            entry_date=entry_date,
            contract_type="put",
            expiration_date=expiration_date,
        )
        if not contracts:
            continue

        underlying_close = raw_close_by_date.get(entry_date)
        if underlying_close is None:
            continue

        dte_days = (expiration_date - entry_date).days
        risk_free_rate = estimate_risk_free_rate(entry_date, expiration_date)
        delta_lookup = maybe_build_contract_delta_lookup(
            selection=selection,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=entry_date,
            underlying_close=underlying_close,
            dte_days=dte_days,
            risk_free_rate=risk_free_rate,
            iv_cache=getattr(gateway, "_iv_cache", None),
        )
        strikes = sorted_unique_strikes(contracts)
        short_strike = resolve_strike(
            strikes,
            underlying_close,
            "put",
            selection,
            dte_days,
            delta_lookup=delta_lookup,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=entry_date,
            expiration_date=expiration_date,
            iv_cache=getattr(gateway, "_iv_cache", None),
            risk_free_rate=risk_free_rate,
        )
        long_strike = resolve_wing_strike(
            strikes,
            short_strike,
            -1,
            underlying_close,
            width_config,
        )
        if long_strike is None or long_strike >= short_strike:
            continue

        short_contract = require_contract_for_strike(contracts, short_strike)
        long_contract = require_contract_for_strike(contracts, long_strike)
        short_entry_quote = gateway.get_quote(short_contract.ticker, entry_date)
        long_entry_quote = gateway.get_quote(long_contract.ticker, entry_date)
        if short_entry_quote is None or long_entry_quote is None:
            continue
        if not valid_entry_mids(short_entry_quote.mid_price, long_entry_quote.mid_price):
            continue

        short_entry_mid = float(short_entry_quote.mid_price)
        long_entry_mid = float(long_entry_quote.mid_price)
        width_per_share = abs(float(short_contract.strike_price) - float(long_contract.strike_price))
        entry_credit = _clamp_spread_value(short_entry_mid - long_entry_mid, width_per_share)
        if entry_credit <= 0:
            continue

        exit_resolution = _resolve_exit_quotes(
            gateway=gateway,
            short_ticker=short_contract.ticker,
            long_ticker=long_contract.ticker,
            entry_date=entry_date,
            expiration_date=expiration_date,
            trade_dates=trade_dates,
            width_per_share=width_per_share,
            entry_credit=entry_credit,
            profit_take_capture_fraction=profit_take_capture_fraction,
            stop_loss_multiple=stop_loss_multiple,
        )
        if exit_resolution is None:
            continue
        exit_date, short_exit_quote, long_exit_quote, exit_reason = exit_resolution

        exit_underlying_close = raw_close_by_date.get(exit_date)
        if exit_underlying_close is None:
            continue

        short_exit_mid = float(short_exit_quote.mid_price)
        long_exit_mid = float(long_exit_quote.mid_price)
        exit_cost = _clamp_spread_value(short_exit_mid - long_exit_mid, width_per_share)
        margin_per_spread = credit_spread_margin(width_per_share)
        max_profit_per_spread = entry_credit * 100.0
        max_loss_per_spread = max(margin_per_spread - max_profit_per_spread, 0.0)
        profit_loss = round((entry_credit - exit_cost) * 100.0, 10)
        equity = round(equity + profit_loss, 10)
        holding_trading_days = sum(1 for trade_date in trade_dates if entry_date < trade_date <= exit_date)
        roi_on_margin_pct = (profit_loss / margin_per_spread) * 100.0 if margin_per_spread else 0.0

        trades.append(
            {
                "entry_date": entry_date.isoformat(),
                "expiration_date": expiration_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "entry_underlying_close_raw": round(underlying_close, 4),
                "exit_underlying_close_raw": round(exit_underlying_close, 4),
                "short_option_ticker": short_contract.ticker,
                "long_option_ticker": long_contract.ticker,
                "short_strike_price": round(float(short_contract.strike_price), 4),
                "long_strike_price": round(float(long_contract.strike_price), 4),
                "width_per_share": round(width_per_share, 4),
                "short_entry_mid": round(short_entry_mid, 4),
                "long_entry_mid": round(long_entry_mid, 4),
                "entry_credit": round(entry_credit, 4),
                "short_exit_mid": round(short_exit_mid, 4),
                "long_exit_mid": round(long_exit_mid, 4),
                "exit_cost": round(exit_cost, 4),
                "margin_per_spread": round(margin_per_spread, 4),
                "max_profit_per_spread": round(max_profit_per_spread, 4),
                "max_loss_per_spread": round(max_loss_per_spread, 4),
                "profit_loss": round(profit_loss, 4),
                "roi_on_margin_pct": round(roi_on_margin_pct, 4),
                "holding_period_days": (exit_date - entry_date).days,
                "holding_period_trading_days": holding_trading_days,
                "ending_equity_after_trade": round(equity, 4),
                "entry_filter": filter_spec.name,
                "profit_take_capture_fraction": round(profit_take_capture_fraction, 4),
                "stop_loss_multiple": round(stop_loss_multiple, 4),
                "exit_reason": exit_reason,
                "entry_indicators": None
                if indicators is None
                else {
                    "adx14": None if indicators["adx14"] is None else round(float(indicators["adx14"]), 4),
                    "rsi14": None if indicators["rsi14"] is None else round(float(indicators["rsi14"]), 4),
                    "roc63": None if indicators["roc63"] is None else round(float(indicators["roc63"]), 4),
                },
            }
        )

    return {
        "filter_name": filter_spec.name,
        "filter_description": filter_spec.description,
        "profit_take_capture_fraction": round(profit_take_capture_fraction, 4),
        "stop_loss_multiple": round(stop_loss_multiple, 4),
        "delta_target": delta_target,
        "width_mode": width_config.mode.value,
        "width_value": str(width_config.value),
        "summary": _summary_from_trades(
            starting_equity=starting_equity,
            ending_equity=equity,
            trades=trades,
            trading_friday_count=len(trading_fridays),
            holiday_fridays_skipped=holiday_fridays_skipped,
            latest_available_date=latest_available_date,
        ),
        "yearly_breakdown": _yearly_breakdown(trades, starting_equity),
        "trades": trades,
    }


def _default_output_path(symbol: str, start_date: date, end_date: date) -> Path:
    return ROOT / "logs" / f"{symbol.lower()}_weekly_bull_put_credit_spread_{start_date.isoformat()}_{end_date.isoformat()}.json"


def main() -> int:
    args = build_parser().parse_args()
    symbol = args.symbol.upper()
    try:
        delta_targets = [int(value.strip()) for value in args.delta_targets.split(",") if value.strip()]
        width_values = [Decimal(value.strip()) for value in args.width_values.split(",") if value.strip()]
        profit_take_capture_fractions = [float(value.strip()) for value in args.profit_take_capture_fractions.split(",") if value.strip()]
        stop_loss_multiples = [float(value.strip()) for value in args.stop_loss_multiples.split(",") if value.strip()]
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Invalid delta target, width value, profit-take fraction, or stop-loss multiple: {exc}") from exc
    if not delta_targets:
        raise SystemExit("At least one delta target is required.")
    if not width_values:
        raise SystemExit("At least one width value is required.")
    if not profit_take_capture_fractions:
        raise SystemExit("At least one profit-take capture fraction is required.")
    if any(value < 0 or value > 1 for value in profit_take_capture_fractions):
        raise SystemExit("Profit-take capture fractions must be between 0 and 1.")
    if not stop_loss_multiples:
        raise SystemExit("At least one stop-loss multiple is required.")
    if any(value < 0 for value in stop_loss_multiples):
        raise SystemExit("Stop-loss multiples must be non-negative.")
    requested_filters = [item.strip() for item in args.filter_modes.split(",") if item.strip()]
    unknown_filters = [name for name in requested_filters if name not in FILTER_SPECS]
    if unknown_filters:
        raise SystemExit(f"Unknown filter mode(s): {', '.join(sorted(unknown_filters))}")

    output_json = args.output_json or _default_output_path(symbol, args.start_date, args.end_date)
    results = []
    for filter_name in requested_filters:
        filter_spec = FILTER_SPECS[filter_name]
        for delta_target in delta_targets:
            for width_value in width_values:
                for profit_take_capture_fraction in profit_take_capture_fractions:
                    for stop_loss_multiple in stop_loss_multiples:
                        width_config = SpreadWidthConfig(mode=SpreadWidthMode(args.width_mode), value=width_value)
                        results.append(
                            run_backtest(
                                symbol=symbol,
                                start_date=args.start_date,
                                end_date=args.end_date,
                                delta_target=delta_target,
                                width_config=width_config,
                                starting_equity=args.starting_equity,
                                filter_spec=filter_spec,
                                profit_take_capture_fraction=profit_take_capture_fraction,
                                stop_loss_multiple=stop_loss_multiple,
                            )
                        )

    ranked = sorted(
        results,
        key=lambda item: (
            float(item["summary"]["total_roi_pct"]),
            float(item["summary"]["roi_on_peak_margin_pct"]),
            -float(item["summary"]["max_drawdown_pct"]),
        ),
        reverse=True,
    )
    payload = {
        "symbol": symbol,
        "period": {
            "start": args.start_date.isoformat(),
            "requested_end": args.end_date.isoformat(),
        },
        "strategy": (
            "Sell 1 weekly bull put credit spread every trading Friday close, "
            "using the nearest listed put expiration at least 5 calendar days later."
        ),
        "assumptions": [
            "Uses raw FAS bars for spot alignment with option strikes.",
            "Uses adjusted FAS bars for entry indicators when a filter is enabled.",
            "Uses historical option day-bar close as the quote proxy for entry and exit.",
            "Exactly 1 spread per trade, no commissions, no assignment conversion to stock, and no compounding.",
            "Short strike is chosen by the configured absolute delta target from the live weekly chain.",
            "Long strike is chosen lower in the same expiration using the configured spread width rule.",
            "Each spread exits at the earliest of a configured profit-take based on captured entry credit, a configured stop-loss based on spread debit versus entry credit, or the last date on or before expiration with both option legs quoted.",
        ],
        "results_ranked_by_total_roi": [
            {
                "filter_name": item["filter_name"],
                "profit_take_capture_fraction": item["profit_take_capture_fraction"],
                "stop_loss_multiple": item["stop_loss_multiple"],
                "delta_target": item["delta_target"],
                "width_mode": item["width_mode"],
                "width_value": item["width_value"],
                "summary": item["summary"],
            }
            for item in ranked
        ],
        "results": results,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["results_ranked_by_total_roi"], indent=2))
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
