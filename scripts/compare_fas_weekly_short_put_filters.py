from __future__ import annotations

import argparse
import json
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import func, select

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.strategies.common import (  # noqa: E402
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
    resolve_strike,
    sorted_unique_strikes,
    valid_entry_mids,
)
from backtestforecast.backtests.margin import naked_put_margin  # noqa: E402
from backtestforecast.backtests.types import estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.indicators.calculations import adx, rsi, roc  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.models import (  # noqa: E402
    HistoricalOptionDayBar,
    HistoricalUnderlyingDayBar,
    HistoricalUnderlyingRawDayBar,
)
from backtestforecast.schemas.backtests import StrikeSelection, StrikeSelectionMode  # noqa: E402


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


@dataclass(frozen=True)
class EntryFilterSpec:
    name: str
    description: str

    def allows(self, indicators: dict[str, float | bool | None]) -> bool:
        adx14 = indicators.get("adx14")
        rsi14 = indicators.get("rsi14")
        roc63 = indicators.get("roc63")
        spy_above_ema200 = indicators.get("spy_above_ema200") is True
        xlf_above_ema200 = indicators.get("xlf_above_ema200") is True

        hot_breakout = (
            isinstance(rsi14, float)
            and isinstance(roc63, float)
            and rsi14 > 60.0
            and roc63 > 10.0
        )
        trend_strength = isinstance(adx14, float) and adx14 > 18.0

        if self.name == "none":
            return True
        if self.name == "adx18":
            return trend_strength
        if self.name == "adx18_or_hot_breakout":
            return trend_strength or hot_breakout
        if self.name == "adx18_or_hot_breakout_and_spy_above_ema200":
            return (trend_strength or hot_breakout) and spy_above_ema200
        if self.name == "adx18_or_hot_breakout_and_xlf_above_ema200":
            return (trend_strength or hot_breakout) and xlf_above_ema200
        if self.name == "adx18_or_hot_breakout_and_spy_and_xlf_above_ema200":
            return (trend_strength or hot_breakout) and spy_above_ema200 and xlf_above_ema200
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
    "adx18_or_hot_breakout_and_spy_above_ema200": EntryFilterSpec(
        name="adx18_or_hot_breakout_and_spy_above_ema200",
        description="Sell when the composite filter passes and adjusted SPY closes above EMA200.",
    ),
    "adx18_or_hot_breakout_and_xlf_above_ema200": EntryFilterSpec(
        name="adx18_or_hot_breakout_and_xlf_above_ema200",
        description="Sell when the composite filter passes and adjusted XLF closes above EMA200.",
    ),
    "adx18_or_hot_breakout_and_spy_and_xlf_above_ema200": EntryFilterSpec(
        name="adx18_or_hot_breakout_and_spy_and_xlf_above_ema200",
        description="Sell when the composite filter passes and both adjusted SPY and XLF close above EMA200.",
    ),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare FAS weekly short-put entry filters.")
    parser.add_argument("--symbol", default="FAS")
    parser.add_argument("--start-date", type=_parse_date, default=date(2015, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=date(2026, 12, 31))
    parser.add_argument("--delta-target", type=int, default=25)
    parser.add_argument("--starting-equity", type=float, default=100_000.0)
    parser.add_argument(
        "--margin-compounded",
        action="store_true",
        help=(
            "Size each entry to the maximum whole-number contracts that fit within current "
            "account equity using the naked-put margin requirement."
        ),
    )
    parser.add_argument(
        "--max-margin-utilization",
        type=float,
        default=1.0,
        help=(
            "When --margin-compounded is enabled, cap position sizing to this fraction of "
            "current equity. Example: 0.25, 0.33, 0.50."
        ),
    )
    parser.add_argument(
        "--stop-loss-multiples",
        default="0",
        help=(
            "Comma-separated option stop-loss multiples based on the entry credit. "
            "Example: 0,1.5,2,2.5. A value of 0 disables the stop."
        ),
    )
    parser.add_argument(
        "--profit-take-fractions",
        default="0",
        help=(
            "Comma-separated option profit-take fractions based on the entry credit. "
            "Example: 0,0.5,0.25. A value of 0 disables the profit take. "
            "A value of 0.5 exits when the option mid falls to 50% of the entry credit."
        ),
    )
    parser.add_argument(
        "--filter-modes",
        default="none,adx18,adx18_or_hot_breakout,adx18_or_hot_breakout_and_spy_above_ema200,adx18_or_hot_breakout_and_xlf_above_ema200,adx18_or_hot_breakout_and_spy_and_xlf_above_ema200",
        help="Comma-separated list of filter modes.",
    )
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


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


def _load_close_vs_ema200(symbol: str, start_date: date, end_date: date) -> dict[date, bool]:
    warmup_start = start_date - timedelta(days=450)
    with create_readonly_session() as session:
        rows = session.execute(
            select(
                HistoricalUnderlyingDayBar.trade_date,
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
    closes = [float(row.close_price) for row in rows]
    ema200 = roc_dummy = None
    # Reuse EMA implementation through the imported indicator module.
    from backtestforecast.indicators.calculations import ema  # noqa: WPS433,E402

    ema200 = ema(closes, 200)
    return {
        trade_date: ema200[index] is not None and closes[index] > ema200[index]
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


def _find_last_quote_date(option_ticker: str, entry_date: date, expiration_date: date) -> date | None:
    with create_readonly_session() as session:
        row = session.execute(
            select(func.max(HistoricalOptionDayBar.trade_date)).where(
                HistoricalOptionDayBar.option_ticker == option_ticker,
                HistoricalOptionDayBar.trade_date >= entry_date,
                HistoricalOptionDayBar.trade_date <= expiration_date,
            )
        ).first()
    return None if row is None else row[0]


def _resolve_exit_quote(
    *,
    gateway: HistoricalOptionGateway,
    option_ticker: str,
    entry_date: date,
    expiration_date: date,
    trade_dates: list[date],
    entry_quote_mid: float,
    stop_loss_multiple: float,
    profit_take_fraction: float,
):
    stop_trigger_mid = None
    if stop_loss_multiple > 0:
        stop_trigger_mid = entry_quote_mid * stop_loss_multiple
    profit_take_trigger_mid = None
    if profit_take_fraction > 0:
        profit_take_trigger_mid = entry_quote_mid * profit_take_fraction

    entry_quote = gateway.get_quote(option_ticker, entry_date)
    if entry_quote is None or entry_quote.mid_price is None:
        return None

    last_quote_date = entry_date
    last_quote = entry_quote

    for quote_date in trade_dates:
        if quote_date <= entry_date or quote_date > expiration_date:
            continue
        quote = gateway.get_quote(option_ticker, quote_date)
        if quote is None or quote.mid_price is None:
            continue
        last_quote_date = quote_date
        last_quote = quote
        if profit_take_trigger_mid is not None and float(quote.mid_price) <= profit_take_trigger_mid:
            return last_quote_date, last_quote, "profit_take", stop_trigger_mid, profit_take_trigger_mid
        if stop_trigger_mid is not None and float(quote.mid_price) >= stop_trigger_mid:
            return last_quote_date, last_quote, "stop_loss", stop_trigger_mid, profit_take_trigger_mid

    return last_quote_date, last_quote, "expiration", stop_trigger_mid, profit_take_trigger_mid


def _build_summary(
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
    trade_count = len(trades)
    return {
        "starting_equity": round(starting_equity, 4),
        "ending_equity": round(ending_equity, 4),
        "total_net_pnl": round(ending_equity - starting_equity, 4),
        "total_roi_pct": round(((ending_equity - starting_equity) / starting_equity) * 100.0, 4),
        "trade_count": trade_count,
        "win_rate_pct": round((len(wins) / trade_count * 100.0) if trade_count else 0.0, 4),
        "average_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
        "average_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
        "max_win": round(max(wins), 4) if wins else 0.0,
        "max_loss": round(min(losses), 4) if losses else 0.0,
        "entered_trading_fridays": trade_count,
        "total_trading_fridays": trading_friday_count,
        "holiday_fridays_skipped": holiday_fridays_skipped,
        "latest_available_date": latest_available_date.isoformat(),
    }


def _append_drawdown(summary: dict[str, object], trades: list[dict[str, object]], starting_equity: float) -> None:
    peak_equity = starting_equity
    max_drawdown_pct = 0.0
    for trade in trades:
        equity = float(trade["ending_equity_after_trade"])
        peak_equity = max(peak_equity, equity)
        drawdown_pct = 0.0 if peak_equity <= 0 else ((peak_equity - equity) / peak_equity) * 100.0
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
    summary["max_drawdown_pct"] = round(max_drawdown_pct, 4)


def _build_yearly_breakdown(trades: list[dict[str, object]], starting_equity: float) -> list[dict[str, object]]:
    yearly_pnl: defaultdict[str, float] = defaultdict(float)
    yearly_trades: defaultdict[str, int] = defaultdict(int)
    for trade in trades:
        year = str(trade["entry_date"])[:4]
        yearly_pnl[year] += float(trade["profit_loss"])
        yearly_trades[year] += 1
    rows = []
    for year in sorted(yearly_pnl):
        pnl = yearly_pnl[year]
        rows.append(
            {
                "year": year,
                "trade_count": yearly_trades[year],
                "net_pnl": round(pnl, 4),
                "roi_pct": round((pnl / starting_equity) * 100.0, 4),
            }
        )
    return rows


def run_filter_backtest(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    delta_target: int,
    starting_equity: float,
    filter_spec: EntryFilterSpec,
    stop_loss_multiple: float,
    profit_take_fraction: float,
    margin_compounded: bool,
    max_margin_utilization: float,
) -> dict[str, object]:
    latest_available_date = _resolve_latest_available_date(symbol, end_date)
    raw_close_by_date = _load_raw_closes(symbol, start_date, latest_available_date)
    indicator_by_date = _load_adjusted_indicators(symbol, start_date, latest_available_date)
    spy_above_ema200_by_date = _load_close_vs_ema200("SPY", start_date, latest_available_date)
    xlf_above_ema200_by_date = _load_close_vs_ema200("XLF", start_date, latest_available_date)

    trade_dates = list(raw_close_by_date.keys())
    trading_fridays = [trade_date for trade_date in trade_dates if start_date <= trade_date <= latest_available_date and trade_date.weekday() == 4]
    holiday_fridays_skipped = _holiday_fridays(start_date, latest_available_date, set(trading_fridays))

    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    gateway = HistoricalOptionGateway(store, symbol)
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal(str(delta_target)))

    equity = starting_equity
    trades: list[dict[str, object]] = []
    skipped_for_insufficient_margin = 0

    for entry_date in trading_fridays:
        indicators = indicator_by_date.get(entry_date)
        if indicators is None:
            continue
        enriched_indicators = {
            **indicators,
            "spy_above_ema200": spy_above_ema200_by_date.get(entry_date),
            "xlf_above_ema200": xlf_above_ema200_by_date.get(entry_date),
        }
        if not filter_spec.allows(enriched_indicators):
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
        strike = resolve_strike(
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
        contract = require_contract_for_strike(contracts, strike)
        entry_quote = gateway.get_quote(contract.ticker, entry_date)
        if entry_quote is None or not valid_entry_mids(entry_quote.mid_price):
            continue

        entry_mid = float(entry_quote.mid_price)
        margin_per_contract = naked_put_margin(underlying_close, float(contract.strike_price), entry_mid)
        quantity_contracts = 1
        if margin_compounded:
            deployable_equity = equity * max_margin_utilization
            quantity_contracts = int(deployable_equity // margin_per_contract) if margin_per_contract > 0 else 0
        if quantity_contracts < 1:
            skipped_for_insufficient_margin += 1
            continue

        exit_resolution = _resolve_exit_quote(
            gateway=gateway,
            option_ticker=contract.ticker,
            entry_date=entry_date,
            expiration_date=expiration_date,
            trade_dates=trade_dates,
            entry_quote_mid=entry_mid,
            stop_loss_multiple=stop_loss_multiple,
            profit_take_fraction=profit_take_fraction,
        )
        if exit_resolution is None:
            continue
        exit_date, exit_quote, exit_reason, stop_trigger_mid, profit_take_trigger_mid = exit_resolution

        exit_underlying_close = raw_close_by_date.get(exit_date)
        if exit_underlying_close is None:
            continue

        exit_mid = float(exit_quote.mid_price)
        profit_loss_per_contract = round((entry_mid - exit_mid) * 100.0, 10)
        profit_loss = round(profit_loss_per_contract * quantity_contracts, 10)
        equity = round(equity + profit_loss, 10)
        holding_trading_days = sum(1 for trade_date in trade_dates if entry_date < trade_date <= exit_date)

        trades.append(
            {
                "entry_date": entry_date.isoformat(),
                "expiration_date": expiration_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "entry_underlying_close_raw": round(underlying_close, 4),
                "exit_underlying_close_raw": round(exit_underlying_close, 4),
                "option_ticker": contract.ticker,
                "strike_price": round(float(contract.strike_price), 4),
                "entry_mid": round(entry_mid, 4),
                "exit_mid": round(exit_mid, 4),
                "quantity_contracts": quantity_contracts,
                "margin_per_contract": round(margin_per_contract, 4),
                "capital_reserved": round(margin_per_contract * quantity_contracts, 4),
                "profit_loss_per_contract": round(profit_loss_per_contract, 4),
                "profit_loss": round(profit_loss, 4),
                "holding_period_days": (exit_date - entry_date).days,
                "holding_period_trading_days": holding_trading_days,
                "ending_equity_after_trade": round(equity, 4),
                "estimated_abs_delta_target": float(delta_target),
                "entry_filter": filter_spec.name,
                "stop_loss_multiple": round(stop_loss_multiple, 4),
                "stop_trigger_mid": None if stop_trigger_mid is None else round(float(stop_trigger_mid), 4),
                "profit_take_fraction": round(profit_take_fraction, 4),
                "profit_take_trigger_mid": None if profit_take_trigger_mid is None else round(float(profit_take_trigger_mid), 4),
                "exit_reason": exit_reason,
                "entry_indicators": {
                    "adx14": None if indicators["adx14"] is None else round(float(indicators["adx14"]), 4),
                    "rsi14": None if indicators["rsi14"] is None else round(float(indicators["rsi14"]), 4),
                    "roc63": None if indicators["roc63"] is None else round(float(indicators["roc63"]), 4),
                    "spy_above_ema200": enriched_indicators["spy_above_ema200"],
                    "xlf_above_ema200": enriched_indicators["xlf_above_ema200"],
                },
            }
        )

    summary = _build_summary(
        starting_equity=starting_equity,
        ending_equity=equity,
        trades=trades,
        trading_friday_count=len(trading_fridays),
        holiday_fridays_skipped=holiday_fridays_skipped,
        latest_available_date=latest_available_date,
    )
    _append_drawdown(summary, trades, starting_equity)
    summary["margin_compounded"] = margin_compounded
    summary["max_margin_utilization"] = round(max_margin_utilization, 4)
    summary["skipped_for_insufficient_margin"] = skipped_for_insufficient_margin
    summary["max_contracts_held"] = max((int(trade["quantity_contracts"]) for trade in trades), default=0)

    return {
        "filter_name": filter_spec.name,
        "filter_description": filter_spec.description,
        "stop_loss_multiple": round(stop_loss_multiple, 4),
        "profit_take_fraction": round(profit_take_fraction, 4),
        "margin_compounded": margin_compounded,
        "max_margin_utilization": round(max_margin_utilization, 4),
        "summary": summary,
        "yearly_breakdown": _build_yearly_breakdown(trades, starting_equity),
        "trades": trades,
    }


def _default_output_path(symbol: str, start_date: date, end_date: date) -> Path:
    return ROOT / "logs" / f"{symbol.lower()}_weekly_short_put_delta25_filter_compare_{start_date.isoformat()}_{end_date.isoformat()}.json"


def main() -> int:
    args = build_parser().parse_args()
    symbol = args.symbol.upper()
    if not 0 < args.max_margin_utilization <= 1:
        raise SystemExit("--max-margin-utilization must be between 0 and 1.")
    requested_filters = [item.strip() for item in args.filter_modes.split(",") if item.strip()]
    requested_stop_multiples = [item.strip() for item in args.stop_loss_multiples.split(",") if item.strip()]
    requested_profit_take_fractions = [item.strip() for item in args.profit_take_fractions.split(",") if item.strip()]
    unknown = [name for name in requested_filters if name not in FILTER_SPECS]
    if unknown:
        raise SystemExit(f"Unknown filter mode(s): {', '.join(sorted(unknown))}")
    try:
        stop_loss_multiples = [float(item) for item in requested_stop_multiples]
        profit_take_fractions = [float(item) for item in requested_profit_take_fractions]
    except ValueError as exc:
        raise SystemExit(f"Invalid stop-loss or profit-take value: {exc}") from exc

    output_json = args.output_json or _default_output_path(symbol, args.start_date, args.end_date)
    results = []
    for name in requested_filters:
        for stop_loss_multiple in stop_loss_multiples:
            for profit_take_fraction in profit_take_fractions:
                results.append(
                    run_filter_backtest(
                        symbol=symbol,
                        start_date=args.start_date,
                        end_date=args.end_date,
                        delta_target=args.delta_target,
                        starting_equity=args.starting_equity,
                        filter_spec=FILTER_SPECS[name],
                        stop_loss_multiple=stop_loss_multiple,
                        profit_take_fraction=profit_take_fraction,
                        margin_compounded=args.margin_compounded,
                        max_margin_utilization=args.max_margin_utilization,
                    )
                )

    payload = {
        "symbol": symbol,
        "period": {
            "start": args.start_date.isoformat(),
            "requested_end": args.end_date.isoformat(),
        },
        "strategy": f"sell 1 weekly put every trading Friday close at ~{args.delta_target} absolute delta, expiring the following week",
        "assumptions": [
            "Uses raw FAS bars for spot alignment with option strikes and adjusted FAS bars for indicator filters.",
            "Uses historical option day-bar close as the quote proxy for entry, marking, and exit.",
            "No commissions and no assignment conversion to stock.",
            (
                f"Position size is the maximum whole-number contracts that fit within up to {args.max_margin_utilization:.2%} of current equity using the naked-put margin requirement."
                if args.margin_compounded
                else "Exactly 1 contract per trade, no compounding of contract count."
            ),
            "Entry only on trading Fridays. Holiday Fridays with no session are skipped.",
            "Expiration is the nearest listed put expiration at least 5 calendar days after the entry Friday.",
            "Exits use the earliest of: a configured profit-take on the option close-mid falling to a fraction of the entry credit, a configured stop-loss on the option close-mid rising to a multiple of the entry credit, or the last available quote on or before expiration if neither triggers.",
            f"{args.delta_target} delta is resolved using the repo delta-target strike helper on the actual weekly chain for that expiration.",
        ],
        "results": results,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps([{item["filter_name"]: item["summary"]} for item in results], indent=2))
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
