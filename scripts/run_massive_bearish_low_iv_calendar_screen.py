from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

from sqlalchemy import text

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import backtestforecast.backtests.engine as engine_module  # noqa: E402
from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, RiskFreeRateCurve, estimate_risk_free_rate  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.integrations.massive_client import AsyncMassiveClient, MassiveClient  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.pipeline.regime import Regime, classify_regime  # noqa: E402
from backtestforecast.pipeline.scoring import compute_backtest_score  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
)


BACKTEST_START = date(2022, 4, 1)
BACKTEST_END = date(2026, 3, 31)
WARMUP_CALENDAR_DAYS = 210 * 3
RECENT_LOCAL_BAR_LIMIT = 260
ACCOUNT_SIZE = Decimal("10000")
RISK_PER_TRADE_PCT = Decimal("5")
COMMISSION_PER_CONTRACT = Decimal("0.65")
SLIPPAGE_PCT = 0.0
DEFAULT_CHAIN_CONCURRENCY = 8
DEFAULT_CHAIN_PAGE_LIMIT = 400


@dataclass(frozen=True, slots=True)
class LiveRegimeRow:
    symbol: str
    live_bar_date: date
    close_price: float
    rsi_14: float
    ema_8: float
    ema_21: float
    sma_50: float
    sma_200: float
    realized_vol_20: float | None
    iv_rank_proxy: float | None
    volume_ratio: float | None
    regimes: str
    has_local_option_history: bool


@dataclass(frozen=True, slots=True)
class LiveCalendarCandidate:
    symbol: str
    live_bar_date: date
    live_chain_date: date
    contract_type: str
    strategy_type: str
    live_spot_price: float
    live_close_price: float
    strike_price: float
    short_expiration: date
    long_expiration: date
    short_dte: int
    long_dte: int
    dte_gap: int
    short_ticker: str
    long_ticker: str
    short_bid: float
    short_ask: float
    short_mid: float
    long_bid: float
    long_ask: float
    long_mid: float
    short_delta: float | None
    long_delta: float | None
    short_open_interest: int | None
    long_open_interest: int | None
    natural_debit: float
    estimated_mid_debit: float


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Use Massive current data to find symbols currently in a target regime bucket, "
            "enumerate cheap ATM call/put calendars, and backtest those live analogs "
            "against the historical DB."
        )
    )
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--backtest-start", type=date.fromisoformat, default=BACKTEST_START)
    parser.add_argument("--backtest-end", type=date.fromisoformat, default=BACKTEST_END)
    parser.add_argument(
        "--direction-regime",
        choices=["bullish", "bearish", "neutral"],
        default="bearish",
    )
    parser.add_argument(
        "--volatility-regime",
        choices=["low_iv", "high_iv", "none", "any"],
        default="low_iv",
    )
    parser.add_argument("--chain-concurrency", type=int, default=DEFAULT_CHAIN_CONCURRENCY)
    parser.add_argument("--chain-max-pages", type=int, default=DEFAULT_CHAIN_PAGE_LIMIT)
    parser.add_argument("--symbol-limit", type=int, default=None)
    parser.add_argument("--skip-backtests", action="store_true")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "logs" / "analysis",
    )
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _suppress_engine_info_logs() -> None:
    existing_logger = engine_module.logger
    engine_module.logger = type(
        "SilentEngineLogger",
        (),
        {
            "info": staticmethod(lambda *args, **kwargs: None),
            "warning": staticmethod(getattr(existing_logger, "warning", lambda *args, **kwargs: None)),
            "debug": staticmethod(getattr(existing_logger, "debug", lambda *args, **kwargs: None)),
        },
    )()


def _fetch_grouped_bars_for_date(client: MassiveClient, trade_date: date) -> dict[str, DailyBar]:
    payload = client._get_json(
        f"/v2/aggs/grouped/locale/us/market/stocks/{trade_date.isoformat()}",
        params={"adjusted": "true"},
    )
    rows = payload.get("results", [])
    if not isinstance(rows, list):
        return {}
    bars: dict[str, DailyBar] = {}
    for row in rows:
        symbol = row.get("T")
        if not isinstance(symbol, str) or not symbol.strip():
            continue
        open_price = _safe_float(row.get("o"))
        high_price = _safe_float(row.get("h"))
        low_price = _safe_float(row.get("l"))
        close_price = _safe_float(row.get("c"))
        volume = _safe_float(row.get("v"))
        if None in {open_price, high_price, low_price, close_price, volume}:
            continue
        bars[symbol.strip().upper()] = DailyBar(
            trade_date=trade_date,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
        )
    return bars


def _latest_completed_massive_bar_date(
    client: MassiveClient,
    *,
    as_of_date: date,
    max_lookback_days: int = 7,
) -> tuple[date, dict[str, DailyBar]]:
    for offset in range(1, max_lookback_days + 1):
        candidate = as_of_date - timedelta(days=offset)
        bars = _fetch_grouped_bars_for_date(client, candidate)
        if bars:
            return candidate, bars
    bars = _fetch_grouped_bars_for_date(client, as_of_date)
    if bars:
        return as_of_date, bars
    raise RuntimeError("Massive did not return any grouped daily stock bars in the lookback window.")


def _load_recent_local_histories(max_bars_per_symbol: int) -> tuple[date, dict[str, list[DailyBar]]]:
    with create_readonly_session() as session:
        local_latest_date = session.execute(
            text("SELECT max(trade_date) FROM historical_underlying_day_bars")
        ).scalar_one()
        session.execute(text("SET LOCAL statement_timeout = '300000'"))
        rows = session.execute(
            text(
                """
                WITH latest_symbols AS (
                    SELECT symbol
                    FROM historical_underlying_day_bars
                    WHERE trade_date = :local_latest_date
                )
                SELECT ls.symbol,
                       hub.trade_date,
                       hub.open_price,
                       hub.high_price,
                       hub.low_price,
                       hub.close_price,
                       hub.volume
                FROM latest_symbols ls
                CROSS JOIN LATERAL (
                    SELECT trade_date,
                           open_price,
                           high_price,
                           low_price,
                           close_price,
                           volume
                    FROM historical_underlying_day_bars hub
                    WHERE hub.symbol = ls.symbol
                    ORDER BY hub.trade_date DESC
                    LIMIT :max_bars
                ) hub
                ORDER BY ls.symbol, hub.trade_date
                """
            ),
            {"local_latest_date": local_latest_date, "max_bars": max_bars_per_symbol},
        ).all()

    histories: dict[str, list[DailyBar]] = defaultdict(list)
    for row in rows:
        histories[row.symbol].append(
            DailyBar(
                trade_date=row.trade_date,
                open_price=float(row.open_price),
                high_price=float(row.high_price),
                low_price=float(row.low_price),
                close_price=float(row.close_price),
                volume=float(row.volume),
            )
        )
    return local_latest_date, histories


def _load_local_option_history_symbols() -> tuple[date, set[str]]:
    with create_readonly_session() as session:
        option_latest_date = session.execute(
            text("SELECT max(trade_date) FROM historical_option_day_bars")
        ).scalar_one()
        rows = session.execute(
            text(
                """
                SELECT DISTINCT underlying_symbol
                FROM historical_option_day_bars
                WHERE trade_date = :option_latest_date
                """
            ),
            {"option_latest_date": option_latest_date},
        ).all()
    return option_latest_date, {str(row.underlying_symbol).upper() for row in rows}


def _merge_live_bar(
    bars: list[DailyBar],
    live_bar: DailyBar,
) -> list[DailyBar]:
    merged = [bar for bar in bars if bar.trade_date != live_bar.trade_date]
    merged.append(live_bar)
    merged.sort(key=lambda item: item.trade_date)
    return merged


def _regime_bucket_label(direction_regime: str, volatility_regime: str) -> str:
    return f"{direction_regime}_{volatility_regime}"


def _matches_regime_bucket(
    regimes: frozenset[Regime],
    *,
    direction_regime: str,
    volatility_regime: str,
) -> bool:
    direction_value = Regime(direction_regime)
    if direction_value not in regimes:
        return False
    if volatility_regime == "any":
        return True
    if volatility_regime == "none":
        return Regime.LOW_IV not in regimes and Regime.HIGH_IV not in regimes
    return Regime(volatility_regime) in regimes


def _classify_symbols_for_regime_bucket(
    *,
    live_bar_date: date,
    live_bars: dict[str, DailyBar],
    local_histories: dict[str, list[DailyBar]],
    option_history_symbols: set[str],
    direction_regime: str,
    volatility_regime: str,
) -> tuple[list[LiveRegimeRow], list[str]]:
    rows: list[LiveRegimeRow] = []
    skipped_symbols: list[str] = []
    for symbol, live_bar in sorted(live_bars.items()):
        history = local_histories.get(symbol)
        if not history:
            skipped_symbols.append(symbol)
            continue
        merged_history = _merge_live_bar(history, live_bar) if history[-1].trade_date < live_bar_date else history
        snapshot = classify_regime(symbol, merged_history)
        if snapshot is None:
            skipped_symbols.append(symbol)
            continue
        regimes = snapshot.regimes
        if not _matches_regime_bucket(
            regimes,
            direction_regime=direction_regime,
            volatility_regime=volatility_regime,
        ):
            continue
        rows.append(
            LiveRegimeRow(
                symbol=symbol,
                live_bar_date=live_bar_date,
                close_price=snapshot.close_price,
                rsi_14=snapshot.rsi_14 or 0.0,
                ema_8=snapshot.ema_8 or 0.0,
                ema_21=snapshot.ema_21 or 0.0,
                sma_50=snapshot.sma_50 or 0.0,
                sma_200=snapshot.sma_200 or 0.0,
                realized_vol_20=snapshot.realized_vol_20,
                iv_rank_proxy=snapshot.iv_rank_proxy,
                volume_ratio=snapshot.volume_ratio,
                regimes=",".join(sorted(regime.value for regime in regimes)),
                has_local_option_history=symbol in option_history_symbols,
            )
        )
    return rows, skipped_symbols


def _build_live_calendar_candidates(
    *,
    symbol: str,
    live_bar_date: date,
    live_close_price: float,
    chain_date: date,
    raw_rows: list[dict[str, Any]],
) -> list[LiveCalendarCandidate]:
    grouped: dict[str, dict[date, dict[float, dict[str, Any]]]] = {
        "call": defaultdict(dict),
        "put": defaultdict(dict),
    }
    live_spot_price: float | None = None
    for row in raw_rows:
        details = row.get("details")
        quote_row = row.get("last_quote")
        if not isinstance(details, dict) or not isinstance(quote_row, dict):
            continue
        contract_type = details.get("contract_type")
        expiration_text = details.get("expiration_date")
        ticker = details.get("ticker")
        strike_price = _safe_float(details.get("strike_price"))
        shares_per_contract = details.get("shares_per_contract")
        bid = _safe_float(quote_row.get("bid"))
        ask = _safe_float(quote_row.get("ask"))
        if contract_type not in {"call", "put"}:
            continue
        if not isinstance(expiration_text, str) or not isinstance(ticker, str):
            continue
        if strike_price is None or shares_per_contract != 100:
            continue
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            continue
        midpoint = _safe_float(quote_row.get("midpoint"))
        if midpoint is None:
            midpoint = (bid + ask) / 2.0
        expiration_date = date.fromisoformat(expiration_text)
        greeks = row.get("greeks") if isinstance(row.get("greeks"), dict) else {}
        underlying_asset = row.get("underlying_asset") if isinstance(row.get("underlying_asset"), dict) else {}
        if live_spot_price is None:
            live_spot_price = _safe_float(underlying_asset.get("price"))
        grouped[contract_type][expiration_date][strike_price] = {
            "ticker": ticker,
            "bid": bid,
            "ask": ask,
            "mid": midpoint,
            "delta": _safe_float(greeks.get("delta")),
            "open_interest": row.get("open_interest"),
        }

    if live_spot_price is None:
        return []

    candidates: list[LiveCalendarCandidate] = []
    for contract_type, expirations_map in grouped.items():
        expirations = sorted(expirations_map)
        strategy_type = "calendar_spread" if contract_type == "call" else "put_calendar_spread"
        for short_index, short_expiration in enumerate(expirations):
            short_dte = (short_expiration - chain_date).days
            if short_dte <= 0 or short_dte > 365:
                continue
            for long_expiration in expirations[short_index + 1 :]:
                long_dte = (long_expiration - chain_date).days
                if long_dte <= short_dte or long_dte > 365:
                    continue
                common_strikes = sorted(
                    set(expirations_map[short_expiration]).intersection(expirations_map[long_expiration])
                )
                if not common_strikes:
                    continue
                strike_price = min(common_strikes, key=lambda strike: (abs(strike - live_spot_price), strike))
                short_row = expirations_map[short_expiration][strike_price]
                long_row = expirations_map[long_expiration][strike_price]
                natural_debit = float(long_row["ask"] - short_row["bid"])
                if natural_debit <= 0 or natural_debit > 0.50:
                    continue
                estimated_mid_debit = float(long_row["mid"] - short_row["mid"])
                candidates.append(
                    LiveCalendarCandidate(
                        symbol=symbol,
                        live_bar_date=live_bar_date,
                        live_chain_date=chain_date,
                        contract_type=contract_type,
                        strategy_type=strategy_type,
                        live_spot_price=live_spot_price,
                        live_close_price=live_close_price,
                        strike_price=strike_price,
                        short_expiration=short_expiration,
                        long_expiration=long_expiration,
                        short_dte=short_dte,
                        long_dte=long_dte,
                        dte_gap=long_dte - short_dte,
                        short_ticker=str(short_row["ticker"]),
                        long_ticker=str(long_row["ticker"]),
                        short_bid=float(short_row["bid"]),
                        short_ask=float(short_row["ask"]),
                        short_mid=float(short_row["mid"]),
                        long_bid=float(long_row["bid"]),
                        long_ask=float(long_row["ask"]),
                        long_mid=float(long_row["mid"]),
                        short_delta=_safe_float(short_row.get("delta")),
                        long_delta=_safe_float(long_row.get("delta")),
                        short_open_interest=int(short_row["open_interest"]) if short_row.get("open_interest") is not None else None,
                        long_open_interest=int(long_row["open_interest"]) if long_row.get("open_interest") is not None else None,
                        natural_debit=natural_debit,
                        estimated_mid_debit=estimated_mid_debit,
                    )
                )
    return candidates


async def _fetch_live_candidates_for_symbols(
    *,
    symbols: list[str],
    live_bar_date: date,
    live_close_by_symbol: dict[str, float],
    chain_date: date,
    concurrency: int,
    max_pages: int,
) -> tuple[list[LiveCalendarCandidate], list[dict[str, Any]]]:
    semaphore = asyncio.Semaphore(concurrency)
    all_candidates: list[LiveCalendarCandidate] = []
    chain_errors: list[dict[str, Any]] = []

    async def _worker(client: AsyncMassiveClient, symbol: str) -> None:
        async with semaphore:
            raw_rows: list[dict[str, Any]] = []
            try:
                async for page_rows in client._iter_paginated_results(
                    f"/v3/snapshot/options/{quote(symbol, safe='')}",
                    params={"limit": 250},
                    max_pages=max_pages,
                ):
                    raw_rows.extend(page_rows)
                candidates = _build_live_calendar_candidates(
                    symbol=symbol,
                    live_bar_date=live_bar_date,
                    live_close_price=live_close_by_symbol[symbol],
                    chain_date=chain_date,
                    raw_rows=raw_rows,
                )
                all_candidates.extend(candidates)
            except Exception as exc:
                chain_errors.append({"symbol": symbol, "error": str(exc)})

    async with AsyncMassiveClient() as client:
        await asyncio.gather(*[_worker(client, symbol) for symbol in symbols])
    return all_candidates, chain_errors


def _build_risk_free_rate_curve(
    store: HistoricalMarketDataStore,
    start_date: date,
    end_date: date,
) -> RiskFreeRateCurve:
    series = store.get_treasury_yield_series(start_date, end_date)
    default_rate = store.get_average_treasury_yield(start_date, start_date)
    if default_rate is None:
        default_rate = estimate_risk_free_rate(start_date, end_date)
    if not series:
        return RiskFreeRateCurve(default_rate=float(default_rate))
    ordered_dates = tuple(sorted(series))
    ordered_rates = tuple(float(series[trade_date]) for trade_date in ordered_dates)
    return RiskFreeRateCurve(
        default_rate=float(default_rate),
        dates=ordered_dates,
        rates=ordered_rates,
    )


def _build_symbol_bundle(
    store: HistoricalMarketDataStore,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> tuple[list[DailyBar], set[date], set[date], HistoricalOptionGateway]:
    warmup_start = start_date - timedelta(days=WARMUP_CALENDAR_DAYS)
    bars = store.get_underlying_day_bars(symbol, warmup_start, end_date)
    earnings_dates = store.list_earnings_event_dates(symbol, warmup_start, end_date)
    ex_dividend_dates = store.list_ex_dividend_dates(symbol, warmup_start, end_date)
    option_gateway = HistoricalOptionGateway(store, symbol)
    return bars, earnings_dates, ex_dividend_dates, option_gateway


def _backtest_candidates(
    *,
    candidates: list[LiveCalendarCandidate],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    risk_free_rate_curve = _build_risk_free_rate_curve(store, start_date, end_date)
    engine = OptionsBacktestEngine()
    atm_selection = StrikeSelection(mode=StrikeSelectionMode.ATM_OFFSET_STEPS, value=Decimal("0"))
    grouped_candidates: dict[str, list[LiveCalendarCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped_candidates[candidate.symbol].append(candidate)

    results: list[dict[str, Any]] = []
    total_backtests = len(candidates)
    completed_backtests = 0

    for symbol_index, (symbol, symbol_candidates) in enumerate(sorted(grouped_candidates.items()), start=1):
        bars, earnings_dates, ex_dividend_dates, option_gateway = _build_symbol_bundle(
            store,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        print(
            f"[backtest] {symbol_index}/{len(grouped_candidates)} symbol={symbol} "
            f"candidates={len(symbol_candidates)}"
        )
        for candidate in symbol_candidates:
            completed_backtests += 1
            overrides = StrategyOverrides(
                short_call_strike=atm_selection if candidate.contract_type == "call" else None,
                short_put_strike=atm_selection if candidate.contract_type == "put" else None,
                calendar_far_leg_target_dte=candidate.long_dte,
            )
            config = BacktestConfig(
                symbol=candidate.symbol,
                strategy_type=candidate.strategy_type,
                start_date=start_date,
                end_date=end_date,
                target_dte=candidate.short_dte,
                dte_tolerance_days=0,
                max_holding_days=max(1, candidate.short_dte),
                account_size=ACCOUNT_SIZE,
                risk_per_trade_pct=RISK_PER_TRADE_PCT,
                commission_per_contract=COMMISSION_PER_CONTRACT,
                entry_rules=[],
                risk_free_rate=risk_free_rate_curve.default_rate,
                risk_free_rate_curve=risk_free_rate_curve,
                dividend_yield=0.0,
                slippage_pct=SLIPPAGE_PCT,
                strategy_overrides=overrides,
            )
            row = asdict(candidate)
            row.update(
                {
                    "score": None,
                    "trade_count": None,
                    "decided_trades": None,
                    "total_roi_pct": None,
                    "win_rate": None,
                    "max_drawdown_pct": None,
                    "sharpe_ratio": None,
                    "average_holding_period_days": None,
                    "warnings": "",
                    "error": "",
                }
            )
            try:
                execution = engine.run(
                    config,
                    bars,
                    earnings_dates,
                    option_gateway,
                    ex_dividend_dates=ex_dividend_dates,
                )
                summary_dict = asdict(execution.summary)
                warning_codes = sorted(
                    {
                        str(warning.get("code"))
                        for warning in execution.warnings
                        if isinstance(warning, dict) and warning.get("code")
                    }
                )
                row.update(
                    {
                        "score": compute_backtest_score(summary_dict),
                        "trade_count": execution.summary.trade_count,
                        "decided_trades": execution.summary.decided_trades,
                        "total_roi_pct": execution.summary.total_roi_pct,
                        "win_rate": execution.summary.win_rate,
                        "max_drawdown_pct": execution.summary.max_drawdown_pct,
                        "sharpe_ratio": execution.summary.sharpe_ratio,
                        "average_holding_period_days": execution.summary.average_holding_period_days,
                        "warnings": ",".join(warning_codes),
                    }
                )
            except Exception as exc:
                row["error"] = str(exc)
            results.append(row)
            if completed_backtests % 100 == 0 or completed_backtests == total_backtests:
                print(
                    f"[backtest] completed={completed_backtests}/{total_backtests}"
                )
    results.sort(
        key=lambda item: (
            float("-inf") if item["score"] is None else float(item["score"]),
            float("-inf") if item["total_roi_pct"] is None else float(item["total_roi_pct"]),
        ),
        reverse=True,
    )
    return results


def main() -> int:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _suppress_engine_info_logs()
    bucket_label = _regime_bucket_label(args.direction_regime, args.volatility_regime)

    with MassiveClient() as client:
        live_bar_date, live_bars = _latest_completed_massive_bar_date(client, as_of_date=args.as_of_date)

    print(f"[screen] latest completed Massive grouped bar date: {live_bar_date.isoformat()} ({len(live_bars)} symbols)")
    local_latest_date, local_histories = _load_recent_local_histories(RECENT_LOCAL_BAR_LIMIT)
    print(f"[screen] local underlying latest date: {local_latest_date.isoformat()} ({len(local_histories)} symbols)")
    option_history_latest_date, option_history_symbols = _load_local_option_history_symbols()
    print(
        f"[screen] local option-history latest date: {option_history_latest_date.isoformat()} "
        f"({len(option_history_symbols)} symbols)"
    )

    live_regime_rows, skipped_symbols = _classify_symbols_for_regime_bucket(
        live_bar_date=live_bar_date,
        live_bars=live_bars,
        local_histories=local_histories,
        option_history_symbols=option_history_symbols,
        direction_regime=args.direction_regime,
        volatility_regime=args.volatility_regime,
    )
    if args.symbol_limit is not None:
        live_regime_rows = live_regime_rows[: args.symbol_limit]
    print(
        f"[screen] {bucket_label} symbols: {len(live_regime_rows)} "
        f"(skipped for missing history: {len(skipped_symbols)})"
    )

    live_symbol_rows = [asdict(row) for row in live_regime_rows]
    live_symbol_csv = args.output_dir / f"massive_{bucket_label}_symbols_{args.as_of_date:%Y%m%d}.csv"
    _write_csv(
        live_symbol_csv,
        live_symbol_rows,
        [
            "symbol",
            "live_bar_date",
            "close_price",
            "rsi_14",
            "ema_8",
            "ema_21",
            "sma_50",
            "sma_200",
            "realized_vol_20",
            "iv_rank_proxy",
            "volume_ratio",
            "regimes",
            "has_local_option_history",
        ],
    )

    symbols_for_chain = [row.symbol for row in live_regime_rows if row.has_local_option_history]
    live_close_by_symbol = {row.symbol: row.close_price for row in live_regime_rows}
    print(f"[screen] fetching live chains for {len(symbols_for_chain)} symbols")

    live_candidates, chain_errors = asyncio.run(
        _fetch_live_candidates_for_symbols(
            symbols=symbols_for_chain,
            live_bar_date=live_bar_date,
            live_close_by_symbol=live_close_by_symbol,
            chain_date=args.as_of_date,
            concurrency=args.chain_concurrency,
            max_pages=args.chain_max_pages,
        )
    )
    print(f"[screen] live cheap ATM calendar candidates: {len(live_candidates)}")
    if chain_errors:
        print(f"[screen] live chain errors: {len(chain_errors)}")

    live_candidate_rows = [asdict(row) for row in live_candidates]
    live_candidate_csv = args.output_dir / f"massive_{bucket_label}_cheap_atm_calendars_{args.as_of_date:%Y%m%d}.csv"
    _write_csv(
        live_candidate_csv,
        live_candidate_rows,
        [
            "symbol",
            "live_bar_date",
            "live_chain_date",
            "contract_type",
            "strategy_type",
            "live_spot_price",
            "live_close_price",
            "strike_price",
            "short_expiration",
            "long_expiration",
            "short_dte",
            "long_dte",
            "dte_gap",
            "short_ticker",
            "long_ticker",
            "short_bid",
            "short_ask",
            "short_mid",
            "long_bid",
            "long_ask",
            "long_mid",
            "short_delta",
            "long_delta",
            "short_open_interest",
            "long_open_interest",
            "natural_debit",
            "estimated_mid_debit",
        ],
    )

    backtest_rows: list[dict[str, Any]] = []
    if not args.skip_backtests:
        backtest_rows = _backtest_candidates(
            candidates=live_candidates,
            start_date=args.backtest_start,
            end_date=args.backtest_end,
        )
    backtest_csv = args.output_dir / (
        f"massive_{bucket_label}_cheap_atm_calendars_backtests_{args.backtest_start:%Y%m%d}_{args.backtest_end:%Y%m%d}_{args.as_of_date:%Y%m%d}.csv"
    )
    _write_csv(
        backtest_csv,
        backtest_rows,
        [
            "symbol",
            "live_bar_date",
            "live_chain_date",
            "contract_type",
            "strategy_type",
            "live_spot_price",
            "live_close_price",
            "strike_price",
            "short_expiration",
            "long_expiration",
            "short_dte",
            "long_dte",
            "dte_gap",
            "short_ticker",
            "long_ticker",
            "short_bid",
            "short_ask",
            "short_mid",
            "long_bid",
            "long_ask",
            "long_mid",
            "short_delta",
            "long_delta",
            "short_open_interest",
            "long_open_interest",
            "natural_debit",
            "estimated_mid_debit",
            "score",
            "trade_count",
            "decided_trades",
            "total_roi_pct",
            "win_rate",
            "max_drawdown_pct",
            "sharpe_ratio",
            "average_holding_period_days",
            "warnings",
            "error",
        ],
    )

    summary_json = args.output_dir / (
        f"massive_{bucket_label}_calendar_screen_summary_{args.as_of_date:%Y%m%d}.json"
    )
    _write_json(
        summary_json,
        {
            "as_of_date": args.as_of_date.isoformat(),
            "direction_regime": args.direction_regime,
            "volatility_regime": args.volatility_regime,
            "bucket_label": bucket_label,
            "live_bar_date": live_bar_date.isoformat(),
            "local_underlying_latest_date": local_latest_date.isoformat(),
            "local_option_history_latest_date": option_history_latest_date.isoformat(),
            "matching_symbol_count": len(live_regime_rows),
            "symbols_with_local_option_history": len(symbols_for_chain),
            "live_candidate_count": len(live_candidates),
            "chain_error_count": len(chain_errors),
            "skipped_history_count": len(skipped_symbols),
            "paths": {
                "symbols_csv": str(live_symbol_csv),
                "live_candidates_csv": str(live_candidate_csv),
                "backtests_csv": str(backtest_csv),
            },
            "chain_errors": chain_errors[:50],
        },
    )

    print(f"[done] symbols csv: {live_symbol_csv}")
    print(f"[done] live candidates csv: {live_candidate_csv}")
    print(f"[done] backtests csv: {backtest_csv}")
    print(f"[done] summary json: {summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
