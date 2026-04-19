from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.request import urlopen

from backtestforecast.market_data.historical_store import HistoricalMarketDataStore


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_VIX_CACHE_CSV = ROOT / "logs" / "reference" / "vixcls_cache.csv"
DEFAULT_VIX_DB_SYMBOLS = ("^VIX", "VIX")
DEFAULT_VIX_FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"


@dataclass(frozen=True, slots=True)
class VixWeeklyChangeSnapshot:
    entry_date: date
    effective_trade_date: date
    close_price: float
    prior_entry_date: date | None
    prior_effective_trade_date: date | None
    prior_close_price: float | None
    weekly_change_pct: float | None


def parse_vix_csv_rows(text: str) -> dict[date, float]:
    close_by_date: dict[date, float] = {}
    for row in csv.DictReader(io.StringIO(text)):
        raw_date = (row.get("DATE") or row.get("observation_date") or row.get("trade_date") or "").strip()
        raw_close = (row.get("VIXCLS") or row.get("close_price") or "").strip()
        if not raw_date or not raw_close or raw_close == ".":
            continue
        close_by_date[date.fromisoformat(raw_date)] = float(raw_close)
    return close_by_date


def load_vix_csv(path: Path) -> dict[date, float]:
    if not path.exists():
        return {}
    return parse_vix_csv_rows(path.read_text(encoding="utf-8"))


def download_fred_vix_csv(*, timeout_seconds: float = 30.0) -> str:
    with urlopen(DEFAULT_VIX_FRED_CSV_URL, timeout=timeout_seconds) as response:
        payload = response.read()
    return payload.decode("utf-8")


def refresh_vix_cache_csv(
    path: Path,
    *,
    timeout_seconds: float = 30.0,
) -> dict[date, float]:
    text = download_fred_vix_csv(timeout_seconds=timeout_seconds)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return parse_vix_csv_rows(text)


def load_vix_close_series(
    *,
    start_date: date,
    end_date: date,
    store: HistoricalMarketDataStore | None = None,
    cache_csv: Path = DEFAULT_VIX_CACHE_CSV,
    allow_cache_refresh: bool = True,
    timeout_seconds: float = 30.0,
) -> dict[date, float]:
    close_by_date: dict[date, float] = {}
    if store is not None:
        for symbol in DEFAULT_VIX_DB_SYMBOLS:
            bars = store.get_underlying_day_bars(symbol, start_date, end_date)
            if not bars:
                continue
            close_by_date.update({bar.trade_date: float(bar.close_price) for bar in bars})
            break

    cache_series = load_vix_csv(cache_csv)
    if allow_cache_refresh and (
        not cache_series
        or min(cache_series) > start_date
        or max(cache_series) < end_date
    ):
        cache_series = refresh_vix_cache_csv(cache_csv, timeout_seconds=timeout_seconds)
    for trade_date, close_price in cache_series.items():
        if start_date <= trade_date <= end_date:
            close_by_date.setdefault(trade_date, close_price)
    return dict(sorted(close_by_date.items()))


def resolve_close_on_or_before(
    close_by_date: dict[date, float],
    target_date: date,
) -> tuple[date, float] | None:
    eligible_dates = [trade_date for trade_date in close_by_date if trade_date <= target_date]
    if not eligible_dates:
        return None
    effective_trade_date = max(eligible_dates)
    return effective_trade_date, close_by_date[effective_trade_date]


def build_weekly_change_snapshots(
    *,
    entry_dates: list[date] | tuple[date, ...],
    close_by_date: dict[date, float],
) -> dict[date, VixWeeklyChangeSnapshot]:
    ordered_entry_dates = sorted(dict.fromkeys(entry_dates))
    snapshots: dict[date, VixWeeklyChangeSnapshot] = {}
    for index, entry_date in enumerate(ordered_entry_dates):
        resolved_current = resolve_close_on_or_before(close_by_date, entry_date)
        if resolved_current is None:
            continue
        effective_trade_date, close_price = resolved_current
        prior_entry_date = ordered_entry_dates[index - 1] if index > 0 else None
        prior_effective_trade_date: date | None = None
        prior_close_price: float | None = None
        weekly_change_pct: float | None = None
        if prior_entry_date is not None:
            resolved_prior = resolve_close_on_or_before(close_by_date, prior_entry_date)
            if resolved_prior is not None:
                prior_effective_trade_date, prior_close_price = resolved_prior
                if prior_close_price > 0:
                    weekly_change_pct = ((close_price / prior_close_price) - 1.0) * 100.0
        snapshots[entry_date] = VixWeeklyChangeSnapshot(
            entry_date=entry_date,
            effective_trade_date=effective_trade_date,
            close_price=close_price,
            prior_entry_date=prior_entry_date,
            prior_effective_trade_date=prior_effective_trade_date,
            prior_close_price=prior_close_price,
            weekly_change_pct=weekly_change_pct,
        )
    return snapshots
