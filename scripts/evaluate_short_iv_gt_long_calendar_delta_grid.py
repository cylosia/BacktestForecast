from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.predict_weekly_price_movement as pwm

from backtestforecast.backtests.rules import implied_volatility_from_price
from backtestforecast.backtests.strategies.common import _approx_bsm_delta, choose_atm_strike
from backtestforecast.models import HistoricalOptionDayBar

DEFAULT_SYMBOLS_FILE = ROOT / "weekly-options-over5-median80-mintrades70.txt"
DEFAULT_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "weekly_price_movement"
    / "weekly_options_over5_median80_mintrades70_auto_v13_surface_features"
    / "summary.csv"
)
DEFAULT_OUTPUT_PREFIX = ROOT / "logs" / "short_iv_gt_long_calendar_delta_grid_2y"
DEFAULT_DELTA_TARGETS = (50, 45, 40)
DEFAULT_ENTRY_START_DATE = date.fromisoformat("2024-04-19")
DEFAULT_ENTRY_END_DATE = date.fromisoformat("2026-04-10")
DEFAULT_SHORT_DTE_MAX = 10
DEFAULT_GAP_DTE_MAX = 10
DEFAULT_MAX_WORKERS = 4
DEFAULT_ENTRY_WEEKDAY = 4
DEFAULT_ENTRY_WEEKDAY_NAME = "Friday"
DEFAULT_SHORT_EXPIRATION_DTE_TARGETS: tuple[int, ...] = ()
DEFAULT_LONG_EXPIRATION_DTE_TARGETS: tuple[int, ...] = ()
SYMBOL_RESULT_CACHE_SCHEMA_VERSION = 3


@dataclass(frozen=True, slots=True)
class OptionRow:
    option_ticker: str
    trade_date: date
    expiration_date: date
    strike_price: float
    close_price: float


@dataclass(frozen=True, slots=True)
class WeeklyCalendarCandidate:
    symbol: str
    entry_date: date
    short_expiration: date
    long_expiration: date
    spot_close_entry: float
    common_atm_strike: float
    short_atm_iv_pct: float
    long_atm_iv_pct: float


@dataclass(frozen=True, slots=True)
class SymbolEvaluationResult:
    symbol: str
    weekly_candidate_rows: list[dict[str, object]]
    detail_rows: list[dict[str, object]]
    status_message: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest weekly call calendar spreads on dates where ATM short IV exceeds long IV, "
            "then determine the best delta per symbol separately for up and abstain predictions."
        )
    )
    parser.add_argument("--symbols-file", type=Path, default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--summary-csv", type=Path, default=DEFAULT_SUMMARY_CSV)
    parser.add_argument("--output-prefix", type=Path, default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--entry-start-date", type=date.fromisoformat, default=DEFAULT_ENTRY_START_DATE)
    parser.add_argument("--entry-end-date", type=date.fromisoformat, default=DEFAULT_ENTRY_END_DATE)
    parser.add_argument(
        "--entry-dates",
        default="",
        help=(
            "Optional comma-separated explicit entry dates. If omitted, "
            f"{DEFAULT_ENTRY_WEEKDAY_NAME}s in the requested date range are used."
        ),
    )
    parser.add_argument(
        "--entry-weekday",
        type=int,
        default=DEFAULT_ENTRY_WEEKDAY,
        help="Entry weekday when --entry-dates is omitted, where Monday=0 and Sunday=6.",
    )
    parser.add_argument(
        "--delta-targets",
        default=",".join(str(value) for value in DEFAULT_DELTA_TARGETS),
        help="Comma-separated call delta targets. Defaults to 50,45,40.",
    )
    parser.add_argument(
        "--short-dte-max",
        type=int,
        default=DEFAULT_SHORT_DTE_MAX,
        help="Maximum days from entry date to short expiration. Defaults to 10.",
    )
    parser.add_argument(
        "--gap-dte-max",
        type=int,
        default=DEFAULT_GAP_DTE_MAX,
        help="Maximum days between short and long expirations. Defaults to 10.",
    )
    parser.add_argument(
        "--short-expiration-dte-targets",
        default=",".join(str(value) for value in DEFAULT_SHORT_EXPIRATION_DTE_TARGETS),
        help=(
            "Optional comma-separated allowed DTEs for the short expiration. "
            "If omitted, the first expiration after entry is used."
        ),
    )
    parser.add_argument(
        "--long-expiration-dte-targets",
        default=",".join(str(value) for value in DEFAULT_LONG_EXPIRATION_DTE_TARGETS),
        help=(
            "Optional comma-separated allowed DTEs for the long expiration. "
            "If omitted, the second expiration after entry is used."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="SQLAlchemy database URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--db-statement-timeout-ms",
        type=int,
        default=120000,
        help="Statement timeout passed to build_engine. Defaults to 120000.",
    )
    parser.add_argument("--limit-symbols", type=int, default=None, help="Optional cap on number of symbols.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="How many symbols to evaluate concurrently. Defaults to 4.",
    )
    parser.add_argument(
        "--symbol-cache-dir",
        type=Path,
        default=None,
        help="Optional directory for per-symbol resume cache. Defaults to <output-prefix>_symbol_cache.",
    )
    parser.add_argument(
        "--disable-symbol-cache",
        action="store_true",
        help="Disable per-symbol resume cache reads/writes.",
    )
    return parser


def _load_database_url(explicit_value: str) -> str:
    if explicit_value:
        return explicit_value
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL is required. Provide --database-url or configure .env.")


def _parse_delta_targets(raw_value: str) -> tuple[int, ...]:
    values = tuple(int(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip())
    if not values:
        raise SystemExit("--delta-targets must contain at least one integer.")
    return values


def _parse_optional_int_targets(raw_value: str) -> tuple[int, ...]:
    if not raw_value.strip():
        return ()
    values = tuple(int(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip())
    if any(value < 1 for value in values):
        raise SystemExit("DTE targets must be positive integers.")
    return values


def _parse_entry_dates(raw_value: str) -> set[date]:
    if not raw_value.strip():
        return set()
    return {
        date.fromisoformat(chunk.strip())
        for chunk in raw_value.split(",")
        if chunk.strip()
    }


def _load_symbols(symbols_file: Path, *, limit: int | None) -> list[str]:
    rows = [
        line.strip().upper()
        for line in symbols_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return rows if limit is None else rows[:limit]


def _load_selected_method_by_symbol(summary_csv: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in csv.DictReader(summary_csv.open(encoding="utf-8")):
        symbol = row["symbol"].strip().upper()
        method_name = row["selected_method"].strip()
        if symbol and method_name:
            mapping[symbol] = method_name
    return mapping


def _load_option_rows_for_dates(
    session: Session,
    *,
    symbol: str,
    trade_dates: set[date],
) -> dict[date, dict[date, list[OptionRow]]]:
    if not trade_dates:
        return {}
    stmt = (
        select(HistoricalOptionDayBar)
        .where(HistoricalOptionDayBar.underlying_symbol == symbol)
        .where(HistoricalOptionDayBar.trade_date.in_(sorted(trade_dates)))
        .where(HistoricalOptionDayBar.contract_type == "call")
        .order_by(
            HistoricalOptionDayBar.trade_date,
            HistoricalOptionDayBar.expiration_date,
            HistoricalOptionDayBar.strike_price,
        )
    )
    grouped: dict[date, dict[date, list[OptionRow]]] = defaultdict(lambda: defaultdict(list))
    for row in session.execute(stmt).scalars():
        grouped[row.trade_date][row.expiration_date].append(
            OptionRow(
                option_ticker=row.option_ticker,
                trade_date=row.trade_date,
                expiration_date=row.expiration_date,
                strike_price=float(row.strike_price),
                close_price=float(row.close_price),
            )
        )
    return {
        trade_date: {expiration: list(rows) for expiration, rows in expiration_map.items()}
        for trade_date, expiration_map in grouped.items()
    }


def _estimate_call_iv_pct(
    *,
    option_price: float,
    spot_price: float,
    strike_price: float,
    trade_date: date,
    expiration_date: date,
) -> float | None:
    if option_price <= 0 or spot_price <= 0 or strike_price <= 0:
        return None
    dte_days = max((expiration_date - trade_date).days, 1)
    iv = implied_volatility_from_price(
        option_price=option_price,
        underlying_price=spot_price,
        strike_price=strike_price,
        time_to_expiry_years=dte_days / 365.0,
        option_type="call",
        risk_free_rate=0.045,
        dividend_yield=0.0,
    )
    return None if iv is None else float(iv) * 100.0


def _call_delta_from_price(
    *,
    option_price: float,
    spot_price: float,
    strike_price: float,
    trade_date: date,
    expiration_date: date,
) -> float | None:
    if option_price <= 0 or spot_price <= 0 or strike_price <= 0:
        return None
    dte_days = max((expiration_date - trade_date).days, 1)
    iv_pct = _estimate_call_iv_pct(
        option_price=option_price,
        spot_price=spot_price,
        strike_price=strike_price,
        trade_date=trade_date,
        expiration_date=expiration_date,
    )
    if iv_pct is None:
        return None
    return _approx_bsm_delta(
        spot=spot_price,
        strike=strike_price,
        dte_days=dte_days,
        contract_type="call",
        vol=iv_pct / 100.0,
        risk_free_rate=0.045,
        dividend_yield=0.0,
    )


def _intrinsic_call(strike_price: float, spot_price: float) -> float:
    return max(spot_price - strike_price, 0.0)


def _mark_call_leg(
    *,
    rows_by_strike: dict[float, OptionRow],
    target_strike: float,
    spot_mark: float,
    is_expiring_leg: bool,
) -> tuple[float | None, str]:
    exact = rows_by_strike.get(target_strike)
    if exact is not None:
        return exact.close_price, "exact"
    intrinsic = _intrinsic_call(target_strike, spot_mark)
    if is_expiring_leg:
        return intrinsic, "expiry_intrinsic"
    if not rows_by_strike:
        return intrinsic, "intrinsic_no_chain"
    nearest_strike = min(rows_by_strike, key=lambda strike: (abs(strike - target_strike), strike))
    nearest = rows_by_strike[nearest_strike]
    adjusted = nearest.close_price + (
        _intrinsic_call(target_strike, spot_mark) - _intrinsic_call(nearest_strike, spot_mark)
    )
    return max(adjusted, 0.0), f"nearest_strike_intrinsic_adjusted({nearest_strike})"


def _has_earnings_before_short_expiration(
    *,
    entry_date: date,
    short_expiration: date,
    earnings_dates: set[date] | None,
) -> bool:
    if not earnings_dates:
        return False
    return any(entry_date <= earnings_date < short_expiration for earnings_date in earnings_dates)


def _select_expiration_pair(
    *,
    entry_date: date,
    expirations: list[date],
    short_expiration_dte_targets: tuple[int, ...],
    long_expiration_dte_targets: tuple[int, ...],
) -> tuple[date, date] | None:
    if not short_expiration_dte_targets and not long_expiration_dte_targets:
        if len(expirations) < 2:
            return None
        return expirations[0], expirations[1]

    short_target_set = set(short_expiration_dte_targets)
    long_target_set = set(long_expiration_dte_targets)
    for short_expiration in expirations:
        short_dte = (short_expiration - entry_date).days
        if short_target_set and short_dte not in short_target_set:
            continue
        for long_expiration in expirations:
            if long_expiration <= short_expiration:
                continue
            long_dte = (long_expiration - entry_date).days
            if long_target_set and long_dte not in long_target_set:
                continue
            return short_expiration, long_expiration
    return None


def _select_weekly_calendar_candidates(
    *,
    symbol: str,
    entry_dates: list[date],
    spot_by_date: dict[date, float],
    option_rows_by_date: dict[date, dict[date, list[OptionRow]]],
    short_dte_max: int,
    gap_dte_max: int,
    short_expiration_dte_targets: tuple[int, ...] = DEFAULT_SHORT_EXPIRATION_DTE_TARGETS,
    long_expiration_dte_targets: tuple[int, ...] = DEFAULT_LONG_EXPIRATION_DTE_TARGETS,
    earnings_dates: set[date] | None = None,
) -> list[WeeklyCalendarCandidate]:
    candidates: list[WeeklyCalendarCandidate] = []
    for entry_date in entry_dates:
        expiration_map = option_rows_by_date.get(entry_date)
        if not expiration_map:
            continue
        expirations = sorted(expiration for expiration in expiration_map if expiration > entry_date)
        expiration_pair = _select_expiration_pair(
            entry_date=entry_date,
            expirations=expirations,
            short_expiration_dte_targets=short_expiration_dte_targets,
            long_expiration_dte_targets=long_expiration_dte_targets,
        )
        if expiration_pair is None:
            continue
        short_expiration, long_expiration = expiration_pair
        short_dte = (short_expiration - entry_date).days
        gap_dte = (long_expiration - short_expiration).days
        if short_dte < 1 or short_dte > short_dte_max:
            continue
        if gap_dte < 1 or gap_dte > gap_dte_max:
            continue
        if _has_earnings_before_short_expiration(
            entry_date=entry_date,
            short_expiration=short_expiration,
            earnings_dates=earnings_dates,
        ):
            continue
        short_rows = [row for row in expiration_map[short_expiration] if row.close_price > 0]
        long_rows = [row for row in expiration_map[long_expiration] if row.close_price > 0]
        if not short_rows or not long_rows:
            continue
        short_rows_by_strike = {row.strike_price: row for row in short_rows}
        long_rows_by_strike = {row.strike_price: row for row in long_rows}
        common_strikes = sorted(set(short_rows_by_strike).intersection(long_rows_by_strike))
        if not common_strikes:
            continue
        spot_close_entry = spot_by_date.get(entry_date)
        if spot_close_entry is None:
            continue
        common_atm_strike = float(choose_atm_strike(common_strikes, spot_close_entry))
        short_row = short_rows_by_strike.get(common_atm_strike)
        long_row = long_rows_by_strike.get(common_atm_strike)
        if short_row is None or long_row is None:
            continue
        short_iv_pct = _estimate_call_iv_pct(
            option_price=short_row.close_price,
            spot_price=spot_close_entry,
            strike_price=common_atm_strike,
            trade_date=entry_date,
            expiration_date=short_expiration,
        )
        long_iv_pct = _estimate_call_iv_pct(
            option_price=long_row.close_price,
            spot_price=spot_close_entry,
            strike_price=common_atm_strike,
            trade_date=entry_date,
            expiration_date=long_expiration,
        )
        if short_iv_pct is None or long_iv_pct is None:
            continue
        if short_iv_pct <= long_iv_pct:
            continue
        candidates.append(
            WeeklyCalendarCandidate(
                symbol=symbol,
                entry_date=entry_date,
                short_expiration=short_expiration,
                long_expiration=long_expiration,
                spot_close_entry=spot_close_entry,
                common_atm_strike=common_atm_strike,
                short_atm_iv_pct=short_iv_pct,
                long_atm_iv_pct=long_iv_pct,
            )
        )
    return candidates


def _prediction_payload_from_snapshot(
    snapshot: pwm.PredictionSnapshot | None,
    *,
    prediction_engine: str,
) -> dict[str, object] | None:
    if snapshot is None:
        return None
    payload = pwm._build_latest_prediction_payload(snapshot, prediction_engine=prediction_engine)
    payload["prediction"] = payload["predicted_direction"]
    return payload


def _build_prediction_map_for_dates(
    *,
    bars: list[pwm.DailyBar],
    features: list[tuple[float, ...] | None],
    candidates: list[pwm.AnalogCandidate | None],
    requested_dates: set[date],
    method: pwm.PredictionMethodConfig,
    horizon_bars: int,
    min_spacing_bars: int,
    min_candidate_count: int,
) -> dict[date, dict[str, object] | None]:
    if not requested_dates:
        return {}
    requested_dates = set(requested_dates)
    results: dict[date, dict[str, object] | None] = {}
    last_prediction_index = len(bars) - 1
    last_scored_index = len(bars) - horizon_bars
    candidate_cursor = -1
    candidate_pool: list[pwm.AnalogCandidate] = []
    ml_estimator: pwm.FittedMlModel | None = None
    ml_last_fit_index: int | None = None
    ml_train_sample_count: int | None = None
    analog_method_cache = pwm.AnalogMethodCache() if method.engine != "ml" else None
    ml_fit_cache: dict[tuple[tuple[str, str, int, int, float, int], int], tuple[pwm.FittedMlModel, int] | None] | None = (
        {}
        if method.engine == "ml"
        else None
    )
    for index in range(last_prediction_index + 1):
        if index < last_scored_index:
            max_candidate_index = index - horizon_bars
            while candidate_cursor < max_candidate_index:
                candidate_cursor += 1
                if candidate_cursor < 0:
                    continue
                candidate = candidates[candidate_cursor]
                if candidate is not None:
                    candidate_pool.append(candidate)
        bar = bars[index]
        if bar.trade_date not in requested_dates:
            continue
        current_features = features[index]
        if current_features is None:
            results[bar.trade_date] = None
            continue
        if method.engine == "ml":
            train_end_index = index - horizon_bars
            if train_end_index < 0:
                results[bar.trade_date] = None
                continue
            if (
                ml_estimator is None
                or ml_last_fit_index is None
                or (train_end_index - ml_last_fit_index) >= method.retrain_every_bars
            ):
                fitted = pwm._fit_ml_model_cached(
                    bars=bars,
                    features=features,
                    horizon_bars=horizon_bars,
                    train_end_index=train_end_index,
                    method=method,
                    fit_cache=ml_fit_cache,
                )
                if fitted is None:
                    results[bar.trade_date] = None
                    continue
                ml_estimator, ml_train_sample_count = fitted
                ml_last_fit_index = train_end_index
            if ml_estimator is None or ml_train_sample_count is None:
                results[bar.trade_date] = None
                continue
            snapshot = pwm._predict_with_ml_model(
                trade_date=bar.trade_date,
                current_features=current_features,
                estimator=ml_estimator,
                train_sample_count=ml_train_sample_count,
                method=method,
            )
            results[bar.trade_date] = _prediction_payload_from_snapshot(snapshot, prediction_engine=method.engine)
        else:
            if analog_method_cache is None:
                snapshot = pwm._predict_with_method(
                    trade_date=bar.trade_date,
                    current_features=current_features,
                    candidate_pool=candidate_pool,
                    min_spacing_bars=min_spacing_bars,
                    min_candidate_count=min_candidate_count,
                    method=method,
                )
            else:
                ranked_candidates = pwm._get_ranked_candidates_for_method(
                    index=index,
                    current_features=current_features,
                    candidates=candidates,
                    horizon_bars=horizon_bars,
                    min_candidate_count=min_candidate_count,
                    method=method,
                    cache=analog_method_cache,
                )
                selected = pwm._get_selected_analogs_for_method(
                    index=index,
                    current_features=current_features,
                    candidates=candidates,
                    horizon_bars=horizon_bars,
                    min_candidate_count=min_candidate_count,
                    min_spacing_bars=min_spacing_bars,
                    method=method,
                    cache=analog_method_cache,
                )
                snapshot = pwm._build_analog_prediction_snapshot(
                    trade_date=bar.trade_date,
                    current_features=current_features,
                    ranked_candidates=ranked_candidates,
                    selected=selected,
                    min_candidate_count=min_candidate_count,
                    method=method,
                )
            results[bar.trade_date] = _prediction_payload_from_snapshot(snapshot, prediction_engine=method.engine)
        if len(results) == len(requested_dates):
            break
    return results


def _pick_calendar_rows_for_delta(
    *,
    entry_rows_by_expiration: dict[date, list[OptionRow]],
    short_expiration: date,
    long_expiration: date,
    spot_close_entry: float,
    entry_date: date,
    common_atm_strike: float,
    delta_target_pct: int,
) -> tuple[OptionRow, OptionRow, float] | None:
    short_rows_by_strike = {
        row.strike_price: row
        for row in entry_rows_by_expiration.get(short_expiration, [])
        if row.close_price > 0
    }
    long_rows_by_strike = {
        row.strike_price: row
        for row in entry_rows_by_expiration.get(long_expiration, [])
        if row.close_price > 0
    }
    common_strikes = sorted(set(short_rows_by_strike).intersection(long_rows_by_strike))
    if not common_strikes:
        return None
    candidate_strikes = [strike for strike in common_strikes if strike >= common_atm_strike]
    if not candidate_strikes:
        candidate_strikes = common_strikes
    scored: list[tuple[float, float, float, float]] = []
    target_delta = delta_target_pct / 100.0
    for strike in candidate_strikes:
        short_row = short_rows_by_strike[strike]
        delta = _call_delta_from_price(
            option_price=short_row.close_price,
            spot_price=spot_close_entry,
            strike_price=strike,
            trade_date=entry_date,
            expiration_date=short_expiration,
        )
        if delta is None:
            continue
        scored.append((abs(delta - target_delta), -strike, strike, delta))
    if not scored:
        return None
    _, _, chosen_strike, resolved_delta = min(scored)
    return (
        short_rows_by_strike[chosen_strike],
        long_rows_by_strike[chosen_strike],
        resolved_delta,
    )


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _default_symbol_cache_dir(output_prefix: Path) -> Path:
    return output_prefix.with_name(f"{output_prefix.name}_symbol_cache")


def _symbol_cache_path(cache_dir: Path, symbol: str) -> Path:
    return cache_dir / f"{symbol.lower()}.json"


def _symbol_cache_metadata(
    *,
    symbol: str,
    method_name: str,
    entry_start_date: date,
    entry_end_date: date,
    explicit_entry_dates: set[date],
    entry_weekday: int,
    short_dte_max: int,
    gap_dte_max: int,
    short_expiration_dte_targets: tuple[int, ...],
    long_expiration_dte_targets: tuple[int, ...],
    delta_targets: tuple[int, ...],
) -> dict[str, object]:
    return {
        "schema_version": SYMBOL_RESULT_CACHE_SCHEMA_VERSION,
        "symbol": symbol,
        "method_name": method_name,
        "entry_start_date": entry_start_date.isoformat(),
        "entry_end_date": entry_end_date.isoformat(),
        "explicit_entry_dates": [value.isoformat() for value in sorted(explicit_entry_dates)],
        "entry_weekday": entry_weekday,
        "short_dte_max": short_dte_max,
        "gap_dte_max": gap_dte_max,
        "short_expiration_dte_targets": list(short_expiration_dte_targets),
        "long_expiration_dte_targets": list(long_expiration_dte_targets),
        "delta_targets": list(delta_targets),
    }


def _load_cached_symbol_result(
    *,
    cache_dir: Path | None,
    metadata: dict[str, object],
) -> SymbolEvaluationResult | None:
    if cache_dir is None:
        return None
    cache_path = _symbol_cache_path(cache_dir, str(metadata["symbol"]))
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("metadata") != metadata:
        return None
    return SymbolEvaluationResult(
        symbol=str(payload["symbol"]),
        weekly_candidate_rows=list(payload.get("weekly_candidate_rows", [])),
        detail_rows=list(payload.get("detail_rows", [])),
        status_message=f"{payload['symbol']}: reused symbol cache",
    )


def _store_cached_symbol_result(
    *,
    cache_dir: Path | None,
    metadata: dict[str, object],
    result: SymbolEvaluationResult,
) -> None:
    if cache_dir is None:
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _symbol_cache_path(cache_dir, result.symbol)
    payload = {
        "symbol": result.symbol,
        "metadata": metadata,
        "weekly_candidate_rows": result.weekly_candidate_rows,
        "detail_rows": result.detail_rows,
    }
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _evaluate_symbol(
    *,
    symbol: str,
    method_name: str,
    factory: sessionmaker,
    benchmark_context_by_date: dict[date, tuple[float, float]],
    entry_start_date: date,
    entry_end_date: date,
    explicit_entry_dates: set[date],
    entry_weekday: int,
    short_dte_max: int,
    gap_dte_max: int,
    short_expiration_dte_targets: tuple[int, ...],
    long_expiration_dte_targets: tuple[int, ...],
    delta_targets: tuple[int, ...],
) -> SymbolEvaluationResult | None:
    with factory() as session:
        bars = pwm._load_bars(
            session,
            symbol=symbol,
            start_date=entry_start_date,
            end_date=entry_end_date + timedelta(days=21),
            warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
        )
        if not bars:
            return SymbolEvaluationResult(
                symbol=symbol,
                weekly_candidate_rows=[],
                detail_rows=[],
                status_message=f"{symbol}: skipped, no underlying bars",
            )
        spot_by_date = {
            bar.trade_date: float(bar.close_price)
            for bar in bars
        }
        entry_dates = [
            bar.trade_date
            for bar in bars
            if (
                (
                    explicit_entry_dates
                    and bar.trade_date in explicit_entry_dates
                )
                or (
                    not explicit_entry_dates
                    and entry_start_date <= bar.trade_date <= entry_end_date
                    and bar.trade_date.weekday() == entry_weekday
                )
            )
        ]
        entry_option_rows_by_date = _load_option_rows_for_dates(
            session,
            symbol=symbol,
            trade_dates=set(entry_dates),
        )
        candidate_earnings_dates = pwm._load_earnings_dates(
            session,
            symbol=symbol,
            start_date=entry_start_date,
            end_date=entry_end_date + timedelta(days=short_dte_max),
        )
        weekly_candidates = _select_weekly_calendar_candidates(
            symbol=symbol,
            entry_dates=entry_dates,
            spot_by_date=spot_by_date,
            option_rows_by_date=entry_option_rows_by_date,
            short_dte_max=short_dte_max,
            gap_dte_max=gap_dte_max,
            short_expiration_dte_targets=short_expiration_dte_targets,
            long_expiration_dte_targets=long_expiration_dte_targets,
            earnings_dates=candidate_earnings_dates,
        )
        if not weekly_candidates:
            return SymbolEvaluationResult(
                symbol=symbol,
                weekly_candidate_rows=[],
                detail_rows=[],
                status_message=f"{symbol}: no ATM short>long weekly calendars in range",
            )
        mark_dates = {candidate.short_expiration for candidate in weekly_candidates}
        mark_option_rows_by_date = _load_option_rows_for_dates(
            session,
            symbol=symbol,
            trade_dates=mark_dates,
        )
        option_feature_rows = pwm._load_option_feature_rows(
            session,
            symbol=symbol,
            start_date=entry_start_date,
            end_date=entry_end_date,
            warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
        )
        earnings_dates = pwm._load_earnings_dates(
            session,
            symbol=symbol,
            start_date=entry_start_date,
            end_date=entry_end_date,
        )

    weekly_candidate_rows = [
        {
            "symbol": candidate.symbol,
            "entry_date": candidate.entry_date.isoformat(),
            "short_expiration": candidate.short_expiration.isoformat(),
            "long_expiration": candidate.long_expiration.isoformat(),
            "spot_close_entry": round(candidate.spot_close_entry, 6),
            "common_atm_strike": round(candidate.common_atm_strike, 6),
            "short_atm_iv_pct": round(candidate.short_atm_iv_pct, 6),
            "long_atm_iv_pct": round(candidate.long_atm_iv_pct, 6),
            "iv_diff_short_minus_long_pct": round(candidate.short_atm_iv_pct - candidate.long_atm_iv_pct, 6),
        }
        for candidate in weekly_candidates
    ]

    requested_dates = {candidate.entry_date for candidate in weekly_candidates}
    store = pwm.HistoricalMarketDataStore(factory, factory)
    option_gateway = pwm.HistoricalOptionGateway(store, symbol)
    symbol_benchmark_context_by_date = (
        pwm._build_benchmark_context_by_date(bars)
        if symbol == pwm.DEFAULT_BENCHMARK_SYMBOL
        else benchmark_context_by_date
    )
    front_iv_series = pwm.build_estimated_iv_series(
        bars,
        option_gateway,
        target_dte=pwm.DEFAULT_FRONT_IV_TARGET_DTE,
        dte_tolerance_days=pwm.DEFAULT_FRONT_IV_DTE_TOLERANCE_DAYS,
    )
    back_iv_series = pwm.build_estimated_iv_series(
        bars,
        option_gateway,
        target_dte=pwm.DEFAULT_BACK_IV_TARGET_DTE,
        dte_tolerance_days=pwm.DEFAULT_BACK_IV_DTE_TOLERANCE_DAYS,
    )
    option_context_by_date = pwm._build_option_context_by_date(
        bars,
        option_feature_rows,
        front_iv_series=front_iv_series,
    )
    iv_context_by_date = pwm._build_iv_context_by_date(
        bars,
        front_iv_series=front_iv_series,
        back_iv_series=back_iv_series,
    )
    features = pwm._build_feature_matrix(
        bars,
        benchmark_context_by_date=symbol_benchmark_context_by_date,
        earnings_dates=earnings_dates,
        option_context_by_date=option_context_by_date,
        iv_context_by_date=iv_context_by_date,
    )
    analog_candidates = pwm._build_analog_candidates(
        bars=bars,
        features=features,
        horizon_bars=5,
    )
    method = pwm._METHOD_NAME_TO_CONFIG[method_name]
    prediction_map = _build_prediction_map_for_dates(
        bars=bars,
        features=features,
        candidates=analog_candidates,
        requested_dates=requested_dates,
        method=method,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=70,
    )

    detail_rows: list[dict[str, object]] = []
    for candidate in weekly_candidates:
        prediction_payload = prediction_map.get(candidate.entry_date)
        prediction_label = "abstain" if prediction_payload is None else str(prediction_payload["prediction"])
        if prediction_label not in {"up", "abstain"}:
            continue
        mark_spot = spot_by_date.get(candidate.short_expiration)
        if mark_spot is None:
            continue
        entry_rows_by_expiration = entry_option_rows_by_date.get(candidate.entry_date, {})
        mark_rows_by_expiration = mark_option_rows_by_date.get(candidate.short_expiration, {})
        if not entry_rows_by_expiration or not mark_rows_by_expiration:
            continue
        for delta_target in delta_targets:
            picked = _pick_calendar_rows_for_delta(
                entry_rows_by_expiration=entry_rows_by_expiration,
                short_expiration=candidate.short_expiration,
                long_expiration=candidate.long_expiration,
                spot_close_entry=candidate.spot_close_entry,
                entry_date=candidate.entry_date,
                common_atm_strike=candidate.common_atm_strike,
                delta_target_pct=delta_target,
            )
            if picked is None:
                continue
            short_row, long_row, resolved_short_delta = picked
            entry_debit = long_row.close_price - short_row.close_price
            short_mark_rows_by_strike = {
                row.strike_price: row
                for row in mark_rows_by_expiration.get(candidate.short_expiration, [])
            }
            long_mark_rows_by_strike = {
                row.strike_price: row
                for row in mark_rows_by_expiration.get(candidate.long_expiration, [])
            }
            short_mark, short_mark_method = _mark_call_leg(
                rows_by_strike=short_mark_rows_by_strike,
                target_strike=short_row.strike_price,
                spot_mark=mark_spot,
                is_expiring_leg=True,
            )
            long_mark, long_mark_method = _mark_call_leg(
                rows_by_strike=long_mark_rows_by_strike,
                target_strike=long_row.strike_price,
                spot_mark=mark_spot,
                is_expiring_leg=False,
            )
            if short_mark is None or long_mark is None:
                continue
            spread_mark = long_mark - short_mark
            pnl = spread_mark - entry_debit
            roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
            detail_rows.append(
                {
                    "symbol": symbol,
                    "entry_date": candidate.entry_date.isoformat(),
                    "prediction": prediction_label,
                    "selected_method": method_name,
                    "prediction_engine": method.engine,
                    "confidence_pct": (
                        None
                        if prediction_payload is None
                        else _round_or_none(float(prediction_payload["confidence_pct"]))
                    ),
                    "entry_prediction_direction": prediction_label,
                    "delta_target_pct": delta_target,
                    "resolved_short_delta": round(resolved_short_delta, 6),
                    "spot_close_entry": round(candidate.spot_close_entry, 6),
                    "spot_close_mark": round(mark_spot, 6),
                    "common_atm_strike": round(candidate.common_atm_strike, 6),
                    "short_expiration": candidate.short_expiration.isoformat(),
                    "long_expiration": candidate.long_expiration.isoformat(),
                    "short_atm_iv_pct": round(candidate.short_atm_iv_pct, 6),
                    "long_atm_iv_pct": round(candidate.long_atm_iv_pct, 6),
                    "iv_diff_short_minus_long_pct": round(
                        candidate.short_atm_iv_pct - candidate.long_atm_iv_pct,
                        6,
                    ),
                    "short_strike": round(short_row.strike_price, 6),
                    "long_strike": round(long_row.strike_price, 6),
                    "short_option_ticker": short_row.option_ticker,
                    "long_option_ticker": long_row.option_ticker,
                    "short_close_entry": round(short_row.close_price, 6),
                    "long_close_entry": round(long_row.close_price, 6),
                    "entry_debit": round(entry_debit, 6),
                    "short_close_mark": round(short_mark, 6),
                    "long_close_mark": round(long_mark, 6),
                    "short_mark_method": short_mark_method,
                    "long_mark_method": long_mark_method,
                    "spread_mark": round(spread_mark, 6),
                    "pnl": round(pnl, 6),
                    "roi_pct": _round_or_none(roi_pct),
                    "nonpositive_debit_flag": int(entry_debit <= 0),
                }
            )

    return SymbolEvaluationResult(
        symbol=symbol,
        weekly_candidate_rows=weekly_candidate_rows,
        detail_rows=detail_rows,
        status_message=(
            f"{symbol}: built predictions for {len(requested_dates)} filtered entry dates using {method_name}; "
            f"produced {len(detail_rows)} trade rows"
        ),
    )


def main() -> int:
    args = build_parser().parse_args()
    if args.entry_start_date > args.entry_end_date:
        raise SystemExit("--entry-start-date must be <= --entry-end-date.")
    if args.max_workers < 1:
        raise SystemExit("--max-workers must be >= 1.")
    if args.entry_weekday < 0 or args.entry_weekday > 6:
        raise SystemExit("--entry-weekday must be between 0 and 6.")
    delta_targets = _parse_delta_targets(args.delta_targets)
    short_expiration_dte_targets = _parse_optional_int_targets(args.short_expiration_dte_targets)
    long_expiration_dte_targets = _parse_optional_int_targets(args.long_expiration_dte_targets)
    explicit_entry_dates = _parse_entry_dates(args.entry_dates)
    database_url = _load_database_url(args.database_url)
    symbols = _load_symbols(args.symbols_file, limit=args.limit_symbols)
    selected_method_by_symbol = _load_selected_method_by_symbol(args.summary_csv)
    missing_symbols = [symbol for symbol in symbols if symbol not in selected_method_by_symbol]
    if missing_symbols:
        raise SystemExit(f"Missing selected_method rows in summary CSV for: {', '.join(missing_symbols)}")
    symbol_cache_dir = None
    if not args.disable_symbol_cache:
        symbol_cache_dir = args.symbol_cache_dir or _default_symbol_cache_dir(args.output_prefix)

    engine = create_engine(database_url, future=True)
    detail_rows: list[dict[str, object]] = []
    weekly_candidate_rows: list[dict[str, object]] = []
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            benchmark_bars = pwm._load_bars(
                session,
                symbol=pwm.DEFAULT_BENCHMARK_SYMBOL,
                start_date=args.entry_start_date,
                end_date=args.entry_end_date + timedelta(days=21),
                warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
            )
        benchmark_context_by_date = pwm._build_benchmark_context_by_date(benchmark_bars)
        if args.max_workers == 1:
            for index, symbol in enumerate(symbols, start=1):
                method_name = selected_method_by_symbol[symbol]
                metadata = _symbol_cache_metadata(
                    symbol=symbol,
                    method_name=method_name,
                    entry_start_date=args.entry_start_date,
                    entry_end_date=args.entry_end_date,
                    explicit_entry_dates=explicit_entry_dates,
                    entry_weekday=args.entry_weekday,
                    short_dte_max=args.short_dte_max,
                    gap_dte_max=args.gap_dte_max,
                    short_expiration_dte_targets=short_expiration_dte_targets,
                    long_expiration_dte_targets=long_expiration_dte_targets,
                    delta_targets=delta_targets,
                )
                cached = _load_cached_symbol_result(cache_dir=symbol_cache_dir, metadata=metadata)
                if cached is not None:
                    print(f"[{index:03d}/{len(symbols):03d}] {symbol}: reused cache")
                    weekly_candidate_rows.extend(cached.weekly_candidate_rows)
                    detail_rows.extend(cached.detail_rows)
                    continue
                print(f"[{index:03d}/{len(symbols):03d}] {symbol}: evaluating")
                result = _evaluate_symbol(
                    symbol=symbol,
                    method_name=method_name,
                    factory=factory,
                    benchmark_context_by_date=benchmark_context_by_date,
                    entry_start_date=args.entry_start_date,
                    entry_end_date=args.entry_end_date,
                    explicit_entry_dates=explicit_entry_dates,
                    entry_weekday=args.entry_weekday,
                    short_dte_max=args.short_dte_max,
                    gap_dte_max=args.gap_dte_max,
                    short_expiration_dte_targets=short_expiration_dte_targets,
                    long_expiration_dte_targets=long_expiration_dte_targets,
                    delta_targets=delta_targets,
                )
                if result is None:
                    continue
                _store_cached_symbol_result(cache_dir=symbol_cache_dir, metadata=metadata, result=result)
                weekly_candidate_rows.extend(result.weekly_candidate_rows)
                detail_rows.extend(result.detail_rows)
                print(f"  {result.status_message}")
        else:
            with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                future_to_symbol: dict[object, tuple[int, str, dict[str, object]]] = {}
                for index, symbol in enumerate(symbols, start=1):
                    method_name = selected_method_by_symbol[symbol]
                    metadata = _symbol_cache_metadata(
                        symbol=symbol,
                        method_name=method_name,
                        entry_start_date=args.entry_start_date,
                        entry_end_date=args.entry_end_date,
                        explicit_entry_dates=explicit_entry_dates,
                        entry_weekday=args.entry_weekday,
                        short_dte_max=args.short_dte_max,
                        gap_dte_max=args.gap_dte_max,
                        short_expiration_dte_targets=short_expiration_dte_targets,
                        long_expiration_dte_targets=long_expiration_dte_targets,
                        delta_targets=delta_targets,
                    )
                    cached = _load_cached_symbol_result(cache_dir=symbol_cache_dir, metadata=metadata)
                    if cached is not None:
                        print(f"[{index:03d}/{len(symbols):03d}] {symbol}: reused cache")
                        weekly_candidate_rows.extend(cached.weekly_candidate_rows)
                        detail_rows.extend(cached.detail_rows)
                        continue
                    future = executor.submit(
                        _evaluate_symbol,
                        symbol=symbol,
                        method_name=method_name,
                        factory=factory,
                        benchmark_context_by_date=benchmark_context_by_date,
                        entry_start_date=args.entry_start_date,
                        entry_end_date=args.entry_end_date,
                        explicit_entry_dates=explicit_entry_dates,
                        entry_weekday=args.entry_weekday,
                        short_dte_max=args.short_dte_max,
                        gap_dte_max=args.gap_dte_max,
                        short_expiration_dte_targets=short_expiration_dte_targets,
                        long_expiration_dte_targets=long_expiration_dte_targets,
                        delta_targets=delta_targets,
                    )
                    future_to_symbol[future] = (index, symbol, metadata)
                for future in as_completed(future_to_symbol):
                    index, symbol, metadata = future_to_symbol[future]
                    result = future.result()
                    print(f"[{index:03d}/{len(symbols):03d}] {symbol}: complete")
                    if result is None:
                        continue
                    _store_cached_symbol_result(cache_dir=symbol_cache_dir, metadata=metadata, result=result)
                    weekly_candidate_rows.extend(result.weekly_candidate_rows)
                    detail_rows.extend(result.detail_rows)
                    print(f"  {result.status_message}")
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No short-IV-greater-than-long-IV call calendar trades were produced.")

    detail_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_trades.csv")
    weekly_candidates_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_weekly_candidates.csv")
    per_delta_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_per_symbol_delta_summary.csv")
    best_delta_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_best_delta_by_symbol.csv")
    aggregate_csv = args.output_prefix.with_name(f"{args.output_prefix.name}_aggregate_by_prediction_delta.csv")
    detail_csv.parent.mkdir(parents=True, exist_ok=True)

    with weekly_candidates_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(weekly_candidate_rows[0].keys()))
        writer.writeheader()
        writer.writerows(weekly_candidate_rows)

    with detail_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    per_delta_map: dict[tuple[str, str, int], dict[str, object]] = {}
    aggregate_map: dict[tuple[str, int], dict[str, object]] = {}
    for row in detail_rows:
        per_key = (
            str(row["symbol"]),
            str(row["prediction"]),
            int(row["delta_target_pct"]),
        )
        aggregate_key = (
            str(row["prediction"]),
            int(row["delta_target_pct"]),
        )
        for target_map, key in ((per_delta_map, per_key), (aggregate_map, aggregate_key)):
            summary = target_map.setdefault(
                key,
                {
                    "symbol": row["symbol"] if target_map is per_delta_map else "ALL",
                    "prediction": row["prediction"],
                    "delta_target_pct": row["delta_target_pct"],
                    "trade_count": 0,
                    "positive_debit_count": 0,
                    "nonpositive_debit_count": 0,
                    "total_debit_paid_positive": 0.0,
                    "total_pnl_positive": 0.0,
                    "total_pnl_all_trades": 0.0,
                    "win_count_positive": 0,
                    "roi_values_positive": [],
                },
            )
            summary["trade_count"] = int(summary["trade_count"]) + 1
            pnl = float(row["pnl"])
            summary["total_pnl_all_trades"] = float(summary["total_pnl_all_trades"]) + pnl
            entry_debit = float(row["entry_debit"])
            if entry_debit > 0:
                summary["positive_debit_count"] = int(summary["positive_debit_count"]) + 1
                summary["total_debit_paid_positive"] = float(summary["total_debit_paid_positive"]) + entry_debit
                summary["total_pnl_positive"] = float(summary["total_pnl_positive"]) + pnl
                if pnl > 0:
                    summary["win_count_positive"] = int(summary["win_count_positive"]) + 1
                roi_value = row["roi_pct"]
                if roi_value is not None:
                    summary["roi_values_positive"].append(float(roi_value))
            else:
                summary["nonpositive_debit_count"] = int(summary["nonpositive_debit_count"]) + 1

    per_delta_rows: list[dict[str, object]] = []
    aggregate_rows: list[dict[str, object]] = []
    for source_map, target_rows in ((per_delta_map, per_delta_rows), (aggregate_map, aggregate_rows)):
        for summary in source_map.values():
            roi_values_positive = list(summary["roi_values_positive"])
            total_debit_paid_positive = float(summary["total_debit_paid_positive"])
            total_pnl_positive = float(summary["total_pnl_positive"])
            positive_debit_count = int(summary["positive_debit_count"])
            target_rows.append(
                {
                    "symbol": summary["symbol"],
                    "prediction": summary["prediction"],
                    "delta_target_pct": summary["delta_target_pct"],
                    "trade_count": summary["trade_count"],
                    "positive_debit_count": positive_debit_count,
                    "nonpositive_debit_count": summary["nonpositive_debit_count"],
                    "total_debit_paid_positive": round(total_debit_paid_positive, 6),
                    "total_pnl_positive": round(total_pnl_positive, 6),
                    "total_pnl_all_trades": round(float(summary["total_pnl_all_trades"]), 6),
                    "avg_roi_positive_debit_pct": (
                        None if not roi_values_positive else round(mean(roi_values_positive), 6)
                    ),
                    "median_roi_positive_debit_pct": (
                        None if not roi_values_positive else round(median(roi_values_positive), 6)
                    ),
                    "weighted_return_positive_debit_pct": (
                        None
                        if total_debit_paid_positive <= 0
                        else round(total_pnl_positive / total_debit_paid_positive * 100.0, 6)
                    ),
                    "win_rate_positive_debit_pct": (
                        None
                        if positive_debit_count == 0
                        else round(int(summary["win_count_positive"]) / positive_debit_count * 100.0, 6)
                    ),
                }
            )

    per_delta_rows.sort(key=lambda row: (row["symbol"], row["prediction"], -int(row["delta_target_pct"])))
    with per_delta_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(per_delta_rows[0].keys()))
        writer.writeheader()
        writer.writerows(per_delta_rows)

    aggregate_rows.sort(key=lambda row: (row["prediction"], -int(row["delta_target_pct"])))
    with aggregate_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(aggregate_rows[0].keys()))
        writer.writeheader()
        writer.writerows(aggregate_rows)

    best_delta_map: dict[tuple[str, str], dict[str, object]] = {}
    for row in per_delta_rows:
        if row["symbol"] == "ALL":
            continue
        weighted_return = row["weighted_return_positive_debit_pct"]
        if weighted_return is None:
            continue
        key = (str(row["symbol"]), str(row["prediction"]))
        incumbent = best_delta_map.get(key)
        if incumbent is None:
            best_delta_map[key] = row
            continue
        incumbent_score = (
            float(incumbent["weighted_return_positive_debit_pct"]),
            int(incumbent["positive_debit_count"]),
            float(incumbent["total_pnl_positive"]),
            int(incumbent["delta_target_pct"]),
        )
        candidate_score = (
            float(weighted_return),
            int(row["positive_debit_count"]),
            float(row["total_pnl_positive"]),
            int(row["delta_target_pct"]),
        )
        if candidate_score > incumbent_score:
            best_delta_map[key] = row

    best_delta_rows: list[dict[str, object]] = []
    all_symbols = sorted(symbols)
    for symbol in all_symbols:
        up_row = best_delta_map.get((symbol, "up"))
        abstain_row = best_delta_map.get((symbol, "abstain"))
        best_delta_rows.append(
            {
                "symbol": symbol,
                "best_up_delta_target_pct": None if up_row is None else up_row["delta_target_pct"],
                "best_up_trade_count": None if up_row is None else up_row["trade_count"],
                "best_up_positive_debit_count": None if up_row is None else up_row["positive_debit_count"],
                "best_up_weighted_return_positive_debit_pct": (
                    None if up_row is None else up_row["weighted_return_positive_debit_pct"]
                ),
                "best_up_avg_roi_positive_debit_pct": (
                    None if up_row is None else up_row["avg_roi_positive_debit_pct"]
                ),
                "best_up_total_debit_paid_positive": None if up_row is None else up_row["total_debit_paid_positive"],
                "best_up_total_pnl_positive": None if up_row is None else up_row["total_pnl_positive"],
                "best_abstain_delta_target_pct": None if abstain_row is None else abstain_row["delta_target_pct"],
                "best_abstain_trade_count": None if abstain_row is None else abstain_row["trade_count"],
                "best_abstain_positive_debit_count": (
                    None if abstain_row is None else abstain_row["positive_debit_count"]
                ),
                "best_abstain_weighted_return_positive_debit_pct": (
                    None if abstain_row is None else abstain_row["weighted_return_positive_debit_pct"]
                ),
                "best_abstain_avg_roi_positive_debit_pct": (
                    None if abstain_row is None else abstain_row["avg_roi_positive_debit_pct"]
                ),
                "best_abstain_total_debit_paid_positive": (
                    None if abstain_row is None else abstain_row["total_debit_paid_positive"]
                ),
                "best_abstain_total_pnl_positive": None if abstain_row is None else abstain_row["total_pnl_positive"],
            }
        )

    with best_delta_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(best_delta_rows[0].keys()))
        writer.writeheader()
        writer.writerows(best_delta_rows)

    print(f"Wrote {weekly_candidates_csv}")
    print(f"Wrote {detail_csv}")
    print(f"Wrote {per_delta_csv}")
    print(f"Wrote {aggregate_csv}")
    print(f"Wrote {best_delta_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
