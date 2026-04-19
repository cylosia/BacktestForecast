from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

try:
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    _SKLEARN_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - depends on optional extra
    GradientBoostingClassifier = None
    LogisticRegression = None
    Pipeline = None
    StandardScaler = None
    _SKLEARN_AVAILABLE = False

try:
    from _bootstrap import bootstrap_repo
except ModuleNotFoundError:  # pragma: no cover - exercised in unit tests
    from scripts._bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import build_engine  # noqa: E402
from backtestforecast.indicators.calculations import ema, rolling_stddev, rsi, sma  # noqa: E402
from backtestforecast.backtests.rules import build_estimated_iv_series, implied_volatility_from_price  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.models import HistoricalEarningsEvent, HistoricalOptionDayBar, HistoricalUnderlyingDayBar  # noqa: E402


DEFAULT_HORIZON_BARS = 5
DEFAULT_MAX_ANALOGS = 25
DEFAULT_MIN_CANDIDATE_COUNT = 60
DEFAULT_MIN_SPACING_BARS = 5
DEFAULT_WARMUP_CALENDAR_DAYS = 120
DEFAULT_START_DATE = date(2015, 1, 1)
DEFAULT_END_DATE = date.today()
DEFAULT_PREDICTION_METHOD = "auto"
DEFAULT_BENCHMARK_SYMBOL = "SPY"
DEFAULT_EARNINGS_CONTEXT_DAYS = 7
DEFAULT_FRONT_IV_TARGET_DTE = 7
DEFAULT_FRONT_IV_DTE_TOLERANCE_DAYS = 3
DEFAULT_BACK_IV_TARGET_DTE = 21
DEFAULT_BACK_IV_DTE_TOLERANCE_DAYS = 7
DEFAULT_IV_RANK_LOOKBACK_BARS = 63
DEFAULT_IV_CHANGE_LOOKBACK_BARS = 5
DEFAULT_MIN_IV_HISTORY = 20
DEFAULT_OPTION_WING_TARGET_MONEYNESS = 0.05
DEFAULT_ML_CALIBRATION_FRACTION = 0.2
DEFAULT_MIN_CALIBRATION_SIZE = 40
_SIGN_LABELS = (-1, 0, 1)


@dataclass(frozen=True, slots=True)
class AnalogCandidate:
    trade_date: date
    feature_index: int
    features: tuple[float, ...]
    forward_return_pct: float
    target_sign: int


@dataclass(frozen=True, slots=True)
class OptionFeatureRow:
    trade_date: date
    expiration_date: date
    contract_type: str
    strike_price: float
    close_price: float
    volume: float


@dataclass(frozen=True, slots=True)
class PredictionSnapshot:
    trade_date: date
    predicted_sign: int
    predicted_return_median_pct: float | None = None
    predicted_return_mean_pct: float | None = None
    candidate_pool_count: int | None = None
    analogs_used: int | None = None
    up_neighbor_ratio_pct: float | None = None
    down_neighbor_ratio_pct: float | None = None
    flat_neighbor_ratio_pct: float | None = None
    analog_dates: tuple[date, ...] = ()
    probability_up_pct: float | None = None
    probability_down_pct: float | None = None
    probability_flat_pct: float | None = None
    confidence_pct: float | None = None
    train_sample_count: int | None = None
    model_name: str | None = None


@dataclass(frozen=True, slots=True)
class PredictionMethodConfig:
    name: str
    vote_mode: str
    engine: str = "analog"
    max_analogs: int = DEFAULT_MAX_ANALOGS
    same_trend_bucket: bool = False
    same_rsi_bucket: bool = False
    ml_model_name: str = ""
    confidence_threshold: float = 0.0
    min_train_size: int = 120
    retrain_every_bars: int = 20
    calibration_method: str = ""
    calibration_fraction: float = DEFAULT_ML_CALIBRATION_FRACTION
    min_calibration_size: int = DEFAULT_MIN_CALIBRATION_SIZE


@dataclass(frozen=True, slots=True)
class FittedMlModel:
    estimator: Any
    calibrator: Any | None = None


_ANALOG_METHOD_CONFIGS: tuple[PredictionMethodConfig, ...] = (
    PredictionMethodConfig(name="median12trend", vote_mode="median_return", max_analogs=12, same_trend_bucket=True),
    PredictionMethodConfig(name="median12rsi", vote_mode="median_return", max_analogs=12, same_rsi_bucket=True),
    PredictionMethodConfig(
        name="vote12rsi",
        vote_mode="weighted_vote",
        max_analogs=12,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(
        name="vote12trendrsi",
        vote_mode="weighted_vote",
        max_analogs=12,
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="vote15", vote_mode="weighted_vote", max_analogs=15),
    PredictionMethodConfig(name="median15trend", vote_mode="median_return", max_analogs=15, same_trend_bucket=True),
    PredictionMethodConfig(
        name="median15trendrsi",
        vote_mode="median_return",
        max_analogs=15,
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="median15rsi", vote_mode="median_return", max_analogs=15, same_rsi_bucket=True),
    PredictionMethodConfig(name="vote15rsi", vote_mode="weighted_vote", max_analogs=15, same_rsi_bucket=True),
    PredictionMethodConfig(
        name="vote15trendrsi",
        vote_mode="weighted_vote",
        max_analogs=15,
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="median20", vote_mode="median_return", max_analogs=20),
    PredictionMethodConfig(name="vote20", vote_mode="weighted_vote", max_analogs=20),
    PredictionMethodConfig(name="median20trend", vote_mode="median_return", max_analogs=20, same_trend_bucket=True),
    PredictionMethodConfig(
        name="median20trendrsi",
        vote_mode="median_return",
        max_analogs=20,
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="vote20trend", vote_mode="weighted_vote", max_analogs=20, same_trend_bucket=True),
    PredictionMethodConfig(name="median20rsi", vote_mode="median_return", max_analogs=20, same_rsi_bucket=True),
    PredictionMethodConfig(name="vote20rsi", vote_mode="weighted_vote", max_analogs=20, same_rsi_bucket=True),
    PredictionMethodConfig(
        name="vote20trendrsi",
        vote_mode="weighted_vote",
        max_analogs=20,
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="median25", vote_mode="median_return"),
    PredictionMethodConfig(name="median25trend", vote_mode="median_return", same_trend_bucket=True),
    PredictionMethodConfig(name="median25rsi", vote_mode="median_return", same_rsi_bucket=True),
    PredictionMethodConfig(
        name="median25trendrsi",
        vote_mode="median_return",
        same_trend_bucket=True,
        same_rsi_bucket=True,
    ),
    PredictionMethodConfig(name="vote25", vote_mode="weighted_vote"),
    PredictionMethodConfig(name="vote25trend", vote_mode="weighted_vote", same_trend_bucket=True),
    PredictionMethodConfig(name="vote25rsi", vote_mode="weighted_vote", same_rsi_bucket=True),
    PredictionMethodConfig(name="vote25trendrsi", vote_mode="weighted_vote", same_trend_bucket=True, same_rsi_bucket=True),
    PredictionMethodConfig(name="median30trend", vote_mode="median_return", max_analogs=30, same_trend_bucket=True),
    PredictionMethodConfig(name="vote30trend", vote_mode="weighted_vote", max_analogs=30, same_trend_bucket=True),
    PredictionMethodConfig(name="median40rsi", vote_mode="median_return", max_analogs=40, same_rsi_bucket=True),
    PredictionMethodConfig(name="vote40rsi", vote_mode="weighted_vote", max_analogs=40, same_rsi_bucket=True),
)
_ML_METHOD_CONFIGS: tuple[PredictionMethodConfig, ...] = (
    PredictionMethodConfig(
        name="mllogreg52",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="logistic_regression",
        confidence_threshold=0.52,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mllogreg56",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="logistic_regression",
        confidence_threshold=0.56,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mllogreg60",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="logistic_regression",
        confidence_threshold=0.60,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mlgb68",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.68,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mlgb70",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.70,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mlgb72",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.72,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mlgb76",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.76,
        min_train_size=120,
        retrain_every_bars=20,
    ),
    PredictionMethodConfig(
        name="mlgbp60",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.60,
        min_train_size=120,
        retrain_every_bars=20,
        calibration_method="platt",
    ),
    PredictionMethodConfig(
        name="mlgbp64",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.64,
        min_train_size=120,
        retrain_every_bars=20,
        calibration_method="platt",
    ),
    PredictionMethodConfig(
        name="mlgbp68",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.68,
        min_train_size=120,
        retrain_every_bars=20,
        calibration_method="platt",
    ),
    PredictionMethodConfig(
        name="mlgbp72",
        vote_mode="ml_classifier",
        engine="ml",
        ml_model_name="gradient_boosting",
        confidence_threshold=0.72,
        min_train_size=120,
        retrain_every_bars=20,
        calibration_method="platt",
    ),
) if _SKLEARN_AVAILABLE else ()
_METHOD_CONFIGS: tuple[PredictionMethodConfig, ...] = _ANALOG_METHOD_CONFIGS + _ML_METHOD_CONFIGS
_METHOD_NAME_TO_CONFIG = {config.name: config for config in _METHOD_CONFIGS}
_METHOD_NAMES = tuple(_METHOD_NAME_TO_CONFIG)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Predict next-week price direction using local historical daily bars and a "
            "walk-forward nearest-analog baseline."
        )
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=30_000)
    parser.add_argument("--symbol", required=True, help="Ticker symbol to score.")
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=DEFAULT_START_DATE,
        help="Earliest trade date to include in the evaluation window. Defaults to 2015-01-01.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=DEFAULT_END_DATE,
        help="Latest trade date to load. Defaults to today.",
    )
    parser.add_argument(
        "--horizon-bars",
        type=int,
        default=DEFAULT_HORIZON_BARS,
        help="Forward trading-bar horizon for the target. Defaults to 5.",
    )
    parser.add_argument(
        "--max-analogs",
        type=int,
        default=None,
        help=(
            "Optional override for how many nearest historical analogs each analog method should use. "
            "Defaults to each method's built-in analog count."
        ),
    )
    parser.add_argument(
        "--min-candidate-count",
        type=int,
        default=DEFAULT_MIN_CANDIDATE_COUNT,
        help="Minimum historical candidates required before emitting a prediction. Defaults to 60.",
    )
    parser.add_argument(
        "--min-spacing-bars",
        type=int,
        default=DEFAULT_MIN_SPACING_BARS,
        help="Minimum spacing between selected analog dates, in bars. Defaults to 5.",
    )
    parser.add_argument(
        "--warmup-calendar-days",
        type=int,
        default=DEFAULT_WARMUP_CALENDAR_DAYS,
        help="Calendar days of extra history to load before start-date for indicators. Defaults to 120.",
    )
    parser.add_argument(
        "--prediction-method",
        choices=(DEFAULT_PREDICTION_METHOD, *_METHOD_NAMES),
        default=DEFAULT_PREDICTION_METHOD,
        help=(
            "Prediction rule to use. 'auto' compares the configured analog and ML rules over the loaded "
            "history and selects the highest-accuracy method for this symbol/window."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional output path. Defaults to logs/<symbol>_weekly_price_movement_<dates>.json",
    )
    return parser


def _load_bars(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    warmup_calendar_days: int,
) -> list[DailyBar]:
    warmup_start = start_date - timedelta(days=warmup_calendar_days)
    rows = session.execute(
        select(
            HistoricalUnderlyingDayBar.trade_date,
            HistoricalUnderlyingDayBar.open_price,
            HistoricalUnderlyingDayBar.high_price,
            HistoricalUnderlyingDayBar.low_price,
            HistoricalUnderlyingDayBar.close_price,
            HistoricalUnderlyingDayBar.volume,
        )
        .where(
            HistoricalUnderlyingDayBar.symbol == symbol,
            HistoricalUnderlyingDayBar.trade_date >= warmup_start,
            HistoricalUnderlyingDayBar.trade_date <= end_date,
        )
        .order_by(HistoricalUnderlyingDayBar.trade_date)
    ).all()
    return [
        DailyBar(
            trade_date=row.trade_date,
            open_price=float(row.open_price),
            high_price=float(row.high_price),
            low_price=float(row.low_price),
            close_price=float(row.close_price),
            volume=float(row.volume),
        )
        for row in rows
    ]


def _load_option_feature_rows(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    warmup_calendar_days: int,
) -> list[OptionFeatureRow]:
    warmup_start = start_date - timedelta(days=warmup_calendar_days)
    expiration_rank = func.dense_rank().over(
        partition_by=HistoricalOptionDayBar.trade_date,
        order_by=HistoricalOptionDayBar.expiration_date,
    ).label("expiration_rank")
    ranked_rows = (
        select(
            HistoricalOptionDayBar.trade_date.label("trade_date"),
            HistoricalOptionDayBar.expiration_date.label("expiration_date"),
            HistoricalOptionDayBar.contract_type.label("contract_type"),
            HistoricalOptionDayBar.strike_price.label("strike_price"),
            HistoricalOptionDayBar.close_price.label("close_price"),
            HistoricalOptionDayBar.volume.label("volume"),
            expiration_rank,
        )
        .where(
            HistoricalOptionDayBar.underlying_symbol == symbol,
            HistoricalOptionDayBar.trade_date >= warmup_start,
            HistoricalOptionDayBar.trade_date <= end_date,
            HistoricalOptionDayBar.expiration_date > HistoricalOptionDayBar.trade_date,
        )
        .subquery()
    )
    rows = session.execute(
        select(
            ranked_rows.c.trade_date,
            ranked_rows.c.expiration_date,
            ranked_rows.c.contract_type,
            ranked_rows.c.strike_price,
            ranked_rows.c.close_price,
            ranked_rows.c.volume,
        )
        .where(ranked_rows.c.expiration_rank <= 2)
        .order_by(
            ranked_rows.c.trade_date,
            ranked_rows.c.expiration_date,
            ranked_rows.c.contract_type,
            ranked_rows.c.strike_price,
        )
    ).all()
    return [
        OptionFeatureRow(
            trade_date=row.trade_date,
            expiration_date=row.expiration_date,
            contract_type=str(row.contract_type),
            strike_price=float(row.strike_price),
            close_price=float(row.close_price),
            volume=float(row.volume),
        )
        for row in rows
    ]


def _default_output_path(*, symbol: str, start_date: date, end_date: date, horizon_bars: int) -> Path:
    return (
        ROOT
        / "logs"
        / f"{symbol.lower()}_weekly_price_movement_h{horizon_bars}_{start_date.isoformat()}_{end_date.isoformat()}.json"
    )


def _sign_from_future_close(*, current_close: float, future_close: float, epsilon: float = 1e-12) -> int:
    if current_close <= 0:
        raise ValueError("current_close must be positive.")
    raw_return = (future_close / current_close) - 1.0
    if raw_return > epsilon:
        return 1
    if raw_return < -epsilon:
        return -1
    return 0


def _sign_from_return_pct(return_pct: float, epsilon: float = 1e-9) -> int:
    if return_pct > epsilon:
        return 1
    if return_pct < -epsilon:
        return -1
    return 0


def _daily_returns(closes: list[float]) -> list[float]:
    returns: list[float] = [0.0]
    for index in range(1, len(closes)):
        previous_close = closes[index - 1]
        current_close = closes[index]
        returns.append(0.0 if previous_close <= 0 else ((current_close - previous_close) / previous_close) * 100.0)
    return returns


def _safe_pct_change(*, current: float, base: float) -> float | None:
    if not math.isfinite(current) or not math.isfinite(base) or base <= 0:
        return None
    return ((current - base) / base) * 100.0


def _load_earnings_dates(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    context_days: int = DEFAULT_EARNINGS_CONTEXT_DAYS,
) -> set[date]:
    lookup_start = start_date - timedelta(days=context_days)
    lookup_end = end_date + timedelta(days=context_days)
    return {
        row[0]
        for row in session.execute(
            select(HistoricalEarningsEvent.event_date)
            .distinct()
            .where(
                HistoricalEarningsEvent.symbol == symbol,
                HistoricalEarningsEvent.event_date >= lookup_start,
                HistoricalEarningsEvent.event_date <= lookup_end,
            )
        ).all()
    }


def _build_benchmark_context_by_date(
    benchmark_bars: list[DailyBar],
) -> dict[date, tuple[float, float]]:
    closes = [bar.close_price for bar in benchmark_bars]
    context_by_date: dict[date, tuple[float, float]] = {}
    for index, bar in enumerate(benchmark_bars):
        if index < 20:
            continue
        ret_5d = _safe_pct_change(current=closes[index], base=closes[index - 5])
        ret_20d = _safe_pct_change(current=closes[index], base=closes[index - 20])
        if ret_5d is None or ret_20d is None:
            continue
        context_by_date[bar.trade_date] = (float(ret_5d), float(ret_20d))
    return context_by_date


def _aggregate_option_rows_by_type_and_strike(
    rows: list[OptionFeatureRow],
) -> dict[str, dict[float, tuple[float, float]]]:
    aggregated_volume: dict[tuple[str, float], float] = defaultdict(float)
    aggregated_premium: dict[tuple[str, float], float] = defaultdict(float)
    max_close_by_key: dict[tuple[str, float], float] = defaultdict(float)
    for row in rows:
        if row.contract_type not in {"call", "put"} or row.strike_price <= 0:
            continue
        key = (row.contract_type, row.strike_price)
        volume = max(row.volume, 0.0)
        close_price = max(row.close_price, 0.0)
        aggregated_volume[key] += volume
        aggregated_premium[key] += close_price * volume
        max_close_by_key[key] = max(max_close_by_key[key], close_price)

    result: dict[str, dict[float, tuple[float, float]]] = {
        "call": {},
        "put": {},
    }
    for key, total_volume in aggregated_volume.items():
        contract_type, strike_price = key
        close_price = (
            aggregated_premium[key] / total_volume
            if total_volume > 0 and aggregated_premium[key] > 0
            else max_close_by_key[key]
        )
        result[contract_type][strike_price] = (float(close_price), float(total_volume))
    return result


def _select_atm_common_strike(
    aggregated: dict[str, dict[float, tuple[float, float]]],
    *,
    current_close: float,
) -> float | None:
    if current_close <= 0:
        return None
    common_strikes = sorted(
        strike_price
        for strike_price in set(aggregated["call"]).intersection(aggregated["put"])
        if aggregated["call"][strike_price][0] > 0 and aggregated["put"][strike_price][0] > 0
    )
    if common_strikes:
        return float(min(common_strikes, key=lambda value: (abs(value - current_close), value)))
    return None


def _select_atm_call_put_prices(
    rows: list[OptionFeatureRow],
    *,
    current_close: float,
) -> tuple[float | None, float | None]:
    if current_close <= 0:
        return None, None
    aggregated = _aggregate_option_rows_by_type_and_strike(rows)
    atm_strike_price = _select_atm_common_strike(aggregated, current_close=current_close)
    if atm_strike_price is not None:
        return aggregated["call"][atm_strike_price][0], aggregated["put"][atm_strike_price][0]

    def _pick_price(contract_type: str) -> float | None:
        candidates = [
            (strike_price, close_price)
            for strike_price, (close_price, _volume) in aggregated[contract_type].items()
            if close_price > 0
        ]
        if not candidates:
            return None
        _, close_price = min(candidates, key=lambda item: (abs(item[0] - current_close), item[0]))
        return close_price

    return _pick_price("call"), _pick_price("put")


def _select_target_otm_option(
    aggregated: dict[str, dict[float, tuple[float, float]]],
    *,
    contract_type: str,
    current_close: float,
    target_moneyness: float = DEFAULT_OPTION_WING_TARGET_MONEYNESS,
) -> tuple[float | None, float | None]:
    if current_close <= 0:
        return None, None
    if contract_type == "put":
        target_strike = current_close * (1.0 - target_moneyness)
        candidates = [
            (strike_price, close_price, volume)
            for strike_price, (close_price, volume) in aggregated["put"].items()
            if strike_price < current_close and close_price > 0
        ]
    elif contract_type == "call":
        target_strike = current_close * (1.0 + target_moneyness)
        candidates = [
            (strike_price, close_price, volume)
            for strike_price, (close_price, volume) in aggregated["call"].items()
            if strike_price > current_close and close_price > 0
        ]
    else:
        raise ValueError(f"Unsupported contract type: {contract_type}")
    if not candidates:
        return None, None
    strike_price, close_price, _volume = min(
        candidates,
        key=lambda item: (abs(item[0] - target_strike), abs(item[0] - current_close), -item[2], item[0]),
    )
    return float(strike_price), float(close_price)


def _estimate_option_iv_pct(
    *,
    option_price: float | None,
    underlying_price: float,
    strike_price: float | None,
    dte_days: float,
    contract_type: str,
) -> float:
    if option_price is None or strike_price is None or option_price <= 0 or strike_price <= 0 or dte_days <= 0:
        return 0.0
    implied_volatility = implied_volatility_from_price(
        option_price=option_price,
        underlying_price=underlying_price,
        strike_price=strike_price,
        time_to_expiry_years=max(dte_days, 1.0) / 365.0,
        option_type=contract_type,
    )
    if implied_volatility is None or implied_volatility <= 0:
        return 0.0
    return implied_volatility * 100.0


def _build_option_context_by_date(
    bars: list[DailyBar],
    option_rows: list[OptionFeatureRow],
    *,
    front_iv_series: list[float | None] | None = None,
) -> dict[date, tuple[float, ...]]:
    bars_by_date = {bar.trade_date: bar for bar in bars}
    grouped_rows: dict[date, dict[date, list[OptionFeatureRow]]] = defaultdict(lambda: defaultdict(list))
    for row in option_rows:
        if row.expiration_date <= row.trade_date:
            continue
        grouped_rows[row.trade_date][row.expiration_date].append(row)

    bar_index_by_date = {bar.trade_date: index for index, bar in enumerate(bars)}
    context_by_date: dict[date, tuple[float, ...]] = {}
    for trade_date, rows_by_expiration in grouped_rows.items():
        bar = bars_by_date.get(trade_date)
        if bar is None or bar.close_price <= 0:
            continue
        bar_index = bar_index_by_date.get(trade_date)
        if bar_index is None:
            continue
        expirations = sorted(rows_by_expiration)
        if not expirations:
            continue
        front_expiration = expirations[0]
        second_expiration = expirations[1] if len(expirations) > 1 else None
        front_rows = rows_by_expiration[front_expiration]
        front_aggregated = _aggregate_option_rows_by_type_and_strike(front_rows)
        front_call_price, front_put_price = _select_atm_call_put_prices(front_rows, current_close=bar.close_price)
        front_atm_reference_strike = _select_atm_common_strike(front_aggregated, current_close=bar.close_price)
        otm_put_strike_limit = front_atm_reference_strike if front_atm_reference_strike is not None else bar.close_price
        otm_call_strike_floor = front_atm_reference_strike if front_atm_reference_strike is not None else bar.close_price
        front_dte_days = float((front_expiration - trade_date).days)
        front_call_volume = sum(volume for _close_price, volume in front_aggregated["call"].values())
        front_put_volume = sum(volume for _close_price, volume in front_aggregated["put"].values())
        front_call_premium = sum(
            close_price * volume
            for close_price, volume in front_aggregated["call"].values()
            if close_price > 0 and volume > 0
        )
        front_put_premium = sum(
            close_price * volume
            for close_price, volume in front_aggregated["put"].values()
            if close_price > 0 and volume > 0
        )
        front_total_premium = front_call_premium + front_put_premium
        front_atm_straddle_pct = (
            ((front_call_price + front_put_price) / bar.close_price) * 100.0
            if front_call_price is not None and front_put_price is not None
            else 0.0
        )
        front_atm_skew_pct = (
            ((front_put_price - front_call_price) / bar.close_price) * 100.0
            if front_call_price is not None and front_put_price is not None
            else 0.0
        )
        put_call_volume_log_ratio = math.log1p(front_put_volume) - math.log1p(front_call_volume)
        put_call_premium_balance = (
            ((front_put_premium - front_call_premium) / front_total_premium)
            if front_total_premium > 0
            else 0.0
        )
        option_activity_log_ratio = math.log1p(((front_call_volume + front_put_volume) * 100.0) / max(bar.volume, 1.0))
        front_otm_put_volume = sum(
            volume
            for strike_price, (_close_price, volume) in front_aggregated["put"].items()
            if strike_price < otm_put_strike_limit
        )
        front_otm_call_volume = sum(
            volume
            for strike_price, (_close_price, volume) in front_aggregated["call"].items()
            if strike_price > otm_call_strike_floor
        )
        front_otm_put_premium = sum(
            close_price * volume
            for strike_price, (close_price, volume) in front_aggregated["put"].items()
            if strike_price < otm_put_strike_limit and close_price > 0 and volume > 0
        )
        front_otm_call_premium = sum(
            close_price * volume
            for strike_price, (close_price, volume) in front_aggregated["call"].items()
            if strike_price > otm_call_strike_floor and close_price > 0 and volume > 0
        )
        front_otm_total_premium = front_otm_put_premium + front_otm_call_premium
        front_otm_put_call_volume_log_ratio = math.log1p(front_otm_put_volume) - math.log1p(front_otm_call_volume)
        front_otm_put_call_premium_balance = (
            ((front_otm_put_premium - front_otm_call_premium) / front_otm_total_premium)
            if front_otm_total_premium > 0
            else 0.0
        )
        front_otm_put_strike, front_otm_put_price = _select_target_otm_option(
            front_aggregated,
            contract_type="put",
            current_close=bar.close_price,
        )
        front_otm_call_strike, front_otm_call_price = _select_target_otm_option(
            front_aggregated,
            contract_type="call",
            current_close=bar.close_price,
        )
        front_otm_put_iv_pct = _estimate_option_iv_pct(
            option_price=front_otm_put_price,
            underlying_price=bar.close_price,
            strike_price=front_otm_put_strike,
            dte_days=front_dte_days,
            contract_type="put",
        )
        front_otm_call_iv_pct = _estimate_option_iv_pct(
            option_price=front_otm_call_price,
            underlying_price=bar.close_price,
            strike_price=front_otm_call_strike,
            dte_days=front_dte_days,
            contract_type="call",
        )
        front_atm_iv_pct = (
            float(front_iv_series[bar_index] * 100.0)
            if front_iv_series is not None and front_iv_series[bar_index] is not None and front_iv_series[bar_index] > 0
            else 0.0
        )
        front_iv_risk_reversal_pct = (
            front_otm_put_iv_pct - front_otm_call_iv_pct
            if front_otm_put_iv_pct > 0 and front_otm_call_iv_pct > 0
            else 0.0
        )
        front_iv_butterfly_pct = (
            ((front_otm_put_iv_pct + front_otm_call_iv_pct) / 2.0) - front_atm_iv_pct
            if front_atm_iv_pct > 0 and front_otm_put_iv_pct > 0 and front_otm_call_iv_pct > 0
            else 0.0
        )

        second_atm_straddle_pct = 0.0
        if second_expiration is not None:
            second_call_price, second_put_price = _select_atm_call_put_prices(
                rows_by_expiration[second_expiration],
                current_close=bar.close_price,
            )
            if second_call_price is not None and second_put_price is not None:
                second_atm_straddle_pct = ((second_call_price + second_put_price) / bar.close_price) * 100.0
        straddle_term_structure_pct = second_atm_straddle_pct - front_atm_straddle_pct
        context_by_date[trade_date] = (
            front_dte_days,
            front_atm_straddle_pct,
            front_atm_skew_pct,
            put_call_volume_log_ratio,
            put_call_premium_balance,
            option_activity_log_ratio,
            straddle_term_structure_pct,
            1.0,
            front_otm_put_iv_pct,
            front_otm_call_iv_pct,
            front_iv_risk_reversal_pct,
            front_iv_butterfly_pct,
            front_otm_put_call_volume_log_ratio,
            front_otm_put_call_premium_balance,
        )
    return context_by_date


def _iv_rank_pct(
    values: list[float],
    *,
    current_value: float,
) -> float:
    if not values:
        return 50.0
    window_min = min(values)
    window_max = max(values)
    if window_max <= window_min:
        return 50.0
    return ((current_value - window_min) / (window_max - window_min)) * 100.0


def _build_iv_context_by_date(
    bars: list[DailyBar],
    *,
    front_iv_series: list[float | None],
    back_iv_series: list[float | None],
    iv_rank_lookback_bars: int = DEFAULT_IV_RANK_LOOKBACK_BARS,
    iv_change_lookback_bars: int = DEFAULT_IV_CHANGE_LOOKBACK_BARS,
) -> dict[date, tuple[float, float, float, float, float, float, float]]:
    context_by_date: dict[date, tuple[float, float, float, float, float, float, float]] = {}
    for index, bar in enumerate(bars):
        front_iv = front_iv_series[index]
        back_iv = back_iv_series[index]
        if front_iv is None or back_iv is None or front_iv <= 0 or back_iv <= 0:
            continue
        history_start = max(0, index - iv_rank_lookback_bars + 1)
        front_iv_window = [
            value
            for value in front_iv_series[history_start : index + 1]
            if value is not None and value > 0
        ]
        if len(front_iv_window) < DEFAULT_MIN_IV_HISTORY:
            continue
        previous_front_iv = None
        if index >= iv_change_lookback_bars:
            previous_front_iv = front_iv_series[index - iv_change_lookback_bars]

        front_iv_pct = front_iv * 100.0
        back_iv_pct = back_iv * 100.0
        iv_term_structure_pct = back_iv_pct - front_iv_pct
        iv_rank_pct = _iv_rank_pct(front_iv_window, current_value=front_iv)
        iv_change_5d_pct = (
            (front_iv - previous_front_iv) * 100.0
            if previous_front_iv is not None and previous_front_iv > 0
            else 0.0
        )
        context_by_date[bar.trade_date] = (
            front_iv_pct,
            back_iv_pct,
            iv_term_structure_pct,
            iv_rank_pct,
            iv_change_5d_pct,
            (front_iv_pct + back_iv_pct) / 2.0,
            1.0,
        )
    return context_by_date


def _has_earnings_in_window(
    *,
    earnings_dates: set[date],
    window_start: date,
    window_end: date,
) -> float:
    return 1.0 if any(window_start <= earnings_date <= window_end for earnings_date in earnings_dates) else 0.0


def _build_feature_matrix(
    bars: list[DailyBar],
    *,
    benchmark_context_by_date: dict[date, tuple[float, float]] | None = None,
    earnings_dates: set[date] | None = None,
    option_context_by_date: dict[date, tuple[float, ...]] | None = None,
    iv_context_by_date: dict[date, tuple[float, float, float, float, float, float, float]] | None = None,
) -> list[tuple[float, ...] | None]:
    closes = [bar.close_price for bar in bars]
    highs = [bar.high_price for bar in bars]
    lows = [bar.low_price for bar in bars]
    volumes = [bar.volume for bar in bars]
    returns = _daily_returns(closes)
    rsi14 = rsi(closes, 14)
    ema8 = ema(closes, 8)
    ema21 = ema(closes, 21)
    avg_volume20 = sma(volumes, 20)
    vol20 = rolling_stddev(returns, 20, ddof=1)
    effective_earnings_dates = earnings_dates or set()

    features: list[tuple[float, ...] | None] = []
    for index, close in enumerate(closes):
        if index < 20:
            features.append(None)
            continue
        ret_1d = _safe_pct_change(current=close, base=closes[index - 1])
        ret_3d = _safe_pct_change(current=close, base=closes[index - 3])
        ret_5d = _safe_pct_change(current=close, base=closes[index - 5])
        ret_10d = _safe_pct_change(current=close, base=closes[index - 10])
        ret_20d = _safe_pct_change(current=close, base=closes[index - 20])
        current_rsi = rsi14[index]
        current_ema8 = ema8[index]
        current_ema21 = ema21[index]
        current_avg_volume20 = avg_volume20[index]
        current_vol20 = vol20[index]
        intraday_range_pct = _safe_pct_change(current=highs[index], base=lows[index]) if lows[index] > 0 else None
        ema_gap_pct = (
            None
            if current_ema8 is None or current_ema21 is None or close <= 0
            else ((current_ema8 - current_ema21) / close) * 100.0
        )
        volume_ratio = (
            None
            if current_avg_volume20 is None or current_avg_volume20 <= 0
            else volumes[index] / current_avg_volume20
        )
        annualized_vol_pct = None if current_vol20 is None else current_vol20 * math.sqrt(252.0)
        benchmark_ret_5d = None
        benchmark_ret_20d = None
        rel_ret_5d = None
        rel_ret_20d = None
        if benchmark_context_by_date is not None:
            benchmark_context = benchmark_context_by_date.get(bars[index].trade_date)
            if benchmark_context is not None:
                benchmark_ret_5d = benchmark_context[0]
                benchmark_ret_20d = benchmark_context[1]
                rel_ret_5d = None if ret_5d is None else ret_5d - benchmark_ret_5d
                rel_ret_20d = None if ret_20d is None else ret_20d - benchmark_ret_20d
        if benchmark_ret_5d is None or benchmark_ret_20d is None or rel_ret_5d is None or rel_ret_20d is None:
            benchmark_ret_5d = 0.0
            benchmark_ret_20d = 0.0
            rel_ret_5d = ret_5d
            rel_ret_20d = ret_20d
        earnings_within_7d = _has_earnings_in_window(
            earnings_dates=effective_earnings_dates,
            window_start=bars[index].trade_date,
            window_end=bars[index].trade_date + timedelta(days=DEFAULT_EARNINGS_CONTEXT_DAYS),
        )
        recent_earnings_7d = _has_earnings_in_window(
            earnings_dates=effective_earnings_dates,
            window_start=bars[index].trade_date - timedelta(days=DEFAULT_EARNINGS_CONTEXT_DAYS),
            window_end=bars[index].trade_date - timedelta(days=1),
        )
        option_context = None if option_context_by_date is None else option_context_by_date.get(bars[index].trade_date)
        if option_context is None:
            front_dte_days = 0.0
            front_atm_straddle_pct = 0.0
            front_atm_skew_pct = 0.0
            put_call_volume_log_ratio = 0.0
            put_call_premium_balance = 0.0
            option_activity_log_ratio = 0.0
            straddle_term_structure_pct = 0.0
            has_option_context = 0.0
            front_otm_put_iv_pct = 0.0
            front_otm_call_iv_pct = 0.0
            front_iv_risk_reversal_pct = 0.0
            front_iv_butterfly_pct = 0.0
            front_otm_put_call_volume_log_ratio = 0.0
            front_otm_put_call_premium_balance = 0.0
        else:
            (
                front_dte_days,
                front_atm_straddle_pct,
                front_atm_skew_pct,
                put_call_volume_log_ratio,
                put_call_premium_balance,
                option_activity_log_ratio,
                straddle_term_structure_pct,
                has_option_context,
                front_otm_put_iv_pct,
                front_otm_call_iv_pct,
                front_iv_risk_reversal_pct,
                front_iv_butterfly_pct,
                front_otm_put_call_volume_log_ratio,
                front_otm_put_call_premium_balance,
            ) = option_context
        iv_context = None if iv_context_by_date is None else iv_context_by_date.get(bars[index].trade_date)
        if iv_context is None:
            front_iv_pct = 0.0
            back_iv_pct = 0.0
            iv_term_structure_pct = 0.0
            iv_rank_pct = 0.0
            iv_change_5d_pct = 0.0
            blended_iv_pct = 0.0
            has_iv_context = 0.0
        else:
            (
                front_iv_pct,
                back_iv_pct,
                iv_term_structure_pct,
                iv_rank_pct,
                iv_change_5d_pct,
                blended_iv_pct,
                has_iv_context,
            ) = iv_context
        rv20_to_front_iv_ratio = (
            annualized_vol_pct / front_iv_pct
            if front_iv_pct > 0
            else 0.0
        )
        values = (
            ret_1d,
            ret_3d,
            ret_5d,
            ret_10d,
            ret_20d,
            current_rsi,
            ema_gap_pct,
            annualized_vol_pct,
            volume_ratio,
            intraday_range_pct,
            benchmark_ret_5d,
            benchmark_ret_20d,
            rel_ret_5d,
            rel_ret_20d,
            earnings_within_7d,
            recent_earnings_7d,
            front_dte_days,
            front_atm_straddle_pct,
            front_atm_skew_pct,
            put_call_volume_log_ratio,
            put_call_premium_balance,
            option_activity_log_ratio,
            straddle_term_structure_pct,
            has_option_context,
            front_otm_put_iv_pct,
            front_otm_call_iv_pct,
            front_iv_risk_reversal_pct,
            front_iv_butterfly_pct,
            front_otm_put_call_volume_log_ratio,
            front_otm_put_call_premium_balance,
            front_iv_pct,
            back_iv_pct,
            iv_term_structure_pct,
            iv_rank_pct,
            iv_change_5d_pct,
            blended_iv_pct,
            rv20_to_front_iv_ratio,
            has_iv_context,
        )
        if any(value is None or not math.isfinite(float(value)) for value in values):
            features.append(None)
            continue
        features.append(tuple(float(value) for value in values))
    return features


def _distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    scales = (
        4.0,
        6.0,
        8.0,
        12.0,
        15.0,
        20.0,
        5.0,
        25.0,
        1.5,
        4.0,
        8.0,
        15.0,
        8.0,
        15.0,
        1.0,
        1.0,
        7.0,
        5.0,
        2.5,
        1.25,
        1.0,
        0.6,
        3.0,
        1.0,
        35.0,
        35.0,
        10.0,
        10.0,
        1.25,
        1.0,
        35.0,
        35.0,
        10.0,
        35.0,
        8.0,
        35.0,
        1.5,
        1.0,
    )
    weights = (
        0.8,
        1.0,
        1.1,
        1.2,
        1.2,
        0.8,
        1.0,
        1.1,
        0.7,
        0.6,
        0.8,
        0.8,
        1.0,
        1.0,
        0.6,
        0.5,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    )
    return sum(
        abs((left_value - right_value) / scale) * weight
        for left_value, right_value, scale, weight in zip(left, right, scales, weights, strict=True)
    )


def _trend_bucket(features: tuple[float, ...]) -> int:
    return 1 if features[4] > 0 and features[6] > 0 else (-1 if features[4] < 0 and features[6] < 0 else 0)


def _rsi_bucket(features: tuple[float, ...]) -> int:
    rsi_value = features[5]
    if rsi_value < 40.0:
        return 0
    if rsi_value > 60.0:
        return 2
    return 1


def _filter_candidate_pool_for_method(
    *,
    current_features: tuple[float, ...],
    candidate_pool: list[AnalogCandidate],
    method: PredictionMethodConfig,
    min_candidate_count: int,
) -> list[AnalogCandidate]:
    if not method.same_trend_bucket and not method.same_rsi_bucket:
        return candidate_pool
    current_trend_bucket = _trend_bucket(current_features)
    current_rsi_bucket = _rsi_bucket(current_features)
    filtered_pool = [
        candidate
        for candidate in candidate_pool
        if (not method.same_trend_bucket or _trend_bucket(candidate.features) == current_trend_bucket)
        and (not method.same_rsi_bucket or _rsi_bucket(candidate.features) == current_rsi_bucket)
    ]
    if len(filtered_pool) >= min_candidate_count:
        return filtered_pool
    return candidate_pool


def _build_analog_candidates(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    horizon_bars: int,
) -> list[AnalogCandidate | None]:
    candidates: list[AnalogCandidate | None] = [None] * len(bars)
    last_trainable_index = len(bars) - horizon_bars
    for index in range(last_trainable_index):
        current_features = features[index]
        if current_features is None:
            continue
        current_close = bars[index].close_price
        future_close = bars[index + horizon_bars].close_price
        if current_close <= 0:
            continue
        forward_return_pct = ((future_close - current_close) / current_close) * 100.0
        candidates[index] = AnalogCandidate(
            trade_date=bars[index].trade_date,
            feature_index=index,
            features=current_features,
            forward_return_pct=forward_return_pct,
            target_sign=_sign_from_future_close(current_close=current_close, future_close=future_close),
        )
    return candidates


def _select_analogs(
    *,
    current_features: tuple[float, ...],
    candidate_pool: list[AnalogCandidate],
    max_analogs: int,
    min_spacing_bars: int,
) -> list[AnalogCandidate]:
    ranked = sorted(
        candidate_pool,
        key=lambda candidate: (_distance(current_features, candidate.features), candidate.trade_date),
    )
    if not ranked:
        return []
    selected: list[AnalogCandidate] = []
    for candidate in ranked:
        if any(abs(candidate.feature_index - item.feature_index) < min_spacing_bars for item in selected):
            continue
        selected.append(candidate)
        if len(selected) >= max_analogs:
            break
    if selected:
        return selected
    return ranked[: min(max_analogs, len(ranked))]


def _predict_with_method(
    *,
    trade_date: date,
    current_features: tuple[float, ...],
    candidate_pool: list[AnalogCandidate],
    min_spacing_bars: int,
    min_candidate_count: int,
    method: PredictionMethodConfig,
) -> PredictionSnapshot | None:
    if len(candidate_pool) < min_candidate_count:
        return None
    filtered_pool = _filter_candidate_pool_for_method(
        current_features=current_features,
        candidate_pool=candidate_pool,
        method=method,
        min_candidate_count=min_candidate_count,
    )
    selected = _select_analogs(
        current_features=current_features,
        candidate_pool=filtered_pool,
        max_analogs=method.max_analogs,
        min_spacing_bars=min_spacing_bars,
    )
    required_analogs = min(method.max_analogs, min_candidate_count)
    if len(selected) < required_analogs:
        return None
    returns = [candidate.forward_return_pct for candidate in selected]
    signs = [candidate.target_sign for candidate in selected]
    sign_counts = Counter(signs)
    predicted_return_median_pct = float(median(returns))
    predicted_return_mean_pct = float(mean(returns))
    probability_by_class: dict[int, float]
    if method.vote_mode == "median_return":
        predicted_sign = _sign_from_return_pct(predicted_return_median_pct)
        total = len(selected)
        probability_by_class = {
            label: sign_counts.get(label, 0) / total
            for label in _SIGN_LABELS
        }
    elif method.vote_mode == "weighted_vote":
        weighted_scores: Counter[int] = Counter()
        for candidate in selected:
            distance = _distance(current_features, candidate.features)
            weighted_scores[candidate.target_sign] += 1.0 / (0.1 + distance)
        for label in _SIGN_LABELS:
            weighted_scores.setdefault(label, 0.0)
        predicted_sign = max(weighted_scores.items(), key=lambda item: (item[1], item[0]))[0]
        total_weight = sum(weighted_scores.values())
        probability_by_class = (
            {
                label: weighted_scores.get(label, 0.0) / total_weight
                for label in _SIGN_LABELS
            }
            if total_weight > 0
            else {label: 0.0 for label in _SIGN_LABELS}
        )
    else:  # pragma: no cover - protected by static config
        raise ValueError(f"Unsupported vote mode: {method.vote_mode}")
    total = len(selected)
    confidence_pct = probability_by_class.get(predicted_sign, 0.0) * 100.0
    snapshot = PredictionSnapshot(
        trade_date=trade_date,
        predicted_sign=predicted_sign,
        predicted_return_median_pct=predicted_return_median_pct,
        predicted_return_mean_pct=predicted_return_mean_pct,
        candidate_pool_count=len(filtered_pool),
        analogs_used=total,
        up_neighbor_ratio_pct=(sign_counts.get(1, 0) / total) * 100.0,
        down_neighbor_ratio_pct=(sign_counts.get(-1, 0) / total) * 100.0,
        flat_neighbor_ratio_pct=(sign_counts.get(0, 0) / total) * 100.0,
        probability_up_pct=probability_by_class.get(1, 0.0) * 100.0,
        probability_down_pct=probability_by_class.get(-1, 0.0) * 100.0,
        probability_flat_pct=probability_by_class.get(0, 0.0) * 100.0,
        confidence_pct=confidence_pct,
        analog_dates=tuple(candidate.trade_date for candidate in selected[:5]),
    )
    if method.confidence_threshold > 0.0 and confidence_pct < (method.confidence_threshold * 100.0):
        return None
    return snapshot


def _predict_from_candidates(
    *,
    trade_date: date,
    current_features: tuple[float, ...],
    candidate_pool: list[AnalogCandidate],
    max_analogs: int,
    min_spacing_bars: int,
    min_candidate_count: int,
) -> PredictionSnapshot | None:
    return _predict_with_method(
        trade_date=trade_date,
        current_features=current_features,
        candidate_pool=candidate_pool,
        min_spacing_bars=min_spacing_bars,
        min_candidate_count=min_candidate_count,
        method=PredictionMethodConfig(name="legacy_median", vote_mode="median_return", max_analogs=max_analogs),
    )


def _build_ml_estimator(method: PredictionMethodConfig) -> Any:
    if not _SKLEARN_AVAILABLE:  # pragma: no cover - protected by method catalog
        raise RuntimeError("scikit-learn is not installed. Install the '[ml]' extra to use ML prediction methods.")
    if method.ml_model_name == "logistic_regression":
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        max_iter=2_500,
                        class_weight="balanced",
                        random_state=7,
                    ),
                ),
            ]
        )
    if method.ml_model_name == "gradient_boosting":
        return GradientBoostingClassifier(
            random_state=7,
            n_estimators=120,
            learning_rate=0.04,
            max_depth=2,
            subsample=0.9,
        )
    raise ValueError(f"Unsupported ML model: {method.ml_model_name}")


def _resolve_ml_model_name(method: PredictionMethodConfig) -> str:
    if not method.calibration_method:
        return method.ml_model_name
    return f"{method.ml_model_name}_{method.calibration_method}"


def _predict_probability_by_class(
    estimator: Any,
    current_features: tuple[float, ...],
) -> dict[int, float]:
    probabilities = estimator.predict_proba([current_features])[0]
    return {
        int(label): float(probability) for label, probability in zip(estimator.classes_, probabilities, strict=True)
    }


def _binary_positive_score_from_probability_by_class(probability_by_class: dict[int, float]) -> float:
    probability_up = min(max(probability_by_class.get(1, 0.5), 1e-6), 1.0 - 1e-6)
    return math.log(probability_up / (1.0 - probability_up))


def _split_training_and_calibration_rows(
    *,
    train_features: list[tuple[float, ...]],
    train_labels: list[int],
    method: PredictionMethodConfig,
) -> tuple[list[tuple[float, ...]], list[int], list[tuple[float, ...]], list[int]]:
    if method.calibration_method != "platt":
        return train_features, train_labels, [], []
    minimum_base_train_size = max(60, method.min_train_size // 2)
    max_calibration_size = len(train_features) - minimum_base_train_size
    if max_calibration_size < method.min_calibration_size:
        return train_features, train_labels, [], []
    requested_calibration_size = max(
        method.min_calibration_size,
        int(round(len(train_features) * method.calibration_fraction)),
    )
    calibration_size = min(requested_calibration_size, max_calibration_size)
    if calibration_size < method.min_calibration_size:
        return train_features, train_labels, [], []
    base_train_features = train_features[:-calibration_size]
    base_train_labels = train_labels[:-calibration_size]
    calibration_features = train_features[-calibration_size:]
    calibration_labels = train_labels[-calibration_size:]
    if len(set(base_train_labels)) < 2 or len(set(calibration_labels)) < 2:
        return train_features, train_labels, [], []
    return base_train_features, base_train_labels, calibration_features, calibration_labels


def _fit_ml_model(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    horizon_bars: int,
    train_end_index: int,
    method: PredictionMethodConfig,
) -> tuple[FittedMlModel, int] | None:
    train_features: list[tuple[float, ...]] = []
    train_labels: list[int] = []
    for index in range(train_end_index + 1):
        current_features = features[index]
        if current_features is None:
            continue
        actual_sign = _sign_from_future_close(
            current_close=bars[index].close_price,
            future_close=bars[index + horizon_bars].close_price,
        )
        if actual_sign == 0:
            continue
        train_features.append(current_features)
        train_labels.append(actual_sign)
    if len(train_features) < method.min_train_size or len(set(train_labels)) < 2:
        return None
    base_train_features, base_train_labels, calibration_features, calibration_labels = _split_training_and_calibration_rows(
        train_features=train_features,
        train_labels=train_labels,
        method=method,
    )
    estimator = _build_ml_estimator(method)
    estimator.fit(base_train_features, base_train_labels)
    calibrator = None
    if calibration_features:
        calibration_scores = [
            _binary_positive_score_from_probability_by_class(
                _predict_probability_by_class(estimator, current_features),
            )
            for current_features in calibration_features
        ]
        calibrator = LogisticRegression(
            max_iter=1_000,
            class_weight="balanced",
            random_state=7,
        )
        calibrator.fit(
            [[score] for score in calibration_scores],
            [1 if label == 1 else 0 for label in calibration_labels],
        )
    return FittedMlModel(estimator=estimator, calibrator=calibrator), len(train_features)


def _predict_with_ml_model(
    *,
    trade_date: date,
    current_features: tuple[float, ...],
    estimator: FittedMlModel,
    train_sample_count: int,
    method: PredictionMethodConfig,
) -> PredictionSnapshot | None:
    probability_by_class = _predict_probability_by_class(estimator.estimator, current_features)
    if estimator.calibrator is not None:
        positive_score = _binary_positive_score_from_probability_by_class(probability_by_class)
        probability_up = float(estimator.calibrator.predict_proba([[positive_score]])[0][1])
        probability_by_class = {
            -1: max(0.0, 1.0 - probability_up),
            1: probability_up,
        }
    predicted_sign, confidence = max(
        probability_by_class.items(),
        key=lambda item: (item[1], item[0]),
    )
    if confidence < method.confidence_threshold:
        return None
    return PredictionSnapshot(
        trade_date=trade_date,
        predicted_sign=predicted_sign,
        probability_up_pct=probability_by_class.get(1, 0.0) * 100.0,
        probability_down_pct=probability_by_class.get(-1, 0.0) * 100.0,
        probability_flat_pct=probability_by_class.get(0, 0.0) * 100.0,
        confidence_pct=confidence * 100.0,
        train_sample_count=train_sample_count,
        model_name=_resolve_ml_model_name(method),
    )


def _build_prediction_row(
    *,
    prediction: PredictionSnapshot,
    actual_sign: int,
    future_trade_date: date,
    actual_return_pct: float,
    prediction_engine: str,
) -> dict[str, object]:
    return {
        "trade_date": prediction.trade_date.isoformat(),
        "future_trade_date": future_trade_date.isoformat(),
        "prediction_engine": prediction_engine,
        "predicted_sign": prediction.predicted_sign,
        "actual_sign": actual_sign,
        "predicted_return_median_pct": (
            None if prediction.predicted_return_median_pct is None else round(prediction.predicted_return_median_pct, 6)
        ),
        "predicted_return_mean_pct": (
            None if prediction.predicted_return_mean_pct is None else round(prediction.predicted_return_mean_pct, 6)
        ),
        "actual_return_pct": round(actual_return_pct, 6),
        "candidate_pool_count": prediction.candidate_pool_count,
        "analogs_used": prediction.analogs_used,
        "up_neighbor_ratio_pct": (
            None if prediction.up_neighbor_ratio_pct is None else round(prediction.up_neighbor_ratio_pct, 4)
        ),
        "down_neighbor_ratio_pct": (
            None if prediction.down_neighbor_ratio_pct is None else round(prediction.down_neighbor_ratio_pct, 4)
        ),
        "flat_neighbor_ratio_pct": (
            None if prediction.flat_neighbor_ratio_pct is None else round(prediction.flat_neighbor_ratio_pct, 4)
        ),
        "probability_up_pct": None if prediction.probability_up_pct is None else round(prediction.probability_up_pct, 4),
        "probability_down_pct": (
            None if prediction.probability_down_pct is None else round(prediction.probability_down_pct, 4)
        ),
        "probability_flat_pct": (
            None if prediction.probability_flat_pct is None else round(prediction.probability_flat_pct, 4)
        ),
        "confidence_pct": None if prediction.confidence_pct is None else round(prediction.confidence_pct, 4),
        "train_sample_count": prediction.train_sample_count,
        "model_name": prediction.model_name,
        "analog_dates": [analog_date.isoformat() for analog_date in prediction.analog_dates],
    }


def _build_latest_prediction_payload(
    prediction: PredictionSnapshot,
    *,
    prediction_engine: str,
) -> dict[str, object]:
    return {
        "as_of_date": prediction.trade_date.isoformat(),
        "prediction_engine": prediction_engine,
        "predicted_sign": prediction.predicted_sign,
        "predicted_direction": {
            -1: "down",
            0: "flat",
            1: "up",
        }[prediction.predicted_sign],
        "predicted_return_median_pct": (
            None if prediction.predicted_return_median_pct is None else round(prediction.predicted_return_median_pct, 6)
        ),
        "predicted_return_mean_pct": (
            None if prediction.predicted_return_mean_pct is None else round(prediction.predicted_return_mean_pct, 6)
        ),
        "candidate_pool_count": prediction.candidate_pool_count,
        "analogs_used": prediction.analogs_used,
        "up_neighbor_ratio_pct": (
            None if prediction.up_neighbor_ratio_pct is None else round(prediction.up_neighbor_ratio_pct, 4)
        ),
        "down_neighbor_ratio_pct": (
            None if prediction.down_neighbor_ratio_pct is None else round(prediction.down_neighbor_ratio_pct, 4)
        ),
        "flat_neighbor_ratio_pct": (
            None if prediction.flat_neighbor_ratio_pct is None else round(prediction.flat_neighbor_ratio_pct, 4)
        ),
        "probability_up_pct": None if prediction.probability_up_pct is None else round(prediction.probability_up_pct, 4),
        "probability_down_pct": (
            None if prediction.probability_down_pct is None else round(prediction.probability_down_pct, 4)
        ),
        "probability_flat_pct": (
            None if prediction.probability_flat_pct is None else round(prediction.probability_flat_pct, 4)
        ),
        "confidence_pct": None if prediction.confidence_pct is None else round(prediction.confidence_pct, 4),
        "train_sample_count": prediction.train_sample_count,
        "model_name": prediction.model_name,
        "analog_dates": [analog_date.isoformat() for analog_date in prediction.analog_dates],
    }


def _walk_forward_predictions(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    candidates: list[AnalogCandidate | None],
    start_date: date,
    horizon_bars: int,
    min_spacing_bars: int,
    min_candidate_count: int,
    method: PredictionMethodConfig,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    last_scored_index = len(bars) - horizon_bars
    ml_estimator: Any | None = None
    ml_last_fit_index: int | None = None
    ml_train_sample_count: int | None = None
    for index in range(len(bars)):
        if index >= last_scored_index:
            break
        bar = bars[index]
        if bar.trade_date < start_date:
            continue
        current_features = features[index]
        if current_features is None:
            continue
        prediction: PredictionSnapshot | None = None
        if method.engine == "ml":
            train_end_index = index - horizon_bars
            if train_end_index < 0:
                continue
            if (
                ml_estimator is None
                or ml_last_fit_index is None
                or (train_end_index - ml_last_fit_index) >= method.retrain_every_bars
            ):
                fitted = _fit_ml_model(
                    bars=bars,
                    features=features,
                    horizon_bars=horizon_bars,
                    train_end_index=train_end_index,
                    method=method,
                )
                if fitted is None:
                    continue
                ml_estimator, ml_train_sample_count = fitted
                ml_last_fit_index = train_end_index
            if ml_estimator is None or ml_train_sample_count is None:
                continue
            prediction = _predict_with_ml_model(
                trade_date=bar.trade_date,
                current_features=current_features,
                estimator=ml_estimator,
                train_sample_count=ml_train_sample_count,
                method=method,
            )
        else:
            max_candidate_index = index - horizon_bars
            if max_candidate_index < 0:
                continue
            candidate_pool = [
                candidate
                for candidate in candidates[: max_candidate_index + 1]
                if candidate is not None
            ]
            prediction = _predict_from_candidates(
                trade_date=bar.trade_date,
                current_features=current_features,
                candidate_pool=candidate_pool,
                max_analogs=method.max_analogs,
                min_spacing_bars=min_spacing_bars,
                min_candidate_count=min_candidate_count,
            )
            if method.vote_mode != "median_return" or method.same_trend_bucket or method.same_rsi_bucket:
                prediction = _predict_with_method(
                    trade_date=bar.trade_date,
                    current_features=current_features,
                    candidate_pool=candidate_pool,
                    min_spacing_bars=min_spacing_bars,
                    min_candidate_count=min_candidate_count,
                    method=method,
                )
        if prediction is None:
            continue
        future_bar = bars[index + horizon_bars]
        actual_sign = _sign_from_future_close(current_close=bar.close_price, future_close=future_bar.close_price)
        actual_return_pct = ((future_bar.close_price - bar.close_price) / bar.close_price) * 100.0
        results.append(
            _build_prediction_row(
                prediction=prediction,
                actual_sign=actual_sign,
                future_trade_date=future_bar.trade_date,
                actual_return_pct=actual_return_pct,
                prediction_engine=method.engine,
            )
        )
    return results


def _count_total_scorable_dates(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    candidates: list[AnalogCandidate | None],
    start_date: date,
    horizon_bars: int,
    min_candidate_count: int,
) -> int:
    total = 0
    last_scored_index = len(bars) - horizon_bars
    for index in range(last_scored_index):
        bar = bars[index]
        if bar.trade_date < start_date:
            continue
        if features[index] is None:
            continue
        max_candidate_index = index - horizon_bars
        if max_candidate_index < 0:
            continue
        candidate_pool_count = sum(1 for candidate in candidates[: max_candidate_index + 1] if candidate is not None)
        if candidate_pool_count < min_candidate_count:
            continue
        total += 1
    return total


def _count_total_ml_scorable_dates(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    start_date: date,
    horizon_bars: int,
    min_train_size: int,
) -> int:
    total = 0
    last_scored_index = len(bars) - horizon_bars
    for index in range(last_scored_index):
        bar = bars[index]
        if bar.trade_date < start_date:
            continue
        if features[index] is None:
            continue
        train_end_index = index - horizon_bars
        if train_end_index < 0:
            continue
        train_row_count = 0
        observed_signs: set[int] = set()
        for train_index in range(train_end_index + 1):
            train_features = features[train_index]
            if train_features is None:
                continue
            actual_sign = _sign_from_future_close(
                current_close=bars[train_index].close_price,
                future_close=bars[train_index + horizon_bars].close_price,
            )
            if actual_sign == 0:
                continue
            train_row_count += 1
            observed_signs.add(actual_sign)
        if train_row_count < min_train_size or len(observed_signs) < 2:
            continue
        total += 1
    return total


def _summarize_method_rows(
    method_rows: dict[str, list[dict[str, object]]],
    *,
    total_scorable_dates_by_method: dict[str, int] | None = None,
) -> dict[str, dict[str, object]]:
    return {
        method_name: _build_evaluation_summary(
            rows,
            total_scorable_dates=None if total_scorable_dates_by_method is None else total_scorable_dates_by_method.get(
                method_name
            ),
        )
        for method_name, rows in method_rows.items()
        if rows
    }


def _select_best_method_name(
    method_rows: dict[str, list[dict[str, object]]],
    *,
    total_scorable_dates_by_method: dict[str, int] | None = None,
) -> str:
    method_summaries = _summarize_method_rows(
        method_rows,
        total_scorable_dates_by_method=total_scorable_dates_by_method,
    )
    if not method_summaries:
        raise ValueError("No method rows were available to score.")
    return max(
        method_summaries,
        key=lambda method_name: (
            float(method_summaries[method_name]["accuracy_pct"]),
            float(method_summaries[method_name]["balanced_accuracy_pct"]),
            int(method_summaries[method_name]["observation_count"]),
            method_name,
        ),
    )


def _confusion_matrix(rows: list[dict[str, object]]) -> dict[str, dict[str, int]]:
    matrix = {str(actual): {str(predicted): 0 for predicted in _SIGN_LABELS} for actual in _SIGN_LABELS}
    for row in rows:
        actual = int(row["actual_sign"])
        predicted = int(row["predicted_sign"])
        matrix[str(actual)][str(predicted)] += 1
    return matrix


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _safe_divide(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _build_evaluation_summary(
    rows: list[dict[str, object]],
    *,
    total_scorable_dates: int | None = None,
) -> dict[str, object]:
    observation_count = len(rows)
    coverage_denominator = max(total_scorable_dates or 0, observation_count)
    exact_hits = sum(1 for row in rows if int(row["predicted_sign"]) == int(row["actual_sign"]))
    nonflat_rows = [row for row in rows if int(row["actual_sign"]) != 0]
    directional_hits = sum(1 for row in nonflat_rows if int(row["predicted_sign"]) == int(row["actual_sign"]))
    matrix = _confusion_matrix(rows)
    per_class_recall_pct: dict[str, float] = {}
    per_class_precision_pct: dict[str, float] = {}
    for label in _SIGN_LABELS:
        actual_total = sum(matrix[str(label)].values())
        predicted_total = sum(matrix[str(actual_label)][str(label)] for actual_label in _SIGN_LABELS)
        true_positive = matrix[str(label)][str(label)]
        per_class_recall_pct[str(label)] = _pct(true_positive, actual_total)
        per_class_precision_pct[str(label)] = _pct(true_positive, predicted_total)
    observed_labels = [label for label in _SIGN_LABELS if sum(matrix[str(label)].values()) > 0]
    balanced_accuracy_pct = round(
        sum(per_class_recall_pct[str(label)] for label in observed_labels) / len(observed_labels),
        4,
    ) if observed_labels else 0.0
    up_precision_pct = per_class_precision_pct["1"]
    down_precision_pct = per_class_precision_pct["-1"]
    up_recall_pct = per_class_recall_pct["1"]
    down_recall_pct = per_class_recall_pct["-1"]
    predicted_sign_counts = Counter(int(row["predicted_sign"]) for row in rows)
    actual_sign_counts = Counter(int(row["actual_sign"]) for row in rows)
    return {
        "observation_count": observation_count,
        "total_scorable_dates": coverage_denominator,
        "abstained_count": max(coverage_denominator - observation_count, 0),
        "coverage_pct": _pct(observation_count, coverage_denominator),
        "accuracy_pct": _pct(exact_hits, observation_count),
        "directional_accuracy_pct": _pct(directional_hits, len(nonflat_rows)),
        "balanced_accuracy_pct": balanced_accuracy_pct,
        "up_precision_pct": up_precision_pct,
        "down_precision_pct": down_precision_pct,
        "up_recall_pct": up_recall_pct,
        "down_recall_pct": down_recall_pct,
        "predicted_sign_counts": {str(label): predicted_sign_counts.get(label, 0) for label in _SIGN_LABELS},
        "actual_sign_counts": {str(label): actual_sign_counts.get(label, 0) for label in _SIGN_LABELS},
        "confusion_matrix": matrix,
    }


def _build_latest_prediction(
    *,
    bars: list[DailyBar],
    features: list[tuple[float, ...] | None],
    candidates: list[AnalogCandidate | None],
    horizon_bars: int,
    min_spacing_bars: int,
    min_candidate_count: int,
    method: PredictionMethodConfig,
) -> dict[str, object] | None:
    if not bars:
        return None
    latest_index = len(bars) - 1
    current_features = features[latest_index]
    if current_features is None:
        return None
    prediction: PredictionSnapshot | None
    if method.engine == "ml":
        train_end_index = latest_index - horizon_bars
        if train_end_index < 0:
            return None
        fitted = _fit_ml_model(
            bars=bars,
            features=features,
            horizon_bars=horizon_bars,
            train_end_index=train_end_index,
            method=method,
        )
        if fitted is None:
            return None
        estimator, train_sample_count = fitted
        prediction = _predict_with_ml_model(
            trade_date=bars[latest_index].trade_date,
            current_features=current_features,
            estimator=estimator,
            train_sample_count=train_sample_count,
            method=method,
        )
    else:
        max_candidate_index = latest_index - horizon_bars
        if max_candidate_index < 0:
            return None
        candidate_pool = [
            candidate
            for candidate in candidates[: max_candidate_index + 1]
            if candidate is not None
        ]
        prediction = _predict_with_method(
            trade_date=bars[latest_index].trade_date,
            current_features=current_features,
            candidate_pool=candidate_pool,
            min_spacing_bars=min_spacing_bars,
            min_candidate_count=min_candidate_count,
            method=method,
        )
    if prediction is None:
        return None
    return _build_latest_prediction_payload(prediction, prediction_engine=method.engine)


def _build_payload(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    bars: list[DailyBar],
    walk_forward_rows: list[dict[str, object]],
    method_summaries: dict[str, dict[str, object]],
    selected_method_name: str,
    selected_method_reason: str,
    selected_method: PredictionMethodConfig,
    latest_prediction: dict[str, object] | None,
    horizon_bars: int,
    min_candidate_count: int,
    min_spacing_bars: int,
    selected_total_scorable_dates: int,
) -> dict[str, object]:
    filtered_bars = [bar for bar in bars if start_date <= bar.trade_date <= end_date]
    return {
        "symbol": symbol,
        "target": f"sign(close[t+{horizon_bars}] / close[t] - 1)",
        "horizon_bars": horizon_bars,
        "parameters": {
            "prediction_engine": selected_method.engine,
            "max_analogs": selected_method.max_analogs if selected_method.engine == "analog" else None,
            "min_candidate_count": min_candidate_count,
            "min_spacing_bars": min_spacing_bars,
            "prediction_method": selected_method_name,
            "ml_model_name": selected_method.ml_model_name or None,
            "ml_confidence_threshold": (
                selected_method.confidence_threshold if selected_method.engine == "ml" else None
            ),
            "ml_min_train_size": selected_method.min_train_size if selected_method.engine == "ml" else None,
            "ml_retrain_every_bars": selected_method.retrain_every_bars if selected_method.engine == "ml" else None,
        },
        "selected_method": selected_method_name,
        "selected_method_reason": selected_method_reason,
        "loaded_bar_count": len(bars),
        "window_bar_count": len(filtered_bars),
        "loaded_start_date": None if not bars else bars[0].trade_date.isoformat(),
        "loaded_end_date": None if not bars else bars[-1].trade_date.isoformat(),
        "requested_window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "method_results": method_summaries,
        "evaluation": _build_evaluation_summary(
            walk_forward_rows,
            total_scorable_dates=selected_total_scorable_dates,
        ),
        "latest_prediction": latest_prediction,
        "sample_predictions": walk_forward_rows[-10:],
    }


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")
    if args.horizon_bars < 1:
        raise SystemExit("--horizon-bars must be >= 1.")
    if args.max_analogs is not None and args.max_analogs < 1:
        raise SystemExit("--max-analogs must be >= 1.")
    if args.min_candidate_count < 1:
        raise SystemExit("--min-candidate-count must be >= 1.")
    if args.min_spacing_bars < 1:
        raise SystemExit("--min-spacing-bars must be >= 1.")
    if args.warmup_calendar_days < 0:
        raise SystemExit("--warmup-calendar-days must be >= 0.")
    if args.start_date >= args.end_date:
        raise SystemExit("--start-date must be earlier than --end-date.")

    symbol = args.symbol.strip().upper()
    engine = build_engine(database_url=args.database_url, statement_timeout_ms=args.db_statement_timeout_ms)
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            bars = _load_bars(
                session,
                symbol=symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                warmup_calendar_days=args.warmup_calendar_days,
            )
            option_feature_rows = _load_option_feature_rows(
                session,
                symbol=symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                warmup_calendar_days=args.warmup_calendar_days,
            )
            benchmark_bars = (
                bars
                if symbol == DEFAULT_BENCHMARK_SYMBOL
                else _load_bars(
                    session,
                    symbol=DEFAULT_BENCHMARK_SYMBOL,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    warmup_calendar_days=args.warmup_calendar_days,
                )
            )
            earnings_dates = _load_earnings_dates(
                session,
                symbol=symbol,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        store = HistoricalMarketDataStore(factory, factory)
        option_gateway = HistoricalOptionGateway(store, symbol)
    finally:
        engine.dispose()

    if len(bars) < 80:
        raise SystemExit(f"Not enough bars for {symbol}. Loaded {len(bars)} rows; need at least 80.")

    benchmark_context_by_date = _build_benchmark_context_by_date(benchmark_bars)
    front_iv_series = build_estimated_iv_series(
        bars,
        option_gateway,
        target_dte=DEFAULT_FRONT_IV_TARGET_DTE,
        dte_tolerance_days=DEFAULT_FRONT_IV_DTE_TOLERANCE_DAYS,
    )
    back_iv_series = build_estimated_iv_series(
        bars,
        option_gateway,
        target_dte=DEFAULT_BACK_IV_TARGET_DTE,
        dte_tolerance_days=DEFAULT_BACK_IV_DTE_TOLERANCE_DAYS,
    )
    option_context_by_date = _build_option_context_by_date(
        bars,
        option_feature_rows,
        front_iv_series=front_iv_series,
    )
    iv_context_by_date = _build_iv_context_by_date(
        bars,
        front_iv_series=front_iv_series,
        back_iv_series=back_iv_series,
    )
    features = _build_feature_matrix(
        bars,
        benchmark_context_by_date=benchmark_context_by_date,
        earnings_dates=earnings_dates,
        option_context_by_date=option_context_by_date,
        iv_context_by_date=iv_context_by_date,
    )
    candidates = _build_analog_candidates(bars=bars, features=features, horizon_bars=args.horizon_bars)
    configured_methods = tuple(
        PredictionMethodConfig(
            name=method.name,
            vote_mode=method.vote_mode,
            engine=method.engine,
            max_analogs=(
                method.max_analogs
                if args.max_analogs is None or method.engine != "analog"
                else args.max_analogs
            ),
            same_trend_bucket=method.same_trend_bucket,
            same_rsi_bucket=method.same_rsi_bucket,
            ml_model_name=method.ml_model_name,
            confidence_threshold=method.confidence_threshold,
            min_train_size=method.min_train_size,
            retrain_every_bars=method.retrain_every_bars,
            calibration_method=method.calibration_method,
            calibration_fraction=method.calibration_fraction,
            min_calibration_size=method.min_calibration_size,
        )
        for method in _METHOD_CONFIGS
    )
    method_name_to_config = {method.name: method for method in configured_methods}
    candidate_methods = (
        configured_methods
        if args.prediction_method == DEFAULT_PREDICTION_METHOD
        else (method_name_to_config[args.prediction_method],)
    )
    total_scorable_dates_by_method = {
        method.name: (
            _count_total_ml_scorable_dates(
                bars=bars,
                features=features,
                start_date=args.start_date,
                horizon_bars=args.horizon_bars,
                min_train_size=method.min_train_size,
            )
            if method.engine == "ml"
            else _count_total_scorable_dates(
                bars=bars,
                features=features,
                candidates=candidates,
                start_date=args.start_date,
                horizon_bars=args.horizon_bars,
                min_candidate_count=args.min_candidate_count,
            )
        )
        for method in candidate_methods
    }
    method_rows = {
        method.name: _walk_forward_predictions(
            bars=bars,
            features=features,
            candidates=candidates,
            start_date=args.start_date,
            horizon_bars=args.horizon_bars,
            min_spacing_bars=args.min_spacing_bars,
            min_candidate_count=args.min_candidate_count,
            method=method,
        )
        for method in candidate_methods
    }
    selected_method_name = (
        _select_best_method_name(
            method_rows,
            total_scorable_dates_by_method=total_scorable_dates_by_method,
        )
        if args.prediction_method == DEFAULT_PREDICTION_METHOD
        else args.prediction_method
    )
    walk_forward_rows = method_rows[selected_method_name]
    if not walk_forward_rows:
        raise SystemExit(
            "No walk-forward predictions were produced. Try an earlier start-date, a longer data window, "
            "or a smaller --min-candidate-count."
        )
    selected_method = method_name_to_config[selected_method_name]
    selected_method_reason = (
        "best_accuracy_full_window"
        if args.prediction_method == DEFAULT_PREDICTION_METHOD
        else "explicit"
    )
    method_summaries = _summarize_method_rows(
        method_rows,
        total_scorable_dates_by_method=total_scorable_dates_by_method,
    )

    latest_prediction = _build_latest_prediction(
        bars=bars,
        features=features,
        candidates=candidates,
        horizon_bars=args.horizon_bars,
        min_spacing_bars=args.min_spacing_bars,
        min_candidate_count=args.min_candidate_count,
        method=selected_method,
    )
    payload = _build_payload(
        symbol=symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        bars=bars,
        walk_forward_rows=walk_forward_rows,
        method_summaries=method_summaries,
        selected_method_name=selected_method_name,
        selected_method_reason=selected_method_reason,
        selected_method=selected_method,
        latest_prediction=latest_prediction,
        horizon_bars=args.horizon_bars,
        min_candidate_count=args.min_candidate_count,
        min_spacing_bars=args.min_spacing_bars,
        selected_total_scorable_dates=total_scorable_dates_by_method[selected_method_name],
    )

    output_json = args.output_json or _default_output_path(
        symbol=symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        horizon_bars=args.horizon_bars,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload["evaluation"], indent=2, sort_keys=True))
    if latest_prediction is not None:
        print(json.dumps(latest_prediction, indent=2, sort_keys=True))
    print(f"Wrote {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
