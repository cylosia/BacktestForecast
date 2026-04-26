from __future__ import annotations

import argparse
import csv
import itertools
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data import vix_regime

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_conditional_management_2y as cond2y
import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as cond


LOGS = ROOT / "logs"

DEFAULT_SELECTED_TRADES_CSV = (
    LOGS / "short_iv_gt_long_best_delta_lookback52warmup_to_2026-04-10_earnings_excl_selected_trades.csv"
)
DEFAULT_CANDIDATE_CACHE_CSV = (
    LOGS / "short_iv_gt_long_base_gate_candidate_rows_lookback52warmup_to_2026-04-10_earnings_excl.csv"
)
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_base_gate_lookback52_sweep_summary.csv"
DEFAULT_OUTPUT_BEST_CSV = LOGS / "short_iv_gt_long_base_gate_lookback52_sweep_best.csv"
DEFAULT_WINDOW_END_DATE = date.fromisoformat("2026-04-10")
DEFAULT_WINDOW_WEEKS = 52
DEFAULT_VIX_CACHE_CSV = cond.DEFAULT_VIX_CACHE_CSV
CANDIDATE_CACHE_SCHEMA_VERSION = "v3"


@dataclass(frozen=True, slots=True)
class GateConfig:
    abstain_high_iv_debit_min: float
    abstain_high_iv_iv_min: float
    band35_50_debit_min: float
    band40_45_debit_min: float
    band55_65_debit_min: float
    up_debit_min: float
    up_iv_max: float

    def label(self) -> str:
        return (
            f"ahi_d{self.abstain_high_iv_debit_min:g}_iv{self.abstain_high_iv_iv_min:g}"
            f"__b3550_d{self.band35_50_debit_min:g}"
            f"__b4045_d{self.band40_45_debit_min:g}"
            f"__b5565_d{self.band55_65_debit_min:g}"
            f"__up_d{self.up_debit_min:g}_iv{self.up_iv_max:g}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep Friday short-IV base debit/IV entry gates over the last 52 weeks using "
            "the exact downstream promoted-portfolio filters."
        )
    )
    parser.add_argument("--selected-trades-csv", type=Path, default=DEFAULT_SELECTED_TRADES_CSV)
    parser.add_argument("--candidate-cache-csv", type=Path, default=DEFAULT_CANDIDATE_CACHE_CSV)
    parser.add_argument("--rebuild-candidate-cache", action="store_true")
    parser.add_argument("--output-summary-csv", type=Path, default=DEFAULT_OUTPUT_SUMMARY_CSV)
    parser.add_argument("--output-best-csv", type=Path, default=DEFAULT_OUTPUT_BEST_CSV)
    parser.add_argument("--window-end-date", type=date.fromisoformat, default=DEFAULT_WINDOW_END_DATE)
    parser.add_argument("--window-weeks", type=int, default=DEFAULT_WINDOW_WEEKS)
    parser.add_argument("--top-k", type=int, default=43)
    parser.add_argument("--method-cap", type=int, default=cond.DEFAULT_TOP43_METHOD_CAP)
    parser.add_argument(
        "--lookback-pnl-over-debit-threshold-pct",
        type=float,
        default=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT,
    )
    parser.add_argument(
        "--lookback-pnl-over-debit-min-history-trades",
        type=int,
        default=cond.DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES,
    )
    parser.add_argument(
        "--vix-max-weekly-change-up-pct",
        type=float,
        default=20.0,
        help="Hard weekly VIX absolute-change filter. Use a negative value to disable.",
    )
    parser.add_argument(
        "--vix-cache-csv",
        type=Path,
        default=DEFAULT_VIX_CACHE_CSV,
    )
    parser.add_argument(
        "--min-short-over-long-iv-premium-pct",
        type=float,
        default=None,
        help="Optional minimum ATM short-over-long IV premium filter to hold fixed during the sweep.",
    )
    parser.add_argument(
        "--disable-vix-cache-refresh",
        action="store_true",
        help="Do not refresh VIX cache from the network; use DB/cache only.",
    )
    parser.add_argument(
        "--abstain-high-iv-debit-thresholds",
        default="1.5,2.0,2.5",
        help="Comma-separated abstain high-IV debit minimums.",
    )
    parser.add_argument(
        "--abstain-high-iv-iv-thresholds",
        default="90,100",
        help="Comma-separated abstain high-IV IV minimums.",
    )
    parser.add_argument(
        "--band35-50-debit-thresholds",
        default="3.0,4.0,5.0",
        help="Comma-separated debit minimums when 35 <= short IV < 50.",
    )
    parser.add_argument(
        "--band40-45-debit-thresholds",
        default="1.0,1.5,2.0",
        help="Comma-separated debit minimums when 40 <= short IV < 45.",
    )
    parser.add_argument(
        "--band55-65-debit-thresholds",
        default="2.0,2.5,3.0",
        help="Comma-separated debit minimums when 55 <= short IV < 65.",
    )
    parser.add_argument(
        "--up-debit-thresholds",
        default="3.5,4.5,5.5",
        help="Comma-separated up-side debit minimums.",
    )
    parser.add_argument(
        "--up-iv-max-thresholds",
        default="40,45",
        help="Comma-separated up-side short-IV maximums.",
    )
    parser.add_argument(
        "--top-n-best",
        type=int,
        default=20,
        help="How many scenarios to keep in the best-output CSV.",
    )
    return parser


def _parse_thresholds(raw_value: str) -> tuple[float, ...]:
    values = tuple(float(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip())
    if not values:
        raise SystemExit("Threshold list cannot be empty.")
    return values


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _prefix_row(prefix: str, row: dict[str, object]) -> dict[str, object]:
    return {f"{prefix}{key}": value for key, value in row.items()}


def _extract_prefixed_row(row: dict[str, object], *, prefix: str) -> dict[str, object]:
    extracted: dict[str, object] = {}
    for key, value in row.items():
        if key.startswith(prefix):
            extracted[key[len(prefix) :]] = value
    return extracted


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return float(text)


def _to_bool(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes"}


def _window_entry_dates(*, end_date: date, weeks: int) -> list[date]:
    start_date = end_date - timedelta(days=7 * (weeks - 1))
    return [start_date + timedelta(days=7 * offset) for offset in range(weeks)]


def _summarize_window(
    rows: list[dict[str, object]],
    *,
    window_entry_dates: list[date],
) -> dict[str, object]:
    window_set = {entry_date.isoformat() for entry_date in window_entry_dates}
    window_rows = [row for row in rows if str(row["entry_date"]) in window_set]
    overall = cond._summarize_rows(window_rows)

    weekly_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in window_rows:
        weekly_rows[str(row["entry_date"])].append(row)

    negative_pnl_weeks = 0
    negative_pnl_over_debit_weeks = 0
    cumulative_pnl = 0.0
    running_peak = 0.0
    max_drawdown = 0.0
    weekly_pnl_series: list[float] = []
    for entry_date in window_entry_dates:
        week_rows = weekly_rows.get(entry_date.isoformat(), [])
        if week_rows:
            week_summary = cond._summarize_rows(week_rows)
            week_total_pnl = float(week_summary["total_pnl_all_trades"])
            week_pnl_over_debit = week_summary["weighted_return_positive_debit_pct"]
        else:
            week_total_pnl = 0.0
            week_pnl_over_debit = None
        if week_total_pnl < 0:
            negative_pnl_weeks += 1
        if week_pnl_over_debit is not None and float(week_pnl_over_debit) < 0:
            negative_pnl_over_debit_weeks += 1
        weekly_pnl_series.append(week_total_pnl)
        cumulative_pnl += week_total_pnl
        running_peak = max(running_peak, cumulative_pnl)
        max_drawdown = max(max_drawdown, running_peak - cumulative_pnl)

    max_drawdown_pct = cond._history_max_drawdown_pct(weekly_pnl_series)
    return {
        **overall,
        "active_weeks": sum(1 for entry_date in window_entry_dates if weekly_rows.get(entry_date.isoformat())),
        "zero_trade_weeks": len(window_entry_dates) - sum(
            1 for entry_date in window_entry_dates if weekly_rows.get(entry_date.isoformat())
        ),
        "negative_pnl_weeks": negative_pnl_weeks,
        "negative_pnl_over_debit_weeks": negative_pnl_over_debit_weeks,
        "max_drawdown": round(max_drawdown, 6),
        "max_drawdown_pct": cond._round_or_none(max_drawdown_pct),
    }


def _best_rows_by_rank(
    rows: list[dict[str, object]],
    *,
    top_n: int,
) -> list[dict[str, object]]:
    ranked = sorted(
        rows,
        key=lambda row: (
            -(float(row["weighted_return_positive_debit_pct"]) if row["weighted_return_positive_debit_pct"] not in (None, "") else -10_000.0),
            int(row["negative_pnl_weeks"]),
            float(row["max_drawdown_pct"]) if row["max_drawdown_pct"] not in (None, "") else 10_000.0,
            -(float(row["median_roi_positive_debit_pct"]) if row["median_roi_positive_debit_pct"] not in (None, "") else -10_000.0),
        ),
    )
    return ranked[:top_n]


def _build_candidate_rows(
    *,
    selected_rows: list[dict[str, str]],
    candidate_cache_csv: Path,
    rebuild: bool,
    vix_cache_csv: Path,
    disable_vix_cache_refresh: bool,
) -> list[dict[str, object]]:
    if candidate_cache_csv.exists() and not rebuild:
        cached_rows = [dict(row) for row in _read_csv_rows(candidate_cache_csv)]
        if cached_rows and all(
            str(row.get("candidate_cache_schema_version", "")).strip() == CANDIDATE_CACHE_SCHEMA_VERSION
            for row in cached_rows
        ):
            return cached_rows

    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    entry_dates = sorted({date.fromisoformat(row["entry_date"]) for row in selected_rows})
    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    store = HistoricalMarketDataStore(factory, factory)
    vix_close_by_date = vix_regime.load_vix_close_series(
        start_date=min(entry_dates) - timedelta(days=14),
        end_date=max(entry_dates),
        store=store,
        cache_csv=vix_cache_csv,
        allow_cache_refresh=not disable_vix_cache_refresh,
    )
    vix_snapshots = vix_regime.build_weekly_change_snapshots(
        entry_dates=entry_dates,
        close_by_date=vix_close_by_date,
    )

    candidate_rows: list[dict[str, object]] = []
    try:
        with factory() as session:
            total_symbols = len(trades_by_symbol)
            symbol_cache: dict[
                str,
                tuple[
                    dict[date, float],
                    dict[date, dict[date, list[object]]],
                    dict[tuple[str, str, str], list[date]],
                ],
            ] = {}
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{total_symbols:03d}] {symbol}: loading path data")
                symbol_cache[symbol] = cond2y._load_symbol_cache(session, symbol=symbol, trades=symbol_trades)

            total_trades = len(selected_rows)
            for index, trade_row in enumerate(selected_rows, start=1):
                symbol = trade_row["symbol"].strip().upper()
                prediction = trade_row["prediction"].strip()
                if prediction not in {"up", "abstain"}:
                    continue

                spot_by_date, option_rows_by_date, path_dates_by_trade = symbol_cache[symbol]
                path_dates = path_dates_by_trade[(trade_row["entry_date"], trade_row["symbol"], trade_row["prediction"])]
                hold_row = mgmt._simulate_hold_to_expiry(
                    trade_row=trade_row,
                    policy_label="hold_best_delta",
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )

                tp25_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=cond.DEFAULT_ABSTAIN_TAKE_PROFIT_PCT,
                    stop_loss_pct=cond.DEFAULT_ABSTAIN_STOP_LOSS_PCT,
                )
                up_tp75_stop65_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=cond.DEFAULT_UP_TAKE_PROFIT_PCT,
                    stop_loss_pct=cond.DEFAULT_UP_STOP_LOSS_PCT,
                )
                abstain_override_rows = {
                    method: mgmt._simulate_tp_stop(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                    )
                    for method, (take_profit_pct, stop_loss_pct) in cond.ABSTAIN_METHOD_SIDE_TP_STOP_OVERRIDES.items()
                }
                managed_row = cond._select_abstain_method_side_exit_row(
                    prediction=prediction,
                    selected_method=str(trade_row["selected_method"]),
                    default_row=tp25_row,
                    override_rows_by_method=abstain_override_rows,
                )
                chosen_row = managed_row if prediction == "abstain" else up_tp75_stop65_row
                short_entry_iv_pct = cond._short_entry_iv_pct(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                )
                short_atm_entry_iv_pct, long_atm_entry_iv_pct, short_over_long_atm_iv_premium_pct = (
                    cond._entry_atm_iv_metrics(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                    )
                )
                vix_snapshot = vix_snapshots.get(date.fromisoformat(trade_row["entry_date"]))
                candidate = {
                    "candidate_cache_schema_version": CANDIDATE_CACHE_SCHEMA_VERSION,
                    "entry_date": trade_row["entry_date"],
                    "symbol": trade_row["symbol"],
                    "prediction": prediction,
                    "selected_method": trade_row["selected_method"],
                    "entry_debit": trade_row["entry_debit"],
                }
                candidate.update(_prefix_row("hold_", dict(hold_row)))
                candidate.update(_prefix_row("managed_", dict(chosen_row)))
                candidate["short_entry_iv_pct"] = cond._round_or_none(short_entry_iv_pct)
                candidate["short_atm_entry_iv_pct"] = cond._round_or_none(short_atm_entry_iv_pct)
                candidate["long_atm_entry_iv_pct"] = cond._round_or_none(long_atm_entry_iv_pct)
                candidate["short_over_long_atm_iv_premium_pct"] = cond._round_or_none(
                    short_over_long_atm_iv_premium_pct
                )
                candidate["vix_effective_trade_date"] = (
                    "" if vix_snapshot is None else vix_snapshot.effective_trade_date.isoformat()
                )
                candidate["vix_close_entry"] = (
                    None if vix_snapshot is None else cond._round_or_none(vix_snapshot.close_price)
                )
                candidate["vix_prior_entry_date"] = (
                    ""
                    if vix_snapshot is None or vix_snapshot.prior_entry_date is None
                    else vix_snapshot.prior_entry_date.isoformat()
                )
                candidate["vix_prior_effective_trade_date"] = (
                    ""
                    if vix_snapshot is None or vix_snapshot.prior_effective_trade_date is None
                    else vix_snapshot.prior_effective_trade_date.isoformat()
                )
                candidate["vix_prior_close_entry"] = (
                    None
                    if vix_snapshot is None or vix_snapshot.prior_close_price is None
                    else cond._round_or_none(vix_snapshot.prior_close_price)
                )
                candidate["vix_weekly_change_pct"] = (
                    None
                    if vix_snapshot is None or vix_snapshot.weekly_change_pct is None
                    else cond._round_or_none(vix_snapshot.weekly_change_pct)
                )
                candidate_rows.append(candidate)
                if index % 250 == 0 or index == total_trades:
                    print(f"  built {index}/{total_trades} candidate rows")
    finally:
        engine.dispose()

    serializable_rows = []
    for row in candidate_rows:
        serializable = {key: value for key, value in row.items()}
        serializable_rows.append(serializable)
    _write_csv(candidate_cache_csv, serializable_rows)
    return serializable_rows


def _passes_iv_premium_filter(
    row: dict[str, object],
    *,
    min_short_over_long_iv_premium_pct: float | None,
) -> bool:
    if min_short_over_long_iv_premium_pct is None:
        return True
    premium_pct = _to_float(row.get("short_over_long_atm_iv_premium_pct"))
    if premium_pct is None:
        return False
    return premium_pct >= min_short_over_long_iv_premium_pct


def _gate_allows(row: dict[str, object], *, config: GateConfig, vix_threshold_pct: float | None) -> bool:
    if vix_threshold_pct is not None:
        vix_weekly_change_pct = _to_float(row.get("vix_weekly_change_pct"))
        if vix_weekly_change_pct is None or abs(vix_weekly_change_pct) > vix_threshold_pct:
            return False

    prediction = str(row["prediction"]).strip()
    entry_debit = float(row["entry_debit"])
    short_entry_iv_pct = _to_float(row.get("short_entry_iv_pct"))
    if short_entry_iv_pct is None:
        return False
    if prediction == "abstain":
        return (
            (entry_debit > config.abstain_high_iv_debit_min and short_entry_iv_pct > config.abstain_high_iv_iv_min)
            or (entry_debit > config.band35_50_debit_min and 35.0 <= short_entry_iv_pct < 50.0)
            or (entry_debit > config.band40_45_debit_min and 40.0 <= short_entry_iv_pct < 45.0)
            or (entry_debit > config.band55_65_debit_min and 55.0 <= short_entry_iv_pct < 65.0)
        )
    if prediction == "up":
        return entry_debit > config.up_debit_min and short_entry_iv_pct < config.up_iv_max
    return False


def _evaluate_scenario(
    *,
    candidate_rows: list[dict[str, object]],
    config: GateConfig,
    window_entry_dates: list[date],
    top_k: int,
    method_cap: int,
    lookback_pnl_over_debit_threshold_pct: float,
    lookback_pnl_over_debit_min_history_trades: int,
    vix_threshold_pct: float | None,
    min_short_over_long_iv_premium_pct: float | None,
) -> dict[str, object]:
    base_rows = []
    for row in candidate_rows:
        if vix_threshold_pct is not None:
            vix_weekly_change_pct = _to_float(row.get("vix_weekly_change_pct"))
            if vix_weekly_change_pct is None or abs(vix_weekly_change_pct) > vix_threshold_pct:
                continue
        if not _passes_iv_premium_filter(
            row,
            min_short_over_long_iv_premium_pct=min_short_over_long_iv_premium_pct,
        ):
            continue

        selected_prefix = "managed_" if _gate_allows(row, config=config, vix_threshold_pct=None) else "hold_"
        candidate = _extract_prefixed_row(row, prefix=selected_prefix)
        if not candidate:
            continue
        candidate["management_applied"] = 1 if selected_prefix == "managed_" else 0
        candidate.setdefault("position_size_weight", 1.0)
        candidate.setdefault("position_sizing_rule", "")
        candidate["policy_label"] = cond.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
        base_rows.append(candidate)

    targeted_rows = cond._derive_targeted_best_combined_variant_rows(
        rows=base_rows,
        source_policy_label=cond.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
        abstain_half_size_entry_debit_threshold=4.0,
    )
    skip_rows_1 = cond._derive_skip_filtered_policy_rows(
        rows=targeted_rows,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
        skip_trade_predicates=(
            cond._is_abstain_median25trend_trade,
            cond._is_high_confidence_up_mllogreg56_trade,
        ),
    )
    skip_rows_2 = cond._derive_skip_filtered_policy_rows(
        rows=skip_rows_1,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
        skip_trade_predicates=(cond._is_debit_sensitive_up_method_trade,),
    )
    method_side_rows = cond._derive_skip_filtered_policy_rows(
        rows=skip_rows_2,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        skip_trade_predicates=(cond._is_debit_sensitive_abstain_method_trade,),
    )
    skip_worst_rows = cond._derive_skip_filtered_policy_rows(
        rows=method_side_rows,
        source_policy_label=cond.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
        skip_trade_predicates=(cond._is_worst_method_trade,),
    )
    top_rows = cond._derive_symbol_median_roi_topk_rows(
        rows=skip_worst_rows,
        source_policy_label=cond.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
        top_k=top_k,
        min_history_trades=3,
        selected_method_cap=method_cap,
    )
    final_rows = cond._derive_symbol_lookback_pnl_over_debit_filtered_rows(
        rows=top_rows,
        source_policy_label=cond.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
        derived_policy_label=cond.BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
        min_history_trades=lookback_pnl_over_debit_min_history_trades,
        min_pnl_over_debit_pct=lookback_pnl_over_debit_threshold_pct,
    )

    summary = _summarize_window(final_rows, window_entry_dates=window_entry_dates)
    return {
        "scenario_label": config.label(),
        "abstain_high_iv_debit_min": config.abstain_high_iv_debit_min,
        "abstain_high_iv_iv_min": config.abstain_high_iv_iv_min,
        "band35_50_debit_min": config.band35_50_debit_min,
        "band40_45_debit_min": config.band40_45_debit_min,
        "band55_65_debit_min": config.band55_65_debit_min,
        "up_debit_min": config.up_debit_min,
        "up_iv_max": config.up_iv_max,
        "min_short_over_long_iv_premium_pct": min_short_over_long_iv_premium_pct,
        "base_trade_count": len(base_rows),
        "post_worst_method_trade_count": len(skip_worst_rows),
        "topk_trade_count": len(top_rows),
        **summary,
    }


def main() -> int:
    args = build_parser().parse_args()
    if args.window_weeks < 1:
        raise SystemExit("--window-weeks must be >= 1.")

    selected_rows = [
        dict(row)
        for row in _read_csv_rows(args.selected_trades_csv)
        if str(row.get("prediction")).strip() in {"up", "abstain"}
    ]
    if not selected_rows:
        raise SystemExit("No selected trades were found.")

    window_entry_dates = _window_entry_dates(
        end_date=args.window_end_date,
        weeks=args.window_weeks,
    )
    print(
        f"Optimizing over {len(window_entry_dates)} entry weeks: "
        f"{window_entry_dates[0].isoformat()} to {window_entry_dates[-1].isoformat()}"
    )

    candidate_rows = _build_candidate_rows(
        selected_rows=selected_rows,
        candidate_cache_csv=args.candidate_cache_csv,
        rebuild=args.rebuild_candidate_cache,
        vix_cache_csv=args.vix_cache_csv,
        disable_vix_cache_refresh=args.disable_vix_cache_refresh,
    )
    print(f"Loaded {len(candidate_rows)} unconditional managed candidate rows.")

    vix_threshold_pct = None if args.vix_max_weekly_change_up_pct < 0 else args.vix_max_weekly_change_up_pct
    configs = [
        GateConfig(*values)
        for values in itertools.product(
            _parse_thresholds(args.abstain_high_iv_debit_thresholds),
            _parse_thresholds(args.abstain_high_iv_iv_thresholds),
            _parse_thresholds(args.band35_50_debit_thresholds),
            _parse_thresholds(args.band40_45_debit_thresholds),
            _parse_thresholds(args.band55_65_debit_thresholds),
            _parse_thresholds(args.up_debit_thresholds),
            _parse_thresholds(args.up_iv_max_thresholds),
        )
    ]
    print(f"Evaluating {len(configs)} gate scenarios.")

    summary_rows: list[dict[str, object]] = []
    for index, config in enumerate(configs, start=1):
        summary_rows.append(
            _evaluate_scenario(
                candidate_rows=candidate_rows,
                config=config,
                window_entry_dates=window_entry_dates,
                top_k=args.top_k,
                method_cap=args.method_cap,
                lookback_pnl_over_debit_threshold_pct=args.lookback_pnl_over_debit_threshold_pct,
                lookback_pnl_over_debit_min_history_trades=args.lookback_pnl_over_debit_min_history_trades,
                vix_threshold_pct=vix_threshold_pct,
                min_short_over_long_iv_premium_pct=args.min_short_over_long_iv_premium_pct,
            )
        )
        if index % 100 == 0 or index == len(configs):
            print(f"  evaluated {index}/{len(configs)} scenarios")

    summary_rows.sort(
        key=lambda row: (
            -(float(row["weighted_return_positive_debit_pct"]) if row["weighted_return_positive_debit_pct"] not in (None, "") else -10_000.0),
            int(row["negative_pnl_weeks"]),
            float(row["max_drawdown_pct"]) if row["max_drawdown_pct"] not in (None, "") else 10_000.0,
            -(float(row["median_roi_positive_debit_pct"]) if row["median_roi_positive_debit_pct"] not in (None, "") else -10_000.0),
        )
    )
    _write_csv(args.output_summary_csv, summary_rows)

    best_return = summary_rows[0]
    best_balanced = min(
        summary_rows,
        key=lambda row: (
            int(row["negative_pnl_weeks"]),
            float(row["max_drawdown_pct"]) if row["max_drawdown_pct"] not in (None, "") else 10_000.0,
            -(float(row["weighted_return_positive_debit_pct"]) if row["weighted_return_positive_debit_pct"] not in (None, "") else -10_000.0),
        ),
    )
    best_rows = [
        {"selection": "best_return", **best_return},
        {"selection": "best_balanced", **best_balanced},
    ]
    for row in _best_rows_by_rank(summary_rows, top_n=args.top_n_best):
        best_rows.append({"selection": "top_ranked", **row})
    _write_csv(args.output_best_csv, best_rows)

    print(f"Wrote {args.output_summary_csv}")
    print(f"Wrote {args.output_best_csv}")
    print("Best return scenario:")
    print(best_return)
    print("Best balanced scenario:")
    print(best_balanced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
