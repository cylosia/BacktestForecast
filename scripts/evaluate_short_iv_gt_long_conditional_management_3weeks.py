from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict, deque
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Callable

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data import vix_regime
from backtestforecast.models import HistoricalOptionDayBar
from backtestforecast.backtests.strategies.common import choose_atm_strike

import scripts.compare_short_iv_gt_long_management_rules_3weeks as mgmt
import scripts.evaluate_short_iv_gt_long_calendar_take_profit_grid as tp_grid

LOGS = ROOT / "logs"

DEFAULT_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_3weeks_selected_trades.csv"
DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_conditional_management_3weeks_selected_trades.csv"
DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_conditional_management_3weeks_summary.csv"
DEFAULT_VIX_CACHE_CSV = vix_regime.DEFAULT_VIX_CACHE_CSV
BASE_BEST_COMBINED_POLICY_LABEL = (
    "best_combined_abstain_tp25_stop35_high_iv_or_piecewise_moderate_iv"
    "__up_tp75_stop65_debit_gt_5_5_short_iv_lt_40"
)
BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL = (
    f"{BASE_BEST_COMBINED_POLICY_LABEL}"
    "__abstain_mlgbp64_tp0_stop50__abstain_mlgbp72_tp0_stop65"
)
BASE_BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    f"{BASE_BEST_COMBINED_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
)
BASE_BEST_COMBINED_METHOD_SIDE_EXIT_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    f"{BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
)
BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL = (
    f"{BASE_BEST_COMBINED_POLICY_LABEL}__up_70_75_negative_method_skip"
)
BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_POLICY_LABEL = (
    f"{BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__up_70_75_negative_method_skip"
)
BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL = (
    f"{BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL}__abstain_debit_gt_4_half_size"
)
BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_POLICY_LABEL}__abstain_debit_gt_4_half_size"
)
BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL}"
    "__skip_abstain_median25trend__skip_up_mllogreg56_conf_90_100"
)
BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL}"
    "__skip_abstain_median25trend__skip_up_mllogreg56_conf_90_100"
)
BEST_COMBINED_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL}"
    "__skip_up_debit_sensitive_methods"
)
BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL}"
    "__skip_up_debit_sensitive_methods"
)
BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_ABSTAIN_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL}"
    "__skip_abstain_debit_sensitive_methods"
)
# Preferred uncapped method-side variant before any portfolio-cap ranking.
BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL = (
    BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_ABSTAIN_FILTER_POLICY_LABEL
)
BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
)
BEST_COMBINED_METHOD_SIDE_EXIT_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
)
BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top18_52w_symbol_median_roi_min1"
)
BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top18_52w_symbol_median_roi_min3"
)
BEST_COMBINED_TOP40_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top40_52w_symbol_median_roi_min3"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min1"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
)
BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__skip_vote40rsi_mlgbp68"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
)
BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__skip_mlgb68"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL = (
    f"{BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_10_10_2_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
    "__top43_52w_symbol_median_roi_min3__mlgbp72_cap10__mlgb76_cap10__median40rsi_cap2"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_9_10_2_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
    "__top43_52w_symbol_median_roi_min3__mlgbp72_cap9__mlgb76_cap10__median40rsi_cap2"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_P25_NONNEG_POLICY_LABEL = (
    f"{BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
    "__p25_nonnegative"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN5_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min5__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_PNL_OVER_DEBIT_15_MIN5_POLICY_LABEL = (
    f"{BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
    "__pnl_over_debit_15_min5"
)
BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__basket_close_70"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_VIX_ABS_GT_10_HALF_SIZE_POLICY_LABEL = (
    f"{BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL}"
    "__vix_abs_weekly_change_gt_10_half_size"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_STRESS_METHOD_HALF_SIZE_POLICY_LABEL = (
    f"{BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
    "__stress_median40rsi_mllogreg56_half_size"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_WEEKLY_DEBIT_BUDGET40_POLICY_LABEL = (
    f"{BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
    "__weekly_debit_budget_40"
)
BEST_COMBINED_TOP43_SYMBOL_LOWEST_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_lowest_drawdown_pct_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
    "__top43_52w_symbol_median_roi_minus_drawdown_pct_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_PLUS_P25_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_plus_p25_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_CVAR10_LOSS_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
    "__top43_52w_symbol_median_roi_minus_cvar10_loss_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_PROFIT_FACTOR_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_profit_factor_guarded_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_SORTINO_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL = (
    f"{BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_sortino_guarded_min3__method_cap12"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_ABSTAIN_CAP29_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min3__abstain_cap29"
)
BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_NEGATIVE_P25_MIN3_POLICY_LABEL = (
    f"{BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}"
    "__top43_52w_symbol_median_roi_minus_negative_p25_min3"
)
# Preferred downstream alias for the current combined policy.
BEST_COMBINED_POLICY_LABEL = BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
# Preferred portfolio variant after the ivpremium10/source retest.
BEST_COMBINED_PORTFOLIO_POLICY_LABEL = BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL
BEST_COMBINED_PORTFOLIO_LIVE_POLICY_LABEL = f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}_live"
MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL = "abstain_mlgbp72_first_breach_exit"
MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL = (
    "abstain_mlgbp72_last_pre_expiration_negative_exit"
)
MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL = (
    "abstain_mlgbp72_roll_forward_one_week_up1"
)
MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL = (
    "abstain_mlgbp72_roll_forward_one_week_up2"
)
MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL = (
    "abstain_mlgbp72_roll_same_week_atm"
)
MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL = (
    "abstain_mlgbp72_roll_both_legs_same_week_atm"
)
MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL = (
    "abstain_mlgbp72_two_sided_call_put_butterfly"
)
MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL = (
    "abstain_mlgbp72_two_sided_call_put_butterfly_w2"
)
MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL = (
    "abstain_mlgbp72_two_sided_call_put_butterfly_w3"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_first_breach"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_last_pre_expiration_negative"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_forward_one_week_up1"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_forward_one_week_up2"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_same_week_atm"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_both_legs_same_week_atm"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly_w2"
)
BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL = (
    f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly_w3"
)
MLGBP72_ABSTAIN_CREDIT_SPREAD_TARGET_DELTAS = (20, 30, 40, 45, 50)
MLGBP72_ABSTAIN_CREDIT_SPREAD_WIDTH_STEPS = (1, 2, 3)


def _mlgbp72_abstain_credit_spread_policy_label(target_delta_pct: int, width_steps: int) -> str:
    return f"abstain_mlgbp72_two_sided_credit_spread_d{target_delta_pct}_w{width_steps}"


def _best_combined_portfolio_mlgbp72_abstain_credit_spread_policy_label(
    target_delta_pct: int,
    width_steps: int,
) -> str:
    return (
        f"{BEST_COMBINED_PORTFOLIO_POLICY_LABEL}"
        f"__mlgbp72_abstain_two_sided_credit_spread_d{target_delta_pct}_w{width_steps}"
    )


DEFAULT_TOP43_ABSTAIN_CAP = 29
DEFAULT_TOP43_METHOD_CAP = 12
DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT = 15.0
DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES = 5
DEFAULT_BASKET_CLOSE_THRESHOLD_PCT = 70.0
DEFAULT_VIX_MAX_WEEKLY_CHANGE_UP_PCT: float | None = None
DEFAULT_MIN_SHORT_OVER_LONG_IV_PREMIUM_PCT: float | None = None
TIGHT_METHOD_CAPS_10_10_2 = {"mlgbp72": 10, "mlgb76": 10, "median40rsi": 2}
TIGHT_METHOD_CAPS_9_10_2 = {"mlgbp72": 9, "mlgb76": 10, "median40rsi": 2}
DEFAULT_SOFT_VIX_HALF_SIZE_THRESHOLD_PCT = 10.0
DEFAULT_STRESS_METHOD_VIX_THRESHOLD_PCT = 10.0
DEFAULT_WEEKLY_DEBIT_BUDGET = 40.0
WORST_METHOD_SKIP_METHODS = frozenset({"vote40rsi", "mlgbp68"})
EXTENDED_WORST_METHOD_SKIP_METHODS = frozenset({"vote40rsi", "mlgbp68", "mlgb68"})
STRESS_HALF_SIZE_METHODS = frozenset({"median40rsi", "mllogreg56"})
NEGATIVE_UP_CONFIDENCE_BUCKET_METHODS = frozenset(
    {
        "mllogreg56",
        "mlgbp64",
        "mlgbp72",
        "median40rsi",
        "vote15rsi",
        "mlgb70",
        "vote30trend",
        "median25trend",
    }
)
DEBIT_SENSITIVE_UP_METHOD_ENTRY_DEBIT_THRESHOLDS: dict[str, float] = {
    "median25": 3.0,
    "median15trend": 2.5,
    "median25rsi": 2.0,
    "mlgb76": 1.2,
    "median30trend": 2.0,
}
DEBIT_SENSITIVE_ABSTAIN_METHOD_ENTRY_DEBIT_RANGES: dict[str, tuple[tuple[float, float], ...]] = {
    "median40rsi": ((1.0, 1.5), (2.0, 3.0)),
    "mlgb70": ((1.0, 1.5),),
    "mlgb72": ((0.5, 1.0),),
}
ABSTAIN_METHOD_SIDE_TP_STOP_OVERRIDES: dict[str, tuple[float, float]] = {
    "mlgbp64": (0.0, 50.0),
    "mlgbp72": (0.0, 65.0),
}
DEFAULT_ABSTAIN_TAKE_PROFIT_PCT = 25.0
DEFAULT_ABSTAIN_STOP_LOSS_PCT = 35.0
DEFAULT_UP_TAKE_PROFIT_PCT = 75.0
DEFAULT_UP_STOP_LOSS_PCT = 65.0
POSITION_SIZED_VALUE_COLUMNS = (
    "original_entry_debit",
    "entry_debit",
    "spread_mark",
    "pnl",
    "roll_net_debit",
)
HistoryScoreFn = Callable[[list[float]], float | None]
HistoryEligibilityFn = Callable[[list[float]], bool]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate conditional management rules for short-IV-greater-than-long-IV weekly "
            "call calendars across the recent 3 weekly windows."
        )
    )
    parser.add_argument(
        "--max-spot-entry",
        type=float,
        default=None,
        help="Optional maximum allowed spot close on entry date. Example: 1000.",
    )
    parser.add_argument(
        "--vix-max-weekly-change-up-pct",
        type=float,
        default=DEFAULT_VIX_MAX_WEEKLY_CHANGE_UP_PCT,
        help="Optional entry-week filter. Skip all trades for weeks where the absolute VIX weekly change exceeds this percent versus the prior entry week.",
    )
    parser.add_argument(
        "--vix-cache-csv",
        type=Path,
        default=DEFAULT_VIX_CACHE_CSV,
        help="Cache CSV for VIX reference data. Defaults to logs/reference/vixcls_cache.csv.",
    )
    parser.add_argument(
        "--disable-vix-cache-refresh",
        action="store_true",
        help="Use only existing DB/cache VIX data and do not auto-refresh the cache from FRED.",
    )
    parser.add_argument(
        "--min-short-over-long-iv-premium-pct",
        type=float,
        default=DEFAULT_MIN_SHORT_OVER_LONG_IV_PREMIUM_PCT,
        help=(
            "Optional minimum ATM IV premium filter. Skip trades unless short ATM IV is at least "
            "this percent above long ATM IV at entry."
        ),
    )
    parser.add_argument("--selected-trades-csv", type=Path, default=DEFAULT_SELECTED_TRADES_CSV)
    parser.add_argument("--output-trades-csv", type=Path, default=DEFAULT_OUTPUT_TRADES_CSV)
    parser.add_argument("--output-summary-csv", type=Path, default=DEFAULT_OUTPUT_SUMMARY_CSV)
    return parser


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.lower() == "none":
        return None
    return float(text)


def _is_vix_weekly_change_within_threshold(
    *,
    weekly_change_pct: float | None,
    threshold_pct: float | None,
) -> bool | None:
    if threshold_pct is None or weekly_change_pct is None:
        return None
    return abs(weekly_change_pct) <= threshold_pct


def _summarize_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    positive_rows = [row for row in rows if float(row["entry_debit"]) > 0]
    total_debit = sum(float(row["entry_debit"]) for row in positive_rows)
    total_pnl = sum(float(row["pnl"]) for row in positive_rows)
    total_pnl_all = sum(float(row["pnl"]) for row in rows)
    roi_values = [float(row["roi_pct"]) for row in positive_rows if row["roi_pct"] is not None]
    return {
        "trade_count": len(rows),
        "positive_debit_count": len(positive_rows),
        "nonpositive_debit_count": len(rows) - len(positive_rows),
        "managed_trade_count": sum(1 for row in rows if int(row["management_applied"]) == 1),
        "total_debit_paid_positive": round(total_debit, 6),
        "total_pnl_positive": round(total_pnl, 6),
        "total_pnl_all_trades": round(total_pnl_all, 6),
        "avg_roi_positive_debit_pct": _round_or_none(mean(roi_values) if roi_values else None),
        "median_roi_positive_debit_pct": _round_or_none(median(roi_values) if roi_values else None),
        "weighted_return_positive_debit_pct": (
            None if total_debit <= 0 else round(total_pnl / total_debit * 100.0, 6)
        ),
        "profit_target_exit_count": sum(1 for row in rows if row["exit_reason"] == "profit_target"),
        "stop_loss_exit_count": sum(1 for row in rows if row["exit_reason"] == "stop_loss"),
        "tested_exit_count": sum(1 for row in rows if row["exit_reason"] == "spot_close_above_short_strike"),
        "expiration_exit_count": sum(1 for row in rows if row["exit_reason"] == "expiration"),
    }


def _load_symbol_cache(
    session: Session,
    *,
    symbol: str,
    trades: list[dict[str, str]],
) -> tuple[
    dict[date, float],
    dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    dict[tuple[str, str, str], list[date]],
    dict[tuple[str, str, str], list[date]],
]:
    entry_dates = [date.fromisoformat(row["entry_date"]) for row in trades]
    short_expirations = [date.fromisoformat(row["short_expiration"]) for row in trades]
    long_expirations = [date.fromisoformat(row["long_expiration"]) for row in trades]
    spot_by_date = tp_grid._load_underlying_closes(
        session,
        symbol=symbol,
        start_date=min(entry_dates),
        end_date=max(long_expirations),
    )
    ordered_trade_dates = sorted(spot_by_date)
    needed_trade_dates: set[date] = set(entry_dates)
    path_dates_by_trade: dict[tuple[str, str, str], list[date]] = {}
    extended_path_dates_by_trade: dict[tuple[str, str, str], list[date]] = {}
    for row in trades:
        entry_date = date.fromisoformat(row["entry_date"])
        short_expiration = date.fromisoformat(row["short_expiration"])
        long_expiration = date.fromisoformat(row["long_expiration"])
        path_dates = [
            trade_date
            for trade_date in ordered_trade_dates
            if entry_date < trade_date <= short_expiration
        ]
        extended_path_dates = [
            trade_date
            for trade_date in ordered_trade_dates
            if entry_date < trade_date <= long_expiration
        ]
        path_dates_by_trade[(row["entry_date"], row["symbol"], row["prediction"])] = path_dates
        extended_path_dates_by_trade[(row["entry_date"], row["symbol"], row["prediction"])] = extended_path_dates
        needed_trade_dates.update(extended_path_dates)
    option_rows_by_date = tp_grid._load_option_rows_for_dates_and_expirations(
        session,
        symbol=symbol,
        trade_dates=needed_trade_dates,
        expirations=set(short_expirations).union(long_expirations),
    )
    put_option_rows_by_date = _load_option_rows_for_dates_and_expirations_by_contract_type(
        session,
        symbol=symbol,
        trade_dates=needed_trade_dates,
        expirations=set(short_expirations).union(long_expirations),
        contract_type="put",
    )
    return (
        spot_by_date,
        option_rows_by_date,
        put_option_rows_by_date,
        path_dates_by_trade,
        extended_path_dates_by_trade,
    )


def _load_option_rows_for_dates_and_expirations_by_contract_type(
    session: Session,
    *,
    symbol: str,
    trade_dates: set[date],
    expirations: set[date],
    contract_type: str,
) -> dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]]:
    if not trade_dates or not expirations:
        return {}
    stmt = (
        select(HistoricalOptionDayBar)
        .where(HistoricalOptionDayBar.underlying_symbol == symbol)
        .where(HistoricalOptionDayBar.trade_date.in_(sorted(trade_dates)))
        .where(HistoricalOptionDayBar.expiration_date.in_(sorted(expirations)))
        .where(HistoricalOptionDayBar.contract_type == contract_type)
        .order_by(
            HistoricalOptionDayBar.trade_date,
            HistoricalOptionDayBar.expiration_date,
            HistoricalOptionDayBar.strike_price,
        )
    )
    grouped: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]] = defaultdict(lambda: defaultdict(list))
    for row in session.execute(stmt).scalars():
        grouped[row.trade_date][row.expiration_date].append(
            tp_grid.delta_grid.OptionRow(
                option_ticker=row.option_ticker,
                trade_date=row.trade_date,
                expiration_date=row.expiration_date,
                strike_price=float(row.strike_price),
                close_price=float(row.close_price),
            )
        )
    return {
        trade_date: {expiration: list(items) for expiration, items in expiration_map.items()}
        for trade_date, expiration_map in grouped.items()
    }


def _find_option_close(
    *,
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
    trade_date: date,
    expiration_date: date,
    strike_price: float,
) -> float | None:
    expiration_map = option_rows_by_date.get(trade_date)
    if expiration_map is None:
        return None
    for row in expiration_map.get(expiration_date, []):
        if abs(float(row.strike_price) - strike_price) < 1e-9:
            return float(row.close_price)
    return None


def _short_entry_iv_pct(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
) -> float | None:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    short_strike = float(trade_row["short_strike"])
    short_close = _find_option_close(
        option_rows_by_date=option_rows_by_date,
        trade_date=entry_date,
        expiration_date=short_expiration,
        strike_price=short_strike,
    )
    if short_close is None:
        return None
    return tp_grid.delta_grid._estimate_call_iv_pct(
        option_price=short_close,
        spot_price=float(trade_row["spot_close_entry"]),
        strike_price=short_strike,
        trade_date=entry_date,
        expiration_date=short_expiration,
    )


def _entry_atm_iv_metrics(
    *,
    trade_row: dict[str, str],
    option_rows_by_date: dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
) -> tuple[float | None, float | None, float | None]:
    entry_date = date.fromisoformat(trade_row["entry_date"])
    short_expiration = date.fromisoformat(trade_row["short_expiration"])
    long_expiration = date.fromisoformat(trade_row["long_expiration"])
    spot_close_entry = float(trade_row["spot_close_entry"])
    expiration_map = option_rows_by_date.get(entry_date)
    if expiration_map is None or spot_close_entry <= 0:
        return None, None, None
    short_rows = [row for row in expiration_map.get(short_expiration, []) if float(row.close_price) > 0]
    long_rows = [row for row in expiration_map.get(long_expiration, []) if float(row.close_price) > 0]
    if not short_rows or not long_rows:
        return None, None, None
    short_rows_by_strike = {float(row.strike_price): row for row in short_rows}
    long_rows_by_strike = {float(row.strike_price): row for row in long_rows}
    common_strikes = sorted(set(short_rows_by_strike).intersection(long_rows_by_strike))
    if not common_strikes:
        return None, None, None
    common_atm_strike = float(choose_atm_strike(common_strikes, spot_close_entry))
    short_row = short_rows_by_strike.get(common_atm_strike)
    long_row = long_rows_by_strike.get(common_atm_strike)
    if short_row is None or long_row is None:
        return None, None, None
    short_atm_iv_pct = tp_grid.delta_grid._estimate_call_iv_pct(
        option_price=float(short_row.close_price),
        spot_price=spot_close_entry,
        strike_price=common_atm_strike,
        trade_date=entry_date,
        expiration_date=short_expiration,
    )
    long_atm_iv_pct = tp_grid.delta_grid._estimate_call_iv_pct(
        option_price=float(long_row.close_price),
        spot_price=spot_close_entry,
        strike_price=common_atm_strike,
        trade_date=entry_date,
        expiration_date=long_expiration,
    )
    if short_atm_iv_pct is None or long_atm_iv_pct is None or long_atm_iv_pct <= 0:
        return short_atm_iv_pct, long_atm_iv_pct, None
    short_over_long_atm_iv_premium_pct = ((short_atm_iv_pct - long_atm_iv_pct) / long_atm_iv_pct) * 100.0
    return short_atm_iv_pct, long_atm_iv_pct, short_over_long_atm_iv_premium_pct


def _with_condition_metadata(
    row: dict[str, object],
    *,
    short_entry_iv_pct: float | None,
    short_atm_entry_iv_pct: float | None,
    long_atm_entry_iv_pct: float | None,
    short_over_long_atm_iv_premium_pct: float | None,
    vix_snapshot: vix_regime.VixWeeklyChangeSnapshot | None,
    vix_max_weekly_change_up_pct: float | None,
    min_short_over_long_iv_premium_pct: float | None,
    condition_debit_gt_1_5: bool,
    condition_short_iv_gt_100: bool,
    condition_short_iv_gt_110: bool,
    condition_short_iv_gt_130: bool,
    condition_abstain_debit_gt_5_0_iv_35_50: bool,
    condition_abstain_debit_gt_2_0_iv_40_45: bool,
    condition_abstain_debit_gt_3_0_iv_55_65: bool,
    condition_abstain_debit_gt_2_5_iv_55_80: bool,
    condition_abstain_piecewise_moderate_iv: bool,
    condition_abstain_midhigh_iv_tested_exit: bool,
    condition_up_debit_gt_5_5: bool,
    condition_up_short_iv_lt_40: bool,
    management_applied: bool,
) -> dict[str, object]:
    enriched = dict(row)
    enriched["source_short_strike"] = row.get("source_short_strike", "")
    enriched["source_long_strike"] = row.get("source_long_strike", "")
    enriched["short_entry_iv_pct"] = _round_or_none(short_entry_iv_pct)
    enriched["short_atm_entry_iv_pct"] = _round_or_none(short_atm_entry_iv_pct)
    enriched["long_atm_entry_iv_pct"] = _round_or_none(long_atm_entry_iv_pct)
    enriched["short_over_long_atm_iv_premium_pct"] = _round_or_none(short_over_long_atm_iv_premium_pct)
    enriched["vix_effective_trade_date"] = "" if vix_snapshot is None else vix_snapshot.effective_trade_date.isoformat()
    enriched["vix_close_entry"] = None if vix_snapshot is None else _round_or_none(vix_snapshot.close_price)
    enriched["vix_prior_entry_date"] = (
        ""
        if vix_snapshot is None or vix_snapshot.prior_entry_date is None
        else vix_snapshot.prior_entry_date.isoformat()
    )
    enriched["vix_prior_effective_trade_date"] = (
        ""
        if vix_snapshot is None or vix_snapshot.prior_effective_trade_date is None
        else vix_snapshot.prior_effective_trade_date.isoformat()
    )
    enriched["vix_prior_close_entry"] = (
        None if vix_snapshot is None or vix_snapshot.prior_close_price is None else _round_or_none(vix_snapshot.prior_close_price)
    )
    enriched["vix_weekly_change_pct"] = (
        None if vix_snapshot is None or vix_snapshot.weekly_change_pct is None else _round_or_none(vix_snapshot.weekly_change_pct)
    )
    vix_change_is_within_threshold = _is_vix_weekly_change_within_threshold(
        weekly_change_pct=None if vix_snapshot is None else vix_snapshot.weekly_change_pct,
        threshold_pct=vix_max_weekly_change_up_pct,
    )
    enriched["condition_vix_weekly_change_up_le_threshold"] = (
        ""
        if vix_change_is_within_threshold is None
        else int(vix_change_is_within_threshold)
    )
    enriched["condition_debit_gt_1_5"] = int(condition_debit_gt_1_5)
    enriched["condition_short_iv_gt_100"] = int(condition_short_iv_gt_100)
    enriched["condition_short_iv_gt_110"] = int(condition_short_iv_gt_110)
    enriched["condition_short_iv_gt_130"] = int(condition_short_iv_gt_130)
    enriched["condition_debit_or_iv"] = int(condition_debit_gt_1_5 or condition_short_iv_gt_100)
    enriched["condition_debit_and_iv130"] = int(condition_debit_gt_1_5 and condition_short_iv_gt_130)
    enriched["condition_abstain_debit_gt_5_0_iv_35_50"] = int(condition_abstain_debit_gt_5_0_iv_35_50)
    enriched["condition_abstain_debit_gt_2_0_iv_40_45"] = int(condition_abstain_debit_gt_2_0_iv_40_45)
    enriched["condition_abstain_debit_gt_3_0_iv_55_65"] = int(condition_abstain_debit_gt_3_0_iv_55_65)
    enriched["condition_abstain_debit_gt_2_5_iv_55_80"] = int(condition_abstain_debit_gt_2_5_iv_55_80)
    enriched["condition_abstain_piecewise_moderate_iv"] = int(condition_abstain_piecewise_moderate_iv)
    enriched["condition_abstain_midhigh_iv_tested_exit"] = int(condition_abstain_midhigh_iv_tested_exit)
    enriched["condition_up_debit_gt_5_5"] = int(condition_up_debit_gt_5_5)
    enriched["condition_up_short_iv_lt_40"] = int(condition_up_short_iv_lt_40)
    enriched["condition_up_debit_and_short_iv_lt_40"] = int(
        condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40
    )
    enriched["condition_short_over_long_atm_iv_premium_ge_threshold"] = (
        ""
        if min_short_over_long_iv_premium_pct is None or short_over_long_atm_iv_premium_pct is None
        else int(short_over_long_atm_iv_premium_pct >= min_short_over_long_iv_premium_pct)
    )
    enriched["management_applied"] = int(management_applied)
    enriched["position_size_weight"] = 1.0
    enriched["position_sizing_rule"] = ""
    return enriched


def _should_apply_first_breach_exit(
    *,
    first_breach_row: dict[str, object],
    is_eligible: bool,
    take_profit_pct: float = 0.0,
    stop_loss_pct: float = 35.0,
) -> bool:
    if not is_eligible:
        return False
    if str(first_breach_row.get("exit_reason")) != "spot_close_above_short_strike":
        return False
    roi_pct = _to_float(str(first_breach_row.get("roi_pct")))
    if roi_pct is None:
        return False
    return roi_pct >= take_profit_pct or roi_pct <= -stop_loss_pct


def _should_apply_piecewise_abstain_tp25_stop35(
    *,
    prediction: str,
    entry_debit: float,
    short_entry_iv_pct: float | None,
) -> bool:
    if prediction != "abstain" or short_entry_iv_pct is None:
        return False
    return (
        (entry_debit > 5.0 and 35.0 <= short_entry_iv_pct < 50.0)
        or (entry_debit > 2.0 and 40.0 <= short_entry_iv_pct < 45.0)
        or (entry_debit > 3.0 and 55.0 <= short_entry_iv_pct < 65.0)
    )


def _should_apply_midhigh_iv_tested_exit(
    *,
    prediction: str,
    entry_debit: float,
    short_entry_iv_pct: float | None,
    already_piecewise_managed: bool,
) -> bool:
    return (
        prediction == "abstain"
        and short_entry_iv_pct is not None
        and entry_debit > 2.5
        and 55.0 <= short_entry_iv_pct < 80.0
        and not already_piecewise_managed
    )


def _derive_symbol_side_lookback_filtered_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    lookback_days: int = 364,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    history_by_key: dict[tuple[str, str], deque[tuple[date, float]]] = defaultdict(deque)
    pnl_sum_by_key: dict[tuple[str, str], float] = defaultdict(float)
    filtered_rows: list[dict[str, object]] = []
    for row in source_rows:
        entry_date = date.fromisoformat(str(row["entry_date"]))
        key = (str(row["symbol"]), str(row["prediction"]))
        cutoff_date = entry_date - timedelta(days=lookback_days)
        while history_by_key[key] and history_by_key[key][0][0] < cutoff_date:
            _, expired_pnl = history_by_key[key].popleft()
            pnl_sum_by_key[key] -= expired_pnl
        if pnl_sum_by_key[key] >= 0.0:
            candidate = dict(row)
            candidate["policy_label"] = derived_policy_label
            filtered_rows.append(candidate)
        if float(row["entry_debit"]) > 0:
            pnl = float(row["pnl"])
            history_by_key[key].append((entry_date, pnl))
            pnl_sum_by_key[key] += pnl
    return filtered_rows


def _derive_symbol_lookback_pnl_over_debit_filtered_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    lookback_days: int = 364,
    min_history_trades: int = 5,
    min_pnl_over_debit_pct: float = 15.0,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    history_by_symbol: dict[str, deque[tuple[date, float, float]]] = defaultdict(deque)
    pnl_sum_by_symbol: dict[str, float] = defaultdict(float)
    debit_sum_by_symbol: dict[str, float] = defaultdict(float)
    filtered_rows: list[dict[str, object]] = []
    for row in source_rows:
        entry_date = date.fromisoformat(str(row["entry_date"]))
        symbol = str(row["symbol"])
        cutoff_date = entry_date - timedelta(days=lookback_days)
        history = history_by_symbol[symbol]
        while history and history[0][0] < cutoff_date:
            _, expired_pnl, expired_entry_debit = history.popleft()
            pnl_sum_by_symbol[symbol] -= expired_pnl
            debit_sum_by_symbol[symbol] -= expired_entry_debit
        trailing_trade_count = len(history)
        trailing_entry_debit = debit_sum_by_symbol[symbol]
        trailing_pnl = pnl_sum_by_symbol[symbol]
        trailing_pnl_over_debit_pct = (
            None
            if trailing_entry_debit <= 0.0
            else (trailing_pnl / trailing_entry_debit) * 100.0
        )
        if (
            trailing_trade_count < min_history_trades
            or trailing_pnl_over_debit_pct is None
            or trailing_pnl_over_debit_pct >= min_pnl_over_debit_pct
        ):
            candidate = dict(row)
            candidate["policy_label"] = derived_policy_label
            filtered_rows.append(candidate)
        pnl = _to_float(str(row.get("pnl")))
        entry_debit = _to_float(str(row.get("entry_debit")))
        history.append(
            (
                entry_date,
                0.0 if pnl is None else pnl,
                0.0 if entry_debit is None else entry_debit,
            )
        )
        pnl_sum_by_symbol[symbol] += 0.0 if pnl is None else pnl
        debit_sum_by_symbol[symbol] += 0.0 if entry_debit is None else entry_debit
    return filtered_rows


def _trade_identity_key(row: dict[str, object]) -> tuple[str, ...]:
    roll_count = int(row.get("roll_count") or 0)
    explicit_source_short_strike = row.get("source_short_strike")
    explicit_source_long_strike = row.get("source_long_strike")
    source_short_strike = (
        explicit_source_short_strike
        if explicit_source_short_strike not in (None, "")
        else row.get("roll_from_strike")
        if roll_count > 0 and row.get("roll_from_strike") not in (None, "")
        else row.get("short_strike")
    )
    source_long_strike = (
        explicit_source_long_strike
        if explicit_source_long_strike not in (None, "")
        else row.get("long_strike")
    )
    return (
        str(row.get("entry_date") or ""),
        str(row.get("symbol") or ""),
        str(row.get("prediction") or ""),
        str(row.get("selected_method") or ""),
        str(row.get("best_delta_target_pct") or ""),
        str(row.get("short_expiration") or ""),
        str(row.get("long_expiration") or ""),
        str(source_short_strike or ""),
        str(source_long_strike or ""),
    )


def _derive_symbol_median_roi_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 1,
    lookback_days: int = 364,
    prediction_caps: dict[str, int] | None = None,
    selected_method_cap: int | None = None,
    selected_method_caps: dict[str, int] | None = None,
    history_eligibility_predicate: HistoryEligibilityFn | None = None,
    weekly_positive_entry_debit_budget: float | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_median_roi,
        prediction_caps=prediction_caps,
        selected_method_cap=selected_method_cap,
        selected_method_caps=selected_method_caps,
        history_eligibility_predicate=history_eligibility_predicate,
        weekly_positive_entry_debit_budget=weekly_positive_entry_debit_budget,
    )


def _derive_symbol_scored_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    history_scorer: HistoryScoreFn,
    min_history_trades: int = 1,
    lookback_days: int = 364,
    prediction_caps: dict[str, int] | None = None,
    selected_method_cap: int | None = None,
    selected_method_caps: dict[str, int] | None = None,
    history_eligibility_predicate: HistoryEligibilityFn | None = None,
    weekly_positive_entry_debit_budget: float | None = None,
) -> list[dict[str, object]]:
    if top_k <= 0:
        return []
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    weekly_rows_by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in source_rows:
        weekly_rows_by_date[str(row["entry_date"])].append(row)
    history_by_symbol: dict[str, deque[tuple[date, float]]] = defaultdict(deque)
    derived_rows: list[dict[str, object]] = []

    for entry_date_text in sorted(weekly_rows_by_date):
        entry_date = date.fromisoformat(entry_date_text)
        cutoff_date = entry_date - timedelta(days=lookback_days)
        ranked_candidates: list[tuple[dict[str, object], float | None, int]] = []
        for row in weekly_rows_by_date[entry_date_text]:
            symbol = str(row["symbol"])
            history = history_by_symbol[symbol]
            while history and history[0][0] < cutoff_date:
                history.popleft()
            score = None
            history_values = [value for _, value in history]
            if len(history_values) >= min_history_trades:
                score = history_scorer(history_values)
            if (
                history_eligibility_predicate is not None
                and (
                    len(history_values) < min_history_trades
                    or not history_eligibility_predicate(history_values)
                )
            ):
                continue
            ranked_candidates.append((row, score, len(history)))
        ranked_candidates.sort(
            key=lambda item: (
                1 if item[1] is None else 0,
                0.0 if item[1] is None else -float(item[1]),
                -item[2],
                str(item[0]["symbol"]),
                str(item[0]["prediction"]),
            )
        )
        selected_rows: list[dict[str, object]] = []
        selected_count_by_prediction: dict[str, int] = defaultdict(int)
        selected_count_by_method: dict[str, int] = defaultdict(int)
        selected_positive_entry_debit = 0.0
        for row, _, _ in ranked_candidates:
            prediction = str(row["prediction"])
            prediction_cap = None if prediction_caps is None else prediction_caps.get(prediction)
            if prediction_cap is not None and selected_count_by_prediction[prediction] >= prediction_cap:
                continue
            selected_method = str(row.get("selected_method"))
            method_specific_cap = None if selected_method_caps is None else selected_method_caps.get(selected_method)
            if (
                selected_method_cap is not None
                and selected_count_by_method[selected_method] >= selected_method_cap
            ):
                continue
            if (
                method_specific_cap is not None
                and selected_count_by_method[selected_method] >= method_specific_cap
            ):
                continue
            entry_debit = _to_float(str(row.get("entry_debit")))
            positive_entry_debit = 0.0 if entry_debit is None or entry_debit <= 0 else entry_debit
            if (
                weekly_positive_entry_debit_budget is not None
                and positive_entry_debit > 0
                and selected_positive_entry_debit + positive_entry_debit > weekly_positive_entry_debit_budget
            ):
                continue
            selected_rows.append(row)
            selected_count_by_prediction[prediction] += 1
            selected_count_by_method[selected_method] += 1
            selected_positive_entry_debit += positive_entry_debit
            if len(selected_rows) >= top_k:
                break
        for row in selected_rows:
            candidate = dict(row)
            candidate["policy_label"] = derived_policy_label
            derived_rows.append(candidate)
        for row in weekly_rows_by_date[entry_date_text]:
            entry_debit = _to_float(str(row.get("entry_debit")))
            roi_pct = _to_float(str(row.get("roi_pct")))
            if entry_debit is None or entry_debit <= 0 or roi_pct is None:
                continue
            history_by_symbol[str(row["symbol"])].append((entry_date, roi_pct))
    return derived_rows


def _derive_symbol_downside_adjusted_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_median_roi_minus_negative_p25,
    )


def _derive_symbol_lowest_drawdown_pct_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_lowest_drawdown_pct,
        selected_method_cap=selected_method_cap,
    )


def _derive_symbol_median_roi_minus_drawdown_pct_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_median_roi_minus_drawdown_pct,
        selected_method_cap=selected_method_cap,
    )


def _derive_symbol_median_roi_plus_p25_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_median_roi_plus_p25,
        selected_method_cap=selected_method_cap,
    )


def _derive_symbol_median_roi_minus_cvar10_loss_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_median_roi_minus_cvar10_loss,
        selected_method_cap=selected_method_cap,
    )


def _derive_symbol_profit_factor_guarded_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_profit_factor_guarded,
        selected_method_cap=selected_method_cap,
    )


def _derive_symbol_sortino_guarded_topk_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    top_k: int,
    min_history_trades: int = 3,
    lookback_days: int = 364,
    selected_method_cap: int | None = None,
) -> list[dict[str, object]]:
    return _derive_symbol_scored_topk_rows(
        rows=rows,
        source_policy_label=source_policy_label,
        derived_policy_label=derived_policy_label,
        top_k=top_k,
        min_history_trades=min_history_trades,
        lookback_days=lookback_days,
        history_scorer=_score_history_by_sortino_guarded,
        selected_method_cap=selected_method_cap,
    )


def _score_history_by_median_roi(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _history_max_drawdown_pct(values: list[float], *, starting_equity: float = 100.0) -> float | None:
    if not values:
        return None
    equity = float(starting_equity)
    running_peak = equity
    max_drawdown_pct = 0.0
    for value in values:
        equity += float(value)
        if equity > running_peak:
            running_peak = equity
        drawdown = running_peak - equity
        drawdown_pct = (drawdown / running_peak * 100.0) if running_peak > 0 else 0.0
        if drawdown_pct > max_drawdown_pct:
            max_drawdown_pct = drawdown_pct
    return max_drawdown_pct


def _linear_percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    if lower_index == upper_index:
        return ordered[lower_index]
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    fraction = position - lower_index
    return lower_value + ((upper_value - lower_value) * fraction)


def _score_history_by_median_roi_minus_negative_p25(values: list[float]) -> float | None:
    median_roi = _score_history_by_median_roi(values)
    p25_roi = _linear_percentile(values, 0.25)
    if median_roi is None or p25_roi is None:
        return None
    return median_roi + min(p25_roi, 0.0)


def _score_history_by_lowest_drawdown_pct(values: list[float]) -> float | None:
    max_drawdown_pct = _history_max_drawdown_pct(values)
    if max_drawdown_pct is None:
        return None
    return -max_drawdown_pct


def _score_history_by_median_roi_minus_drawdown_pct(values: list[float]) -> float | None:
    median_roi = _score_history_by_median_roi(values)
    max_drawdown_pct = _history_max_drawdown_pct(values)
    if median_roi is None or max_drawdown_pct is None:
        return None
    return median_roi - max_drawdown_pct


def _history_cvar_loss_pct(values: list[float], *, tail_fraction: float = 0.1) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    tail_count = max(1, int(math.ceil(len(ordered) * tail_fraction)))
    tail_mean = mean(ordered[:tail_count])
    return abs(min(tail_mean, 0.0))


def _score_history_by_median_roi_plus_p25(values: list[float]) -> float | None:
    median_roi = _score_history_by_median_roi(values)
    p25_roi = _linear_percentile(values, 0.25)
    if median_roi is None or p25_roi is None:
        return None
    return median_roi + p25_roi


def _score_history_by_median_roi_minus_cvar10_loss(values: list[float]) -> float | None:
    median_roi = _score_history_by_median_roi(values)
    cvar10_loss = _history_cvar_loss_pct(values, tail_fraction=0.1)
    if median_roi is None or cvar10_loss is None:
        return None
    return median_roi - cvar10_loss


def _score_history_by_profit_factor_guarded(
    values: list[float],
    *,
    max_profit_factor: float = 10.0,
) -> float | None:
    if not values:
        return None
    median_roi = _score_history_by_median_roi(values)
    if median_roi is None:
        return None
    gross_profit = sum(float(value) for value in values if value > 0)
    gross_loss = abs(sum(float(value) for value in values if value < 0))
    if gross_profit <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        profit_factor = max_profit_factor
    else:
        profit_factor = min(gross_profit / gross_loss, max_profit_factor)
    return median_roi * math.log1p(profit_factor)


def _score_history_by_sortino_guarded(
    values: list[float],
    *,
    target_return: float = 0.0,
    max_sortino: float = 5.0,
) -> float | None:
    if not values:
        return None
    ordered = [float(value) for value in values]
    average_return = mean(ordered)
    downside_terms = [min(value - target_return, 0.0) for value in ordered]
    downside_variance = mean(term * term for term in downside_terms)
    downside_deviation = math.sqrt(downside_variance)
    if downside_deviation <= 1e-9:
        if average_return > target_return:
            return max_sortino
        if average_return < target_return:
            return -max_sortino
        return 0.0
    raw_sortino = (average_return - target_return) / downside_deviation
    return max(-max_sortino, min(raw_sortino, max_sortino))


def _has_nonnegative_history_p25(values: list[float]) -> bool:
    p25_roi = _linear_percentile(values, 0.25)
    return p25_roi is not None and p25_roi >= 0.0


def _is_negative_up_confidence_bucket_method_trade(row: dict[str, object]) -> bool:
    confidence_pct = _to_float(str(row.get("confidence_pct")))
    return (
        str(row.get("prediction")) == "up"
        and confidence_pct is not None
        and 70.0 < confidence_pct <= 75.0
        and str(row.get("selected_method")) in NEGATIVE_UP_CONFIDENCE_BUCKET_METHODS
    )


def _is_abstain_median25trend_trade(row: dict[str, object]) -> bool:
    return (
        str(row.get("prediction")) == "abstain"
        and str(row.get("selected_method")) == "median25trend"
    )


def _is_abstain_mlgbp72_trade(row: dict[str, object]) -> bool:
    return (
        str(row.get("prediction")) == "abstain"
        and str(row.get("selected_method")) == "mlgbp72"
    )


def _is_high_confidence_up_mllogreg56_trade(row: dict[str, object]) -> bool:
    confidence_pct = _to_float(str(row.get("confidence_pct")))
    return (
        str(row.get("prediction")) == "up"
        and str(row.get("selected_method")) == "mllogreg56"
        and confidence_pct is not None
        and 90.0 < confidence_pct <= 100.0
    )


def _is_debit_sensitive_up_method_trade(row: dict[str, object]) -> bool:
    if str(row.get("prediction")) != "up":
        return False
    method = str(row.get("selected_method"))
    entry_debit_threshold = DEBIT_SENSITIVE_UP_METHOD_ENTRY_DEBIT_THRESHOLDS.get(method)
    if entry_debit_threshold is None:
        return False
    raw_entry_debit = row.get("entry_debit")
    entry_debit = _to_float(None if raw_entry_debit is None else str(raw_entry_debit))
    return entry_debit is not None and entry_debit >= entry_debit_threshold


def _is_debit_sensitive_abstain_method_trade(row: dict[str, object]) -> bool:
    if str(row.get("prediction")) != "abstain":
        return False
    method = str(row.get("selected_method"))
    entry_debit_ranges = DEBIT_SENSITIVE_ABSTAIN_METHOD_ENTRY_DEBIT_RANGES.get(method)
    if entry_debit_ranges is None:
        return False
    raw_entry_debit = row.get("entry_debit")
    entry_debit = _to_float(None if raw_entry_debit is None else str(raw_entry_debit))
    if entry_debit is None:
        return False
    return any(lower <= entry_debit < upper for lower, upper in entry_debit_ranges)


def _is_worst_method_trade(row: dict[str, object]) -> bool:
    return str(row.get("selected_method")) in WORST_METHOD_SKIP_METHODS


def _is_extended_worst_method_trade(row: dict[str, object]) -> bool:
    return str(row.get("selected_method")) in EXTENDED_WORST_METHOD_SKIP_METHODS


def _select_abstain_method_side_exit_row(
    *,
    prediction: str,
    selected_method: str,
    default_row: dict[str, object],
    override_rows_by_method: dict[str, dict[str, object]],
) -> dict[str, object]:
    if prediction != "abstain":
        return default_row
    return override_rows_by_method.get(selected_method, default_row)


def resolve_method_side_tp_stop(*, prediction: str, selected_method: str) -> tuple[float, float]:
    if prediction == "up":
        return DEFAULT_UP_TAKE_PROFIT_PCT, DEFAULT_UP_STOP_LOSS_PCT
    if prediction == "abstain":
        return ABSTAIN_METHOD_SIDE_TP_STOP_OVERRIDES.get(
            selected_method,
            (DEFAULT_ABSTAIN_TAKE_PROFIT_PCT, DEFAULT_ABSTAIN_STOP_LOSS_PCT),
        )
    raise ValueError(f"Unsupported prediction for method-side TP/SL mapping: {prediction}")


def _scale_position_sized_value(value: object, *, position_size_weight: float) -> object:
    if value in (None, ""):
        return value
    try:
        return _round_or_none(float(value) * position_size_weight)
    except (TypeError, ValueError):
        return value


def _clone_position_sized_trade_row(
    row: dict[str, object],
    *,
    derived_policy_label: str,
    position_size_weight: float,
    position_sizing_rule: str,
) -> dict[str, object]:
    candidate = dict(row)
    candidate["policy_label"] = derived_policy_label
    candidate["position_size_weight"] = _round_or_none(position_size_weight, digits=4)
    candidate["position_sizing_rule"] = position_sizing_rule
    if position_size_weight != 1.0:
        for field in POSITION_SIZED_VALUE_COLUMNS:
            candidate[field] = _scale_position_sized_value(
                candidate.get(field),
                position_size_weight=position_size_weight,
            )
    return candidate


def _compose_position_sized_trade_row(
    row: dict[str, object],
    *,
    derived_policy_label: str,
    additional_position_size_weight: float,
    additional_position_sizing_rule: str,
) -> dict[str, object]:
    candidate = dict(row)
    candidate["policy_label"] = derived_policy_label
    current_weight = _to_float(None if row.get("position_size_weight") in (None, "") else str(row.get("position_size_weight")))
    combined_weight = (1.0 if current_weight is None else current_weight) * additional_position_size_weight
    existing_rule = str(row.get("position_sizing_rule") or "").strip()
    combined_rule = existing_rule
    if additional_position_sizing_rule:
        combined_rule = (
            f"{existing_rule}|{additional_position_sizing_rule}"
            if existing_rule
            else additional_position_sizing_rule
        )
    candidate["position_size_weight"] = _round_or_none(combined_weight, digits=4)
    candidate["position_sizing_rule"] = combined_rule
    if additional_position_size_weight != 1.0:
        for field in POSITION_SIZED_VALUE_COLUMNS:
            candidate[field] = _scale_position_sized_value(
                candidate.get(field),
                position_size_weight=additional_position_size_weight,
            )
    return candidate


def _derive_targeted_best_combined_variant_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    abstain_half_size_entry_debit_threshold: float | None = None,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        position_size_weight = 1.0
        position_sizing_rule = ""
        if _is_negative_up_confidence_bucket_method_trade(row):
            continue
        if (
            abstain_half_size_entry_debit_threshold is not None
            and str(row.get("prediction")) == "abstain"
            and float(row["entry_debit"]) > abstain_half_size_entry_debit_threshold
        ):
            position_size_weight = 0.5
            position_sizing_rule = (
                f"half_size_abstain_entry_debit_gt_{abstain_half_size_entry_debit_threshold:g}"
            )
        derived_rows.append(
            _clone_position_sized_trade_row(
                row,
                derived_policy_label=derived_policy_label,
                position_size_weight=position_size_weight,
                position_sizing_rule=position_sizing_rule,
            )
        )
    return derived_rows


def _derive_skip_filtered_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    skip_trade_predicates: tuple[Callable[[dict[str, object]], bool], ...],
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        if any(predicate(row) for predicate in skip_trade_predicates):
            continue
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        derived_rows.append(candidate)
    return derived_rows


def _derive_soft_vix_half_size_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    abs_vix_weekly_change_threshold_pct: float,
    position_size_weight: float = 0.5,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        weekly_change_pct = _to_float(None if row.get("vix_weekly_change_pct") in (None, "") else str(row.get("vix_weekly_change_pct")))
        if weekly_change_pct is not None and abs(weekly_change_pct) > abs_vix_weekly_change_threshold_pct:
            derived_rows.append(
                _compose_position_sized_trade_row(
                    row,
                    derived_policy_label=derived_policy_label,
                    additional_position_size_weight=position_size_weight,
                    additional_position_sizing_rule=(
                        f"half_size_vix_abs_weekly_change_gt_{abs_vix_weekly_change_threshold_pct:g}"
                    ),
                )
            )
            continue
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        derived_rows.append(candidate)
    return derived_rows


def _derive_targeted_replacement_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    replacement_policy_label: str,
    replacement_trade_predicate: Callable[[dict[str, object]], bool],
    replacement_row_predicate: Callable[[dict[str, object]], bool] | None = None,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    replacement_rows_by_key = {
        _trade_identity_key(row): dict(row)
        for row in rows
        if str(row.get("policy_label")) == replacement_policy_label
    }
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        if replacement_trade_predicate(row):
            replacement_row = replacement_rows_by_key.get(_trade_identity_key(row))
            if replacement_row is not None and (
                replacement_row_predicate is None or replacement_row_predicate(replacement_row)
            ):
                current_weight = _to_float(
                    None if row.get("position_size_weight") in (None, "") else str(row.get("position_size_weight"))
                )
                derived_rows.append(
                    _clone_position_sized_trade_row(
                        replacement_row,
                        derived_policy_label=derived_policy_label,
                        position_size_weight=1.0 if current_weight is None else current_weight,
                        position_sizing_rule=str(row.get("position_sizing_rule") or ""),
                    )
                )
                continue
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        derived_rows.append(candidate)
    return derived_rows


def _derive_weekly_basket_close_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    threshold_pct: float,
    session: Session,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    if not source_rows:
        return []

    rows_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in source_rows:
        rows_by_symbol[str(row["symbol"])].append(row)

    marked_rows_by_trade_and_date: dict[tuple[object, ...], dict[str, object]] = {}
    basket_marks_by_week_date: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)

    for symbol, symbol_rows in sorted(rows_by_symbol.items()):
        spot_by_date, option_rows_by_date, path_dates_by_trade = mgmt._load_symbol_path_cache(
            session,
            symbol=symbol,
            trades=symbol_rows,
        )
        for row in symbol_rows:
            entry_date_text = str(row["entry_date"])
            trade_key = _trade_identity_key(row)
            path_key = (entry_date_text, str(row["symbol"]), str(row["prediction"]))
            path_dates = path_dates_by_trade.get(path_key, [])
            entry_date = date.fromisoformat(entry_date_text)
            short_expiration = date.fromisoformat(str(row["short_expiration"]))
            long_expiration = date.fromisoformat(str(row["long_expiration"]))
            short_strike = float(row["short_strike"])
            long_strike = float(row["long_strike"])
            position_size_weight = _to_float(
                None if row.get("position_size_weight") in (None, "") else str(row.get("position_size_weight"))
            )
            if position_size_weight is None:
                position_size_weight = 1.0
            entry_debit = float(row["entry_debit"])
            for mark_date in path_dates:
                spot_mark = spot_by_date.get(mark_date)
                if spot_mark is None:
                    continue
                mark = mgmt._mark_position(
                    option_rows_by_date=option_rows_by_date,
                    mark_date=mark_date,
                    short_expiration=short_expiration,
                    long_expiration=long_expiration,
                    short_strike=short_strike,
                    long_strike=long_strike,
                    spot_mark=spot_mark,
                )
                if mark is None:
                    continue
                scaled_spread_mark = float(mark["spread_mark"]) * position_size_weight
                pnl = scaled_spread_mark - entry_debit
                roi_pct = None if entry_debit <= 0 else (pnl / entry_debit) * 100.0
                marked_rows_by_trade_and_date[(trade_key, mark_date.isoformat())] = {
                    "policy_label": derived_policy_label,
                    "exit_date": mark_date.isoformat(),
                    "spread_mark": _round_or_none(scaled_spread_mark),
                    "pnl": _round_or_none(pnl),
                    "roi_pct": _round_or_none(roi_pct),
                    "exit_reason": f"basket_close_{threshold_pct:g}",
                    "holding_days_calendar": (mark_date - entry_date).days,
                    "short_mark_method": mark["short_mark_method"],
                    "long_mark_method": mark["long_mark_method"],
                }
                if entry_debit > 0:
                    basket_marks_by_week_date[(entry_date_text, mark_date.isoformat())].append((entry_debit, pnl))

    trigger_date_by_week: dict[str, str] = {}
    week_dates = sorted({entry_date_text for entry_date_text, _ in basket_marks_by_week_date})
    for entry_date_text in week_dates:
        candidate_dates = sorted(
            trade_date_text
            for week_text, trade_date_text in basket_marks_by_week_date
            if week_text == entry_date_text
        )
        for trade_date_text in candidate_dates:
            marks = basket_marks_by_week_date[(entry_date_text, trade_date_text)]
            total_debit = sum(entry_debit for entry_debit, _ in marks)
            if total_debit <= 0:
                continue
            total_pnl = sum(pnl for _, pnl in marks)
            if (total_pnl / total_debit) * 100.0 >= threshold_pct:
                trigger_date_by_week[entry_date_text] = trade_date_text
                break

    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        trigger_date_text = trigger_date_by_week.get(str(row["entry_date"]))
        if trigger_date_text is not None:
            override = marked_rows_by_trade_and_date.get((_trade_identity_key(row), trigger_date_text))
            if override is not None:
                candidate.update(override)
        derived_rows.append(candidate)
    return derived_rows


def _derive_stress_method_half_size_policy_rows(
    *,
    rows: list[dict[str, object]],
    source_policy_label: str,
    derived_policy_label: str,
    stressed_methods: frozenset[str],
    abs_vix_weekly_change_threshold_pct: float,
    position_size_weight: float = 0.5,
) -> list[dict[str, object]]:
    source_rows = [dict(row) for row in rows if str(row["policy_label"]) == source_policy_label]
    source_rows.sort(key=lambda row: (str(row["entry_date"]), str(row["symbol"]), str(row["prediction"])))
    derived_rows: list[dict[str, object]] = []
    for row in source_rows:
        weekly_change_pct = _to_float(
            None if row.get("vix_weekly_change_pct") in (None, "") else str(row.get("vix_weekly_change_pct"))
        )
        selected_method = str(row.get("selected_method"))
        if (
            weekly_change_pct is not None
            and abs(weekly_change_pct) > abs_vix_weekly_change_threshold_pct
            and selected_method in stressed_methods
        ):
            derived_rows.append(
                _compose_position_sized_trade_row(
                    row,
                    derived_policy_label=derived_policy_label,
                    additional_position_size_weight=position_size_weight,
                    additional_position_sizing_rule=(
                        f"half_size_stress_methods_vix_abs_weekly_change_gt_{abs_vix_weekly_change_threshold_pct:g}"
                    ),
                )
            )
            continue
        candidate = dict(row)
        candidate["policy_label"] = derived_policy_label
        derived_rows.append(candidate)
    return derived_rows


def main() -> int:
    args = build_parser().parse_args()
    selected_rows = list(csv.DictReader(args.selected_trades_csv.open(encoding="utf-8")))
    selected_rows = [row for row in selected_rows if row["prediction"] in {"up", "abstain"}]
    if not selected_rows:
        raise SystemExit("No selected trade rows were found.")

    trades_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        trades_by_symbol[row["symbol"].strip().upper()].append(row)

    detail_rows: list[dict[str, object]] = []
    spot_filtered_out_by_week_prediction: dict[tuple[str, str], list[str]] = defaultdict(list)
    vix_filtered_out_by_week_prediction: dict[tuple[str, str], list[str]] = defaultdict(list)
    iv_premium_filtered_out_by_week_prediction: dict[tuple[str, str], list[str]] = defaultdict(list)
    spot_cache: dict[tuple[str, str], float | None] = {}
    symbol_cache: dict[
        str,
        tuple[
            dict[date, float],
            dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
            dict[date, dict[date, list[tp_grid.delta_grid.OptionRow]]],
            dict[tuple[str, str, str], list[date]],
            dict[tuple[str, str, str], list[date]],
        ],
    ] = {}

    engine = create_engine(mgmt._load_database_url(), future=True)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    store = HistoricalMarketDataStore(factory, factory)
    entry_dates = sorted({date.fromisoformat(row["entry_date"]) for row in selected_rows})
    vix_snapshots_by_entry_date: dict[date, vix_regime.VixWeeklyChangeSnapshot] = {}
    blocked_vix_entry_dates: set[str] = set()
    try:
        if args.vix_max_weekly_change_up_pct is not None:
            try:
                vix_close_by_date = vix_regime.load_vix_close_series(
                    start_date=min(entry_dates) - timedelta(days=14),
                    end_date=max(entry_dates),
                    store=store,
                    cache_csv=args.vix_cache_csv,
                    allow_cache_refresh=not args.disable_vix_cache_refresh,
                )
            except Exception as exc:
                raise SystemExit(f"Unable to load VIX regime data: {exc}") from exc
            vix_snapshots_by_entry_date = vix_regime.build_weekly_change_snapshots(
                entry_dates=entry_dates,
                close_by_date=vix_close_by_date,
            )
            missing_vix_dates = [entry_date.isoformat() for entry_date in entry_dates if entry_date not in vix_snapshots_by_entry_date]
            if missing_vix_dates:
                raise SystemExit(
                    "Missing VIX reference data for entry dates: " + ", ".join(missing_vix_dates)
                )
            blocked_vix_entry_dates = {
                entry_date.isoformat()
                for entry_date, snapshot in vix_snapshots_by_entry_date.items()
                if _is_vix_weekly_change_within_threshold(
                    weekly_change_pct=snapshot.weekly_change_pct,
                    threshold_pct=args.vix_max_weekly_change_up_pct,
                )
                is False
            }
        with factory() as session:
            for index, (symbol, symbol_trades) in enumerate(sorted(trades_by_symbol.items()), start=1):
                print(f"[{index:03d}/{len(trades_by_symbol):03d}] {symbol}: loading path data")
                symbol_cache[symbol] = _load_symbol_cache(session, symbol=symbol, trades=symbol_trades)

            for trade_row in selected_rows:
                symbol = trade_row["symbol"].strip().upper()
                prediction = trade_row["prediction"]
                entry_date_text = trade_row["entry_date"]
                entry_date = date.fromisoformat(entry_date_text)
                spot_key = (symbol, entry_date_text)
                if spot_key not in spot_cache:
                    spot_cache[spot_key] = mgmt._load_spot_close(session, symbol=symbol, trade_date=entry_date)
                spot_close_entry = spot_cache[spot_key]
                if args.max_spot_entry is not None and (
                    spot_close_entry is None or spot_close_entry > args.max_spot_entry
                ):
                    spot_filtered_out_by_week_prediction[(entry_date_text, prediction)].append(symbol)
                    continue
                if entry_date_text in blocked_vix_entry_dates:
                    vix_filtered_out_by_week_prediction[(entry_date_text, prediction)].append(symbol)
                    continue

                (
                    spot_by_date,
                    option_rows_by_date,
                    put_option_rows_by_date,
                    path_dates_by_trade,
                    extended_path_dates_by_trade,
                ) = symbol_cache[symbol]
                trade_key = (trade_row["entry_date"], trade_row["symbol"], trade_row["prediction"])
                path_dates = path_dates_by_trade[trade_key]
                extended_path_dates = extended_path_dates_by_trade[trade_key]
                hold_row = mgmt._simulate_hold_to_expiry(
                    trade_row=trade_row,
                    policy_label="hold_best_delta",
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                short_iv_pct = _short_entry_iv_pct(trade_row=trade_row, option_rows_by_date=option_rows_by_date)
                (
                    short_atm_iv_pct,
                    long_atm_iv_pct,
                    short_over_long_atm_iv_premium_pct,
                ) = _entry_atm_iv_metrics(trade_row=trade_row, option_rows_by_date=option_rows_by_date)
                if (
                    args.min_short_over_long_iv_premium_pct is not None
                    and (
                        short_over_long_atm_iv_premium_pct is None
                        or short_over_long_atm_iv_premium_pct < args.min_short_over_long_iv_premium_pct
                    )
                ):
                    iv_premium_filtered_out_by_week_prediction[(entry_date_text, prediction)].append(symbol)
                    continue
                vix_snapshot = vix_snapshots_by_entry_date.get(entry_date)
                entry_debit = float(trade_row["entry_debit"])
                condition_debit_gt_1_5 = prediction == "abstain" and float(trade_row["entry_debit"]) > 1.5
                condition_debit_gt_2_5 = prediction == "abstain" and entry_debit > 2.5
                condition_abstain_debit_gt_5_0_iv_35_50 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 5.0
                    and 35.0 <= short_iv_pct < 50.0
                )
                condition_abstain_debit_gt_2_0_iv_40_45 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 2.0
                    and 40.0 <= short_iv_pct < 45.0
                )
                condition_abstain_debit_gt_3_0_iv_55_65 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 3.0
                    and 55.0 <= short_iv_pct < 65.0
                )
                condition_abstain_debit_gt_2_5_iv_55_80 = (
                    prediction == "abstain"
                    and short_iv_pct is not None
                    and entry_debit > 2.5
                    and 55.0 <= short_iv_pct < 80.0
                )
                condition_abstain_piecewise_moderate_iv = _should_apply_piecewise_abstain_tp25_stop35(
                    prediction=prediction,
                    entry_debit=entry_debit,
                    short_entry_iv_pct=short_iv_pct,
                )
                condition_abstain_midhigh_iv_tested_exit = _should_apply_midhigh_iv_tested_exit(
                    prediction=prediction,
                    entry_debit=entry_debit,
                    short_entry_iv_pct=short_iv_pct,
                    already_piecewise_managed=condition_abstain_piecewise_moderate_iv,
                )
                condition_up_debit_gt_5_5 = prediction == "up" and entry_debit > 5.5
                condition_up_short_iv_lt_40 = prediction == "up" and short_iv_pct is not None and short_iv_pct < 40.0
                condition_short_iv_gt_100 = prediction == "abstain" and short_iv_pct is not None and short_iv_pct > 100.0
                condition_short_iv_gt_110 = prediction == "abstain" and short_iv_pct is not None and short_iv_pct > 110.0
                condition_short_iv_gt_130 = prediction == "abstain" and short_iv_pct is not None and short_iv_pct > 130.0
                detail_rows.append(
                    _with_condition_metadata(
                        hold_row,
                        short_entry_iv_pct=short_iv_pct,
                        short_atm_entry_iv_pct=short_atm_iv_pct,
                        long_atm_entry_iv_pct=long_atm_iv_pct,
                        short_over_long_atm_iv_premium_pct=short_over_long_atm_iv_premium_pct,
                        vix_snapshot=vix_snapshot,
                        vix_max_weekly_change_up_pct=args.vix_max_weekly_change_up_pct,
                        min_short_over_long_iv_premium_pct=args.min_short_over_long_iv_premium_pct,
                        condition_debit_gt_1_5=condition_debit_gt_1_5,
                        condition_short_iv_gt_100=condition_short_iv_gt_100,
                        condition_short_iv_gt_110=condition_short_iv_gt_110,
                        condition_short_iv_gt_130=condition_short_iv_gt_130,
                        condition_abstain_debit_gt_5_0_iv_35_50=condition_abstain_debit_gt_5_0_iv_35_50,
                        condition_abstain_debit_gt_2_0_iv_40_45=condition_abstain_debit_gt_2_0_iv_40_45,
                        condition_abstain_debit_gt_3_0_iv_55_65=condition_abstain_debit_gt_3_0_iv_55_65,
                        condition_abstain_debit_gt_2_5_iv_55_80=condition_abstain_debit_gt_2_5_iv_55_80,
                        condition_abstain_piecewise_moderate_iv=condition_abstain_piecewise_moderate_iv,
                        condition_abstain_midhigh_iv_tested_exit=condition_abstain_midhigh_iv_tested_exit,
                        condition_up_debit_gt_5_5=condition_up_debit_gt_5_5,
                        condition_up_short_iv_lt_40=condition_up_short_iv_lt_40,
                        management_applied=False,
                    )
                )

                tested_row = mgmt._simulate_exit_on_tested_strike_abstain(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                last_pre_expiration_negative_row = mgmt._simulate_exit_last_pre_expiration_if_negative(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                roll_forward_one_week_up1_row = (
                    mgmt._simulate_abstain_roll_short_forward_one_week_on_first_breach(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=extended_path_dates,
                        strike_steps=1,
                    )
                )
                roll_forward_one_week_up2_row = (
                    mgmt._simulate_abstain_roll_short_forward_one_week_on_first_breach(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=extended_path_dates,
                        strike_steps=2,
                    )
                )
                roll_same_week_atm_row = mgmt._simulate_abstain_roll_short_same_week_atm_once(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                roll_both_legs_same_week_atm_row = (
                    mgmt._simulate_abstain_roll_both_legs_same_week_atm_once(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                    )
                )
                two_sided_butterfly_row = mgmt._simulate_abstain_convert_to_same_week_atm_butterfly_on_first_breach(
                    trade_row=trade_row,
                    call_option_rows_by_date=option_rows_by_date,
                    put_option_rows_by_date=put_option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                )
                two_sided_butterfly_w2_row = (
                    mgmt._simulate_abstain_convert_to_same_week_atm_butterfly_on_first_breach(
                        trade_row=trade_row,
                        call_option_rows_by_date=option_rows_by_date,
                        put_option_rows_by_date=put_option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        wing_steps=2,
                    )
                )
                two_sided_butterfly_w3_row = (
                    mgmt._simulate_abstain_convert_to_same_week_atm_butterfly_on_first_breach(
                        trade_row=trade_row,
                        call_option_rows_by_date=option_rows_by_date,
                        put_option_rows_by_date=put_option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        wing_steps=3,
                    )
                )
                credit_spread_variant_rows = {
                    (target_delta_pct, width_steps): (
                        mgmt._simulate_abstain_convert_to_same_week_credit_spread_on_first_breach(
                            trade_row=trade_row,
                            call_option_rows_by_date=option_rows_by_date,
                            put_option_rows_by_date=put_option_rows_by_date,
                            spot_by_date=spot_by_date,
                            path_dates=path_dates,
                            target_abs_delta_pct=target_delta_pct,
                            width_steps=width_steps,
                        )
                    )
                    for target_delta_pct in MLGBP72_ABSTAIN_CREDIT_SPREAD_TARGET_DELTAS
                    for width_steps in MLGBP72_ABSTAIN_CREDIT_SPREAD_WIDTH_STEPS
                }
                tp25_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=25.0,
                    stop_loss_pct=35.0,
                )
                up_tp75_stop50_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=75.0,
                    stop_loss_pct=50.0,
                )
                up_tp75_stop65_row = mgmt._simulate_tp_stop(
                    trade_row=trade_row,
                    option_rows_by_date=option_rows_by_date,
                    spot_by_date=spot_by_date,
                    path_dates=path_dates,
                    take_profit_pct=75.0,
                    stop_loss_pct=65.0,
                )
                base_abstain_tp_stop_row = (
                    tp25_row
                    if (DEFAULT_ABSTAIN_TAKE_PROFIT_PCT, DEFAULT_ABSTAIN_STOP_LOSS_PCT) == (25.0, 35.0)
                    else mgmt._simulate_tp_stop(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=DEFAULT_ABSTAIN_TAKE_PROFIT_PCT,
                        stop_loss_pct=DEFAULT_ABSTAIN_STOP_LOSS_PCT,
                    )
                )
                base_up_tp_stop_row = (
                    up_tp75_stop65_row
                    if (DEFAULT_UP_TAKE_PROFIT_PCT, DEFAULT_UP_STOP_LOSS_PCT) == (75.0, 65.0)
                    else mgmt._simulate_tp_stop(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=DEFAULT_UP_TAKE_PROFIT_PCT,
                        stop_loss_pct=DEFAULT_UP_STOP_LOSS_PCT,
                    )
                )
                abstain_method_side_override_rows = {
                    method: mgmt._simulate_tp_stop(
                        trade_row=trade_row,
                        option_rows_by_date=option_rows_by_date,
                        spot_by_date=spot_by_date,
                        path_dates=path_dates,
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                    )
                    for method, (take_profit_pct, stop_loss_pct) in ABSTAIN_METHOD_SIDE_TP_STOP_OVERRIDES.items()
                }
                method_side_abstain_row = _select_abstain_method_side_exit_row(
                    prediction=prediction,
                    selected_method=str(trade_row["selected_method"]),
                    default_row=base_abstain_tp_stop_row,
                    override_rows_by_method=abstain_method_side_override_rows,
                )
                condition_abstain_high_iv_or_piecewise_moderate_iv = (
                    (condition_debit_gt_2_5 and condition_short_iv_gt_100)
                    or condition_abstain_piecewise_moderate_iv
                )
                combined_abstain_extended_row = (
                    tp25_row
                    if condition_abstain_high_iv_or_piecewise_moderate_iv
                    else tested_row if condition_abstain_midhigh_iv_tested_exit else hold_row
                )
                should_manage_compromise = _should_apply_first_breach_exit(
                    first_breach_row=tested_row,
                    is_eligible=condition_debit_gt_1_5 and condition_short_iv_gt_110,
                )
                should_manage_first_breach_iv130 = _should_apply_first_breach_exit(
                    first_breach_row=tested_row,
                    is_eligible=condition_debit_gt_1_5 and condition_short_iv_gt_130,
                )
                if prediction == "abstain" and str(trade_row["selected_method"]) == "mlgbp72":
                    targeted_mlgbp72_rows = [
                        (
                            MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL,
                            tested_row,
                            str(tested_row.get("exit_reason")) == "spot_close_above_short_strike",
                        ),
                        (
                            MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL,
                            last_pre_expiration_negative_row,
                            str(last_pre_expiration_negative_row.get("exit_reason")) == "last_pre_expiration_negative",
                        ),
                        (
                            MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL,
                            roll_forward_one_week_up1_row,
                            int(roll_forward_one_week_up1_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL,
                            roll_forward_one_week_up2_row,
                            int(roll_forward_one_week_up2_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL,
                            roll_same_week_atm_row,
                            int(roll_same_week_atm_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL,
                            roll_both_legs_same_week_atm_row,
                            int(roll_both_legs_same_week_atm_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL,
                            two_sided_butterfly_row,
                            int(two_sided_butterfly_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL,
                            two_sided_butterfly_w2_row,
                            int(two_sided_butterfly_w2_row.get("roll_count") or 0) > 0,
                        ),
                        (
                            MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL,
                            two_sided_butterfly_w3_row,
                            int(two_sided_butterfly_w3_row.get("roll_count") or 0) > 0,
                        ),
                        *[
                            (
                                _mlgbp72_abstain_credit_spread_policy_label(target_delta_pct, width_steps),
                                credit_spread_variant_rows[(target_delta_pct, width_steps)],
                                int(
                                    credit_spread_variant_rows[(target_delta_pct, width_steps)].get("roll_count") or 0
                                )
                                > 0,
                            )
                            for target_delta_pct in MLGBP72_ABSTAIN_CREDIT_SPREAD_TARGET_DELTAS
                            for width_steps in MLGBP72_ABSTAIN_CREDIT_SPREAD_WIDTH_STEPS
                        ],
                    ]
                    for policy_label, candidate_row, management_applied in targeted_mlgbp72_rows:
                        candidate = dict(candidate_row)
                        candidate["policy_label"] = policy_label
                        candidate["source_short_strike"] = trade_row["short_strike"]
                        candidate["source_long_strike"] = trade_row.get("long_strike", trade_row["short_strike"])
                        detail_rows.append(
                            _with_condition_metadata(
                                candidate,
                                short_entry_iv_pct=short_iv_pct,
                                short_atm_entry_iv_pct=short_atm_iv_pct,
                                long_atm_entry_iv_pct=long_atm_iv_pct,
                                short_over_long_atm_iv_premium_pct=short_over_long_atm_iv_premium_pct,
                                vix_snapshot=vix_snapshot,
                                vix_max_weekly_change_up_pct=args.vix_max_weekly_change_up_pct,
                                min_short_over_long_iv_premium_pct=args.min_short_over_long_iv_premium_pct,
                                condition_debit_gt_1_5=condition_debit_gt_1_5,
                                condition_short_iv_gt_100=condition_short_iv_gt_100,
                                condition_short_iv_gt_110=condition_short_iv_gt_110,
                                condition_short_iv_gt_130=condition_short_iv_gt_130,
                                condition_abstain_debit_gt_5_0_iv_35_50=condition_abstain_debit_gt_5_0_iv_35_50,
                                condition_abstain_debit_gt_2_0_iv_40_45=condition_abstain_debit_gt_2_0_iv_40_45,
                                condition_abstain_debit_gt_3_0_iv_55_65=condition_abstain_debit_gt_3_0_iv_55_65,
                                condition_abstain_debit_gt_2_5_iv_55_80=condition_abstain_debit_gt_2_5_iv_55_80,
                                condition_abstain_piecewise_moderate_iv=condition_abstain_piecewise_moderate_iv,
                                condition_abstain_midhigh_iv_tested_exit=condition_abstain_midhigh_iv_tested_exit,
                                condition_up_debit_gt_5_5=condition_up_debit_gt_5_5,
                                condition_up_short_iv_lt_40=condition_up_short_iv_lt_40,
                                management_applied=management_applied,
                            )
                        )

                policies = [
                    ("cond_tested_exit_debit_gt_1_5", tested_row, condition_debit_gt_1_5),
                    ("cond_tested_exit_short_iv_gt_100", tested_row, condition_short_iv_gt_100),
                    (
                        "cond_tested_exit_debit_and_iv",
                        tested_row,
                        condition_debit_gt_1_5 and condition_short_iv_gt_100,
                    ),
                    (
                        "best_abstain_tested_exit_debit_gt_1_5_short_iv_gt_130",
                        tested_row,
                        condition_debit_gt_1_5 and condition_short_iv_gt_130,
                    ),
                    (
                        "best_abstain_first_breach_debit_gt_1_5_short_iv_gt_130_tp0_sl35",
                        tested_row,
                        should_manage_first_breach_iv130,
                    ),
                    (
                        "best_compromise_first_breach_debit_gt_1_5_short_iv_gt_110_tp0_sl35",
                        tested_row,
                        should_manage_compromise,
                    ),
                    ("cond_tested_exit_debit_or_iv", tested_row, condition_debit_gt_1_5 or condition_short_iv_gt_100),
                    ("cond_tp25_stop35_debit_gt_1_5", tp25_row, condition_debit_gt_1_5),
                    ("cond_tp25_stop35_short_iv_gt_100", tp25_row, condition_short_iv_gt_100),
                    (
                        "best_abstain_tp25_stop35_debit_gt_2_5_short_iv_gt_100",
                        tp25_row,
                        condition_debit_gt_2_5 and condition_short_iv_gt_100,
                    ),
                    (
                        "best_abstain_tp25_stop35_piecewise_moderate_iv",
                        tp25_row,
                        condition_abstain_piecewise_moderate_iv,
                    ),
                    (
                        "best_abstain_tp25_stop35_high_iv_or_piecewise_moderate_iv",
                        tp25_row,
                        condition_abstain_high_iv_or_piecewise_moderate_iv,
                    ),
                    (
                        "best_abstain_tested_exit_midhigh_iv_55_80_excluding_piecewise",
                        tested_row,
                        condition_abstain_midhigh_iv_tested_exit,
                    ),
                    (
                        "best_abstain_high_iv_or_piecewise_moderate_iv_or_midhigh_tested_exit",
                        combined_abstain_extended_row,
                        condition_abstain_high_iv_or_piecewise_moderate_iv
                        or condition_abstain_midhigh_iv_tested_exit,
                    ),
                    (
                        "best_up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        up_tp75_stop50_row,
                        condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40,
                    ),
                    (
                        "best_up_tp75_stop65_debit_gt_5_5_short_iv_lt_40",
                        up_tp75_stop65_row,
                        condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40,
                    ),
                    (
                        "best_combined_abstain_tp25_stop35_debit_gt_2_5_short_iv_gt_100__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        tp25_row if prediction == "abstain" else up_tp75_stop50_row,
                        (
                            (condition_debit_gt_2_5 and condition_short_iv_gt_100)
                            or (condition_up_debit_gt_5_5 and condition_up_short_iv_lt_40)
                        ),
                    ),
                    (
                        "best_combined_abstain_tp25_stop35_high_iv_or_piecewise_moderate_iv__up_tp75_stop50_debit_gt_5_5_short_iv_lt_40",
                        tp25_row if prediction == "abstain" else up_tp75_stop50_row,
                        (
                            (
                                prediction == "abstain"
                                and condition_abstain_high_iv_or_piecewise_moderate_iv
                            )
                            or (
                                prediction == "up"
                                and condition_up_debit_gt_5_5
                                and condition_up_short_iv_lt_40
                            )
                        ),
                    ),
                    (
                        BASE_BEST_COMBINED_POLICY_LABEL,
                        base_abstain_tp_stop_row if prediction == "abstain" else base_up_tp_stop_row,
                        (
                            (
                                prediction == "abstain"
                                and condition_abstain_high_iv_or_piecewise_moderate_iv
                            )
                            or (
                                prediction == "up"
                                and condition_up_debit_gt_5_5
                                and condition_up_short_iv_lt_40
                            )
                        ),
                    ),
                    (
                        BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
                        method_side_abstain_row if prediction == "abstain" else base_up_tp_stop_row,
                        (
                            (
                                prediction == "abstain"
                                and condition_abstain_high_iv_or_piecewise_moderate_iv
                            )
                            or (
                                prediction == "up"
                                and condition_up_debit_gt_5_5
                                and condition_up_short_iv_lt_40
                            )
                        ),
                    ),
                    (
                        "cond_tp25_stop35_debit_and_iv",
                        tp25_row,
                        condition_debit_gt_1_5 and condition_short_iv_gt_100,
                    ),
                    ("cond_tp25_stop35_debit_or_iv", tp25_row, condition_debit_gt_1_5 or condition_short_iv_gt_100),
                ]
                for policy_label, managed_candidate_row, should_manage in policies:
                    candidate = dict(managed_candidate_row if should_manage else hold_row)
                    candidate["policy_label"] = policy_label
                    detail_rows.append(
                        _with_condition_metadata(
                            candidate,
                            short_entry_iv_pct=short_iv_pct,
                            short_atm_entry_iv_pct=short_atm_iv_pct,
                            long_atm_entry_iv_pct=long_atm_iv_pct,
                            short_over_long_atm_iv_premium_pct=short_over_long_atm_iv_premium_pct,
                            vix_snapshot=vix_snapshot,
                            vix_max_weekly_change_up_pct=args.vix_max_weekly_change_up_pct,
                            min_short_over_long_iv_premium_pct=args.min_short_over_long_iv_premium_pct,
                            condition_debit_gt_1_5=condition_debit_gt_1_5,
                            condition_short_iv_gt_100=condition_short_iv_gt_100,
                            condition_short_iv_gt_110=condition_short_iv_gt_110,
                            condition_short_iv_gt_130=condition_short_iv_gt_130,
                            condition_abstain_debit_gt_5_0_iv_35_50=condition_abstain_debit_gt_5_0_iv_35_50,
                            condition_abstain_debit_gt_2_0_iv_40_45=condition_abstain_debit_gt_2_0_iv_40_45,
                            condition_abstain_debit_gt_3_0_iv_55_65=condition_abstain_debit_gt_3_0_iv_55_65,
                            condition_abstain_debit_gt_2_5_iv_55_80=condition_abstain_debit_gt_2_5_iv_55_80,
                            condition_abstain_piecewise_moderate_iv=condition_abstain_piecewise_moderate_iv,
                            condition_abstain_midhigh_iv_tested_exit=condition_abstain_midhigh_iv_tested_exit,
                            condition_up_debit_gt_5_5=condition_up_debit_gt_5_5,
                            condition_up_short_iv_lt_40=condition_up_short_iv_lt_40,
                            management_applied=should_manage,
                        )
                    )
    finally:
        engine.dispose()

    if not detail_rows:
        raise SystemExit("No conditional management rows were produced.")

    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            abstain_half_size_entry_debit_threshold=4.0,
        )
    )
    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_targeted_best_combined_variant_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            abstain_half_size_entry_debit_threshold=4.0,
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_abstain_median25trend_trade,
                _is_high_confidence_up_mllogreg56_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_abstain_median25trend_trade,
                _is_high_confidence_up_mllogreg56_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_debit_sensitive_up_method_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_debit_sensitive_up_method_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_ABSTAIN_FILTER_POLICY_LABEL,
            skip_trade_predicates=(
                _is_debit_sensitive_abstain_method_trade,
            ),
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_POLICY_LABEL,
            derived_policy_label=BASE_BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_symbol_side_lookback_filtered_rows(
            rows=detail_rows,
            source_policy_label=BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BASE_BEST_COMBINED_METHOD_SIDE_EXIT_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL,
            top_k=18,
            min_history_trades=1,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL,
            top_k=18,
            min_history_trades=3,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP40_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL,
            top_k=40,
            min_history_trades=3,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL,
            top_k=43,
            min_history_trades=1,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            skip_trade_predicates=(_is_worst_method_trade,),
        )
    )
    detail_rows.extend(
        _derive_skip_filtered_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL,
            skip_trade_predicates=(_is_extended_worst_method_trade,),
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_lookback_pnl_over_debit_filtered_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_PNL_OVER_DEBIT_15_MIN5_POLICY_LABEL,
            min_history_trades=DEFAULT_LOOKBACK_PNL_OVER_DEBIT_MIN_HISTORY_TRADES,
            min_pnl_over_debit_pct=DEFAULT_LOOKBACK_PNL_OVER_DEBIT_THRESHOLD_PCT,
        )
    )
    engine = create_engine(mgmt._load_database_url())
    SessionLocal = sessionmaker(bind=engine)
    try:
        with SessionLocal() as session:
            detail_rows.extend(
                _derive_weekly_basket_close_policy_rows(
                    rows=detail_rows,
                    source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
                    derived_policy_label=BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL,
                    threshold_pct=DEFAULT_BASKET_CLOSE_THRESHOLD_PCT,
                    session=session,
                )
            )
    finally:
        engine.dispose()
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    detail_rows.extend(
        _derive_targeted_replacement_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL,
            replacement_policy_label=MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL,
            replacement_trade_predicate=_is_abstain_mlgbp72_trade,
            replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
        )
    )
    for target_delta_pct in MLGBP72_ABSTAIN_CREDIT_SPREAD_TARGET_DELTAS:
        for width_steps in MLGBP72_ABSTAIN_CREDIT_SPREAD_WIDTH_STEPS:
            detail_rows.extend(
                _derive_targeted_replacement_policy_rows(
                    rows=detail_rows,
                    source_policy_label=BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
                    derived_policy_label=_best_combined_portfolio_mlgbp72_abstain_credit_spread_policy_label(
                        target_delta_pct,
                        width_steps,
                    ),
                    replacement_policy_label=_mlgbp72_abstain_credit_spread_policy_label(
                        target_delta_pct,
                        width_steps,
                    ),
                    replacement_trade_predicate=_is_abstain_mlgbp72_trade,
                    replacement_row_predicate=lambda row: int(row.get("roll_count") or 0) > 0,
                )
            )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_10_10_2_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_caps=TIGHT_METHOD_CAPS_10_10_2,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_9_10_2_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_caps=TIGHT_METHOD_CAPS_9_10_2,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_P25_NONNEG_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
            history_eligibility_predicate=_has_nonnegative_history_p25,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN5_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=5,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_soft_vix_half_size_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_VIX_ABS_GT_10_HALF_SIZE_POLICY_LABEL,
            abs_vix_weekly_change_threshold_pct=DEFAULT_SOFT_VIX_HALF_SIZE_THRESHOLD_PCT,
        )
    )
    detail_rows.extend(
        _derive_stress_method_half_size_policy_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_STRESS_METHOD_HALF_SIZE_POLICY_LABEL,
            stressed_methods=STRESS_HALF_SIZE_METHODS,
            abs_vix_weekly_change_threshold_pct=DEFAULT_STRESS_METHOD_VIX_THRESHOLD_PCT,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_WEEKLY_DEBIT_BUDGET40_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
            weekly_positive_entry_debit_budget=DEFAULT_WEEKLY_DEBIT_BUDGET,
        )
    )
    detail_rows.extend(
        _derive_symbol_lowest_drawdown_pct_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_LOWEST_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_minus_drawdown_pct_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_plus_p25_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_PLUS_P25_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_minus_cvar10_loss_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_CVAR10_LOSS_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_profit_factor_guarded_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_PROFIT_FACTOR_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_sortino_guarded_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_SORTINO_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            selected_method_cap=DEFAULT_TOP43_METHOD_CAP,
        )
    )
    detail_rows.extend(
        _derive_symbol_median_roi_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_ABSTAIN_CAP29_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
            prediction_caps={"abstain": DEFAULT_TOP43_ABSTAIN_CAP},
        )
    )
    detail_rows.extend(
        _derive_symbol_downside_adjusted_topk_rows(
            rows=detail_rows,
            source_policy_label=BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            derived_policy_label=BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_NEGATIVE_P25_MIN3_POLICY_LABEL,
            top_k=43,
            min_history_trades=3,
        )
    )

    args.output_trades_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_trades_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
        writer.writeheader()
        writer.writerows(detail_rows)

    summary_rows: list[dict[str, object]] = []
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    overall_grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    selected_entry_prediction_groups = sorted(
        {(row["entry_date"], row["prediction"]) for row in selected_rows}
    )
    for row in detail_rows:
        grouped[(str(row["entry_date"]), str(row["prediction"]), str(row["policy_label"]))].append(row)
        overall_grouped[(str(row["prediction"]), str(row["policy_label"]))].append(row)

    policy_labels = sorted({policy_label for _, _, policy_label in grouped})
    existing_group_keys = set(grouped)

    def _build_summary_row(
        *,
        entry_date_text: str,
        prediction: str,
        policy_label: str,
        rows: list[dict[str, object]],
    ) -> dict[str, object]:
        summary = _summarize_rows(rows)
        spot_filtered_symbols = sorted(set(spot_filtered_out_by_week_prediction[(entry_date_text, prediction)]))
        vix_filtered_symbols = sorted(set(vix_filtered_out_by_week_prediction[(entry_date_text, prediction)]))
        iv_premium_filtered_symbols = sorted(
            set(iv_premium_filtered_out_by_week_prediction[(entry_date_text, prediction)])
        )
        filtered_symbols = sorted(
            set(spot_filtered_symbols).union(vix_filtered_symbols).union(iv_premium_filtered_symbols)
        )
        return {
            "entry_date": entry_date_text,
            "prediction": prediction,
            "policy_label": policy_label,
            **summary,
            "spot_filter_max_entry": args.max_spot_entry,
            "vix_max_weekly_change_up_pct": args.vix_max_weekly_change_up_pct,
            "min_short_over_long_iv_premium_pct": args.min_short_over_long_iv_premium_pct,
            "filtered_out_symbol_count": len(filtered_symbols),
            "filtered_out_symbols": ", ".join(filtered_symbols),
            "spot_filtered_out_symbol_count": len(spot_filtered_symbols),
            "spot_filtered_out_symbols": ", ".join(spot_filtered_symbols),
            "vix_filtered_out_symbol_count": len(vix_filtered_symbols),
            "vix_filtered_out_symbols": ", ".join(vix_filtered_symbols),
            "iv_premium_filtered_out_symbol_count": len(iv_premium_filtered_symbols),
            "iv_premium_filtered_out_symbols": ", ".join(iv_premium_filtered_symbols),
        }

    for (entry_date_text, prediction, policy_label), rows in sorted(grouped.items()):
        summary_rows.append(
            _build_summary_row(
                entry_date_text=entry_date_text,
                prediction=prediction,
                policy_label=policy_label,
                rows=rows,
            )
        )

    for entry_date_text, prediction in selected_entry_prediction_groups:
        filtered_symbols = (
            set(spot_filtered_out_by_week_prediction[(entry_date_text, prediction)])
            .union(vix_filtered_out_by_week_prediction[(entry_date_text, prediction)])
            .union(iv_premium_filtered_out_by_week_prediction[(entry_date_text, prediction)])
        )
        if not filtered_symbols:
            continue
        for policy_label in policy_labels:
            key = (entry_date_text, prediction, policy_label)
            if key in existing_group_keys:
                continue
            summary_rows.append(
                _build_summary_row(
                    entry_date_text=entry_date_text,
                    prediction=prediction,
                    policy_label=policy_label,
                    rows=[],
                )
            )

    for (prediction, policy_label), rows in sorted(overall_grouped.items()):
        summary = _summarize_rows(rows)
        summary_rows.append(
            {
                "entry_date": "ALL",
                "prediction": prediction,
                "policy_label": policy_label,
                **summary,
                "spot_filter_max_entry": args.max_spot_entry,
                "vix_max_weekly_change_up_pct": args.vix_max_weekly_change_up_pct,
                "min_short_over_long_iv_premium_pct": args.min_short_over_long_iv_premium_pct,
                "filtered_out_symbol_count": "",
                "filtered_out_symbols": "",
                "spot_filtered_out_symbol_count": "",
                "spot_filtered_out_symbols": "",
                "vix_filtered_out_symbol_count": "",
                "vix_filtered_out_symbols": "",
                "iv_premium_filtered_out_symbol_count": "",
                "iv_premium_filtered_out_symbols": "",
            }
        )

    with args.output_summary_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote {args.output_trades_csv}")
    print(f"Wrote {args.output_summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
