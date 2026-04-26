from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as _base


LOGS = ROOT / "logs"


def _only_vix_up_gt_threshold(
    *,
    weekly_change_pct: float | None,
    threshold_pct: float | None,
) -> bool | None:
    if threshold_pct is None or weekly_change_pct is None:
        return None
    return weekly_change_pct > threshold_pct


_base.DEFAULT_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_vix_up_gt20_selected_trades.csv"
_base.DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_conditional_management_vix_up_gt20_selected_trades.csv"
_base.DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_conditional_management_vix_up_gt20_summary.csv"
_base._is_vix_weekly_change_within_threshold = _only_vix_up_gt_threshold
_base.BEST_COMBINED_PORTFOLIO_POLICY_LABEL = _base.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
_base.BEST_COMBINED_PORTFOLIO_LIVE_POLICY_LABEL = f"{_base.BEST_COMBINED_PORTFOLIO_POLICY_LABEL}_live"

globals().update({name: getattr(_base, name) for name in dir(_base) if not name.startswith("__")})


def main() -> int:
    if "--vix-max-weekly-change-up-pct" not in sys.argv[1:]:
        sys.argv.extend(["--vix-max-weekly-change-up-pct", "20"])
    if "--min-short-over-long-iv-premium-pct" not in sys.argv[1:]:
        sys.argv.extend(["--min-short-over-long-iv-premium-pct", "15"])
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
