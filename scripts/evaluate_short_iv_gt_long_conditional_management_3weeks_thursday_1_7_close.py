from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks as _base

LOGS = ROOT / "logs"
_base.DEFAULT_SELECTED_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_thursday_1_7_close_selected_trades.csv"
_base.DEFAULT_OUTPUT_TRADES_CSV = (
    LOGS
    / "short_iv_gt_long_conditional_management_thursday_1_7_close_3weeks_no_basket_vixabs35_ivpremium60_selected_trades.csv"
)
_base.DEFAULT_OUTPUT_SUMMARY_CSV = (
    LOGS
    / "short_iv_gt_long_conditional_management_thursday_1_7_close_3weeks_no_basket_vixabs35_ivpremium60_summary.csv"
)
_base.DEFAULT_BASKET_CLOSE_THRESHOLD_PCT = None
_base.DEFAULT_VIX_MAX_WEEKLY_CHANGE_UP_PCT = 35.0
_base.DEFAULT_MIN_SHORT_OVER_LONG_IV_PREMIUM_PCT = 60.0

_THURSDAY_1_7_BASKET_SOURCE_POLICY_LABEL = _base.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
_base.BEST_COMBINED_BASKET_CLOSE_SOURCE_POLICY_LABEL = _THURSDAY_1_7_BASKET_SOURCE_POLICY_LABEL
_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL = f"{_THURSDAY_1_7_BASKET_SOURCE_POLICY_LABEL}__no_basket_close"
_base.BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL = _THURSDAY_1_7_PORTFOLIO_POLICY_LABEL
_base.BEST_COMBINED_PORTFOLIO_POLICY_LABEL = _THURSDAY_1_7_PORTFOLIO_POLICY_LABEL
_base.BEST_COMBINED_PORTFOLIO_LIVE_POLICY_LABEL = f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}_live"
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_FIRST_BREACH_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_first_breach"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_last_pre_expiration_negative"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP1_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_forward_one_week_up1"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_FORWARD_ONE_WEEK_UP2_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_forward_one_week_up2"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_SAME_WEEK_ATM_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_same_week_atm"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_ROLL_BOTH_LEGS_SAME_WEEK_ATM_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_roll_both_legs_same_week_atm"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W2_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly_w2"
)
_base.BEST_COMBINED_PORTFOLIO_MLGBP72_ABSTAIN_TWO_SIDED_BUTTERFLY_W3_POLICY_LABEL = (
    f"{_THURSDAY_1_7_PORTFOLIO_POLICY_LABEL}__mlgbp72_abstain_two_sided_call_put_butterfly_w3"
)

globals().update({name: getattr(_base, name) for name in dir(_base) if not name.startswith("__")})


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
