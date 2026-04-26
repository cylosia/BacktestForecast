from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_conditional_management_3weeks_vix_down_gt20 as _cond
import scripts.search_short_iv_gt_long_symbol_swaps as _base


LOGS = ROOT / "logs"

_base.cond = _cond
_base.DEFAULT_BASELINE_LEDGERS = (
    LOGS / "short_iv_gt_long_conditional_management_vix_down_gt20_selected_trades.csv",
)
_base.DEFAULT_CHALLENGER_LEDGERS = (
    LOGS / "short_iv_gt_long_conditional_management_part2_vix_down_gt20_selected_trades.csv",
)
_base.DEFAULT_OUTPUT_CSV = LOGS / "short_iv_gt_long_swap_search_vix_down_gt20_orig112_vs_part2.csv"


def _source_policy_rows(rows, *, allowed_symbols):
    return [
        dict(row)
        for row in rows
        if row["policy_label"] == _cond.BEST_COMBINED_PORTFOLIO_POLICY_LABEL
        and row["symbol"].strip().upper() in allowed_symbols
    ]


def _derive_promoted_rows(source_rows):
    return [dict(row) for row in source_rows]


_base._source_policy_rows = _source_policy_rows
_base._derive_promoted_rows = _derive_promoted_rows


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
