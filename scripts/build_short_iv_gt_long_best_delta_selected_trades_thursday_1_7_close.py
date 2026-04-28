from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.build_short_iv_gt_long_best_delta_selected_trades as _base

LOGS = ROOT / "logs"
_base.DEFAULT_BEST_DELTA_CSV = LOGS / "short_iv_gt_long_calendar_delta_grid_thursday_1_7_close_2y_best_delta_by_symbol.csv"
_base.DEFAULT_OUTPUT_TRADES_CSV = LOGS / "short_iv_gt_long_best_delta_thursday_1_7_close_selected_trades.csv"
_base.DEFAULT_OUTPUT_SUMMARY_CSV = LOGS / "short_iv_gt_long_best_delta_thursday_1_7_close_selected_summary.csv"

globals().update({name: getattr(_base, name) for name in dir(_base) if not name.startswith("__")})


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
