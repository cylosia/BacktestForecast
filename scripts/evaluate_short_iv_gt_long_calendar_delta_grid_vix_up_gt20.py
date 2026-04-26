from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_calendar_delta_grid as _base


_base.DEFAULT_OUTPUT_PREFIX = ROOT / "logs" / "short_iv_gt_long_calendar_delta_grid_vix_up_gt20_2y"


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
