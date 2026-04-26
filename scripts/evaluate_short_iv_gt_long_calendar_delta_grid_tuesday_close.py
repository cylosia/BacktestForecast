from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.evaluate_short_iv_gt_long_calendar_delta_grid as _base

_base.DEFAULT_ENTRY_WEEKDAY = 1
_base.DEFAULT_ENTRY_WEEKDAY_NAME = "Tuesday"
_base.DEFAULT_SHORT_EXPIRATION_DTE_TARGETS = (9, 10)
_base.DEFAULT_LONG_EXPIRATION_DTE_TARGETS = (16, 17)
_base.DEFAULT_OUTPUT_PREFIX = ROOT / "logs" / "short_iv_gt_long_calendar_delta_grid_tuesday_close_2y"

globals().update({name: getattr(_base, name) for name in dir(_base) if not name.startswith("__")})


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
