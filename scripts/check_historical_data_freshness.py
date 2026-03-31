from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a fast freshness summary for historical market-data tables.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()

    store = HistoricalMarketDataStore(
        session_factory=create_session,
        readonly_session_factory=create_readonly_session,
    )
    payload = store.get_freshness_summary()

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    for dataset, fields in payload.items():
        latest_date = fields.get("latest_date") or "none"
        latest_source_file_date = fields.get("latest_source_file_date") or "none"
        row_estimate = fields.get("row_estimate")
        row_estimate_text = row_estimate if row_estimate is not None else "unknown"
        print(
            f"{dataset}: latest_date={latest_date} "
            f"latest_source_file_date={latest_source_file_date} "
            f"row_estimate={row_estimate_text}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
