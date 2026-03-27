from __future__ import annotations

import argparse
import json

from _bootstrap import bootstrap_repo

bootstrap_repo()

from backtestforecast.integrations.massive_status import fetch_massive_status


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Massive status and optionally fail when options REST is degraded.")
    parser.add_argument("--base-url", default="https://massive-status.com")
    parser.add_argument("--timeout-seconds", type=int, default=10)
    parser.add_argument("--fail-on-options-rest-degraded", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = fetch_massive_status(
        base_url=args.base_url,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    if args.fail_on_options_rest_degraded and summary.options_rest_degraded:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
