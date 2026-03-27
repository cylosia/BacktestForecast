from __future__ import annotations

import csv
import os
import sys
from pathlib import Path


def _to_float(value: str | None) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def main() -> int:
    csv_path = Path(os.environ.get("LOCUST_STATS_CSV", "artifacts/locust_stats.csv"))
    max_failures = float(os.environ.get("LOCUST_MAX_FAILURES", "0"))
    max_p95_ms = float(os.environ.get("LOCUST_MAX_P95_MS", "2000"))
    min_requests = float(os.environ.get("LOCUST_MIN_REQUESTS", "1"))

    if not csv_path.exists():
        print(f"Locust stats file not found: {csv_path}", file=sys.stderr)
        return 1

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    aggregate = next((row for row in rows if row.get("Name") == "Aggregated"), None)
    if aggregate is None:
        print("Locust aggregate row not found in stats CSV.", file=sys.stderr)
        return 1

    failures = _to_float(aggregate.get("Failure Count"))
    p95_ms = _to_float(aggregate.get("95%"))
    request_count = _to_float(aggregate.get("Request Count"))

    print(
        "Locust aggregate: "
        f"requests={request_count}, failures={failures}, p95_ms={p95_ms}"
    )

    if request_count < min_requests:
        print(
            f"Locust request threshold not met: {request_count} < {min_requests}",
            file=sys.stderr,
        )
        return 1

    if failures > max_failures:
        print(
            f"Locust failure threshold exceeded: {failures} > {max_failures}",
            file=sys.stderr,
        )
        return 1
    if p95_ms > max_p95_ms:
        print(
            f"Locust p95 threshold exceeded: {p95_ms}ms > {max_p95_ms}ms",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
