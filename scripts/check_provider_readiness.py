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

from apps.api.app.routers.health import _check_massive_health
from backtestforecast.config import get_settings
from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Massive provider readiness using repo bootstrapping instead of ad hoc shell env state."
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()

    settings = get_settings()
    payload: dict[str, object] = {
        "massive_api_key_present": bool(settings.massive_api_key),
        "configured_base_url": settings.massive_base_url,
        "circuit_status": _check_massive_health(settings),
        "probe_status": "unconfigured",
        "probe_detail": None,
    }

    if settings.massive_api_key:
        client = MassiveClient()
        try:
            holidays = client.get_market_holidays()
            payload["probe_status"] = "ok"
            payload["probe_detail"] = {
                "upcoming_market_holidays": len(holidays),
                "sample_dates": [item.isoformat() for item in holidays[:5]],
            }
        except ExternalServiceError as exc:
            payload["probe_status"] = "failed"
            payload["probe_detail"] = str(exc)
        finally:
            client.close()

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    print(f"massive_api_key_present={payload['massive_api_key_present']}")
    print(f"configured_base_url={payload['configured_base_url']}")
    print(f"circuit_status={payload['circuit_status']}")
    print(f"probe_status={payload['probe_status']}")
    print(f"probe_detail={payload['probe_detail']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
