from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def _request(url: str, *, token: str | None = None, method: str = "GET", payload: dict | None = None) -> tuple[int, str]:
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310 -- workflow smoke target is operator-provided HTTP(S) URL
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 -- workflow smoke target is operator-provided HTTP(S) URL
            return resp.getcode(), resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body


def _expect(ok: bool, message: str) -> None:
    if not ok:
        raise SystemExit(message)


def main() -> None:
    base_url = os.environ["API_BASE_URL"].rstrip("/")
    token = os.environ.get("SMOKE_BEARER_TOKEN")
    wait_attempts = int(os.environ.get("SMOKE_WAIT_ATTEMPTS", "12"))
    wait_seconds = int(os.environ.get("SMOKE_WAIT_SECONDS", "10"))

    for path in ("/health/live", "/health/ready", "/v1/meta"):
        last_status = None
        for attempt in range(1, wait_attempts + 1):
            status, _ = _request(base_url + path)
            last_status = status
            if status == 200:
                print(f"PASS {path} on attempt {attempt}")
                break
            time.sleep(wait_seconds)
        else:
            raise SystemExit(f"Smoke check failed for {path}; last status={last_status}")

    if not token:
        print("No SMOKE_BEARER_TOKEN provided; authenticated workflow checks skipped.")
        return

    for path in ("/v1/me", "/v1/backtests?limit=1", "/v1/me/feature-access", "/v1/templates"):
        status, body = _request(base_url + path, token=token)
        _expect(status == 200, f"Authenticated smoke failed for {path}; status={status}; body={body[:400]}")
        print(f"PASS {path}")

    if os.environ.get("SMOKE_ENABLE_MUTATIONS", "false").lower() != "true":
        print("SMOKE_ENABLE_MUTATIONS is false; create/export smoke checks skipped.")
        return

    payload = {
        "symbol": os.environ.get("SMOKE_SYMBOL", "SPY"),
        "strategy_type": "long_call",
        "start_date": os.environ.get("SMOKE_START_DATE", "2024-01-02"),
        "end_date": os.environ.get("SMOKE_END_DATE", "2024-03-01"),
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}],
        "idempotency_key": f"smoke-{int(time.time())}",
    }
    status, body = _request(base_url + "/v1/backtests", token=token, method="POST", payload=payload)
    _expect(status in (200, 202), f"Backtest create smoke failed; status={status}; body={body[:400]}")
    print("PASS /v1/backtests [POST]")


if __name__ == "__main__":
    main()
