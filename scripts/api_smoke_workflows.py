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


def _json(status: int, body: str) -> dict:
    _expect(status < 500, f"Expected JSON response, got status={status}; body={body[:400]}")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Expected JSON response body; status={status}; body={body[:400]}") from exc
    _expect(isinstance(payload, dict), f"Expected JSON object; status={status}; body={body[:400]}")
    return payload


def _poll_status(
    base_url: str,
    path: str,
    *,
    token: str,
    terminal_statuses: set[str],
    wait_attempts: int,
    wait_seconds: int,
) -> dict:
    last_payload: dict | None = None
    for attempt in range(1, wait_attempts + 1):
        status, body = _request(base_url + path, token=token)
        _expect(status == 200, f"Status poll failed for {path}; status={status}; body={body[:400]}")
        payload = _json(status, body)
        last_payload = payload
        current = payload.get("status")
        if current in terminal_statuses:
            print(f"PASS {path} terminal={current} on attempt {attempt}")
            return payload
        time.sleep(wait_seconds)
    raise SystemExit(f"Timed out waiting for terminal status on {path}; last payload={last_payload!r}")


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

    for path in (
        "/v1/me",
        "/v1/backtests?limit=1",
        "/v1/multi-symbol-backtests?limit=1",
        "/v1/multi-step-backtests?limit=1",
        "/v1/exports?limit=1",
        "/v1/me/feature-access",
        "/v1/templates",
    ):
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
    created = _json(status, body)
    run_id = created.get("id")
    _expect(isinstance(run_id, str) and run_id, f"Backtest create response missing id; body={body[:400]}")

    run_status = created.get("status")
    if run_status not in {"succeeded", "failed", "cancelled"}:
        created = _poll_status(
            base_url,
            f"/v1/backtests/{run_id}/status",
            token=token,
            terminal_statuses={"succeeded", "failed", "cancelled"},
            wait_attempts=wait_attempts,
            wait_seconds=wait_seconds,
        )
        run_status = created.get("status")

    _expect(run_status == "succeeded", f"Backtest smoke did not succeed; run_id={run_id}; payload={created!r}")

    status, body = _request(base_url + f"/v1/backtests/{run_id}", token=token)
    _expect(status == 200, f"Backtest detail smoke failed; status={status}; body={body[:400]}")
    detail = _json(status, body)
    summary = detail.get("summary")
    _expect(isinstance(summary, dict), f"Backtest detail missing summary; body={body[:400]}")
    print(f"PASS /v1/backtests/{run_id} [GET]")

    export_payload = {
        "run_id": run_id,
        "format": os.environ.get("SMOKE_EXPORT_FORMAT", "csv"),
        "idempotency_key": f"smoke-export-{int(time.time())}",
    }
    status, body = _request(base_url + "/v1/exports", token=token, method="POST", payload=export_payload)
    _expect(status in (200, 202), f"Export create smoke failed; status={status}; body={body[:400]}")
    export = _json(status, body)
    export_id = export.get("id")
    _expect(isinstance(export_id, str) and export_id, f"Export create response missing id; body={body[:400]}")
    export_status = export.get("status")
    if export_status not in {"succeeded", "failed", "cancelled", "expired"}:
        export = _poll_status(
            base_url,
            f"/v1/exports/{export_id}/status",
            token=token,
            terminal_statuses={"succeeded", "failed", "cancelled", "expired"},
            wait_attempts=wait_attempts,
            wait_seconds=wait_seconds,
        )
        export_status = export.get("status")

    _expect(export_status == "succeeded", f"Export smoke did not succeed; export_id={export_id}; payload={export!r}")
    status, body = _request(base_url + f"/v1/exports/{export_id}", token=token)
    _expect(status == 200, f"Export download smoke failed; status={status}; body={body[:400]}")
    _expect(len(body) > 0, f"Export download smoke returned empty body for export_id={export_id}")
    print(f"PASS /v1/exports/{export_id} [GET]")


if __name__ == "__main__":
    main()
