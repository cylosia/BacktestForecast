from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


def _prepare_local_canary_overrides() -> None:
    if os.environ.get("CANARY_LOCAL_REDIS_NOAUTH", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    os.environ.setdefault("REDIS_URL", redis_url)
    os.environ.setdefault("REDIS_CACHE_URL", redis_url)
    os.environ.setdefault("CELERY_RESULT_BACKEND_URL", redis_url)
    # Protect an explicit no-auth local Redis override before bootstrap reads .env.
    os.environ["REDIS_PASSWORD"] = ""


_prepare_local_canary_overrides()

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

import structlog

from backtestforecast.db.session import create_session
from backtestforecast.models import BacktestRun, OutboxMessage, User
from backtestforecast.services.dispatch_recovery import repair_stranded_jobs


logger = structlog.get_logger("scripts.prod_like_canary")


def _request(
    url: str,
    *,
    token: str | None = None,
    method: str = "GET",
    payload: dict | None = None,
) -> tuple[int, str]:
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310
            return resp.getcode(), resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


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


def _poll_until_not_queued(
    base_url: str,
    path: str,
    *,
    token: str,
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
        if current != "queued":
            print(f"PASS {path} left queued state with status={current} on attempt {attempt}")
            return payload
        time.sleep(wait_seconds)
    raise SystemExit(f"Timed out waiting for {path} to leave queued state; last payload={last_payload!r}")


def _build_request_payload(symbol: str, start_date_text: str, end_date_text: str, *, idempotency_key: str) -> dict:
    return {
        "symbol": symbol,
        "strategy_type": "long_call",
        "start_date": start_date_text,
        "end_date": end_date_text,
        "target_dte": 21,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "risk_free_rate": "0.02",
        "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}],
        "idempotency_key": idempotency_key,
    }


def _seed_queued_backtest_run(
    *,
    user_id: UUID,
    payload: dict,
    created_at: datetime,
) -> UUID:
    run_id = uuid4()
    with create_session() as session:
        user = session.get(User, user_id)
        _expect(user is not None, f"Authenticated user {user_id} not found in database.")
        run = BacktestRun(
            id=run_id,
            user_id=user_id,
            status="queued",
            symbol=str(payload["symbol"]),
            strategy_type=str(payload["strategy_type"]),
            date_from=date.fromisoformat(str(payload["start_date"])),
            date_to=date.fromisoformat(str(payload["end_date"])),
            target_dte=int(payload["target_dte"]),
            dte_tolerance_days=int(payload["dte_tolerance_days"]),
            max_holding_days=int(payload["max_holding_days"]),
            account_size=Decimal(str(payload["account_size"])),
            risk_per_trade_pct=Decimal(str(payload["risk_per_trade_pct"])),
            commission_per_contract=Decimal(str(payload["commission_per_contract"])),
            risk_free_rate=Decimal(str(payload["risk_free_rate"])),
            input_snapshot_json=dict(payload),
            warnings_json=[],
            created_at=created_at,
            updated_at=created_at,
        )
        session.add(run)
        session.commit()
    return run_id


def _assert_outbox_written(run_id: UUID) -> None:
    with create_session() as session:
        run = session.get(BacktestRun, run_id)
        _expect(run is not None, f"Recovered canary run {run_id} was not found.")
        _expect(bool(run.celery_task_id), f"Recovered canary run {run_id} has no celery_task_id after repair.")
        outbox = session.query(OutboxMessage).filter(
            OutboxMessage.correlation_id == run_id,
            OutboxMessage.task_name == "backtests.run",
        ).one_or_none()
        _expect(outbox is not None, f"Recovered canary run {run_id} has no outbox dispatch row.")
        _expect(outbox.status in {"pending", "sent"}, f"Unexpected outbox status for {run_id}: {outbox.status}")


def _delete_if_possible(base_url: str, run_id: str, *, token: str) -> None:
    status, body = _request(base_url + f"/v1/backtests/{run_id}", token=token)
    if status != 200:
        return
    detail = _json(status, body)
    if detail.get("status") in {"queued", "running"}:
        return
    delete_status, delete_body = _request(base_url + f"/v1/backtests/{run_id}", token=token, method="DELETE")
    _expect(
        delete_status in {204, 404},
        f"Cleanup delete failed for run_id={run_id}; status={delete_status}; body={delete_body[:400]}",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prod-like canary that exercises readiness, launch, cancel, and recovery against running services."
    )
    parser.add_argument("--api-base-url", default=os.environ.get("API_BASE_URL", "").rstrip("/"))
    parser.add_argument("--bearer-token", default=os.environ.get("CANARY_BEARER_TOKEN") or os.environ.get("SMOKE_BEARER_TOKEN"))
    parser.add_argument("--symbol", default=os.environ.get("CANARY_SYMBOL", "F"))
    parser.add_argument("--start-date", default=os.environ.get("CANARY_START_DATE", "2015-01-02"))
    parser.add_argument("--end-date", default=os.environ.get("CANARY_END_DATE", "2015-03-31"))
    parser.add_argument("--wait-attempts", type=int, default=int(os.environ.get("CANARY_WAIT_ATTEMPTS", "18")))
    parser.add_argument("--wait-seconds", type=int, default=int(os.environ.get("CANARY_WAIT_SECONDS", "10")))
    args = parser.parse_args()

    _expect(bool(args.api_base_url), "API_BASE_URL or --api-base-url is required.")
    _expect(bool(args.bearer_token), "CANARY_BEARER_TOKEN or --bearer-token is required.")

    base_url = args.api_base_url
    token = args.bearer_token

    for path in ("/health/live", "/health/ready", "/v1/meta"):
        status, body = _request(base_url + path)
        _expect(status == 200, f"Readiness failed for {path}; status={status}; body={body[:400]}")
        print(f"PASS {path}")

    me_status, me_body = _request(base_url + "/v1/me", token=token)
    _expect(me_status == 200, f"Authenticated canary failed for /v1/me; status={me_status}; body={me_body[:400]}")
    me_payload = _json(me_status, me_body)
    user_id = UUID(str(me_payload["id"]))
    print("PASS /v1/me")

    create_payload = _build_request_payload(
        args.symbol,
        args.start_date,
        args.end_date,
        idempotency_key=f"canary-launch-{int(time.time())}",
    )
    create_status, create_body = _request(base_url + "/v1/backtests", token=token, method="POST", payload=create_payload)
    _expect(
        create_status in {200, 202},
        f"Launch canary failed for /v1/backtests; status={create_status}; body={create_body[:400]}",
    )
    created = _json(create_status, create_body)
    launched_run_id = str(created.get("id"))
    _expect(launched_run_id, f"Launch canary response missing run id; body={create_body[:400]}")
    print("PASS /v1/backtests [POST]")

    launched_status = created.get("status")
    if launched_status not in {"succeeded", "failed", "cancelled"}:
        launched = _poll_status(
            base_url,
            f"/v1/backtests/{launched_run_id}/status",
            token=token,
            terminal_statuses={"succeeded", "failed", "cancelled"},
            wait_attempts=args.wait_attempts,
            wait_seconds=args.wait_seconds,
        )
        launched_status = launched.get("status")
    _expect(
        launched_status == "succeeded",
        f"Launch canary did not succeed; run_id={launched_run_id}; final_status={launched_status}",
    )

    cancel_payload = _build_request_payload(
        args.symbol,
        args.start_date,
        args.end_date,
        idempotency_key=f"canary-cancel-{uuid4().hex}",
    )
    cancel_seed_id = _seed_queued_backtest_run(
        user_id=user_id,
        payload=cancel_payload,
        created_at=datetime.now(UTC),
    )
    cancel_status, cancel_body = _request(
        base_url + f"/v1/backtests/{cancel_seed_id}/cancel",
        token=token,
        method="POST",
    )
    _expect(
        cancel_status == 200,
        f"Cancel canary failed; status={cancel_status}; body={cancel_body[:400]}",
    )
    cancelled = _json(cancel_status, cancel_body)
    _expect(
        cancelled.get("status") == "cancelled",
        f"Cancel canary did not cancel run_id={cancel_seed_id}; payload={cancelled!r}",
    )
    print(f"PASS /v1/backtests/{cancel_seed_id}/cancel [POST]")
    _delete_if_possible(base_url, str(cancel_seed_id), token=token)

    recovery_payload = _build_request_payload(
        args.symbol,
        args.start_date,
        args.end_date,
        idempotency_key=f"canary-recovery-{uuid4().hex}",
    )
    recovery_seed_id = _seed_queued_backtest_run(
        user_id=user_id,
        payload=recovery_payload,
        created_at=datetime(2000, 1, 1, tzinfo=UTC),
    )
    with create_session() as session:
        counts = repair_stranded_jobs(
            session,
            logger=logger,
            action="requeue",
            older_than=timedelta(days=3650),
        )
    _expect(
        counts.get("requeued", 0) >= 1,
        f"Recovery canary did not requeue the stranded run; counts={counts!r}",
    )
    _assert_outbox_written(recovery_seed_id)
    recovered = _poll_until_not_queued(
        base_url,
        f"/v1/backtests/{recovery_seed_id}/status",
        token=token,
        wait_attempts=args.wait_attempts,
        wait_seconds=args.wait_seconds,
    )
    _expect(
        recovered.get("status") in {"running", "succeeded", "failed", "cancelled"},
        f"Recovery canary returned an unexpected status payload: {recovered!r}",
    )
    if recovered.get("status") == "running":
        recovered = _poll_status(
            base_url,
            f"/v1/backtests/{recovery_seed_id}/status",
            token=token,
            terminal_statuses={"succeeded", "failed", "cancelled"},
            wait_attempts=args.wait_attempts,
            wait_seconds=args.wait_seconds,
        )
    print(f"PASS recovery for /v1/backtests/{recovery_seed_id}")
    _delete_if_possible(base_url, str(recovery_seed_id), token=token)

    ready_status, ready_body = _request(base_url + "/health/ready")
    _expect(ready_status == 200, f"Post-canary readiness failed; status={ready_status}; body={ready_body[:400]}")
    print("PASS /health/ready [post-canary]")

    _delete_if_possible(base_url, launched_run_id, token=token)
    print("PASS prod-like canary completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
